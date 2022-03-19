package main

import (
	"encoding/json"
	"fmt"
	client "gfs.com/master/client"
	helper "gfs.com/master/helper"
	structs "gfs.com/master/structs"
	"log"
	"net"
)

type MetaData struct {

	// key: file id int, value: chunk array
	// eg file 1 = [file1_chunk1, file1_chunk2, file1_chunk3]

	file_id_to_chunkId map[string][]string

	// map each file chunk to a chunk server (port number)
	chunkId_to_port map[string][]string
}

// server listening to client on their respective ports
func listenToClient(Client_id int, Port string, metaData MetaData) {

	address := "localhost:" + Port

	fmt.Printf("Master listening on Port %v\n", Port)

	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	go acceptConnection(Client_id, listener, metaData)
}

// connection to client established
func acceptConnection(Client_id int, listener net.Listener, metaData MetaData) {
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Master receives a new connection\n")
		go listenClient(conn, metaData)
	}
}

func listenClient(conn net.Conn, metaData MetaData) {
	fmt.Printf("Master connected to Client\n ")
	for {
		buffer := make([]byte, 1400)
		dataSize, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Connection has closed")
			return
		}

		//This is the message you received
		data := buffer[:dataSize]
		var message structs.Message
		json.Unmarshal([]byte(data), &message)

		last_chunk := ""

		if _, ok := metaData.file_id_to_chunkId[message.Filename]; ok {
			// if file does not exist in metaData, create a new entry
			if ok == false {
				metaData.file_id_to_chunkId[message.Filename] = []string{message.Filename + "_c0"}
				last_chunk = message.Filename + "_c0"
			} else {
				// if file exist, take the last chunk of the file from the metadata
				array := metaData.file_id_to_chunkId[message.Filename]
				last_chunk = metaData.file_id_to_chunkId[message.Filename][len(array)-1]
			}
		}

		dest_chunkserver := []int{8001, 8002, 8003}
		return_message := structs.CreateMessage(helper.DATA_APPEND, 8000, last_chunk, message.Filename, 8000, dest_chunkserver, "", 0)

		data, err = json.Marshal(return_message)

		if err != nil {
			log.Fatalln(err)
		}

		// Send the message back
		_, err = conn.Write(data)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Print("Message sent: ", string(data))
	}
}

func main() {

	var metaData MetaData
	metaData.file_id_to_chunkId = make(map[string][]string)
	metaData.chunkId_to_port = make(map[string][]string)

	// listening to client on port 8000
	listenToClient(1, "8000", metaData)
	client.StartClient()

}
