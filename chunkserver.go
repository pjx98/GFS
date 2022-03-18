package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

func main() {

	var start_port int = 8000
	//loop from 8000 to 8002
	var end_port int = 8003

	for i := start_port; i < end_port; i++ {

		chunkserver, error1 := net.Listen("tcp", "localhost:"+strconv.Itoa(i))
		fmt.Println("Chunkserver" + strconv.Itoa(i) + " is listening on Port:" + strconv.Itoa(i))
		if error1 != nil {
			log.Fatal(error1)
		}

		go acceptconnection(chunkserver)

	}
	initiateClient()
	// Listen for connections
}

//Listen for messages and reply
func listenConnection(conn net.Conn) {
	for {

		buffer := make([]byte, 1400)
		dataSize, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Connection has closed")
			return
		}
		//array index should contain

		//This is the message you received
		//problem with decoding the message
		data := buffer[:dataSize]
		s := string(data)
		result := strings.Split(s, ".")
		fmt.Println(result)

		//gob.NewDecoder(data).Decode(&result)
		// the 0th index of the data array stores the name of the chunk server
		chunk_server_name := string(result[0])
		fmt.Println("chunkservername is:" + string(chunk_server_name))
		// the 1st index of this data array is the data which is supposed to be written to the chunk servers
		data_to_be_written := result[1]
		fmt.Println("data to be written is: " + string(chunk_server_name))
		// the 2nd index of this data array which specifies which file to be written to since there are 3 chunk servers
		file_name_to_be_written := result[2]
		fmt.Println("file to be written to is:" + file_name_to_be_written)
		//the 3rd index of this data array specifies the message type ,like it can append, read , heartbeat etc
		message_type := result[3]
		fmt.Println("MessageType:" + message_type)
		switch {
		//this hasn't been implemented yet
		case message_type == "Heartbeat":
			//invoke some function here
			continue
		case message_type == "Append":
			//so this
			appendtofile(chunk_server_name, data_to_be_written, file_name_to_be_written)
			b := []byte("Data received and appended to file as requested\n")
			//write back to the connection that sent it initally
			_, err = conn.Write(b)
			if err != nil {
				log.Fatalln(err)
			}
			fmt.Print("Reply has been sent to the sender\n")

		}

	}
}

//Create a connection between the sender and the chunk server.
func acceptconnection(listen net.Listener) {
	defer listen.Close()
	for {
		connection, error := listen.Accept()
		if error != nil {
			log.Fatal(error)
		}
		fmt.Println("New connection found!")
		go listenConnection(connection)
	}
}
func appendtofile(name string, data string, filenametobewritten string) {
	// so each chunk server will append to the directory that is listed in the array, the chunk servers each will contain a folder which have chunk servers
	file, err := os.OpenFile("./"+name+"/"+filenametobewritten+".txt", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer file.Close()
	if _, err := file.WriteString(data); err != nil {
		log.Fatal(err)
	}

}
func convert(b []byte) string {
	s := make([]string, len(b))
	for i := range b {
		s[i] = strconv.Itoa(int(b[i]))
	}
	return strings.Join(s, ",")
}
