package master

import (
	"encoding/json"
	"fmt"
	"log"
	"net"

	"gfs.com/master/helper"
	structs "gfs.com/master/structs"
)

// server listening to client on their respective ports
func listenToClient(Client_id int, Port string) {

	address := "localhost:" + Port

	fmt.Printf("Master listening on Port %v\n", Port)

	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	go acceptConnection(Client_id, listener)
}

// connection to client established
func acceptConnection(Client_id int, listener net.Listener) {
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Master receives a new connection\n")
		go listenClient(conn)
	}
}

func listenClient(conn net.Conn) {
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

		last_chunk := message.Filename
		dest_chunkserver := []int{8001, 8002, 8003}
		return_message := structs.CreateMessage(helper.DATA_APPEND, last_chunk+"_c0", message.Filename, 8000, dest_chunkserver)

		data, err = json.Marshal(return_message)

		// Send the message back
		_, err = conn.Write(data)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Print("Message sent: ", string(data))
	}
}

func main() {

	// listening to client on port 8000
	listenToClient(1, "8000")

}
