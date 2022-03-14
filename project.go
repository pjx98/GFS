package main

import (
	"fmt"
	"log"
	"net"
	"os"
)

func main() {

	chunkserver1, error1 := net.Listen("tcp", "localhost:8000")
	fmt.Println("Chunkserver1 is listening on Port 8000")
	if error1 != nil {
		log.Fatal(error1)
	}

	chunkserver2, error2 := net.Listen("tcp", "localhost:8001")
	fmt.Println("Chunkserver2 is listening on Port 8001")
	if error2 != nil {
		log.Fatal(error2)
	}

	chunkserver3, error3 := net.Listen("tcp", "localhost:8002")
	fmt.Println("Chunkserver3 is listening on Port 8002")
	if error3 != nil {
		log.Fatal(error3)
	}

	// Listen for connections
	go acceptconnection(chunkserver1)
	go acceptconnection(chunkserver2)
	go acceptconnection(chunkserver3)
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
		//byte array
		data := buffer[:dataSize]
		// the 0th index of the data array stores the name of the chunk server
		chunk_server_name := string(data[0])
		// the 1st index of this data array is the data which is supposed to be written to the chunk servers
		data_to_be_written := data[1]
		// the 2nd index of this data array which specifies which file to be written to since there are 3 chunk servers
		file_name_to_be_written := string(data[2])

		fmt.Print("Received message: ", string(data))
		switch {
		case string(data) == "Heartbeat":
			//invoke some function here
		default:
			//so this
			appendtofile(chunk_server_name, data_to_be_written, file_name_to_be_written)
			b := []byte("Data received and appended to file as requested")
			//write back to the connection that sent it initally
			_, err = conn.Write(b)
			if err != nil {
				log.Fatalln(err)
			}
			fmt.Print("Reply has been sent to the sender")

		}

	}
}
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
func appendtofile(name string, data byte, filenametobewritten string) {
	// so each chunk servver has one
	file, err := os.OpenFile("C:/Users/legac/OneDrive/Desktop/DSCproject/"+name+"/"+filenametobewritten+".txt", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer file.Close()
	if _, err := file.WriteString(string(data)); err != nil {
		log.Fatal(err)
	}

}
