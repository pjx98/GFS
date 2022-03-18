package main

import (

	//"fmt"

	"fmt"
	"log"
	"net"
	"strconv"
)

func initiateClient() {

	// simulate client to server connection
	var number_of_chunk int = 3
	for i := 8000; i < 8003; i++ {
		address := "localhost:" + strconv.Itoa(i)
		conn, err := net.Dial("tcp", address)
		if err != nil {
			log.Fatalln(err)
		}
		file_number := 0
		for j := file_number; j < number_of_chunk; j++ {

			//	print("hello world")
			//
			message := ""
			//the 0 index is the chunk server to append
			message += "chunk_server_" + strconv.Itoa(i) + "."
			// the 1st index is to just write a message to the file
			message += "hello world" + "."
			//2nd index is the file to be appended
			message += "file" + strconv.Itoa(j) + "."

			//3rd index message type
			message += "Append"

			//send the value across the port in byte array
			_, err = conn.Write([]byte(message))
			fmt.Println("message sent is " + message)

			buffer := make([]byte, 1400)
			dataSize, err := conn.Read(buffer)
			if err != nil {
				fmt.Println("The connection has closed!")
				return
			}

			data := buffer[:dataSize]
			fmt.Println("Received message: ", string(data))
			continue

		}

	}
}
