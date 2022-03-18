package main

import (

	//"fmt"
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"strconv"
)

func initiateClient() {

	// simulate client to server connection
	var file_number int = 1
	for i := 8000; i < 8003; i++ {
		address := "localhost:" + strconv.Itoa(i)
		conn, err := net.Dial("tcp", address)
		if err != nil {
			log.Fatalln(err)
		}
		//	print("hello world")
		//
		message := [4]string{}
		//the 0 index is the chunk server to append
		message[0] = "chunk_server_" + strconv.Itoa(i)
		// the 1st index is to just write a message to the file
		message[1] = "hello world"
		//2nd index is the file to be appended
		message[2] = "file" + strconv.Itoa(file_number)
		file_number += 1
		//3rd index message type
		message[3] = "Append"
		buf := &bytes.Buffer{}
		gob.NewEncoder(buf).Encode(message)
		bs := buf.Bytes()
		//send the value across the port in byte array
		_, err = conn.Write(bs)
		fmt.Println("message sent is " + string(bs))

		buffer := make([]byte, 1400)
		dataSize, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("The connection has closed!")
			return
		}

		data := buffer[:dataSize]
		fmt.Println("Received message: ", string(data))
		break

	}
}
