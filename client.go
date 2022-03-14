package main

import (

	//"fmt"
	"log"
	"net"
	"strconv"
)

func initiateClient() {

	// simulate client to server connection
	for i:= 8000; i < 8003; i++{
		address := "localhost:" + strconv.Itoa(i)
		_, err := net.Dial("tcp", address)
		if err != nil {
			log.Fatalln(err)
		}
	}
	for {
		
	}
}