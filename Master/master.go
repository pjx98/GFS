package main

import (
	"fmt"
	"log"
	"net"
)

type message struct{

}

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
func acceptConnection(Client_id int, listener net.Listener){
	defer listener.Close()

	for {
		_, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Server connected to Client %v\n", Client_id)

	}
}


func main() {

	// create servers with different ports
	listenToClient(1, "8000")
	listenToClient(2, "8001")
	listenToClient(3, "8002")
	listenToClient(4, "8003")
	listenToClient(5, "8004")

	

}
