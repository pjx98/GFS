package client

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"encoding/json"
	structs "gfs.com/master/structs"
)

// Connect client to master [Done]
func connectMaster(master_port string){

	address := "localhost:" + master_port
	fmt.Println("Master port is " + address)

	// Dial master port
	conn, err := net.Dial("tcp", address)
	if err != nil {
		log.Fatalln(err)
	}

	// Create append message json
	msgJson := &structs.Message{
		Message_type: "Append",
		Filename: "f1", 
	}
	data, _ := json.Marshal(msgJson)
	fmt.Println(string(data)) // debug

	//Write append request
	callAppend(conn, data)

}

// Send APPEND request to master [Done]
func callAppend(conn net.Conn, req []byte) {

	// write to master port
	_, err := conn.Write(req)
	if err != nil {
			log.Fatalln(err)
	}

	for {
		buffer := make([]byte, 1400)
		dataSize, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("The connection has closed!")
			return
		}
		
		// Read the reply from master
		data := buffer[:dataSize]
		fmt.Println("Received message: ", string(data))
		break
	}

}

// Connect client to chunk servers
func connectChunks() {

	// for ports from response from master, conenct via tcp
	for i:= 8000; i < 8003; i++{
		address := "localhost:" + strconv.Itoa(i)
		_, err := net.Dial("tcp", address)
		if err != nil {
			log.Fatalln(err)
		}
	}
}



// Chunkserver ports, data to send, chunk_id
func sendChunks(){

}

// Receive first ACK (data_received) from primary chunk server
func checkFirstACK(){

}

// Send write data signal to primary chunk server
func sendWriteData(){

}

// Receive second ACK from primary chunk server after successful write
func checkSuccessWrite(){

}


func StartClient(){
	connectMaster("8000")
}