package client

import (
	"os"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strconv"
	"path/filepath"
	//"reflect"
	"gfs.com/master/helper"
	structs "gfs.com/master/structs"
)

// Get size of file trying to write to [Done]
func getFileSize(filename string) (int64) {
	

	// Get relative path of the text file
	// First arg is the main directory, second arg is where file is stored
	rel, err := filepath.Rel("GFS/Master", "GFS/Client/test.txt")
    if err != nil {
        panic(err)
    }
    fmt.Println(rel) // debug

	file, _ := os.Open(rel)
	fi, err := file.Stat()
	if err != nil {
	// Could not obtain stat, handle error
	}
	fmt.Printf("%s is %d bytes long\n", filename, fi.Size()) // debug
	return fi.Size()

}

// Connect client to master [Done]
func connectMaster(master_port string, filename string) {

	address := "localhost:" + master_port
	fmt.Println("Master port is " + address)

	// Dial master port
	conn, err := net.Dial("tcp", address)
	if err != nil {
		log.Fatalln(err)
	}

	path, err := os.Getwd()
	if err != nil {
		log.Println(err)
	}
	fmt.Println(path) 

	fileByteSize := getFileSize(filename)

	// Create append message json
	msgJson := &structs.Message{
		MessageType: helper.DATA_APPEND,
		Filename:    filename,
		PayloadSize: fileByteSize,
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

		var reply structs.Message
		json.Unmarshal(data, &reply)
		//fmt.Println(reflect.TypeOf(reply)) // debug
		connectChunks(reply)
		break
	}

}

// Connect client to chunk servers [Done]
func connectChunks(message structs.Message) {

	for _, s := range message.TargetPorts {
		address := "localhost:" + strconv.Itoa(s)
		_, err := net.Dial("tcp", address)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("Connected to chunk server: %d\n", s)
	}
}

// Chunkserver ports, data to send, chunk_id
func sendChunks() {

}

// Receive first ACK (data_received) from primary chunk server
func checkFirstACK() {

}

// Send write data signal to primary chunk server
func sendWriteData() {

}

// Receive second ACK from primary chunk server after successful write
func checkSuccessWrite() {

}

func StartClient() {
	connectMaster("8000", "test.txt")
}

// func main(){
// 	getFileSize("../test.txt")
// }