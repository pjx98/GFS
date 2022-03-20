package client

import (
	"os"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strconv"
	"path/filepath"
	"math"
	"strings"
	//"reflect"
	"bytes"
	"io/ioutil"
	"net/http"
	"gfs.com/master/helper"
	structs "gfs.com/master/structs"
	
)

// Connect client to master 
func callMasterAppend(filename string) {
	var numChunks uint64
	// debug to check whats the main directory, just leave it here
	path, err := os.Getwd()
	if err != nil {
		log.Println(err)
	}
	fmt.Println(path) 

	// Call helper function to read file size
	fileByteSize := getFileSize(filename)

	// Check byte size of file, if more than 2.5kb split
	if(fileByteSize > 2500){
		numChunks = splitFile(filename)
	} else{
		numChunks = 1
	}

	// If no split, just append as normal
	// Otherwise append for each file split
	if numChunks == 1{
		callAppend(filename, fileByteSize)
	} else{
		filePrefix := removeExtension(filename)
		for i := uint64(0); i < numChunks; i++{
			smallFile := filePrefix + strconv.FormatUint(i, 10) + ".txt"
			smallFileSize := getFileSize(smallFile)
			fmt.Println(smallFile) // debug
			fmt.Println(smallFileSize) // debug

			//callAppend(smallFile,smallFileSize)
		}
	}
}

// Get size of file trying to write to [Done]
func getFileSize(filename string) (int64) {
	
	// Get relative path of the text file
	// First arg is the main directory, second arg is where file is stored
	rel, err := filepath.Rel("GFS/Master", "GFS/Client/" + filename)
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

// Used to split files that are larger than 2.5kb [Done]
func splitFile(oldFilename string)(uint64){
		
		// Relative file path
		rel, err := filepath.Rel("GFS/Master", "GFS/Client/" + oldFilename)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Open the file
        file, err := os.Open(rel)
        if err != nil {
                fmt.Println(err)
                os.Exit(1)
        }
		defer file.Close()

		// Get file data
		fileInfo, _ := file.Stat()
		var fileSize int64 = fileInfo.Size()
		const fileChunk = 2500 // 2.5 KB

		// calculate total number of parts the file will be chunked into
		totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))
		fmt.Printf("Splitting to %d pieces.\n", totalPartsNum)
		for i := uint64(0); i < totalPartsNum; i++ {

			partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
			partBuffer := make([]byte, partSize)
			file.Read(partBuffer)

			// write to disk
			rel, err := filepath.Rel("GFS/Master", "GFS/Client/" + oldFilename)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			rel = removeExtension(rel)
			fileName := rel + strconv.FormatUint(i, 10) + ".txt"
			_, err = os.Create(fileName)
			if err != nil {
					fmt.Println(err)
					os.Exit(1)
			}
			// write/save buffer to disk
			ioutil.WriteFile(fileName, partBuffer, os.ModeAppend)
			fmt.Println("Split to : ", fileName)
		}
		return totalPartsNum
}

// Helper to remove extensions [Done]
func removeExtension(fpath string) string {
	ext := filepath.Ext(fpath)
	return strings.TrimSuffix(fpath, ext)
}


func callAppend(filename string, fileByteSize int64){
	// Create append message json
	msgJson := &structs.Message{
		MessageType: helper.DATA_APPEND,
		
		ClientPort: 8086,
		PrimaryChunkServer: 0,
		SecondaryChunkServers: nil,
		
		Filename: filename,
		ChunkId: "",
		Payload: "",
		PayloadSize: fileByteSize,
		ChunkOffset: 0,

		SourcePort: 8086,
		TargetPorts: nil,
	}

	post, _ := json.Marshal(msgJson)
	fmt.Println(string(post)) // debug
	responseBody := bytes.NewBuffer(post)

	// Master port number = 8080
	resp, err := http.Post("https://localhost:8080/client/append", "application/json", responseBody)
	
	//Handle Error
	if err != nil {
		log.Fatalf("An Error Occured %v", err)
	}
	defer resp.Body.Close()
	
	//Read the response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}
	sb := string(body)
	log.Printf(sb)

}

// Connect client to chunk servers [TO-DO]
func connectChunks(message structs.Message) {

	for _, s := range message.TargetPorts {
		address := "localhost:" + strconv.Itoa(s)
		_, err := net.Dial("tcp", address)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("Connected to chunk server: %d\n", s)
	}

	// should only be able to get here if all connected
	sendPrimaryChunk(message.TargetPorts, "placeholder", message.ChunkId)
}


// parameters: Chunkserver ports, data to send, chunk_id (Append_last_chunk)
// Sends data to primary chunk [TO-DO]
func sendPrimaryChunk(ports []int, data string, chunkID string){

	fmt.Println("here") // debug
	
}

// Receive first ACK (data_received) from primary chunk server [TO-DO]
func checkFirstACK() {

}

// Send write data signal to primary chunk server [TO-DO]
func sendWriteData() {

}
 
// Receive second ACK from primary chunk server after successful write [TO-DO]
func checkSuccessWrite() {

}

func StartClient() {
	callMasterAppend("test.txt")
}

// func main(){
// 	getFileSize("../test.txt")
// }