package client

import (
	"os"
	"encoding/json"
	"fmt"
	"log"
	//"net"
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

// Connect client to master [Done]
func callMasterAppend(filename string) {
	var numChunks uint64
	var masterData []byte
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
		masterData = callAppendMaster(filename, fileByteSize)
		connectChunks(masterData)
	} else{
		filePrefix := removeExtension(filename)
		for i := uint64(0); i < numChunks; i++{
			smallFile := filePrefix + strconv.FormatUint(i, 10) + ".txt"
			smallFileSize := getFileSize(smallFile)
			// fmt.Println(smallFile) // debug
			// fmt.Println(smallFileSize) // debug
			masterData = callAppendMaster(smallFile,smallFileSize)
			connectChunks(masterData)
			//fmt.Printf("Response from Master: %s ", string(masterData)) // debug
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

// Send API request to master for append [Done]
func callAppendMaster(filename string, fileByteSize int64)[]byte{
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
	//fmt.Println(string(post)) // debug
	responseBody := bytes.NewBuffer(post)

	// Master port number = 8080
	resp, err := http.Post("http://localhost:8080/client/append", "application/json", responseBody)
	
	// Handle Error
	if err != nil {
		log.Fatalf("An Error Occured %v", err)
	}
	defer resp.Body.Close()
	
	// Read the response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}
	// Print body
	//sb := string(body)
	//log.Printf(sb)
	return body

}

// Connect client to chunk servers [TO-DO]
func connectChunks(masterData []byte) {
	var message structs.Message

	json.Unmarshal(masterData, &message)
	// Add relevant info into the message	
	message.SourcePort = message.ClientPort

	// Read the file
	rel, err := filepath.Rel("GFS/Master", "GFS/Client/" + message.Filename)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	text, err := ioutil.ReadFile(rel)
    if err != nil {
        fmt.Print(err)
		os.Exit(1)
    }
    //fmt.Println(string(text)) // debug

	message.Payload = string(text)

	fmt.Println(message) // debug, just check the final message

	// Send to the primary chunk
	chunkReply := sendPrimaryChunk(message)
	fmt.Println(chunkReply)
}

// Sends data to primary chunk [TO-DO]
func sendPrimaryChunk(message structs.Message)[]byte{
	
	// Which chunkserver to contact
	primary := message.PrimaryChunkServer
	post, _ := json.Marshal(message)
	//fmt.Println(string(post)) // debug
	responseBody := bytes.NewBuffer(post)

	url := "http://localhost:" + strconv.Itoa(primary) + "/message"
	resp, err := http.Post(url, "application/json", responseBody)
	
	// Handle Error
	if err != nil {
		log.Fatalf("An Error Occured %v", err)
	}
	defer resp.Body.Close()
	
	// Read the response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}
	return body
}

// Receive first ACK (data_received) from primary chunk server [TO-DO]
func checkFirstACK() {

}

// Send write data signal to primary chunk server [TO-DO]
func sendCommietData() {

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