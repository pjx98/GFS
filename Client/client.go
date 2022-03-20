package client

import (
	"os"
	//"encoding/json"
	"fmt"
	"log"
	"github.com/gin-gonic/gin"
	"strconv"
	"path/filepath"
	"math"
	"strings"
	//"reflect"
	//"bytes"
	"io/ioutil"
	"net/http"
	"gfs.com/master/helper"
	structs "gfs.com/master/structs"
	
)

// Go routine to check for ACK
func listen(pid int, portNo int){
	router := gin.Default()
	router.POST("/message", messageHandler)

	fmt.Printf("Client %d listening on port %d \n", pid, portNo)
	router.Run("localhost:" + strconv.Itoa(portNo))
}

func messageHandler(context *gin.Context){
	var message structs.Message

	// Call BindJSON to bind the received JSON to message.
	if err := context.BindJSON(&message); err != nil {
		fmt.Println("Invalid message object received.")
		return
	}
	context.IndentedJSON(http.StatusOK, message.MessageType+" ACK message from Node "+strconv.Itoa(message.SourcePort)+" was received by Client "+strconv.Itoa(message.ClientPort))
	
	fmt.Println("hello pls print")
	fmt.Println(message)


	switch message.MessageType {
	case helper.DATA_APPEND:
		go connectChunks(message)
	case helper.ACK_APPEND:
		go sendCommitData(message)
	case helper.ACK_COMMIT:
		fmt.Println("Append successfully finished")
	}
}


// Connect client to master [Done]
func callMasterAppend(pid int, portNo int, filename string) {
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

	// If no split, just append as normal, otherwise append for each file split
	if numChunks == 1{
		// send to master
		fmt.Println("Sending append request to Master")
		helper.SendMessage(8080, helper.DATA_APPEND, portNo, 0, nil, filename, "", "", fileByteSize, 0, portNo, nil)
	} else{
		filePrefix := removeExtension(filename)
		for i := uint64(0); i < numChunks; i++{
			smallFile := filePrefix + strconv.FormatUint(i, 10) + ".txt"
			smallFileSize := getFileSize(smallFile)
			// fmt.Println(smallFile) // debug
			// fmt.Println(smallFileSize) // debug

			// send to master
			fmt.Println("Sending append request to Master")
			helper.SendMessage(8080, helper.DATA_APPEND, portNo, 0, nil, smallFile, "", "", smallFileSize, 0, portNo, nil)
		}
	}
}

// Connect client to chunk servers [Done]
func connectChunks(message structs.Message){
	fmt.Println("Connecting to primary chunk")

	// Add relevant info into the message
	message.SourcePort = message.ClientPort
	primary := message.PrimaryChunkServer
	secondary := message.SecondaryChunkServers
	var targetPorts []int
	targetPorts = append(targetPorts, []int{primary}...)
	targetPorts = append(targetPorts, secondary...)

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
	message.Payload = string(text)
	fmt.Println(message) // debug, just check the final message
	helper.SendMessageV2(primary, message, message.ClientPort, targetPorts )
}

// Send write data signal to primary chunk server [Done]
func sendCommitData(message structs.Message) {

	message.MessageType = helper.DATA_COMMIT
	// fmt.Println(message) // debug
	helper.SendMessageV2(message.PrimaryChunkServer, message, message.ClientPort, message.TargetPorts)

}

// ===== HELPER FUNCTIONS =====
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

// Master calls this function once it has finished set-up
func StartClient(pid int, portNo int) {
	go listen(pid, portNo)
	callMasterAppend(pid, portNo, "test.txt")
}

// func main(){
// 	getFileSize("../test.txt")
// }