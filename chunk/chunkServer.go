package chunk

// TODO: Look through and decide which part of the code will be run as a seperate go routine.

import (
	"fmt"
	helper "gfs.com/master/helper"
	structs "gfs.com/master/structs"
	"github.com/gin-gonic/gin"
	"net/http"
	"os"
	"strconv"
)

var (
	chunkIdAppendDataMap *map[string]map[int]structs.Queue // A map where the chunkId is the key and the value is another map whose keys are the portNo that made the append request and the values are queues whose elemnts are the data that is to be appended.
	ACKMap               *map[string]map[int]int           // TODO: Should we serialize this based on requests (request type / request ID) as well ? (If we impose a condition that the client can make only one append request at once, we will not need serialization by requests)
	chunkLocks           *map[string]bool                  // Default value is false.
)

/*
******************************************************
***** Handler functions for processing API Calls *****
******************************************************
 */
func landingPageHandler(context *gin.Context) {
	context.IndentedJSON(http.StatusOK, "Welcome to the Okay File System ! This is a chunk server.")
}

func postMessageHandler(context *gin.Context) {
	var message structs.Message

	// Call BindJSON to bind the received JSON to message.
	if err := context.BindJSON(&message); err != nil {
		fmt.Println("Invalid message object received.")
		return
	}
	context.IndentedJSON(http.StatusOK, message.MessageType+" message from Node "+strconv.Itoa(message.SourcePort)+" was received by Node "+strconv.Itoa(message.TargetPorts[0]))

	switch message.MessageType {
	case helper.DATA_APPEND:
		appendMessageHandler(message)
	case helper.ACK_APPEND:
		appendACKHandler(message)
	case helper.DATA_COMMIT:
		commitDataHandler(message)
	case helper.ACK_COMMIT:
		commitACKHandler(message)
	}
}

func listen(nodePid int, portNo int) {
	router := gin.Default()
	router.GET("/", landingPageHandler)
	router.POST("/message", postMessageHandler)

	fmt.Printf("Node %d listening on port %d \n", nodePid, portNo)
	router.Run("localhost:" + strconv.Itoa(portNo))
}

/*
****************************
***** Helper Fucntions *****
****************************
 */

func appendMessageHandler(message structs.Message) {
	storeTempData(message.ChunkId, message.ClientPort, message.Payload)
	if len(message.TargetPorts) > 1 { // Only for the Primary Chunk Server.
		for index, targetPort := range message.TargetPorts[1:] {
			helper.SendMessageV2(targetPort, message, message.TargetPorts[0], []int{message.TargetPorts[index]}) // The TargetPorts attribute of the Message object is set to just one element.
			// This is so that this for loop is only trigerred in the Primary Chunk Server and not the Secondary Chunk Servers.
		}
		waitForACKs(len(message.TargetPorts), message.ChunkId, message.ClientPort)
		helper.SendMessage(message.ClientPort, helper.ACK_APPEND, message.ClientPort, message.ChunkId, message.Filename, message.TargetPorts[0], []int{message.ClientPort}, "", 0) // ACK to Client.
	}
}

func appendACKHandler(message structs.Message) {
	(*ACKMap)[message.ChunkId][message.ClientPort] -= 1
}

func commitACKHandler(message structs.Message) { // TODO: Currently, this function is same as appendACKHandler - will have to change if we decide to index the messages further.
	(*ACKMap)[message.ChunkId][message.ClientPort] -= 1
}

func commitDataHandler(message structs.Message) {
	lockChunk(message.ChunkId)
	writeMutations(message.ChunkId, message.ClientPort, message.ChunkOffset)
	if len(message.TargetPorts) > 1 { // Only for the Primary Chunk Server.
		for index, targetPort := range message.TargetPorts[1:] {
			helper.SendMessageV2(targetPort, message, message.TargetPorts[0], []int{message.TargetPorts[index]}) // The TargetPorts attribute of the Message object is set to just one element.
			// This is so that this for loop is only trigerred in the Primary Chunk Server and not the Secondary Chunk Servers.
		}
		waitForACKs(len(message.TargetPorts), message.ChunkId, message.ClientPort)
		helper.SendMessage(message.ClientPort, helper.ACK_COMMIT, message.ClientPort, message.ChunkId, message.Filename, message.TargetPorts[0], []int{message.ClientPort}, "", 0) // ACK to Client.
	}
	releaseChunk(message.ChunkId)
}

func sendACK() {} //TODO: Need to see if we need this fucntion since sending ACKs will be specific to the type of fucntion being performed.

func writeMutations(chunkId string, clientPort int, chunkOffset int64) {
	fh, err := os.OpenFile(chunkId+".txt", os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer fh.Close()
	writeData, _ := (*chunkIdAppendDataMap)[chunkId][clientPort].Peek()
	writeDataBytes := []byte(writeData)
	if _, err := fh.WriteAt(writeDataBytes, chunkOffset); err != nil {
		panic(err)
	}
}

func replicate() {} //TODO: No longer required ?

func sendData() {} //TODO: No longer required ?

func waitForACKs(noOfACKs int, chunkId string, clientPort int) {
	(*ACKMap)[chunkId][clientPort] += 2
	for {
		if (*ACKMap)[chunkId][clientPort] == 0 {
			break
		}
	}
}

func storeTempData(chunkId string, clientPort int, payload string) {
	(*chunkIdAppendDataMap)[chunkId][clientPort].Enqueue(payload)
}

func lockChunk(chunkId string) {
	for {
		if !(*chunkLocks)[chunkId] {
			(*chunkLocks)[chunkId] = true
		}
	}
}

func releaseChunk(chunkId string) {
	(*chunkLocks)[chunkId] = false
}

func ChunkServer(nodePid int, portNo int) {
	go listen(nodePid, portNo)
}
