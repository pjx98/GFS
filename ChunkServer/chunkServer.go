package chunk

// TODO: Look through and decide which part of the code will be run as a seperate go routine.
// TODO: When to pad and how to know to Pad.
// TODO: Fault tolerance, handle cases when secondary fails. How to retry?
// TODO: Return correct offset to client

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"

	helper "gfs.com/master/helper"
	structs "gfs.com/master/structs"
	"github.com/gin-gonic/gin"
)

var (
	chunkIdAppendDataMap *map[string]map[int]structs.Queue // A map where the chunkId is the key and the value is another map whose keys are the portNo that made the append request and the values are queues whose elemnts are the data that is to be appended.
	ACKMap               map[string]map[int]map[string]*int32
	chunkLocks           *map[string]bool // Default value is false.
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
		go appendMessageHandler(message)
	case helper.ACK_APPEND:
		go appendACKHandler(message)
	case helper.DATA_COMMIT:
		go commitDataHandler(message)
	case helper.ACK_COMMIT:
		go commitACKHandler(message)
	case helper.CREATE_NEW_CHUNK:
		go createNewChunkHandler(message)
	case helper.DATA_PAD:
		go padHandler(message)
	case helper.ACK_PAD:
		go padACKHandler(message)
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
		atomic.AddInt32(ACKMap[message.ChunkId][message.ClientPort][message.MessageType], int32(len(message.TargetPorts)-1))
		for index, targetPort := range message.TargetPorts[1:] {
			helper.SendMessageV2(targetPort, message, message.TargetPorts[0], []int{message.TargetPorts[index]}) // The TargetPorts attribute of the Message object is set to just one element.
			// This is so that this for loop is only trigerred in the Primary Chunk Server and not the Secondary Chunk Servers.
		}
		waitForACKs(message.ChunkId, message.ClientPort, message.MessageType)
		helper.SendMessage(message.ClientPort, helper.ACK_APPEND, message.ClientPort, message.PrimaryChunkServer, message.SecondaryChunkServers, message.Filename, message.ChunkId, "", 0, 0, message.PrimaryChunkServer, []int{message.ClientPort}) // ACK to Client.
	} else { // Only for the Secondary Chunk Servers.
		helper.SendMessage(message.PrimaryChunkServer, helper.ACK_APPEND, message.ClientPort, message.PrimaryChunkServer, message.SecondaryChunkServers, message.Filename, message.ChunkId, "", 0, 0, message.TargetPorts[0], []int{message.PrimaryChunkServer})
	}
}

func appendACKHandler(message structs.Message) {
	atomic.AddInt32(ACKMap[message.ChunkId][message.ClientPort][message.MessageType], -1)
}

func commitACKHandler(message structs.Message) { // TODO: Currently, this function is same as appendACKHandler - will have to change if we decide to index the messages further.
	atomic.AddInt32(ACKMap[message.ChunkId][message.ClientPort][message.MessageType], -1)
}

func commitDataHandler(message structs.Message) {
	lockChunk(message.ChunkId)
	writeMutations(message.ChunkId, message.ClientPort, message.ChunkOffset)
	if len(message.TargetPorts) > 1 { // Only for the Primary Chunk Server.
		atomic.AddInt32(ACKMap[message.ChunkId][message.ClientPort][message.MessageType], int32(len(message.TargetPorts)-1))
		for index, targetPort := range message.TargetPorts[1:] {
			helper.SendMessageV2(targetPort, message, message.TargetPorts[0], []int{message.TargetPorts[index]}) // The TargetPorts attribute of the Message object is set to just one element.
			// This is so that this for loop is only trigerred in the Primary Chunk Server and not the Secondary Chunk Servers.
		}
		waitForACKs(message.ChunkId, message.ClientPort, message.MessageType)
		helper.SendMessage(message.ClientPort, helper.ACK_COMMIT, message.ClientPort, message.PrimaryChunkServer, message.SecondaryChunkServers, message.Filename, message.ChunkId, "", 0, 0, message.PrimaryChunkServer, []int{message.ClientPort}) // ACK to Client.
	} else { // Only for the Secondary Chunk Servers.
		helper.SendMessage(message.PrimaryChunkServer, helper.ACK_COMMIT, message.ClientPort, message.PrimaryChunkServer, message.SecondaryChunkServers, message.Filename, message.ChunkId, "", 0, 0, message.TargetPorts[0], []int{message.PrimaryChunkServer})
	}
	releaseChunk(message.ChunkId)
}

func createNewChunkHandler(message structs.Message) {
	createChunk(message.TargetPorts[0], message.ChunkId)
	helper.SendMessage(message.SourcePort, helper.ACK_CHUNK_CREATE, message.ClientPort, message.PrimaryChunkServer, message.SecondaryChunkServers, message.Filename, message.ChunkId, "", 0, 0, message.TargetPorts[0], []int{message.SourcePort}) // ACK to Client.
}

func writeMutations(chunkId string, clientPort int, chunkOffset int64) {
	fh, err := os.OpenFile(chunkId+".txt", os.O_RDWR, 0644)
	if err != nil {
		fmt.Println("ERROR 2")
		fmt.Println(err)
	}
	defer fh.Close()
	
	// write data
	writeData, _ := (*chunkIdAppendDataMap)[chunkId][clientPort].Peek()
	writeDataBytes := []byte(writeData)
	if _, err := fh.WriteAt(writeDataBytes, chunkOffset); err != nil {
		fmt.Println("ERROR 3")
		fmt.Println(err)
	}
}

func waitForACKs(chunkId string, clientPort int, messageType string) {
	for {
		if (*ACKMap[chunkId][clientPort][messageType]) == 0 {
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
			break
		}
	}
}

func releaseChunk(chunkId string) {
	(*chunkLocks)[chunkId] = false
}

func createChunk(portNo int, chunkId string) {
	pwd, _ := os.Getwd()
	dataDirPath := filepath.Join(pwd, "../"+helper.DATA_DIR)
	helper.CreateFolder(dataDirPath)
	portDataDirPath := filepath.Join(dataDirPath, strconv.Itoa(portNo))
	helper.CreateFolder(portDataDirPath)
	chunkPath := filepath.Join(portDataDirPath, chunkId+".txt")
	helper.CreateFile(chunkPath)
}

func ChunkServer(nodePid int, portNo int) {
	//chunkIdAppendDataMap = make(*map[string]map[int]structs.Queue) // A map where the chunkId is the key and the value is another map whose keys are the portNo that made the append request and the values are queues whose elemnts are the data that is to be appended.
	ACKMap = make(map[string]map[int]map[string]*int32)
	//chunkLocks  = make(*map[string]bool) // Default value is false.
	go listen(nodePid, portNo)
}

func padHandler(message structs.Message) {
	lockChunk(message.ChunkId)
	padFile(message.ChunkId, message.ChunkOffset)
	if len(message.TargetPorts) > 1 { // Only for the Primary Chunk Server.
		atomic.AddInt32(ACKMap[message.ChunkId][message.ClientPort][message.MessageType], int32(len(message.TargetPorts)-1))
		for index, targetPort := range message.TargetPorts[1:] {
			helper.SendMessageV2(targetPort, message, message.TargetPorts[0], []int{message.TargetPorts[index]}) // The TargetPorts attribute of the Message object is set to just one element.
			// This is so that this for loop is only trigerred in the Primary Chunk Server and not the Secondary Chunk Servers.
		}
		waitForACKs(message.ChunkId, message.ClientPort, message.MessageType)
		// ACK Master
		helper.SendMessage(message.SourcePort, helper.ACK_PAD, message.ClientPort, message.PrimaryChunkServer, message.SecondaryChunkServers, message.Filename, message.ChunkId, "", 0, 0, message.PrimaryChunkServer, []int{message.ClientPort}) // ACK to Master.
	} else { // Only for the Secondary Chunk Servers.
		helper.SendMessage(message.SourcePort, helper.ACK_PAD, message.ClientPort, message.PrimaryChunkServer, message.SecondaryChunkServers, message.Filename, message.ChunkId, "", 0, 0, message.TargetPorts[0], []int{message.PrimaryChunkServer})
	}
	releaseChunk(message.ChunkId)
}

func padFile(chunkId string, chunkOffset int64) {
	fh, err := os.OpenFile(chunkId+".txt", os.O_RDWR, 0644)
	if err != nil {
		fmt.Println("ERROR 4")
		fmt.Println(err)
	}
	defer fh.Close()
	// pad chunk until full
	padDataBytes := []byte(strings.Repeat("~", 10000-int(chunkOffset)))
	if _, err := fh.WriteAt(padDataBytes, chunkOffset); err != nil {
		fmt.Println("ERROR 5")
		fmt.Println(err)
	}
}

func padACKHandler(message structs.Message) { // TODO: Currently, this function is same as appendACKHandler - will have to change if we decide to index the messages further.
	(*ACKMap[message.ChunkId][message.ClientPort][message.MessageType]) -= 1
}
