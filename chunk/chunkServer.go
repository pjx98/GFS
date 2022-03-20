package chunk

// TODO: Look through and decide which part of the code will be run as a seperate go routine.
// TODO: When to pad and how to know to Pad.
// TODO: Fault tolerance, handle cases when secondary fails. How to retry?
// TODO: Return correct offset to client
// TODO: Create chunk function
import (
	"fmt"
	helper "gfs.com/master/helper"
	structs "gfs.com/master/structs"
	"github.com/gin-gonic/gin"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
)

var (
	chunkIdAppendDataMap *map[string]map[int]structs.Queue // A map where the chunkId is the key and the value is another map whose keys are the portNo that made the append request and the values are queues whose elemnts are the data that is to be appended.
	ACKMap               map[string]map[int]*int32         // TODO: Should we serialize this based on requests (request type / request ID) as well ? (If we impose a condition that the client can make only one append request at once, we will not need serialization by requests)
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
	case helper.CREATE_NEW_CHUNK:
		createNewChunkHandler(message)
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
		atomic.AddInt32(ACKMap[message.ChunkId][message.ClientPort], int32(len(message.TargetPorts)-1))
		for index, targetPort := range message.TargetPorts[1:] {
			helper.SendMessageV2(targetPort, message, message.TargetPorts[0], []int{message.TargetPorts[index]}) // The TargetPorts attribute of the Message object is set to just one element.
			// This is so that this for loop is only trigerred in the Primary Chunk Server and not the Secondary Chunk Servers.
		}
		waitForACKs(message.ChunkId, message.ClientPort)
		helper.SendMessage(message.ClientPort, helper.ACK_APPEND, message.ClientPort, message.PrimaryChunkServer, message.SecondaryChunkServers, message.Filename, message.ChunkId, "", 0, 0, message.PrimaryChunkServer, []int{message.ClientPort}) // ACK to Client.
	} else { // Only for the Secondary Chunk Servers.
		helper.SendMessage(message.PrimaryChunkServer, helper.ACK_APPEND, message.ClientPort, message.PrimaryChunkServer, message.SecondaryChunkServers, message.Filename, message.ChunkId, "", 0, 0, message.TargetPorts[0], []int{message.PrimaryChunkServer})
	}
}

func appendACKHandler(message structs.Message) {
	atomic.AddInt32(ACKMap[message.ChunkId][message.ClientPort], -1)
}

func commitACKHandler(message structs.Message) { // TODO: Currently, this function is same as appendACKHandler - will have to change if we decide to index the messages further.
	atomic.AddInt32(ACKMap[message.ChunkId][message.ClientPort], -1)
}

func commitDataHandler(message structs.Message) {
	lockChunk(message.ChunkId)
	writeMutations(message.ChunkId, message.ClientPort, message.ChunkOffset)
	if len(message.TargetPorts) > 1 { // Only for the Primary Chunk Server.
		atomic.AddInt32(ACKMap[message.ChunkId][message.ClientPort], int32(len(message.TargetPorts)-1))
		for index, targetPort := range message.TargetPorts[1:] {
			helper.SendMessageV2(targetPort, message, message.TargetPorts[0], []int{message.TargetPorts[index]}) // The TargetPorts attribute of the Message object is set to just one element.
			// This is so that this for loop is only trigerred in the Primary Chunk Server and not the Secondary Chunk Servers.
		}
		waitForACKs(message.ChunkId, message.ClientPort)
		helper.SendMessage(message.ClientPort, helper.ACK_COMMIT, message.ClientPort, message.PrimaryChunkServer, message.SecondaryChunkServers, message.Filename, message.ChunkId, "", 0, 0, message.PrimaryChunkServer, []int{message.ClientPort}) // ACK to Client.
	} else { // Only for the Secondary Chunk Servers.
		helper.SendMessage(message.PrimaryChunkServer, helper.ACK_COMMIT, message.ClientPort, message.PrimaryChunkServer, message.SecondaryChunkServers, message.Filename, message.ChunkId, "", 0, 0, message.TargetPorts[0], []int{message.PrimaryChunkServer})
	}
	releaseChunk(message.ChunkId)
}

func createNewChunkHandler(message structs.Message) {
	createChunk(message.TargetPorts[0], message.ChunkId)
}

func writeMutations(chunkId string, clientPort int, chunkOffset int64) {
	fh, err := os.OpenFile(chunkId+".txt", os.O_RDWR, 0644)
	if err != nil {
		fmt.Println(err)
	}
	defer fh.Close()
	writeData, _ := (*chunkIdAppendDataMap)[chunkId][clientPort].Peek()
	writeDataBytes := []byte(writeData)
	if _, err := fh.WriteAt(writeDataBytes, chunkOffset); err != nil {
		fmt.Println(err)
	}
}

func waitForACKs(chunkId string, clientPort int) {
	for {
		if *(ACKMap[chunkId][clientPort]) == 0 {
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

func createChunk(portNo int, chunkId string) {
	pwd, _ := os.Getwd()
	dataDirPath := filepath.Join(pwd, "../"+helper.DATA_DIR)
	helper.CreateFolder(dataDirPath)
	portDataDirPath := filepath.Join(dataDirPath, strconv.Itoa(portNo))
	helper.CreateFolder(portDataDirPath)
	chunkPath := filepath.Join(portDataDirPath, chunkId + ".txt")
	helper.CreateFile(chunkPath)
}

func ChunkServer(nodePid int, portNo int) {
	go listen(nodePid, portNo)
}
