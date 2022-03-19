package chunk
// TODO: Look through and decide which part of the code will be run as a seperate go routine.

import (
	"fmt"
	"net/http"
	"strconv"
	helper "gfs.com/master/helper"
	structs "gfs.com/master/structs"
	"github.com/gin-gonic/gin"
)

var port int
var chunkIdAppendDataMap *map[string]map[int]structs.Queue // A map where the chunkId is the key and the value is another map whose keys are the portNo that
// made the append request and the values are queues whose elemnts are the data that is to be appended.
var ACKMap map[string]map[int]int // TODO: Should we serialize this based on requests as well ? (If we impose a condition that the client can make only one append
// request at once, we will not need serialization by requests)
// chunkLocks := map[string]bool

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
	}
}


func listen(nodePid int, portNo int) {
	router := gin.Default()
	router.GET("/", landingPageHandler)
	router.POST("/message", postMessageHandler)

	fmt.Printf("Node %d listening on port %d \n", nodePid, portNo)
	router.Run("localhost:" + strconv.Itoa(portNo))
}

func appendMessageHandler(message structs.Message) {
	storeTempFile(message.ChunkId, message.SourcePort, message.Payload)
	if len(message.TargetPorts) > 1 { // Only for the Primary Chunk Server.
		for index, targetPort := range message.TargetPorts[1:] {
			helper.SendMessageV2(targetPort, message, []int{message.TargetPorts[index]}) // The TargetPorts attribute of the Message object is set to just one element.
			// This is so that this for loop is only trigerred in the Primary Chunk Server and not the Secondary Chunk Servers.
		}
		waitForACKs(len(message.TargetPorts), message.ChunkId, message.SourcePort)
		helper.SendMessage(message.SourcePort, helper.ACK_APPEND, message.ChunkId, message.Filename, message.TargetPorts[0], []int{message.SourcePort}, "", 0)
	}
}

func waitForACKs(noOfACKs int, chunkId string, sourcePort int) {
	ACKMap[chunkId][sourcePort] += 2
	for {
		if (ACKMap[chunkId][sourcePort] == 0) {
			break
		}
	}
}

func sendACK() {} //TODO: Need to see if we need this fucntion since sending ACKs will be specific to the type of fucntion being performed.

func writeMutations() {}

func replicate() {}

func sendData() {}

func storeTempFile(chunkId string, sourcePort int, payload string) {
	(*chunkIdAppendDataMap)[chunkId][sourcePort].Enqueue(payload)
}

func ChunkServer(nodePid int, portNo int) {
	go listen(nodePid, portNo)
}
