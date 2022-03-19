package m

import (
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"

	helper "gfs.com/master/helper"
	structs "gfs.com/master/structs"
	"github.com/gin-gonic/gin"
)

var port int

// chunkLocks := map[string]bool
var chunkSizeMap = make(map[string]*int32)

func landing_page(context *gin.Context) {
	context.IndentedJSON(http.StatusOK, "Welcome to the Okay File System ! This is a chunk server.")
}

func post_message(context *gin.Context) {
	var message structs.Message

	// Call BindJSON to bind the received JSON to message.
	if err := context.BindJSON(&message); err != nil {
		fmt.Println("Invalid message object received.")
		return
	}
	context.IndentedJSON(http.StatusOK, message.MessageType+" message from Node "+strconv.Itoa(message.SourcePort)+" was received by Node "+strconv.Itoa(message.TargetPorts[0]))

	switch message.MessageType {
	case helper.DATA_APPEND:
		handleAppendMessage(message)
	}
}

func listen(nodePid int, portNo int) {
	router := gin.Default()
	router.GET("/", landing_page)
	router.POST("/message", post_message)

	fmt.Printf("Node %d listening on port %d \n", nodePid, portNo)
	router.Run("localhost:" + strconv.Itoa(portNo))
}

func handleAppendMessage(message structs.Message) {
	storeTempFile(message.Payload)
}

func append() {}

func waitForACK() {}

func sendACK() {}

func writeMutations() {}

func replicate() {}

func sendData() {}

func storeTempFile(chunkId string, payload string) {

}

func ChunkServer(nodePid int, portNo int) {
	go listen(nodePid, portNo)
}
