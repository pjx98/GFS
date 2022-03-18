package chunk

import (
	"fmt"
	helper "gfs.com/master/helper"
	structs "gfs.com/master/structs"
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
	"sync/atomic"
)

// chunkLocks := map[string]bool
var chunkSizeMap = make(map[string]*int32)

func landing_page(context *gin.Context) {
	context.IndentedJSON(http.StatusOK, "Welcome to the Okay File System !")
}

func post_message(context *gin.Context) {
	var message structs.Message

	// Call BindJSON to bind the received JSON to message.
	if err := context.BindJSON(&message); err != nil {
		fmt.Println("Invalid message object received.")
		return
	}
	context.IndentedJSON(http.StatusOK, message.MessageType+" message from Node "+strconv.Itoa(message.SourcePid)+" was received by Node "+strconv.Itoa(message.TargetPid[0]))

	switch message.MessageType {
	case helper.DATA_APPEND:
		checkChunkSpace(message.ChunkId, message.Size)
	}
}

func Listen(node_pid int, port_no int) {
	router := gin.Default()
	router.GET("/", landing_page)
	router.POST("/message", post_message)

	fmt.Printf("Node %d listening on port %d \n", node_pid, port_no)
	router.Run("localhost:" + strconv.Itoa(port_no))
}

func listen() {}

func checkChunkSpace(ChunkId string, dataSize int32) {
	if dataSize < *chunkSizeMap[ChunkId] {
		atomic.AddInt32(chunkSizeMap[ChunkId], -1*dataSize)
	}
}

func append() {}

func waitForACK() {}

func sendACK() {}

func writeMutations() {}

func replicate() {}

func sendData() {}

func connectToChunk() {}
