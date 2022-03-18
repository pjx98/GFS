package chunk

import (
	"fmt"
	// "sync/atomic"
	"net/http"
	"strconv"

	structs "gfs.com/master/structs"
	"github.com/gin-gonic/gin"
)

// chunkLocks := map[string]bool
// chunkSizeMap := map[string]int

func landing_page(context *gin.Context) {
	context.IndentedJSON(http.StatusOK, "Welcome to the Okay File System !")
}

func post_message(context *gin.Context) {
	var message structs.Message

	// Call BindJSON to bind the received JSON to
    // newAlbum.
    if err := context.BindJSON(&message); err != nil {
		fmt.Println("Invalid message object received.")
        return
    }
	context.IndentedJSON(http.StatusOK, message.Message_type + " message from Node " + strconv.Itoa(message.Source_pid) +" was received by Node " + strconv.Itoa(message.Target_pid))
}


func Listen(node_pid int, port_no int) {
	router := gin.Default()
	router.GET("/", landing_page)
	router.POST("/message", post_message)

	fmt.Printf("Node %d listening on port %d \n", node_pid, port_no)
	router.Run("localhost:" + strconv.Itoa(port_no))
}

func listen(){}

func checkChunkSpace(){}

func waitForACK(){}

func sendACK(){}

func append(){}

func replicate(){}

func sendData(){}

func connectToChunk(){}

