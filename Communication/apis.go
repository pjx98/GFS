package Communication

import (
	"fmt"
	"strconv"
	"net/http"
	"github.com/gin-gonic/gin"
)

func test(context *gin.Context) {
	context.IndentedJSON(http.StatusOK, "Welcome to the Okay File System")
}

func post_message(context *gin.Context) {
	var message Message

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
	router.GET("/", test)
	router.GET("/test", test)
	router.POST("/message", post_message)

	fmt.Printf("Node %d listening on port %d \n", node_pid, port_no)
	router.Run("localhost:" + strconv.Itoa(port_no))
}
