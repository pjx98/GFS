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

func send_message(context *gin.Context) {
	var message Message

	// Call BindJSON to bind the received JSON to
    // newAlbum.
    if err := context.BindJSON(&message); err != nil {
		fmt.Println("Invalid message object received.")
        return
    }
	context.IndentedJSON(http.StatusOK, message)
}


func Listen(node_pid int, port_no int) {
	router := gin.Default()
	router.GET("/", test)
	router.GET("/test", test)
	router.POST("/message", send_message)

	fmt.Printf("Node %d listening on port %d \n", node_pid, port_no)
	router.Run("localhost:" + strconv.Itoa(port_no))
}
