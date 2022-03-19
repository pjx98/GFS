package helper

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	structs "gfs.com/master/structs"
)

// Capitalize function names to export them.
// Just call this function with the respective params to send a post request to the intended port.
func Send_message(portNo int, messageType string, chunkId string, filename string, sourcePort int, targetPorts []int, payload string, payloadSize int32) {
	request_url := BASE_URL + strconv.Itoa(portNo) + "/message"
	message, _ := json.Marshal(structs.CreateMessage(messageType, chunkId, filename, sourcePort, targetPorts, payload, payloadSize))
	response, err := http.Post(request_url, "application/json", bytes.NewBuffer(message))

	//Handle Error
	if err != nil {
		log.Fatalf("Send_message: An Error Occured - %v", err)
	}

	defer response.Body.Close()
	//Read the response body
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf(string(body))
}
