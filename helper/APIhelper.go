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
func SendMessage(portNo int, messageType string, clientPort int, PCS int, SCS []int, fileName string, chunkId string, payload string, payloadSize int64, chunkOffset int64, sourcePort int, targetPorts []int) {
	message := structs.CreateMessage(messageType, clientPort, PCS, SCS, fileName, chunkId, payload, payloadSize, chunkOffset, sourcePort, targetPorts)
	SendMessageV2(portNo, message, sourcePort, targetPorts)
}

func SendMessageV2(portNo int, message structs.Message, sourcePort int, targetPorts []int) { // V2 takes in a Message object directly.
	message.SourcePort, message.TargetPorts = sourcePort, targetPorts // Used to reset the TargetPorts attribute of the Message struct.
	request_url := BASE_URL + ":" + strconv.Itoa(portNo) + "/message"
	messageJSON, _ := json.Marshal(message)
	response, err := http.Post(request_url, "application/json", bytes.NewBuffer(messageJSON))

	//Handle Error
	if err != nil {
		log.Fatalf("SendMessage: An Error Occured - %v", err)
	}

	defer response.Body.Close()
	//Read the response body
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(string(body))
}