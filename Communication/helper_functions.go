package Communication

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
)

// Capitalize function names to export them.
// Just call this function with the respective params to send a post request to the intended port.
func Send_message(port_no int, message_type string, source_pid int, target_pid int) {
	request_url := BASE_URL + strconv.Itoa(port_no) + "/message"
	message, _ := json.Marshal(create_message(message_type, source_pid, target_pid))
	response, err := http.Post(request_url, "application/json" ,bytes.NewBuffer(message))

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
