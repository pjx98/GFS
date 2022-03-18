package structs

type Message struct {
	Message_type string `json:"message_type"`
	Source_pid   int    `json:"source_pid"`
	Target_pid   int    `json:"target_pid"`
}

func create_message(message_type string, source_pid int, target_pid int) Message {
	message := Message{
		Message_type: message_type,
		Source_pid:   source_pid,
		Target_pid:   target_pid,
	}
	return message
}