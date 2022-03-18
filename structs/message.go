package structs

type Message struct {
	Message_type string `json:"message_type"`
	Append_last_chunk string `json:"append_last_chunk"`
	Source_pid   int    `json:"source_pid"`
	Target_pid   []int    `json:"target_pid"`
	// 0 index is the primary chunkserver
	// 1 and 2 index is the secondary chunkserver
}

func create_message(message_type string, append_last_chunk string, source_pid int, target_pid []int) Message {
	message := Message{
		Message_type: message_type,
		Append_last_chunk: append_last_chunk,
		Source_pid:   source_pid,
		Target_pid:   target_pid,
	}
	return message
}