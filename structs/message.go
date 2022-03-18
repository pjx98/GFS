package structs

type Message struct {
	MessageType string
	ChunkId     string
	Filename    string // filename which client requests to append
	SourcePid   int
	TargetPid   []int
	Payload     string
	Size        int32
	// 0 index is the primary chunkserver
	// 1 and 2 index is the secondary chunkserver
}

func CreateMessage(messageType string, chunkId string, filename string, sourcePid int, targetPid []int, payload string, size int32) Message {
	message := Message{
		MessageType: messageType,
		ChunkId:     chunkId,
		Filename:    filename,
		SourcePid:   sourcePid,
		TargetPid:   targetPid,
		Payload:     payload,
		Size:        size,
	}
	return message
}
