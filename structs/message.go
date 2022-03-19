package structs

type Message struct {
	MessageType string
	ClientPort  int
	Filename    string // filename which client requests to append
	ChunkId     string
	SourcePort  int
	TargetPorts []int // 0 index is the primary chunkserver & 1 and 2 index is the secondary chunkserver
	Payload     string
	PayloadSize int32
}

func CreateMessage(messageType string, clientPort int, chunkId string, fileName string, sourcePort int, targetPorts []int, payload string, payloadSize int32) Message {
	message := Message{
		MessageType: messageType,
		ClientPort:  clientPort,
		ChunkId:     chunkId,
		Filename:    fileName,
		SourcePort:  sourcePort,
		TargetPorts: targetPorts,
		Payload:     payload,
		PayloadSize: payloadSize,
	}
	return message
}
