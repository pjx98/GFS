package structs

type Message struct {
	MessageType string

	// Data Corresponding to the whole operation
	ClientPort            int   // Node that started operation
	PrimaryChunkServer    int   // PCS responsible for handling the operation
	SecondaryChunkServers []int // SCS for handling operation

	// File, Chunk & Payload Data
	Filename    string // filename which client requests to append
	ChunkId     string
	Payload     string
	PayloadSize int64
	ChunkOffset int64 // Offset at which the data is to be written

	// Source & Target Ports
	SourcePort  int   // Node that is sending this message
	TargetPorts []int // 0 index is the primary chunkserver & 1 and 2 index is the secondary chunkserver
}

func CreateMessage(messageType string, clientPort int, PCS int, SCS []int, fileName string, chunkId string, payload string, payloadSize int64, chunkOffset int64, sourcePort int, targetPorts []int) Message {
	message := Message{
		MessageType:           messageType,
		ClientPort:            clientPort,
		PrimaryChunkServer:    PCS,
		SecondaryChunkServers: SCS,
		Filename:              fileName,
		ChunkId:               chunkId,
		Payload:               payload,
		PayloadSize:           payloadSize,
		ChunkOffset:           chunkOffset,
		SourcePort:            sourcePort,
		TargetPorts:           targetPorts,
	}
	return message
}
