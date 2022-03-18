package helper

const (
	BASE_URL = "http://localhost"

	// IDs
	LOCK_SERVER_PID = 1

	// Ports
	LOCK_SERVER_PORT = 8080

	// Message Types
	DATA_APPEND = "DATA_APPEND" // send chunk data to append
	DATA_COMMIT = "DATA_COMMIT" // tell chunk to write

	ACK_APPEND = "ACK_APPEND" // chunk ACK data to appened has been received
	ACK_COMMIT = "ACK_COMMIT" // chunk ACK data has been committed
)
