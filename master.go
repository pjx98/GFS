package main

import (
	"fmt"
	"log"
	"net"
	"io/ioutil"
	"math"
	"os"
	"strconv"
)

type MetaData struct {

	// key: file id int, value: chunk array
	// eg file 1 = [file1_chunk1, file1_chunk2, file1_chunk3]
	file_id_to_chunkId map[string][]string

	// map each file chunk to a chunk server (port number)
	chunkId_to_port map[string][]string

}

/* 
Split file into equally chunked sizes
Edit metaData to include the new chunks
*/
func splitFile(filePath string, fileId string) {

	fileToBeChunked := filePath

	file, err := os.Open(fileToBeChunked)

	if err != nil {
			fmt.Println(err)
			os.Exit(1)
	}

	defer file.Close()

	fileInfo, _ := file.Stat()

	var fileSize int64 = fileInfo.Size()

	const fileChunk = 10 << (10 * 1) // 10KB

	// calculate total number of parts the file will be chunked into

	totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))

	fmt.Printf("Splitting to %d pieces.\n", totalPartsNum)

	file_id_to_chunkId_array := []string{}
	chunkId_to_port_array := []string{}
	port_number := 8000

	for i := uint64(0); i < totalPartsNum; i++ {

			partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
			partBuffer := make([]byte, partSize)

			file.Read(partBuffer)

			// write to disk
			// create file chunk
			fileName := fileId + "_chunk" + strconv.FormatUint(i, 10)
			_, err := os.Create(fileName)

			if err != nil {
					fmt.Println(err)
					os.Exit(1)
			}

			// record file chunk in metaData
			file_id_to_chunkId_array := append(file_id_to_chunkId_array, fileName)
			metaData.file_id_to_chunkId[fileId] = file_id_to_chunkId_array

			// write/save buffer to respective port
			ioutil.WriteFile(fileName, partBuffer, os.ModeAppend)
			final_location := "./Port" + strconv.Itoa(port_number) + "/" + fileName + ".txt"
			port_number ++
			current_location := "./" + fileName + ".txt"
			err = os.Rename(current_location, final_location)

			if err != nil {
				log.Fatal(err)
			}

			// record chunk to port in metaData
			chunkId_to_port_array := append(chunkId_to_port_array, fileName)
			metaData.chunkId_to_port[fileName] = chunkId_to_port_array

			fmt.Println("Split to : ", fileName)
	}
}

// server listening to client on their respective ports
func listenToClient(Client_id int, Port string) {

	address := "localhost:" + Port

	fmt.Printf("Master listening on Port %v\n", Port)

	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	go acceptConnection(Client_id, listener)
}

// connection to client established
func acceptConnection(Client_id int, listener net.Listener){
	defer listener.Close()

	for {
		_, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Server connected to Client %v\n", Client_id)

	}
}

func main() {

	// create servers with different ports
	listenToClient(1, "8000")
	listenToClient(2, "8001")
	listenToClient(3, "8002")

	initiateClient()

	var metaData MetaData
	metaData.file_id_to_chunkId = make(map[string][]string)
	metaData.chunkId_to_port = make(map[string][]string)
	splitFile("./file2.txt", "file2")


	for {

	}
	


}