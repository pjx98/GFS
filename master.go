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
func splitFile(filePath string, fileId string, metaData MetaData) {

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
	//port_number := 8000

	for i := uint64(0); i < totalPartsNum; i++ {
			chunkId_to_port_array := []string{}

			partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
			partBuffer := make([]byte, partSize)

			file.Read(partBuffer)

			// write to disk
			// create file chunk
			fileName := fileId + "_chunk" + strconv.FormatUint(i, 10)

			// create replicated chunks at other chunkserver
			for i := 8000; i < 8003; i++{
				replicated_location :=  "./Files/Port" + strconv.Itoa(i) + "/" + fileName
				_, err := os.Create(replicated_location)
				
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
			}

			// record file chunk in metaData
			// [file2 : [chunk0, chunk1, chunk2]]
			file_id_to_chunkId_array = append(file_id_to_chunkId_array, fileName)
			metaData.file_id_to_chunkId[fileId] = file_id_to_chunkId_array

			

			// write/save buffer to respective port
			for i := 8000; i < 8003; i++{
				replicated_location :=  "./Files/Port" + strconv.Itoa(i) + "/" + fileName
				ioutil.WriteFile(replicated_location, partBuffer, os.ModeAppend)
				fmt.Println("Split to : ", fileName)
				fmt.Printf("Replicating %v to %v", fileName, replicated_location)

				// record chunk to port in metaData
				chunkId_to_port_array = append(chunkId_to_port_array, strconv.Itoa(i))
				metaData.chunkId_to_port[fileName] = chunkId_to_port_array
			}
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
	splitFile("./file2.txt", "file2", metaData)
	fmt.Println(metaData.file_id_to_chunkId)
	fmt.Println(metaData.chunkId_to_port)

	for {

	}
	


}