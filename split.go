// package main

// import (
// 		"fmt"
// 		"io/ioutil"
// 		"math"
// 		"os"
// 		"strconv"
// 		"log"
// )

// func splitFile(filePath string, fileId string) {

// 		fileToBeChunked := filePath

// 		file, err := os.Open(fileToBeChunked)

// 		if err != nil {
// 				fmt.Println(err)
// 				os.Exit(1)
// 		}

// 		defer file.Close()

// 		fileInfo, _ := file.Stat()

// 		var fileSize int64 = fileInfo.Size()

// 		const fileChunk = 10 << (10 * 1) // 10KB

// 		// calculate total number of parts the file will be chunked into

// 		totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))

// 		fmt.Printf("Splitting to %d pieces.\n", totalPartsNum)

// 		file_id_to_chunkId_array := []string{}
// 		chunkId_to_port_array := []string{}
// 		port_number := 8000

// 		for i := uint64(0); i < totalPartsNum; i++ {

// 				partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
// 				partBuffer := make([]byte, partSize)
 
// 				file.Read(partBuffer)

// 				// write to disk
// 				// create file chunk
// 				fileName := fileId + "_chunk" + strconv.FormatUint(i, 10)
// 				_, err := os.Create(fileName)

// 				if err != nil {
// 						fmt.Println(err)
// 						os.Exit(1)
// 				}

// 				// record file chunk in metaData
// 				file_id_to_chunkId_array := append(file_id_to_chunkId_array, fileName)
// 				MetaData.file_id_to_chunkId[fileId] = file_id_to_chunkId_array

// 				// write/save buffer to respective port
// 				ioutil.WriteFile(fileName, partBuffer, os.ModeAppend)
// 				final_location := "./Port" + strconv.Itoa(port_number) + "/" + fileName + ".txt"
// 				port_number ++
// 				current_location := "./" + fileName + ".txt"
// 				err = os.Rename(current_location, final_location)

// 				if err != nil {
// 					log.Fatal(err)
// 				}

// 				// record chunk to port in metaData
// 				chunkId_to_port_array := append(chunkId_to_port_array, fileName)
// 				MetaData.chunkId_to_port[fileName] = chunkId_to_port_array

// 				fmt.Println("Split to : ", fileName)
// 		}
// }