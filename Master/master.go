package main

import (
  "encoding/json"
  "fmt"
  client "gfs.com/master/client"
  helper "gfs.com/master/helper"
  structs "gfs.com/master/structs"
  chunk "gfs.com/master/chunk"
  "log"
  //"net"
  "github.com/gin-gonic/gin"
  "net/http"
  "strings"
  "strconv"
  "math/rand"
  "bytes"
  "reflect"
  "sync/atomic"

)

type MetaData struct {

  // key: file id int, value: chunk array
  // eg file 1 = [f1_c0, file1_chunk2, file1_chunk3]
  file_id_to_chunkId map[string][]string

  // map each file chunk to a chunk server (port number)
  chunkId_to_chunkserver map[string][]int

  // map each chunkserver to the amount of storage it has
  // max chunk is 10 KB
  // max append data is 2.5KB
  // {f1 : {c0 : 0KB, c1 : 2KB} }
  file_id_to_chunkId_offset map[string]map[string]*int64


}

var metaData MetaData


func choose_3_random_chunkServers() []int {

  
  chunkServerArray := map[int]bool{
    8081 : false,
    8082 : false,
    8083 : false,
    8084 : false,
    8085 : false,
  }

  res := []int{}

  for len(res) < 3 {
    //random key stores the key from the chunkS
    random_key := MapRandomKeyGet(chunkServerArray).(int)
    // checking if this key boolean is false or true, if false append this key to the res and set the key value true instead
    if chunkServerArray[random_key] == false {
      chunkServerArray[random_key] = true
      res = append(res, random_key)
      fmt.Println(res)

    } else {
      //if the chunkS[random_key]==true, it means that the random key has been added into the res array
      continue
    }

  }
  return res

}

//this will select random keys in the map
func MapRandomKeyGet(mapI interface{}) interface{} {
  keys := reflect.ValueOf(mapI).MapKeys()

  return keys[rand.Intn(len(keys))].Interface()
}

func listen(nodePid int, portNo int) {
	router := gin.Default()
	router.POST("/message", postMessageHandler)

	fmt.Printf("Node %d listening on port %d \n", nodePid, portNo)
	router.Run("localhost:" + strconv.Itoa(portNo))
}

func postMessageHandler(context *gin.Context) {
	var message structs.Message

	// Call BindJSON to bind the received JSON to message.
	if err := context.BindJSON(&message); err != nil {
		fmt.Println("Invalid message object received.")
		return
	}
	context.IndentedJSON(http.StatusOK, message.MessageType+" message from Node "+strconv.Itoa(message.SourcePort)+" was received by Master")



  fmt.Printf("Master connected to Client\n")
  last_chunk := ""

  if _, ok := metaData.file_id_to_chunkId[message.Filename]; ok {

    // if file does not exist in metaData, create a new entry
    if ok == false {

      newFileAppend(message) 

    } else {
      // if file exist
      // get last chunk 
      array := metaData.file_id_to_chunkId[message.Filename]
      last_chunk = metaData.file_id_to_chunkId[message.Filename][len(array)-1]

      current_offset := metaData.file_id_to_chunkId_offset[message.Filename][last_chunk]
      remaining_space := 10000 - current_offset

      // check if append data < chunk size
      if message.PayloadSize < remaining_space{

        sendClientChunkServers(message, last_chunk)

      }else if message.PayloadSize == remaining_space{
        // append == chunksize
        sendClientChunkServers(message, last_chunk)

        createNewChunk(message, chunkServerArray)

      }else{
        // append > chunksize
        appendGreaterThanChunk(message, chunkServerArray)
      }
    }
  }
}

func create_new_chunkId(message structs.Message) string{
  // map[string]map[string]int64
  array := metaData.file_id_to_chunkId[message.Filename]
  current_chunkId := metaData.file_id_to_chunkId[message.Filename][len(array)-1]
  c_index := strings.Index(current_chunkId, "c")
  chunkId := current_chunkId[c_index+1:]

  //increment by 1
  int_chunkId, err := strconv.Atoi(chunkId) 
  if err != nil {
    log.Fatalln(err)
  }

  int_chunkId++
  string_chunkId := string(int_chunkId)
  new_chunkId := message.Filename + "_c" + string_chunkId
  metaData.file_id_to_chunkId[message.Filename] = append(metaData.file_id_to_chunkId[message.Filename], new_chunkId)

  return new_chunkId
}


// ask chunkservers to create new chunks
func createNewChunk(message structs.Message, chunkServerArray map[int]bool){
  new_chunkServers := choose_3_random_chunkServers(chunkServerArray)
  chunkId := create_new_chunkId(message)
  // ask the 3 chunkserver to create chunks

  for i := 0; i < 3; i++ {
    chunkServer := new_chunkServers[i]
    helper.SendMessage(chunkServer, "CREATE_NEW_CHUNK", helper.MASTER_SERVER_PORT, chunkServer, []int{chunkServer}, message.Filename, chunkId, "",
      0, 0, helper.MASTER_SERVER_PORT, []int{chunkServer})
  }
 
  // set new chunk offset to 0
  metaData.file_id_to_chunkId_offset[message.Filename][chunkId] = 0

}

// Client wants to append to a new file
func newFileAppend(message structs.Message){
  // create new entry in MetaData
  chunkId := message.Filename + "_c0"
  metaData.file_id_to_chunkId[message.Filename] = []string{chunkId}

  // ask 3 chunkserver to create chunks
  new_chunkServer := choose_3_random_chunkServers()
  for i := 0; i < 3; i++ {
    chunkServer := new_chunkServer[i]
    helper.SendMessage(chunkServer, helper.CREATE_NEW_CHUNK, helper.MASTER_SERVER_PORT, chunkServer, []int{chunkServer}, message.Filename, chunkId, "",
      0, 0, helper.MASTER_SERVER_PORT, []int{chunkServer})
  }
}

// send client chunkservers 
func sendClientChunkServers(message structs.Message, lastChunk string){

  helper.SendMessage(metaData.chunkId_to_chunkserver[lastChunk][0], helper.DATA_APPEND, message.ClientPort, metaData.chunkId_to_chunkserver[lastChunk][0], metaData.chunkId_to_chunkserver[lastChunk][1:], message.Filename, lastChunk, "",
      0, 0, helper.MASTER_SERVER_PORT, metaData.chunkId_to_chunkserver[lastChunk])
  
  fmt.Printf("Master replying Client with locations of filename %v", message.Filename)

  // increment offset
  metaData.file_id_to_chunkId_offset[message.Filename][lastChunk] += message.PayloadSize

  atomic.

}

/*
increase current last chunk offset to 10KB
send message to old Chunkserver to ask them to pad chunk to 10KB
Send message to 3 random chunkserver to ask them to create new chunks for next append
*/
func appendGreaterThanChunk(message structs.Message, chunkServerArray map[int]bool ){

  // get last chunk
  array := metaData.file_id_to_chunkId[message.Filename]
  last_chunk := metaData.file_id_to_chunkId[message.Filename][len(array)-1]



  // old chunkId
  // increase offset to 10KB
  metaData.file_id_to_chunkId_offset[message.Filename][last_chunk] = 10000

  // tell old CS to pad chunk
  old_chunkServers := metaData.chunkId_to_chunkserver[last_chunk]
  for i := 0; i < 3; i++ {
    chunkServer := old_chunkServers[i]
    helper.SendMessage(chunkServer, "DATA_PAD", helper.MASTER_SERVER_PORT, chunkServer, []int{chunkServer}, message.Filename, last_chunk, "",
      0, 0, helper.MASTER_SERVER_PORT, []int{chunkServer})
    fmt.Printf("Master asking ChunkServer %v to pad old chunk for %v", chunkServer, last_chunk)
  }
  
  //get new chunkId
  new_chunkId := create_new_chunkId(message)

  // choose 3 chunkserver to create chunks
  new_chunkServer := choose_3_random_chunkServers(chunkServerArray)

  // ask 3 CS to create new chunk
  for i := 0; i < 3; i++ {
    chunkServer := new_chunkServer[i]
    helper.SendMessage(chunkServer, "CREATE_NEW_CHUNK", helper.MASTER_SERVER_PORT, chunkServer, []int{chunkServer}, message.Filename, new_chunkId, "",
    0, 0, helper.MASTER_SERVER_PORT, []int{chunkServer})
    fmt.Printf("Master asking ChunkServer %v to create new chunk for %v", new_chunkServer[i], message.Filename)
  }
  

}

 




func main(){

  metaData.file_id_to_chunkId = make(map[string][]string)
  metaData.chunkId_to_chunkserver = make(map[string][]int)
  metaData.file_id_to_chunkId_offset = make(map[string]map[string]int64)


  go listen(0, 8080)
  chunk.ChunkServer(2,8081)
  chunk.ChunkServer(3,8082)
  chunk.ChunkServer(4,8083)
  client.StartClient(7, 8086)
  // // listening to client on port 8000
  // listenToClient(1, "8000", metaData)
  // client.StartClient()
}