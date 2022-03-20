package main

import (
  "encoding/json"
  "fmt"
  client "gfs.com/master/client"
  helper "gfs.com/master/helper"
  structs "gfs.com/master/structs"
  chunk "gfs.com/master/chunk"
  "log"
  "net"
  "github.com/gin-gonic/gin"
  "net/http"
  "strings"
  "strconv"
  "math/rand"
  "bytes"
  "reflect"

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
  file_id_to_chunkId_offset map[string]map[string]int64


}

var metaData MetaData



func choose_3_random_chunkServers(chunkS map[int]bool) []int {

  res := []int{}

  for len(res) < 3 {
    //random key stores the key from the chunkS
    random_key := MapRandomKeyGet(chunkS).(int)
    // checking if this key boolean is false or true, if false append this key to the res and set the key value true instead
    if chunkS[random_key] == false {
      chunkS[random_key] = true
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

func listen(){
  router := gin.Default()
  router.POST("/client/append", listenClientAppend)
  router.POST("/message", receiveChunkServerAck)

  fmt.Printf("Master listening on port 8080 \n")
  router.Run("localhost:8080")
}

func listenClientAppend(context *gin.Context) {
  var message structs.Message

  // Call BindJSON to bind the received JSON to message.
  if err := context.BindJSON(&message); err != nil {
    fmt.Println("Invalid message object received.")
    return
  }
  append_response := appendMessageHandler(message)
  context.IndentedJSON(http.StatusOK, append_response) // this is the reply
  //context.IndentedJSON(http.StatusOK, message.MessageType+" message from Client "+strconv.Itoa(message.SourcePort)+" was received by Master")  
  fmt.Printf("Message sent back to Client %d\n", message.SourcePort)
}

func appendMessageHandler(message structs.Message)(structs.Message){
  chunkServerArray := map[int]bool{
    8081 : false,
    8082 : false,
    8083 : false,
    8084 : false,
    8085 : false,
  }
  fmt.Printf("Master connected to Client\n")
  last_chunk := ""

  if _, ok := metaData.file_id_to_chunkId[message.Filename]; ok {

    // if file does not exist in metaData, create a new entry
    if ok == false {

      newFileAppend(message, chunkServerArray) 

    } else {
      // if file exist
      // get last chunk 
      array := metaData.file_id_to_chunkId[message.Filename]
      last_chunk = metaData.file_id_to_chunkId[message.Filename][len(array)-1]

      current_offset := metaData.file_id_to_chunkId_offset[message.Filename][last_chunk]
      remaining_space := 10000 - current_offset

      // check if append data < chunk size
      if message.PayloadSize < remaining_space{

        return_message := sendClientChunkServers(message, last_chunk)

      }else if message.PayloadSize == remaining_space{
        // append == chunksize
        return_message := sendClientChunkServers(message, last_chunk)

        createNewChunk(message, chunkServerArray)

      }else{
        // append > chunksize
        appendGreaterThanChunk(message)
      }
    }
  }
  return return_message

  // dest_chunkserver := []int{8081, 8082, 8083}
  // return_message := structs.CreateMessage(helper.DATA_APPEND, 8086, dest_chunkserver[0], dest_chunkserver[1:], message.Filename, last_chunk, "DATA", message.PayloadSize, 0, 8080, dest_chunkserver)
  // return return_message
}

// ask chunkservers to create new chunks
func createNewChunk(message structs.Message, chunkServerArray map[int]bool){
  new_chunkServers := choose_3_random_chunkServers(chunkServerArray)
  
  // ask the 3 chunkserver to create chunks
  for i := 0; i < 3; i++ {

    msgJson := &structs.Message{
      MessageType: helper.CREATE_NEW_CHUNK,
      
      ClientPort: 8086,
      PrimaryChunkServer: 0,
      SecondaryChunkServers: nil,
      
      Filename: message.Filename,
      ChunkId: "",
      Payload: "",
      PayloadSize: 0,
      ChunkOffset: 0,
  
      SourcePort: 8080,
      TargetPorts: []int{new_chunkServers[i]},
    }

    post, _ := json.Marshal(msgJson)
    fmt.Println(string(post)) // debug
    responseBody := bytes.NewBuffer(post)

    resp, err := http.Post("http://localhost:" + string(new_chunkServers[i]) + "/message", "application/json", responseBody)
    fmt.Printf("Master asking ChunkServer %v to create new chunk for %v", new_chunkServers[i], message.Filename)
    // Handle Error
    if err != nil {
      log.Fatalf("An Error Occured %v", err)
    }

    defer resp.Body.Close()

  }
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
  // set new chunk offset to 0
  metaData.file_id_to_chunkId_offset[message.Filename][new_chunkId] = 0

}

// Client wants to append to a new file
func newFileAppend(message structs.Message, chunkServerArray map[int]bool){
  // create new entry in MetaData
  chunkId := message.Filename + "_c0"
  metaData.file_id_to_chunkId[message.Filename] = []string{chunkId}
  
  

  // ask 3 chunkserver to create chunks
  new_chunkServer := choose_3_random_chunkServers(chunkServerArray)
  
  // ask the 3 chunkserver to create chunks
  for i := 0; i < 3; i++ {

    msgJson := &structs.Message{
      MessageType: helper.CREATE_NEW_CHUNK,
      
      ClientPort: 8086,
      PrimaryChunkServer: 0,
      SecondaryChunkServers: nil,
      
      Filename: message.Filename,
      ChunkId: "",
      Payload: "",
      PayloadSize: 0,
      ChunkOffset: 0,
  
      SourcePort: 8080,
      TargetPorts: []int{new_chunkServer[i]},
    }

    post, _ := json.Marshal(msgJson)
    fmt.Println(string(post)) // debug
    responseBody := bytes.NewBuffer(post)

    resp, err := http.Post("http://localhost:" + string(new_chunkServer[i]) + "/message", "application/json", responseBody)
    fmt.Printf("Master asking ChunkServer %v to create new chunk for %v", new_chunkServer[i], message.Filename)
    // Handle Error
    if err != nil {
      log.Fatalf("An Error Occured %v", err)
    }

    // increment offset in new file
    // map[string]map[string]int64
    metaData.file_id_to_chunkId_offset[message.Filename][chunkId] = 0

    defer resp.Body.Close()

  }
}

// send client chunkservers 
func sendClientChunkServers(message structs.Message, lastChunk string)(structs.Message){
  // get location of last chunk
  msgJson := structs.Message{
    MessageType: helper.DATA_APPEND,
    
    ClientPort: 8086,
    PrimaryChunkServer: metaData.chunkId_to_chunkserver[lastChunk][0],
    SecondaryChunkServers: metaData.chunkId_to_chunkserver[lastChunk][1:],
    
    Filename: message.Filename,
    ChunkId: lastChunk,
    Payload: "",
    PayloadSize: 0,
    ChunkOffset: metaData.file_id_to_chunkId_offset[message.Filename][lastChunk],

    SourcePort: 8080,
    TargetPorts: metaData.chunkId_to_chunkserver[lastChunk],
  }


  fmt.Printf("Master replying Client with locations of filename %v", message.Filename)
  // Handle Error

  // increment offset in new file
  // map[string]map[string]int64
  metaData.file_id_to_chunkId_offset[message.Filename][lastChunk] += message.PayloadSize

  return msgJson
}

/*
increase current last chunk offset to 10KB
send message to old Chunkserver to ask them to pad chunk to 10KB
Send message to 3 random chunkserver to ask them to create new chunks for next append
*/
func appendGreaterThanChunk(message structs.Message, chunkServerArray map[int]bool ){

  cka := chunkServerArray

  // get last chunk
  array := metaData.file_id_to_chunkId[message.Filename]
  last_chunk := metaData.file_id_to_chunkId[message.Filename][len(array)-1]

  // ask 3 chunkserver to create chunks
  new_chunkServer := choose_3_random_chunkServers(chunkServerArray)

  // old chunkId
  // increase offset to 10KB
  metaData.file_id_to_chunkId_offset[message.Filename][last_chunk] = 10000

  // tell old CS to pad chunk
  for i := 0; i < 3; i++ {

    msgJson := &structs.Message{
      MessageType: helper.PAD_CHUNK,
      
      ClientPort: 8086,
      PrimaryChunkServer: 0,
      SecondaryChunkServers: nil,
      
      Filename: last_chunk,
      ChunkId: "",
      Payload: "",
      PayloadSize: 0,
      ChunkOffset: 0,
  
      SourcePort: 8080,
      TargetPorts: []int{new_chunkServer[i]},
    }

    post, _ := json.Marshal(msgJson)
    fmt.Println(string(post)) // debug
    responseBody := bytes.NewBuffer(post)

    resp, err := http.Post("http://localhost:" + string(new_chunkServer[i]) + "/message", "application/json", responseBody)
    fmt.Printf("Master asking ChunkServer %v to create new chunk for %v", new_chunkServer[i], message.Filename)
    // Handle Error
    if err != nil {
      log.Fatalf("An Error Occured %v", err)
    }
    defer resp.Body.Close()
  }

  // Tell 3 new CS to create chunk
  createNewChunk(message,cka)

}

// server listening to client on their respective ports
func listenToClient(Client_id int, Port string, metaData MetaData) {

  address := "localhost:" + Port

  fmt.Printf("Master listening on Port %v\n", Port)

  listener, err := net.Listen("tcp", address)
  if err != nil {
    log.Fatal(err)
  }
  go acceptConnection(Client_id, listener, metaData)
}

// connection to client established
func acceptConnection(Client_id int, listener net.Listener, metaData MetaData) {
  defer listener.Close()

  for {
    conn, err := listener.Accept()
    if err != nil {
      log.Fatal(err)
    }
    fmt.Printf("Master receives a new connection\n")
    go listenClient(conn, metaData)
  }
}

func listenClient(conn net.Conn, metaData MetaData) {
  fmt.Printf("Master connected to Client\n ")
  for {
    buffer := make([]byte, 1400)
    dataSize, err := conn.Read(buffer)

    if err != nil {
      fmt.Println("Connection has closed")
      return
    }

    //This is the message you received
    data := buffer[:dataSize]
    var message structs.Message
    json.Unmarshal([]byte(data), &message)

    last_chunk := ""

    // check message Type
    // if message is createAck
    if (message.MessageType == "createAck"){
      //do something
    } else{
    // if message type is an append request from client

      if _, ok := metaData.file_id_to_chunkId[message.Filename]; ok {
        // if file does not exist in metaData, create a new entry and named it fn.c0
        if ok == false {
          metaData.file_id_to_chunkId[message.Filename] = []string{message.Filename + "_c0"}
        } else {



// check if append < chunk size
  if (message.PayloadSize < 10000){

  }


    // if file exist, increment chunkId by 1
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
    metaData.file_id_to_chunkId[message.Filename] = append(metaData.file_id_to_chunkId[message.Filename], message.Filename + "_c" + string_chunkId)
  }
}
  // check if chunkserver is full
  array := metaData.file_id_to_chunkId[message.Filename]
  last_chunk = metaData.file_id_to_chunkId[message.Filename][len(array)-1]



  dest_chunkserver := []int{8001, 8002, 8003}
  return_message := structs.CreateMessage(helper.DATA_APPEND, 8000, dest_chunkserver[0], dest_chunkserver[1:], message.Filename, last_chunk, "DATA", 4, 0, 8000, dest_chunkserver)
  data, err = json.Marshal(return_message)

  }



    if err != nil {
      log.Fatalln(err)
    }

  // Send the message back
    _, err = conn.Write(data)
    if err != nil {
      log.Fatalln(err)
    }
    fmt.Print("Message sent: ", string(data))
    }
  }

func main(){

  metaData.file_id_to_chunkId = make(map[string][]string)
  metaData.chunkId_to_chunkserver = make(map[string][]int)
  metaData.file_id_to_chunkId_offset = make(map[string]map[string]int64)


  go listen()
  chunk.ChunkServer(2,8081)
  chunk.ChunkServer(3,8082)
  chunk.ChunkServer(4,8083)
  client.StartClient()
  // // listening to client on port 8000
  // listenToClient(1, "8000", metaData)
  // client.StartClient()
}