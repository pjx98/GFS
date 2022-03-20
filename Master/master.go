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

)

type MetaData struct {

  // key: file id int, value: chunk array
  // eg file 1 = [file1_chunk1, file1_chunk2, file1_chunk3]
  file_id_to_chunkId map[string][]string

  // map each file chunk to a chunk server (port number)
  chunkId_to_chunkserver map[string][]int

  // map each chunkserver to the amount of storage it has
  // max chunk is 10 KB
  // max append data is 2.5KB
  // {f1 : {c0 : 0KB, c1 : 2KB} }
  file_id_to_chunkId_offset map[string]map[string]float32


}

var metaData MetaData


func listen(){
  router := gin.Default()
  router.POST("/client/append", listenClientAppend)

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
  fmt.Printf("Master connected to Client\n")
  last_chunk := ""

  if _, ok := metaData.file_id_to_chunkId[message.Filename]; ok {
    // if file does not exist in metaData, create a new entry
    if ok == false {
      metaData.file_id_to_chunkId[message.Filename] = []string{message.Filename + "_c0"}
      last_chunk = message.Filename + "_c0"
    } else {
      // if file exist, take the last chunk of the file from the metadata
      array := metaData.file_id_to_chunkId[message.Filename]
      last_chunk = metaData.file_id_to_chunkId[message.Filename][len(array)-1]
    }
  }

  dest_chunkserver := []int{8081, 8082, 8083}
  return_message := structs.CreateMessage(helper.DATA_APPEND, 8086, dest_chunkserver[0], dest_chunkserver[1:], message.Filename, last_chunk, "DATA", message.PayloadSize, 0, 8080, dest_chunkserver)
  return return_message
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
  //metaData.chunkserver_to_storage = make(map[int]float32)
  //metaData.chunkserver_to_client = make(map[int]int)


  go listen()
  chunk.ChunkServer(2,8081)
  chunk.ChunkServer(3,8082)
  chunk.ChunkServer(4,8083)
  client.StartClient()
  // // listening to client on port 8000
  // listenToClient(1, "8000", metaData)
  // client.StartClient()
}