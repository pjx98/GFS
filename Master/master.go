package main

import (
  "encoding/json"
  "fmt"
  client "gfs.com/master/client"
  helper "gfs.com/master/helper"
  structs "gfs.com/master/structs"
  "log"
  "net"
  "strconv"
  "strings"
)

type MetaData struct {

  // key: file id int, value: chunk array
  // eg file 1 = [file1_chunk1, file1_chunk2, file1_chunk3]
  file_id_to_chunkId map[string][]string

  // map each file chunk to a chunk server (port number)
  chunkId_to_chunkserver map[string][]int

  // map each chunkserver to the amount of storage it has
  // max chunk is 10 KB
  // max chunk sent is 2.5KB
  chunkserver_to_storage map[int]float32


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

    if _, ok := metaData.file_id_to_chunkId[message.Filename]; ok {
      // if file does not exist in metaData, create a new entry and named it fn.c0
      if ok == false {
        metaData.file_id_to_chunkId[message.Filename] = []string{message.Filename + "_c0"}
      } else {
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

func main() {

  var metaData MetaData
  metaData.file_id_to_chunkId = make(map[string][]string)
  metaData.chunkId_to_chunkserver = make(map[string][]int)
  metaData.chunkserver_to_storage = make(map[int]float32)
  metaData.chunkserver_to_client = make(map[int]int)


  // listening to client on port 8000
  listenToClient(1, "8000", metaData)
  client.StartClient()

}