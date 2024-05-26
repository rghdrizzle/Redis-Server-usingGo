package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
  "reflect"
  "strconv"
)

type Item struct {
	Value  string
	Expiry time.Time
}
type Entry struct{
  Stream_key string
  Entry_id string
  Entries  map[string]string
}
type ReplicaConfig struct {
	MasterPort     string
	MasterHostname string
	SlavePort      string
	SlaveAddr      string
  MasterConn     net.Conn
}
type RedisConfig struct {
	Role          string
	Master_rep_ID string
	Master_Offset int
	ReplicaCon    ReplicaConfig
}

var config RedisConfig
var m = make(map[string]Item)
var entries []Entry 

const emptyRDBcontent = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2" // hex
var replica []net.Conn = []net.Conn{}

func main() {

	fmt.Println("Logs from your program will appear here!")
	var portNumber string
	var argArray []string
	argArray = os.Args[1:]
	portNumber = "6379"
	config.Role = "master"
	config.Master_Offset = 0
	config.Master_rep_ID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	if len(argArray) >= 2 {
		if argArray[0] == "--port" {
			portNumber = argArray[1]
		}
		if len(argArray) > 2 {
			if argArray[2] == "--replicaof" {
				config.Role = "slave"
				config.ReplicaCon.MasterPort = argArray[4]
				config.ReplicaCon.MasterHostname = argArray[3]
				config.ReplicaCon.SlavePort = portNumber

			}
		}

	}
	l, err := net.Listen("tcp", "0.0.0.0:"+portNumber)
	if err != nil {
		fmt.Println("Failed to bind to port " + portNumber)
		os.Exit(1)
	}
	if config.Role == "slave" {
		handleHandShaketoMaster()
    fmt.Println("Handshake successfull")
    //go handlePropagatedCommands(config.ReplicaCon.MasterConn)

	}
	for { // infinite loop which accepts multiple connections
		conn, err := l.Accept() // connect to the server using redis-cli client
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

    
	go handleConn(conn) // a go routine which is responsible to handle for a particular connection
	}

}

func handleConn(conn net.Conn) {
	defer conn.Close() // Close the connection once the functions ends
	for {              // infinite loop to send multiple requests to the server from the same client or connection.
		buf := make([]byte, 1024)
		_, err := conn.Read(buf)
		if err != nil {
			conn.Write([]byte("-ERROR\r\n"))
			return
		}
		if err == io.EOF { // if client didnt send anything then return.
			return
		}
		cmd, args, maps := parseCommand(string(buf)) // the input given here has to be in the form of RESP which is a protocol used by Redis
		res := processCommands(cmd, args, maps, config)
		fmt.Println(res)
		fmt.Println(maps)
		_, err = conn.Write([]byte(res))
		if cmd == "PSYNC" {
			rdbcontent, _ := hex.DecodeString(emptyRDBcontent)
			fmt.Println(emptyRDBcontent)
			fmt.Println(rdbcontent)
			_, err = conn.Write([]byte(fmt.Sprintf("$%d\r\n%s", len(rdbcontent), rdbcontent)))
			if err != nil {
				panic(err)
			}
			replica = append(replica, conn)

		}

	}
}
func parseCommand(req string) (string, []string, map[string]Item) {
  tokens := strings.Split(req, "\r\n")
	tokens = tokens[1:]
	cmd := tokens[1]
	fmt.Println(cmd)
	tokens = tokens[2:]
	var args []string
	for _, token := range tokens {
		if !strings.HasPrefix(token, "$") && !strings.HasPrefix(token, "*")|| token=="*"{
			args = append(args, token)
		}
	}
  fmt.Println(args)
	return cmd, args, m
}
func handleHandShaketoMaster() {
	addr := config.ReplicaCon.MasterHostname + ":" + config.ReplicaCon.MasterPort
    
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		panic(err.Error())
	} 
//defer conn.Close()
	sendCommand("*1\r\n$4\r\nping\r\n", conn)
	sendCommand("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n", conn)
	sendCommand("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n", conn)
	sendCommand("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n", conn)
	//replica = conn
  go handlePropagatedCommands(conn)
  config.ReplicaCon.MasterConn = conn

}
func handlePropagatedCommands(conn net.Conn){
  defer conn.Close()
  for {
		buf := make([]byte, 1024)
		_, err := conn.Read(buf)
    fmt.Println(strings.Split(string(buf),"*"))
    fmt.Println("test1"+string(buf))
		if err != nil {
			conn.Write([]byte("-ERROR\r\n"))
			return
		}
		if err == io.EOF { // if client didnt send anything then return.
			return
		}
    fmt.Println(string(buf))
		cmd, args, maps := parseCommand(string(buf)) // the input given here has to be in the form of RESP which is a protocol used by Redis
		res := processCommands(cmd, args, maps, config)
    fmt.Println(res)
}
}
func sendCommand(req string, conn net.Conn) {
	 _, err := conn.Write([]byte(req))
	if err != nil {
		panic(err)
	}
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading from connection: ", err.Error())
		os.Exit(1)
	}
	fmt.Println(n)
}
func processCommands(cmd string, arg []string, m map[string]Item, config RedisConfig) string {
	cmd = strings.ToLower(cmd)
  if config.Role == "slave"{
    fmt.Println("Slave is running now ")
    fmt.Println(cmd)

  }
	if cmd == "ping" {
		return "+PONG\r\n"
	} else if cmd == "echo" {
		return "+" + arg[0] + "\r\n"
	} else if cmd == "set" {
		if strings.ToLower(arg[2]) != "px" {
			m[arg[0]] = Item{Value: arg[1]}
			sendToSlave(cmd, arg[0], arg[1])
			return "+OK\r\n"
		}
		if strings.ToLower(arg[2]) == "px" {
			duration, _ := time.ParseDuration(arg[3] + "ms")
			m[arg[0]] = Item{Value: arg[1], Expiry: time.Now().Add(duration)}
			return "+OK\r\n"
		}
	} else if cmd == "get" {
		if val, ok := m[arg[0]]; ok {
			if val.Expiry.IsZero() || val.Expiry.After(time.Now()) {
				return "$" + fmt.Sprint(len(val.Value)) + "\r\n" + val.Value + "\r\n"
			}
			delete(m, arg[0])
		}
	} else if strings.ToUpper(cmd) == "INFO" {
		if arg[0] == "replication" {
			role := fmt.Sprintf("\r\nrole:%s", config.Role)
			mas_rep_ID := "\r\n" + "master_replid:" + config.Master_rep_ID
			mas_offset := "\r\n" + "master_repl_offset:" + fmt.Sprint(config.Master_Offset)
			res := role + mas_rep_ID + mas_offset
			return "$" + fmt.Sprint(len(res)) + "\r\n" + res + "\r\n"
		}
	} else if cmd == "replconf" {
		if arg[0] == "listening-port" {
			// check if the resp is valid
			return "+OK\r\n"
		}
		if arg[0] == "capa" {
			//check if the resp is valid
			return "+OK\r\n"
		}
	} else if cmd == "psync" {
		msg := "+FULLRESYNC" + " " + config.Master_rep_ID + " " + fmt.Sprint(config.Master_Offset) + "\r\n"
    	return msg
	} else if cmd == "type" {
    val, ok := m[arg[0]]
    if ok{
      typeOfVariable := reflect.TypeOf(val.Value)
	  res := fmt.Sprintf("+%s\r\n",typeOfVariable)
      return res
    }
   for i:=0;i<len(entries);i++{
    checkExist := entries[i].Stream_key
    if arg[0]==checkExist {
     res := fmt.Sprintf("+stream\r\n")
     return res
     }
   }
    
      return "+none\r\n"

    } else if cmd == "xadd"{
        entriesMap := make(map[string]string)
        fmt.Println("CHECKKK::"+arg[0]+" "+arg[1]+" "+arg[2])
        if arg[1]=="*"{
          arg[1]= fmt.Sprintf("%d-%d",time.Now().UnixMilli(),0)
        }
        checkCurrentId := strings.Split(arg[1],"-")
        currentTime := checkCurrentId[0]
        currentSeqNo := checkCurrentId[1]
        if currentTime == "0" && currentSeqNo=="*"{
            arg[1]="0-1"
          }
         if currentSeqNo =="*"{
            if len(entries)==0{
              arg[1]=currentTime+"-1"
            }
          }
        if len(entries)!=0{
          idCheck := entries[len(entries)-1].Entry_id
          checkString:= strings.Split(idCheck,"-")
          previousTime := checkString[0]
          previousSeqNo := checkString[1]
         // checkCurrentId := strings.Split(arg[1],"-")
         // currentTime := checkCurrentId[0]
         // currentSeqNo := checkCurrentId[1]
          if arg[1]=="0-0"{
          return "-ERR The ID specified in XADD must be greater than 0-0\r\n"
          }
         // if currentTime == "0" && currentSeqNo=="*"{
         //   arg[1]="0-1"
        //  }
          if currentSeqNo =="*"{
            if idCheck =="0-1"{
            newSeq := fmt.Sprintf("%d",0)
            arg[1]=currentTime+"-"+newSeq

            }else{
            i,_ := strconv.Atoi(previousSeqNo)
            newSeq := fmt.Sprintf("%d",i+1)
            arg[1]=currentTime+"-"+newSeq
            checkCurrentId = strings.Split(arg[1],"-")
            currentSeqNo = checkCurrentId[1]
          } 
        }

          if currentTime<previousTime || currentSeqNo<=previousSeqNo && idCheck !="0-1"{
            fmt.Println("CHECK:::"+previousTime+" CHECKKK:::"+previousSeqNo)
            return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
          }

        }
        for i:=2;i<len(arg)-1;i+=2{
          key := arg[i]
          value:= arg[i+1]
          entriesMap[key]=value
          fmt.Println("key:"+key)
          fmt.Println("value:"+value)
        }
        new_entry := Entry{
          Stream_key: arg[0],
          Entry_id: arg[1],
          Entries: entriesMap,
        }

		entries = append(entries,new_entry)
    fmt.Println(entries)  
      return "$"+fmt.Sprint(len(arg[1]))+"\r\n"+arg[1]+"\r\n"
    }
	return "$-1\r\n"
}
func toRespArrays(arr []string) string {
	res := fmt.Sprintf("*%d\r\n", len(arr))
	for _, element := range arr {
		// add elements in RESP format
		res += fmt.Sprintf("$%d\r\n%s\r\n", len(element), element)
	}
	return res
}
func sendToSlave(cmd string, key string, value string) {
	if replica != nil && config.Role=="master"{
		for _, slave := range replica {
			_, err := slave.Write([]byte(toRespArrays([]string{cmd, key, value})))
      fmt.Println("Command sent: "+toRespArrays([]string{cmd, key, value}))
      if err != nil {
				panic(err)
      }   
		}
	}
 }
