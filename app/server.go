package main

import (
	"fmt"
	"net"
	"os"
  "io"
  "strings"
  "time"
)

type Item struct {
	Value  string
	Expiry time.Time
}
type RedisConfig struct{
  Role string
  Master_rep_ID string
  Master_Offset int
}
var config RedisConfig
var m = make(map[string]Item)

func main() {
	
	fmt.Println("Logs from your program will appear here!")
   var portNumber string 
   var argArray []string
   argArray = os.Args[1:]
   portNumber = "6379"
   config.Role = "master"
   config.Master_Offset=0
   config.Master_rep_ID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
   if len(argArray)>=2{
     if argArray[0]=="--port"{
       portNumber = argArray[1]
     }
     if len(argArray)>2 {
       if argArray[2]=="--replicaof" {
          config.Role = "slave"
       }
     }

   }
   //if len(argArray)>2{
     //if argArray[2]=="--replicaof"{
       //config.Role ="slave"
     //}
   //}
	 l, err := net.Listen("tcp", "0.0.0.0:"+portNumber)
	 if err != nil {
	 	fmt.Println("Failed to bind to port "+portNumber)
	 	os.Exit(1)
	}
  for{ // infinite loop which accepts multiple connections 
  conn, err := l.Accept()
	 if err != nil {
	 	fmt.Println("Error accepting connection: ", err.Error())
	 	os.Exit(1)
	}
  go handleConn(conn)// a go routine which is responsible to handle for a particular connection
}
}

func handleConn(conn net.Conn){
  defer conn.Close()// Close the connection once the functions ends 
  for{ // infinite loop to send multiple requests to the server from the same client or connection.
    buf:=make([]byte,1024)
    _,err:= conn.Read(buf)
    if err!=nil{
      conn.Write([]byte("-ERROR\r\n"))
      return
    }
    if err==io.EOF{// if client didnt send anything then return.
      return
    }
    cmd,args,maps := parseCommand(string(buf))// the input given here has to be in the form of RESP which is a protocol used by Redis
    res := processCommands(cmd,args,maps,config)
    fmt.Println(res)
    fmt.Println(maps)
    _, err = conn.Write([]byte(res))
  }
}
func parseCommand(req string)(string,[]string,map[string]Item){
  tokens:= strings.Split(req,"\r\n")
  tokens = tokens[1:]
  cmd := tokens[1]
  fmt.Println(cmd)
  tokens = tokens[2:]
  var args []string
  for _, token := range tokens{
    if !strings.HasPrefix(token,"$"){
      args = append(args,token)
    }
  }
  return cmd,args,m
}

func processCommands(cmd string, arg []string,m map[string]Item,config RedisConfig) string{
  cmd = strings.ToLower(cmd)
    if cmd == "ping" {
        return "+PONG\r\n"
    } else if cmd == "echo" {
        return "+" + arg[0] + "\r\n"
    } else if cmd == "set" {
      if strings.ToLower(arg[2])!="px" {
        m[arg[0]] = Item{Value: arg[1]}
        return "+OK\r\n"
      } 
      if strings.ToLower(arg[2])== "px" {
        duration,_ := time.ParseDuration(arg[3]+"ms")
        m[arg[0]]= Item{Value: arg[1],Expiry: time.Now().Add(duration)}
        return "+OK\r\n"
      } 
    } else if cmd == "get" {
      if val,ok:=m[arg[0]];ok {
        if val.Expiry.IsZero() || val.Expiry.After(time.Now()){
          return "$"+fmt.Sprint(len(val.Value))+"\r\n"+ val.Value+"\r\n"
        }
        delete(m,arg[0])
      }
    } else if strings.ToUpper(cmd) == "INFO" {
        if arg[0]=="replication" {
          role := fmt.Sprintf("\r\nrole:%s", config.Role)
          mas_rep_ID := "\r\n"+"master_replid:"+config.Master_rep_ID
          mas_offset := "\r\n"+"master_repl_offset:"+fmt.Sprint(config.Master_Offset)
          res :=  role+mas_rep_ID+mas_offset
          return "$"+fmt.Sprint(len(res))+"\r\n"+res+"\r\n"
        }
    }
    return "$-1\r\n"
}
