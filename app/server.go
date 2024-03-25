package main

import (
	"fmt"
	"net"
	"os"
  "io"
  "strings"
)

var m =make(map[string]string)

func main() {
	
	fmt.Println("Logs from your program will appear here!")
	 l, err := net.Listen("tcp", "0.0.0.0:6379")
	 if err != nil {
	 	fmt.Println("Failed to bind to port 6379")
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
    res := processCommands(cmd,args,maps)
    fmt.Println(res)
    fmt.Println(maps)
    _, err = conn.Write([]byte(res))
  }
}
func parseCommand(req string)(string,[]string,map[string]string){
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

func processCommands(cmd string, arg []string,m map[string]string) string{
  cmd = strings.ToLower(cmd)
    if cmd == "ping" {
        return "+PONG\r\n"
    } else if cmd == "echo" {
        return "+" + arg[0] + "\r\n"
    } else if cmd == "set" {
        m[arg[0]]= arg[1]
        return "+OK\r\n"
    } else if cmd == "get" {
        return "+"+m[arg[0]]+"\r\n"
    }
    return ""
}
