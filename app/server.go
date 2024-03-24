package main

import (
	"fmt"
	"net"
	"os"
  "io"
)

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
    cmd:=make([]byte,1024)
    _,err:= conn.Read(cmd)
    if err!=nil{
      conn.Write([]byte("+ERROR\r\n"))
      return
    }
    if err==io.EOF{// if client didnt send anything then return.
      return
    }
    conn.Write([]byte("+PONG\r\n"))
  }
}
