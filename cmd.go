package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"
)

func main() {
	// Start the TCP server on port 6379
	listener, err := net.Listen("tcp", ":6379")
	myStore := make(map[string]string, 10)
	if err != nil {
		log.Fatalf("Failed to bind to port 6379: %v\n", err)
	}
	defer listener.Close()

	fmt.Println("Redis server listening on port 6379...")

	for {
		// Accept incoming connections
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v\n", err)
			continue
		}

		// Handle the connection concurrently
		go handleConnection(conn, myStore)
	}
}

func handleConnection(conn net.Conn, myStore map[string]string) {
	defer conn.Close()

	buf := make([]byte, 512)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Printf("Read error: %v\n", err)
			}
			break
		}

		request := string(buf[:n])
		reqArr := strings.Split(request, "\r\n")

		// Simple RESP parser for PING
		if strings.Contains(strings.ToUpper(request), "PING") {
			// RESP simple string response for PONG
			conn.Write([]byte("*1\r\n$4\r\nPONG\r\n"))
		} else if strings.Contains(strings.ToUpper(request), "SET") {
			myStore[reqArr[4]] = reqArr[6]
			conn.Write([]byte("+1\r\n"))
		} else if strings.Contains(strings.ToUpper(request), "GET") {
			value, ok := myStore[reqArr[4]]
			if ok == true {
				conn.Write([]byte("+" + value + "\r\n"))
			} else {
				conn.Write([]byte("-" + "-1" + "\r\n"))
			}
		} else {
			// Basic fallback
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}
