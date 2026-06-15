package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"strconv"
	"time"
)

func main() {
	// Start the TCP server on port 6379
	listener, err := net.Listen("tcp", ":6379")
	myStore  := make(map[string]string)
	expStore := make(map[string]time.Time)
	hashStore := make(map[string]map[string]string)
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
		go handleConnection(conn, myStore, expStore, hashStore)
	}
}

func handleConnection(conn net.Conn, myStore map[string]string, expStore map[string]time.Time, hashStore map[string]map[string]string) {
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

		switch {
			case strings.ToUpper(reqArr[2]) == "PING":
				conn.Write([]byte("*1\r\n$4\r\nPONG\r\n"))

			case strings.ToUpper(reqArr[2]) == "ECHO":
				fmt.Println(reqArr)
				conn.Write([]byte("+" + reqArr[4] + "\r\n"))
			
			case strings.ToUpper(reqArr[2]) == "FLUSHALL":
				myStore = make(map[string]string)
				conn.Write([]byte("+OK\r\n"))
				fmt.Println(myStore)
			
			case strings.ToUpper(reqArr[2]) == "INCRBY":
				key := reqArr[4]
				incrStr := reqArr[6]
				incrVal, err := strconv.Atoi(incrStr)
				if err != nil {
					conn.Write([]byte("-ERR increment value is not an integer\r\n"))
					break
				}
				val, ok := myStore[key]
				if !ok {
					myStore[key] = strconv.Itoa(incrVal)
					conn.Write([]byte(":" + strconv.Itoa(incrVal) + "\r\n"))
				} else {
					currVal, err := strconv.Atoi(val)
					if err != nil {
						conn.Write([]byte("-ERR value is not an integer\r\n"))
					} else {
						currVal += incrVal
						myStore[key] = strconv.Itoa(currVal)
						conn.Write([]byte(":" + strconv.Itoa(currVal) + "\r\n"))
					}
				}
				fmt.Println(myStore)

			case strings.ToUpper(reqArr[2]) == "INCR":
				key := reqArr[4]
				val, ok := myStore[key]
				if !ok {
					myStore[key] = "1"
					conn.Write([]byte(":1\r\n"))
				} else {
					value, err := strconv.Atoi(val)
					if err != nil {
						conn.Write([]byte("value is not an integer\r\n"))
					} else {
						value++
						myStore[key] = strconv.Itoa(value)
						conn.Write([]byte(":" + strconv.Itoa(value) + "\r\n"))
					}
				}
				fmt.Println(myStore)
			
			case strings.ToUpper(reqArr[2]) ==  "DECRBY":
				key := reqArr[4]
				decrStr := reqArr[6]
				decrVal, err := strconv.Atoi(decrStr)
				if err != nil{
					conn.Write([]byte("-ERR decrement value is not an integer\r\n"))
					break
				}
				val, ok := myStore[key]
				if !ok {
					myStore[key] = strconv.Itoa(decrVal)
					conn.Write([]byte(":" + strconv.Itoa(decrVal) + "\r\n"))
				} else {
					currVal, err := strconv.Atoi(val)
					if err != nil {
						conn.Write([]byte("-ERR value is not an integer\r\n"))
					} else {
						currVal -= decrVal
						myStore[key] = strconv.Itoa(currVal)
						conn.Write([]byte(":" + strconv.Itoa(currVal) + "\r\n"))
					}
				}
				fmt.Println(myStore)

			case strings.ToUpper(reqArr[2]) ==  "DECR":
				key := reqArr[4]
				val, ok := myStore[key]
				if !ok {
					myStore[key] = "-1"
					conn.Write([]byte(":-1\r\n"))
				} else {
					value, err := strconv.Atoi(val)
					if err != nil {
						conn.Write([]byte("value is not an integer\r\n"))
					} else {
						value--
						myStore[key] = strconv.Itoa(value)
						conn.Write([]byte(":" + strconv.Itoa(value) + "\r\n"))
					}
				}
				fmt.Println(myStore)

			case strings.ToUpper(reqArr[2]) ==  "MSETNX":		
				exists := false
				for i := 4; i < len(reqArr); i += 4 {
					key := reqArr[i]
					if _, ok := myStore[key]; ok {
						exists = true
						break
					}
				}
				if exists{
					conn.Write([]byte(":0\r\n"))
				}else{
					for i:=4;i<len(reqArr);i+=4{					
						myStore[reqArr[i]] = reqArr[i+2]
					}
					conn.Write([]byte(":1\r\n"))
				}
				fmt.Println(reqArr)
				fmt.Println(myStore)

			case strings.ToUpper(reqArr[2]) == "SETNX":
				keyPresent := false
				for key := range myStore{
					if key == reqArr[4]{
						keyPresent = true
						conn.Write([]byte("+0\r\n"))
					}
				}
				if(keyPresent == false){
					myStore[reqArr[4]] = reqArr[6]
					conn.Write([]byte("+1\r\n"))
				}
				fmt.Println(myStore)	
				
			case strings.ToUpper(reqArr[2]) ==  "MSET":				
				for i:=4;i<len(reqArr);i+=4{					
					myStore[reqArr[i]] = reqArr[i+2]
				}
				fmt.Println(reqArr)
				fmt.Println(myStore)
				conn.Write([]byte("+OK\r\n"))

			case strings.ToUpper(reqArr[2]) == "SET":
				myStore[reqArr[4]] = reqArr[6]
				fmt.Println(myStore)
				conn.Write([]byte("+1\r\n"))
			
			case strings.ToUpper(reqArr[2]) == "APPEND":
				key := reqArr[4]
				value := reqArr[6]
				val, ok := myStore[key]
				if !ok {
					myStore[key] = value
					newLen := len(value)
					conn.Write([]byte(":" + strconv.Itoa(newLen) + "\r\n"))
				} else {
					_, err := strconv.Atoi(val)
					if err != nil {
						myStore[key] = val + value
						newLen := len(myStore[key])
						conn.Write([]byte(":" + strconv.Itoa(newLen) + "\r\n"))
					} else {
						conn.Write([]byte("ERR not string type\r\n"))
					}
				}


			case strings.ToUpper(reqArr[2]) ==  "MGET":
				keys := []string{}
				for i := 4; i < len(reqArr); i+=2 {
					keys = append(keys, reqArr[i])
				}
				conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(keys))))
				for _, key := range keys {
					if val, ok := myStore[key]; ok {
						conn.Write([]byte("$" + strconv.Itoa(len(val)) + "\r\n" + val + "\r\n"))
					} else {
						conn.Write([]byte("$-1\r\n")) 
					}
				}
				fmt.Println(reqArr)
				fmt.Println(myStore)

			case strings.ToUpper(reqArr[2]) == "GET":
				value, ok := myStore[reqArr[4]]
				if ok {
					conn.Write([]byte("+" + value + "\r\n"))
				} else {
					conn.Write([]byte("-1\r\n"))
				}
				fmt.Println(myStore)
			
			case strings.ToUpper(reqArr[2]) ==  "DEL":
				keysDel := 0
				for i:=4;i<len(reqArr);i=i+2{
					if i % 2 == 0{ 
						if _, ok := myStore[reqArr[i]]; ok {
							delete(myStore,reqArr[i])
							keysDel++
						}
					}
				}
				conn.Write([]byte("+" +  strconv.Itoa(keysDel)  + "\r\n"))
				fmt.Println(myStore)

			case strings.ToUpper(reqArr[2]) == "EXISTS":
				keyExists := 0
				for i:=4;i<len(reqArr);i=i+2{
					if i % 2 == 0{ 
						if _, ok := myStore[reqArr[i]]; ok {
							keyExists++
						}
					}
				}
				conn.Write([]byte("+" +  strconv.Itoa(keyExists)  + "\r\n"))
				fmt.Println(myStore)
			
			case strings.ToUpper(reqArr[2]) == "KEYS":
				if _, ok := myStore[reqArr[4]]; ok {
					conn.Write([]byte("+1\r\n"))
				}else{
					conn.Write([]byte("+0\r\n"))							
				}
				fmt.Println(myStore)
			
			case strings.ToUpper(reqArr[2]) == "STRLEN":
				fmt.Println(myStore)
				conn.Write([]byte(":" + strconv.Itoa(len(myStore[reqArr[4]])) + "\r\n"))

			case strings.ToUpper(reqArr[2]) == "EXPIRE":
				key := reqArr[4]
				seconds, err := strconv.Atoi(reqArr[6])
				if err != nil {
					conn.Write([]byte("-ERR value is not an integer\r\n"))
					break
				}
				if _, ok := myStore[key]; !ok {
					conn.Write([]byte(":0\r\n"))
				} else {
					expStore[key] = time.Now().Add(time.Duration(seconds) * time.Second)
					conn.Write([]byte(":1\r\n"))
				}
				fmt.Println(myStore)

			case strings.ToUpper(reqArr[2]) == "TTL":
				key := reqArr[4]
				_, ok := myStore[key]
				if !ok {
					conn.Write([]byte(":-2\r\n"))
					break
				}
				exp, ok := expStore[key]
				if !ok {
					conn.Write([]byte(":-1\r\n"))
					break
				}
				remaining := int(time.Until(exp).Seconds())
    			conn.Write([]byte(":" + strconv.Itoa(remaining) + "\r\n"))

			case strings.ToUpper(reqArr[2]) == "PERSIST":
				key := reqArr[4]
				_, ok := myStore[key]
				if !ok {
					conn.Write([]byte(":0\r\n"))
					break
				}
				_, hasExpiry := expStore[key]
				if !hasExpiry {
					conn.Write([]byte(":0\r\n"))
					break
				}
				delete(expStore, key)
				conn.Write([]byte(":1\r\n"))
			
			case strings.ToUpper(reqArr[2]) == "HSET":
				key := reqArr[4]
				if _, ok := hashStore[key]; !ok {
					hashStore[key] = make(map[string]string)
				}	
				_, exists := hashStore[key][reqArr[6]]
				if !exists {
					hashStore[key][reqArr[6]] = reqArr[8]
					conn.Write([]byte(":1\r\n"))
				}else{
					hashStore[key][reqArr[6]] = reqArr[8]
					conn.Write([]byte(":0\r\n"))
				}	
				fmt.Println(reqArr)
				fmt.Println(hashStore)
				
			case strings.ToUpper(reqArr[2]) == "HMSET":
				key := reqArr[4]
				addF := 0
				if _, ok := hashStore[key]; !ok {
					hashStore[key] = make(map[string]string)
				}
				for i:=6;i<len(reqArr);i+=4{	
					_, exists := hashStore[key][reqArr[i]]
					if !exists {
						addF++
					}				
					hashStore[key][reqArr[i]] = reqArr[i+2]
				}
				fmt.Println(reqArr)
				fmt.Println(hashStore)
				conn.Write([]byte(":" + strconv.Itoa(addF) + "\r\n"))
				
			case strings.ToUpper(reqArr[2]) == "HSETNX":
				key := reqArr[4]
				if _, ok := hashStore[key]; !ok {
					hashStore[key] = make(map[string]string)
				}
				_, exists := hashStore[key][reqArr[6]]
				if !exists {
					hashStore[key][reqArr[6]] = reqArr[8]
					conn.Write([]byte(":1\r\n"))
				}else{
					conn.Write([]byte(":0\r\n"))
				}
				fmt.Println(reqArr)
				fmt.Println(hashStore)
			
			case strings.ToUpper(reqArr[2]) == "HGET":
				if len(reqArr) < 7{
					conn.Write([]byte("-ERR no fields given for HGET\r\n"))
        			break
				}
				key := reqArr[4]
				innerMap, ok := hashStore[key]
				if !ok {
					conn.Write([]byte("$-1\r\n"))
					fmt.Println(reqArr)
					break
				}
				val, exists := innerMap[reqArr[6]]
				if !exists {
					conn.Write([]byte("$-1\r\n"))
					fmt.Println(reqArr)
					break
				}		
				conn.Write([]byte("$" + strconv.Itoa(len(val)) + "\r\n" + val + "\r\n"))
				fmt.Println(reqArr)
				fmt.Println(hashStore)

			case strings.ToUpper(reqArr[2]) == "HMGET":
				if len(reqArr) < 7{
					conn.Write([]byte("-ERR no fields given for HMGET\r\n"))
        			break
				}
				key := reqArr[4]
				innerMap, ok := hashStore[key]
				fields := []string{}
				for i := 6; i < len(reqArr); i+=2 {
					fields = append(fields, reqArr[i])
				}
				conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(fields))))
				for i:=0;i<len(fields);i++{
					if !ok {
						conn.Write([]byte("$-1\r\n"))
						continue
					}
					val, exists := innerMap[fields[i]]
					if !exists {
						conn.Write([]byte("$-1\r\n"))
					}	else{	
						conn.Write([]byte("$" + strconv.Itoa(len(val)) + "\r\n" + val + "\r\n"))
					}
				}
				fmt.Println(reqArr)
				fmt.Println(hashStore)

			case strings.ToUpper(reqArr[2]) == "HGETALL":
				if len(reqArr) < 5{
					conn.Write([]byte("-ERR no minimmum arguments given for HGETALL\r\n"))
        			break
				}
				key := reqArr[4]
				innerMap, ok := hashStore[key]
				if !ok {
					conn.Write([]byte("$-1\r\n"))
					fmt.Println(reqArr)
					break
				}
				conn.Write([]byte(fmt.Sprintf("*%d\r\n", 2*len(innerMap))))
				for field,val := range innerMap{
					conn.Write([]byte("$" + strconv.Itoa(len(field)) + "\r\n" + field + "\r\n"))
        			conn.Write([]byte("$" + strconv.Itoa(len(val)) + "\r\n" + val + "\r\n"))
				}
				fmt.Println(innerMap)
				fmt.Println(hashStore)
			
			case strings.ToUpper(reqArr[2]) == "HDEL":
				key := reqArr[4]
				innerMap, ok := hashStore[key]
				if !ok {
					conn.Write([]byte(":0\r\n")) 
					break
				}
				rmF := 0
				for i:=6;i<len(reqArr);i+=2{	
					field := reqArr[i]
					_, exists := innerMap[field]
					if exists {
						rmF++
						delete(innerMap,field)
					}
				}
				fmt.Println(reqArr)
				fmt.Println(hashStore)
				conn.Write([]byte(":" + strconv.Itoa(rmF) + "\r\n"))

			case strings.ToUpper(reqArr[2]) == "HKEYS":
				if len(reqArr) < 5{
					conn.Write([]byte("-ERR no minimmum arguments given for HKEYS\r\n"))
        			break
				}
				key := reqArr[4]
				innerMap, ok := hashStore[key]
				if !ok {
					conn.Write([]byte("*0\r\n"))
					break
				}
				conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(innerMap))))
				for field:= range innerMap{
					conn.Write([]byte("$" + strconv.Itoa(len(field)) + "\r\n" + field + "\r\n"))
				}
				fmt.Println(innerMap)
				fmt.Println(hashStore)

			case strings.ToUpper(reqArr[2]) == "HVALS":
				if len(reqArr) < 5{
					conn.Write([]byte("-ERR number of arguments given for HVALS\r\n"))
        			break
				}
				key := reqArr[4]
				innerMap, ok := hashStore[key]
				if !ok {
					conn.Write([]byte("*0\r\n"))
					break
				}
				conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(innerMap))))
				for _,val:= range innerMap{
					conn.Write([]byte("$" + strconv.Itoa(len(val)) + "\r\n" + val + "\r\n"))
				}
				fmt.Println(innerMap)
				fmt.Println(hashStore)
			
			case strings.ToUpper(reqArr[2]) == "HLEN":
				if len(reqArr) < 5{
					conn.Write([]byte("-ERR no minimmum arguments given for HLEN\r\n"))
        			break
				}
				key := reqArr[4]
				innerMap, ok := hashStore[key]
				if !ok {
					conn.Write([]byte(":0\r\n"))
				}else{
					conn.Write([]byte(":" + strconv.Itoa(len(innerMap)) + "\r\n"))
				}
				fmt.Println(innerMap)
				fmt.Println(hashStore)

			default:
				conn.Write([]byte("-ERR unknown command\r\n"))
		}

		
	}
}
