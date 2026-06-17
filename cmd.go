package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Store struct {
	mu        sync.RWMutex
	data      map[string]string
	expiry    map[string]time.Time
	hashes 	  map[string]map[string]string
	hashesExp map[string]time.Time
}

func main() {
	// Start the TCP server on port 6379
	listener, err := net.Listen("tcp", ":6379")
	store := &Store{
		data:      make(map[string]string),
		expiry:    make(map[string]time.Time),
		hashes:    make(map[string]map[string]string),
		hashesExp: make(map[string]time.Time),
	}
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
		go handleConnection(conn, store)
	}
}

func checkExp(conn net.Conn, key string, store *Store) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	if time.Now().Unix() > store.expiry[key].Unix() {
		// Need to write, so upgrade to write lock
		store.mu.RUnlock()
		store.mu.Lock()
		delete(store.data, key)
		delete(store.expiry, key)
		store.mu.Unlock()
		store.mu.RLock()
		conn.Write([]byte("$-1\r\n"))
	} else {
		conn.Write([]byte("+" + store.data[key] + "\r\n"))
	}
}

func checkHashesExp(conn net.Conn, key string, store *Store, field string) {
	store.mu.RLock()
	if time.Now().Unix() > store.hashesExp[key].Unix() {
		store.mu.RUnlock()
		store.mu.Lock()
		delete(store.hashes, key)
		delete(store.hashesExp, key)
		store.mu.Unlock()
		conn.Write([]byte("$-1\r\n"))
	} else {
		val := store.hashes[key][field]
		store.mu.RUnlock()
		conn.Write([]byte("+" + val + "\r\n"))
	}
}

func handleConnection(conn net.Conn, store *Store) {
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
			conn.Write([]byte("+" + reqArr[4] + "\r\n"))

		case strings.ToUpper(reqArr[2]) == "FLUSHALL":
			store.mu.Lock()
			store.data = make(map[string]string)
			store.expiry = make(map[string]time.Time)
			store.hashes = make(map[string]map[string]string)
			store.mu.Unlock()
			conn.Write([]byte("+OK\r\n"))

		case strings.ToUpper(reqArr[2]) == "INCRBY":
			key := reqArr[4]
			incrStr := reqArr[6]
			incrVal, err := strconv.Atoi(incrStr)
			if err != nil {
				conn.Write([]byte("-ERR increment value is not an integer\r\n"))
				break
			}
			store.mu.Lock()
			val, ok := store.data[key]
			if !ok {
				store.data[key] = strconv.Itoa(incrVal)
				store.mu.Unlock()
				conn.Write([]byte(":" + strconv.Itoa(incrVal) + "\r\n"))
			} else {
				currVal, err := strconv.Atoi(val)
				if err != nil {
					store.mu.Unlock()
					conn.Write([]byte("-ERR value is not an integer\r\n"))
				} else {
					currVal += incrVal
					store.data[key] = strconv.Itoa(currVal)
					store.mu.Unlock()
					conn.Write([]byte(":" + strconv.Itoa(currVal) + "\r\n"))
				}
			}

		case strings.ToUpper(reqArr[2]) == "INCR":
			key := reqArr[4]
			store.mu.Lock()
			val, ok := store.data[key]
			if !ok {
				store.data[key] = "1"
				store.mu.Unlock()
				conn.Write([]byte(":1\r\n"))
			} else {
				value, err := strconv.Atoi(val)
				if err != nil {
					store.mu.Unlock()
					conn.Write([]byte("value is not an integer\r\n"))
				} else {
					value++
					store.data[key] = strconv.Itoa(value)
					store.mu.Unlock()
					conn.Write([]byte(":" + strconv.Itoa(value) + "\r\n"))
				}
			}

		case strings.ToUpper(reqArr[2]) == "DECRBY":
			key := reqArr[4]
			decrStr := reqArr[6]
			decrVal, err := strconv.Atoi(decrStr)
			if err != nil {
				conn.Write([]byte("-ERR decrement value is not an integer\r\n"))
				break
			}
			store.mu.Lock()
			val, ok := store.data[key]
			if !ok {
				store.data[key] = strconv.Itoa(decrVal)
				store.mu.Unlock()
				conn.Write([]byte(":" + strconv.Itoa(decrVal) + "\r\n"))
			} else {
				currVal, err := strconv.Atoi(val)
				if err != nil {
					store.mu.Unlock()
					conn.Write([]byte("-ERR value is not an integer\r\n"))
				} else {
					currVal -= decrVal
					store.data[key] = strconv.Itoa(currVal)
					store.mu.Unlock()
					conn.Write([]byte(":" + strconv.Itoa(currVal) + "\r\n"))
				}
			}

		case strings.ToUpper(reqArr[2]) == "DECR":
			key := reqArr[4]
			store.mu.Lock()
			val, ok := store.data[key]
			if !ok {
				store.data[key] = "-1"
				store.mu.Unlock()
				conn.Write([]byte(":-1\r\n"))
			} else {
				value, err := strconv.Atoi(val)
				if err != nil {
					store.mu.Unlock()
					conn.Write([]byte("value is not an integer\r\n"))
				} else {
					value--
					store.data[key] = strconv.Itoa(value)
					store.mu.Unlock()
					conn.Write([]byte(":" + strconv.Itoa(value) + "\r\n"))
				}
			}

		case strings.ToUpper(reqArr[2]) == "MSETNX":
			store.mu.Lock()
			exists := false
			for i := 4; i < len(reqArr); i += 4 {
				key := reqArr[i]
				if _, ok := store.data[key]; ok {
					exists = true
					break
				}
			}
			if exists {
				store.mu.Unlock()
				conn.Write([]byte(":0\r\n"))
			} else {
				for i := 4; i < len(reqArr); i += 4 {
					store.data[reqArr[i]] = reqArr[i+2]
				}
				store.mu.Unlock()
				conn.Write([]byte(":1\r\n"))
			}

		case strings.ToUpper(reqArr[2]) == "SETNX":
			store.mu.Lock()
			keyPresent := false
			for key := range store.data {
				if key == reqArr[4] {
					keyPresent = true
					store.mu.Unlock()
					conn.Write([]byte("+0\r\n"))
					break
				}
			}
			if keyPresent == false {
				store.data[reqArr[4]] = reqArr[6]
				store.mu.Unlock()
				conn.Write([]byte("+1\r\n"))
			}

		case strings.ToUpper(reqArr[2]) == "MSET":
			store.mu.Lock()
			for i := 4; i < len(reqArr); i += 4 {
				store.data[reqArr[i]] = reqArr[i+2]
			}
			store.mu.Unlock()
			conn.Write([]byte("+OK\r\n"))

		case strings.ToUpper(reqArr[2]) == "SET":
			store.mu.Lock()
			store.data[reqArr[4]] = reqArr[6]
			store.mu.Unlock()
			conn.Write([]byte("+1\r\n"))

		case strings.ToUpper(reqArr[2]) == "SETEX":
			key := reqArr[4]
			seconds, err := strconv.Atoi(reqArr[6])
			if err != nil {
				conn.Write([]byte("-ERR value is not an integer\r\n"))
				break
			}
			store.mu.Lock()
			store.data[key] = reqArr[8]
			store.expiry[key] = time.Now().Add(time.Duration(seconds) * time.Second)
			store.mu.Unlock()
			conn.Write([]byte("+OK\r\n"))

		case strings.ToUpper(reqArr[2]) == "APPEND":
			key := reqArr[4]
			value := reqArr[6]
			store.mu.Lock()
			val, ok := store.data[key]
			if !ok {
				store.data[key] = value
				newLen := len(value)
				store.mu.Unlock()
				conn.Write([]byte(":" + strconv.Itoa(newLen) + "\r\n"))
			} else {
				_, err := strconv.Atoi(val)
				if err != nil {
					store.data[key] = val + value
					newLen := len(store.data[key])
					store.mu.Unlock()
					conn.Write([]byte(":" + strconv.Itoa(newLen) + "\r\n"))
				} else {
					store.mu.Unlock()
					conn.Write([]byte("ERR not string type\r\n"))
				}
			}

		case strings.ToUpper(reqArr[2]) == "MGET":
			keys := []string{}
			for i := 4; i < len(reqArr); i += 2 {
				keys = append(keys, reqArr[i])
			}
			store.mu.RLock()
			conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(keys))))
			for _, key := range keys {
				_, okExp := store.expiry[key]
				val, exists := store.data[key]
				if okExp && exists {
					checkExp(conn,key,store)
				} else {
					if exists {
						conn.Write([]byte("+" + val + "\r\n"))
					} else {
						conn.Write([]byte("$-1\r\n"))
					}
				}
			}
			store.mu.RUnlock()

		case strings.ToUpper(reqArr[2]) == "GET":
			store.mu.RLock()
			_, okExp := store.expiry[reqArr[4]]
			val, exists := store.data[reqArr[4]]
			if okExp && exists {
				checkExp(conn,reqArr[4],store)
			} else {
				if exists {
					store.mu.RUnlock()
					conn.Write([]byte("+" + val + "\r\n"))
				} else {
					store.mu.RUnlock()
					conn.Write([]byte("$-1\r\n"))
				}
			}

		case strings.ToUpper(reqArr[2]) == "DEL":
			keysDel := 0
			store.mu.Lock()
			for i := 4; i < len(reqArr); i = i + 2 {
				if i%2 == 0 {
					if _, ok := store.data[reqArr[i]]; ok {
						delete(store.data, reqArr[i])
						delete(store.expiry, reqArr[i])
						keysDel++
					}
				}
			}
			store.mu.Unlock()
			conn.Write([]byte("+" + strconv.Itoa(keysDel) + "\r\n"))

		case strings.ToUpper(reqArr[2]) == "EXISTS":
			keyExists := 0
			store.mu.RLock()
			for i := 4; i < len(reqArr); i = i + 2 {
				if i%2 == 0 {
					if _, ok := store.data[reqArr[i]]; ok {
						keyExists++
					}
				}
			}
			store.mu.RUnlock()
			conn.Write([]byte("+" + strconv.Itoa(keyExists) + "\r\n"))

		case strings.ToUpper(reqArr[2]) == "KEYS":
			result := make(map[string]string)
			pattern := reqArr[4]
			store.mu.RLock()
			// Case 1: Match everything
			if pattern == "*" {
				for k, v := range store.data {
					result[k] = v
				}
			}
			// Case 2: Ends with (e.g., *x)
			if strings.HasPrefix(pattern, "*") {
				suffix := pattern[1:]
				for k, v := range store.data {
					if strings.HasSuffix(k, suffix) {
						result[k] = v
					}
				}
			}
			// Case 3: Starts with (e.g., x*)
			if strings.HasSuffix(pattern, "*") {
				prefix := pattern[:len(pattern)-1]
				for k, v := range store.data {
					if strings.HasPrefix(k, prefix) {
						result[k] = v
					}
				}
			}
			// Case 4: Exact match (no wildcards)
			if val, exists := store.data[pattern]; exists {
				result[pattern] = val
			}
			// Case 5: Both Starts and Ends with (eg. *x*)
			if strings.HasPrefix(pattern, "*") && strings.HasSuffix(pattern, "*") {
				prefix := pattern[1 : len(pattern)-1]
				for k, v := range store.data {
					if strings.Contains(k, prefix) {
						result[k] = v
					}
				}
			}
			store.mu.RUnlock()
			conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(result))))
			for key := range result {
				conn.Write([]byte("$" + strconv.Itoa(len(key)) + "\r\n" + key + "\r\n"))
			}

		case strings.ToUpper(reqArr[2]) == "STRLEN":
			store.mu.RLock()
			length := len(store.data[reqArr[4]])
			store.mu.RUnlock()
			conn.Write([]byte(":" + strconv.Itoa(length) + "\r\n"))

		case strings.ToUpper(reqArr[2]) == "EXPIRE":
			key := reqArr[4]
			seconds, err := strconv.Atoi(reqArr[6])
			if err != nil {
				conn.Write([]byte("-ERR value is not an integer\r\n"))
				break
			}
			store.mu.Lock()
			if _, ok := store.data[key]; !ok {
				store.mu.Unlock()
				conn.Write([]byte(":0\r\n"))
			} else {
				store.expiry[key] = time.Now().Add(time.Duration(seconds) * time.Second)
				store.mu.Unlock()
				conn.Write([]byte(":1\r\n"))
			}

		case strings.ToUpper(reqArr[2]) == "TTL":
			key := reqArr[4]
			store.mu.RLock()
			_, ok := store.data[key]
			if !ok {
				store.mu.RUnlock()
				conn.Write([]byte(":-2\r\n"))
				break
			}
			exp, ok := store.expiry[key]
			if !ok {
				store.mu.RUnlock()
				conn.Write([]byte(":-1\r\n"))
				break
			}
			remaining := int(exp.Unix() - time.Now().Unix()) 
			if remaining <= 0 {
				store.mu.RUnlock()
				conn.Write([]byte(":-2\r\n")) 
				break
			}
			store.mu.RUnlock()
			conn.Write([]byte(":" + strconv.Itoa(remaining) + "\r\n"))

		case strings.ToUpper(reqArr[2]) == "PERSIST":
			key := reqArr[4]
			store.mu.Lock()
			_, ok := store.data[key]
			if !ok {
				store.mu.Unlock()
				conn.Write([]byte(":0\r\n"))
				break
			}
			_, hasExpiry := store.expiry[key]
			if !hasExpiry {
				store.mu.Unlock()
				conn.Write([]byte(":0\r\n"))
				break
			}
			delete(store.expiry, key)
			store.mu.Unlock()
			conn.Write([]byte(":1\r\n"))

		case strings.ToUpper(reqArr[2]) == "HSET":
			key := reqArr[4]
			store.mu.Lock()
			if _, ok := store.hashes[key]; !ok {
				store.hashes[key] = make(map[string]string)
			}
			_, exists := store.hashes[key][reqArr[6]]
			if !exists {
				store.hashes[key][reqArr[6]] = reqArr[8]
				store.mu.Unlock()
				conn.Write([]byte(":1\r\n"))
			} else {
				store.hashes[key][reqArr[6]] = reqArr[8]
				store.mu.Unlock()
				conn.Write([]byte(":0\r\n"))
			}

		case strings.ToUpper(reqArr[2]) == "HMSET":
			key := reqArr[4]
			addF := 0
			store.mu.Lock()
			if _, ok := store.hashes[key]; !ok {
				store.hashes[key] = make(map[string]string)
			}
			for i := 6; i < len(reqArr); i += 4 {
				_, exists := store.hashes[key][reqArr[i]]
				if !exists {
					addF++
				}
				store.hashes[key][reqArr[i]] = reqArr[i+2]
			}
			store.mu.Unlock()
			conn.Write([]byte(":" + strconv.Itoa(addF) + "\r\n"))

		case strings.ToUpper(reqArr[2]) == "HSETNX":
			key := reqArr[4]
			store.mu.Lock()
			if _, ok := store.hashes[key]; !ok {
				store.hashes[key] = make(map[string]string)
			}
			_, exists := store.hashes[key][reqArr[6]]
			if !exists {
				store.hashes[key][reqArr[6]] = reqArr[8]
				store.mu.Unlock()
				conn.Write([]byte(":1\r\n"))
			} else {
				store.mu.Unlock()
				conn.Write([]byte(":0\r\n"))
			}

		case strings.ToUpper(reqArr[2]) == "HGET":
			if len(reqArr) < 7 {
				conn.Write([]byte("-ERR no fields given for HGET\r\n"))
				break
			}
			key := reqArr[4]
			store.mu.RLock()
			_,okexp := store.hashesExp[key]
			innerMap, ok := store.hashes[key]
			if okexp && ok {
				store.mu.RUnlock()
				checkHashesExp(conn, key, store, reqArr[6])
			}else{
				if ok{
					store.mu.RUnlock()
					conn.Write([]byte("$" + strconv.Itoa(len(innerMap[reqArr[6]])) + "\r\n" + innerMap[reqArr[6]] + "\r\n"))
				}else{
					store.mu.RUnlock()
					conn.Write([]byte("$-1\r\n"))
				}
			}

		case strings.ToUpper(reqArr[2]) == "HMGET":
			if len(reqArr) < 7 {
				conn.Write([]byte("-ERR no fields given for HMGET\r\n"))
				break
			}
			key := reqArr[4]
			fields := []string{}
			for i := 6; i < len(reqArr); i += 2 {
				fields = append(fields, reqArr[i])
			}
			store.mu.RLock()
			innerMap, ok := store.hashes[key]
			conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(fields))))
			for i := 0; i < len(fields); i++ {
				if !ok {
					conn.Write([]byte("$-1\r\n"))
					continue
				}
				val, exists := innerMap[fields[i]]
				if !exists {
					conn.Write([]byte("$-1\r\n"))
				} else {
					conn.Write([]byte("$" + strconv.Itoa(len(val)) + "\r\n" + val + "\r\n"))
				}
			}
			store.mu.RUnlock()

		case strings.ToUpper(reqArr[2]) == "HGETALL":
			if len(reqArr) < 5 {
				conn.Write([]byte("-ERR no minimmum arguments given for HGETALL\r\n"))
				break
			}
			key := reqArr[4]
			store.mu.RLock()
			innerMap, ok := store.hashes[key]
			if !ok {
				store.mu.RUnlock()
				conn.Write([]byte("$-1\r\n"))
				break
			}
			conn.Write([]byte(fmt.Sprintf("*%d\r\n", 2*len(innerMap))))
			for field, val := range innerMap {
				conn.Write([]byte("$" + strconv.Itoa(len(field)) + "\r\n" + field + "\r\n"))
				conn.Write([]byte("$" + strconv.Itoa(len(val)) + "\r\n" + val + "\r\n"))
			}
			store.mu.RUnlock()

		case strings.ToUpper(reqArr[2]) == "HDEL":
			key := reqArr[4]
			store.mu.Lock()
			innerMap, ok := store.hashes[key]
			if !ok {
				store.mu.Unlock()
				conn.Write([]byte(":0\r\n"))
				break
			}
			rmF := 0
			for i := 6; i < len(reqArr); i += 2 {
				field := reqArr[i]
				_, exists := innerMap[field]
				if exists {
					rmF++
					delete(innerMap, field)
				}
			}
			store.mu.Unlock()
			conn.Write([]byte(":" + strconv.Itoa(rmF) + "\r\n"))

		case strings.ToUpper(reqArr[2]) == "HKEYS":
			if len(reqArr) < 5 {
				conn.Write([]byte("-ERR no minimmum arguments given for HKEYS\r\n"))
				break
			}
			key := reqArr[4]
			store.mu.RLock()
			innerMap, ok := store.hashes[key]
			if !ok {
				store.mu.RUnlock()
				conn.Write([]byte("*0\r\n"))
				break
			}
			conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(innerMap))))
			for field := range innerMap {
				conn.Write([]byte("$" + strconv.Itoa(len(field)) + "\r\n" + field + "\r\n"))
			}
			store.mu.RUnlock()

		case strings.ToUpper(reqArr[2]) == "HVALS":
			if len(reqArr) < 5 {
				conn.Write([]byte("-ERR number of arguments given for HVALS\r\n"))
				break
			}
			key := reqArr[4]
			store.mu.RLock()
			innerMap, ok := store.hashes[key]
			if !ok {
				store.mu.RUnlock()
				conn.Write([]byte("*0\r\n"))
				break
			}
			conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(innerMap))))
			for _, val := range innerMap {
				conn.Write([]byte("$" + strconv.Itoa(len(val)) + "\r\n" + val + "\r\n"))
			}
			store.mu.RUnlock()

		case strings.ToUpper(reqArr[2]) == "HLEN":
			if len(reqArr) < 5 {
				conn.Write([]byte("-ERR no minimmum arguments given for HLEN\r\n"))
				break
			}
			key := reqArr[4]
			store.mu.RLock()
			innerMap, ok := store.hashes[key]
			store.mu.RUnlock()
			if !ok {
				conn.Write([]byte(":0\r\n"))
			} else {
				conn.Write([]byte(":" + strconv.Itoa(len(innerMap)) + "\r\n"))
			}

		case strings.ToUpper(reqArr[2]) == "HSETEX":
			key := reqArr[4]
			seconds, err := strconv.Atoi(reqArr[6])
			if err != nil {
				conn.Write([]byte("-ERR value is not an integer\r\n"))
				conn.Write([]byte(":0\r\n"))

				break
			}
			store.mu.Lock()
			store.hashesExp[key] = time.Now().Add(time.Duration(seconds) * time.Second)
			if _, ok := store.hashes[key]; !ok {
				store.hashes[key] = make(map[string]string)
			}
			for i := 12; i < len(reqArr); i += 4 {
				store.hashes[key][reqArr[i]] = reqArr[i+2]
			}
			store.mu.Unlock()
			conn.Write([]byte(":1\r\n"))
			fmt.Print(reqArr)

			
		default:
			conn.Write([]byte("-ERR unknown command\r\n"))
		}

	}
}
