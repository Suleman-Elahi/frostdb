package main

import (
	"fmt"
	"net"
	"strings"
)

func sendCmd(conn net.Conn, cmd string) string {
	_, err := conn.Write([]byte(cmd))
	if err != nil {
		panic(err)
	}
	buf := make([]byte, 1024*1024)
	n, err := conn.Read(buf)
	if err != nil {
		panic(err)
	}
	return string(buf[:n])
}

func sendCmdBytes(conn net.Conn, payload []byte) []byte {
	_, err := conn.Write(payload)
	if err != nil {
		panic(err)
	}
	buf := make([]byte, 1024*1024)
	n, err := conn.Read(buf)
	if err != nil {
		panic(err)
	}
	return buf[:n]
}

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	fmt.Println("SELECT 9:", sendCmd(conn, "*2\r\n$6\r\nSELECT\r\n$1\r\n9\r\n"))
	fmt.Println("CONFIG SET:", sendCmd(conn, "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$22\r\nlist-max-listpack-size\r\n$2\r\n-1\r\n"))
	fmt.Println("DEL k:", sendCmd(conn, "*2\r\n$3\r\nDEL\r\n$1\r\nk\r\n"))

	// Push elements
	elements := []string{
		"127",
		"32767",
		"2147483647",
		"9223372036854775807",
		"-1000",
		"1000000",
		"0",
		"-1",
		"1",
		"13",
		"2147483648",
		"short",
		strings.Repeat("x", 40),
		strings.Repeat("x", 1000),
	}

	for _, elem := range elements {
		cmd := fmt.Sprintf("*3\r\n$5\r\nLPUSH\r\n$1\r\nk\r\n$%d\r\n%s\r\n", len(elem), elem)
		sendCmd(conn, cmd)
	}

	// DUMP k
	dumpResp := sendCmdBytes(conn, []byte("*2\r\n$4\r\nDUMP\r\n$1\r\nk\r\n"))
	
	// Parse RESP bulk string response
	if dumpResp[0] != '$' {
		fmt.Printf("DUMP response is not bulk string: %s\n", dumpResp)
		return
	}
	
	// Find first CRLF
	idx := strings.Index(string(dumpResp), "\r\n")
	if idx == -1 {
		fmt.Println("Invalid DUMP response")
		return
	}
	var length int
	fmt.Sscanf(string(dumpResp[1:idx]), "%d", &length)
	fmt.Printf("Dump length: %d\n", length)

	dumpData := dumpResp[idx+2 : idx+2+length]

	// Send RESTORE kk 0 <dumpData> REPLACE
	restoreCmd := fmt.Sprintf("*5\r\n$7\r\nRESTORE\r\n$2\r\nkk\r\n$1\r\n0\r\n$%d\r\n", len(dumpData))
	restorePayload := append([]byte(restoreCmd), dumpData...)
	restorePayload = append(restorePayload, []byte("\r\n$7\r\nREPLACE\r\n")...)

	restoreResp := sendCmdBytes(conn, restorePayload)
	fmt.Printf("RESTORE response: %s\n", restoreResp)
}
