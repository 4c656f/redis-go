package main

import (
	"flag"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)

type Config struct {
	host            string
	port            int
	connections     int
	commandsPerConn int
}

func main() {
	config := parseFlags()
	fmt.Printf("Starting load test with %d connections to %s:%d\n",
		config.connections, config.host, config.port)

	var wg sync.WaitGroup
	startTime := time.Now()

	// Launch connections in parallel
	for i := 0; i < config.connections; i++ {
		wg.Add(1)
		go func(connID int) {
			defer wg.Done()
			runConnection(connID, config)
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)
	totalCommands := config.connections * config.commandsPerConn

	fmt.Printf("\nTest completed in %v\n", duration)
	fmt.Printf("Total commands executed: %d\n", totalCommands)
	fmt.Printf("Average commands/second: %.2f\n",
		float64(totalCommands)/duration.Seconds())
}

func parseFlags() Config {
	host := flag.String("host", "localhost", "Redis server host")
	port := flag.Int("port", 6379, "Redis server port")
	connections := flag.Int("c", 10, "Number of concurrent connections")
	commandsPerConn := flag.Int("n", 10000, "Number of commands per connection")

	flag.Parse()

	return Config{
		host:            *host,
		port:            *port,
		connections:     *connections,
		commandsPerConn: *commandsPerConn,
	}
}

func runConnection(connID int, config Config) {
	addr := fmt.Sprintf("%s:%d", config.host, config.port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("Connection %d failed: %v\n", connID, err)
		return
	}
	defer conn.Close()

	for i := 0; i < config.commandsPerConn; i++ {
		// Send SET command
		key := fmt.Sprintf("key%s", strconv.Itoa(i))
		setCmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%s\r\n%s\r\n$5\r\nvalue\r\n", strconv.Itoa(len(key)), key)
		_, err := conn.Write([]byte(setCmd))
		if err != nil {
			fmt.Printf("Connection %d: Write error: %v\n", connID, err)
			return
		}

		if i%100 == 0 {
			fmt.Printf("Connection %d: Processed %d commands\n", connID, i)
		}
	}
	
	for i := 0; i < config.commandsPerConn; i++ {
		// Send SET command
		key := fmt.Sprintf("key%s", strconv.Itoa(i))
		setCmd := fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%s\r\n%s\r\n", strconv.Itoa(len(key)), key)
		_, err := conn.Write([]byte(setCmd))
		if err != nil {
			fmt.Printf("Connection %d: Write error: %v\n", connID, err)
			return
		}

		if i%100 == 0 {
			fmt.Printf("Connection %d: Processed %d GET commands\n", connID, i)
		}
	}
}
