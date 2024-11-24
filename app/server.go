package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"

	"net"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/conn_processor"
	"github.com/codecrafters-io/redis-starter-go/app/executor"
	"github.com/codecrafters-io/redis-starter-go/app/handshake"
	"github.com/codecrafters-io/redis-starter-go/app/logger"
	"github.com/codecrafters-io/redis-starter-go/app/offset_counter"
	"github.com/codecrafters-io/redis-starter-go/app/rdb"
	"github.com/codecrafters-io/redis-starter-go/app/replicas_storage"
	"github.com/codecrafters-io/redis-starter-go/app/storage"
)

type Server struct {
	storage          storage.Storage
	executor         executor.CommandExecutor
	config           *config.Config
	replicas_storage *replicas_storage.ReplStorage
	processor        conn_processor.ConnProcessor
}

func main() {
	runtime.SetBlockProfileRate(1)
	go func() {
		fmt.Println(http.ListenAndServe("localhost:8080", nil))
	}()
	server := NewServer()
	err := server.SendHandShake()
	if err != nil {
		logger.Logger.Fatal("server handshake error:", logger.String("error", err.Error()))
		os.Exit(1)
	}
	err = server.Listen()
	if err != nil {
		os.Exit(1)
	}
}

func NewServer() *Server {

	storage := storage.New()
	config, err := config.New()
	counter := offset_counter.New()
	repl_storage := replicas_storage.New(config)
	if err != nil {
		logger.Logger.Fatal("server configure error:", logger.String("error", err.Error()))
		os.Exit(1)
	}
	executor := executor.New(counter, repl_storage, storage, config)
	processor := conn_processor.NewMasterProcessor(repl_storage, executor)

	err = rdb.LoadRdbFromFile(config, storage)
	if err != nil {
		logger.Logger.Error("load rdb error", logger.String("error", err.Error()))
	}

	server := Server{
		storage:          storage,
		executor:         executor,
		config:           config,
		replicas_storage: repl_storage,
		processor:        processor,
	}
	return &server
}

func (this *Server) SendHandShake() error {
	if this.GetConfig().GetRole() != config.SLAVE {
		return nil
	}
	conn, reader, err := handshake.SendHandshake(this.config)
	replicaProcessor := conn_processor.NewReplicaProcessor(this.executor, reader)
	if err != nil {
		return err
	}
	go replicaProcessor.Process(conn)
	return err
}

func (this *Server) Listen() error {
	port := this.GetConfig().GetServerPort()
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%v", port))
	if err != nil {
		logger.Logger.Error("Failed to bind to port", logger.Int("port", int(port)), logger.String("Error", err.Error()))
		return err
	}

	logger.Logger.Info("server listen on port", logger.Int("port", int(port)))

	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			logger.Logger.Error("Failed to accept connection", logger.String("error", err.Error()))
			continue
		}
		go this.processor.Process(conn)
	}
}

func (this *Server) GetConfig() *config.Config {
	return this.config
}
