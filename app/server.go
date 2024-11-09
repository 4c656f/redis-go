package main

import (
	"fmt"
	"os"

	"net"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/conn_processor"
	"github.com/codecrafters-io/redis-starter-go/app/executor"
	"github.com/codecrafters-io/redis-starter-go/app/handshake"
	"github.com/codecrafters-io/redis-starter-go/app/logger"
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
	repl_storage := replicas_storage.New(config)
	if err != nil {
		logger.Logger.Fatal("server configure error:", logger.String("error", err.Error()))
		os.Exit(1)
	}
	executor := executor.New(storage, config)
	processor := conn_processor.New(repl_storage, executor)
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
	replicaProcessor := conn_processor.New(this.replicas_storage, this.executor)
	err := handshake.SendHandshake(this.config, replicaProcessor)
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