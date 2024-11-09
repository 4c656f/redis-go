package handshake

import (
	"bufio"
	"fmt"
	"net"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	"github.com/codecrafters-io/redis-starter-go/app/conn_processor"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/logger"
	"github.com/codecrafters-io/redis-starter-go/app/reader"
)

type Handshake struct {
	config    *config.Config
	con       net.Conn
	reader    reader.Reader
	processor conn_processor.ConnProcessor
}

func SendHandshake(config *config.Config, processor conn_processor.ConnProcessor) error {
	logger.Logger.Info("start sending handshake")
	slaveInfo := config.GetReplicationSlaveInfo()
	if slaveInfo == nil {
		return fmt.Errorf("slave info is nil")
	}
	conn, err := net.Dial("tcp", fmt.Sprintf("%v:%v", slaveInfo.GetHost(), slaveInfo.GetPort()))
	if err != nil {
		return err
	}

	rd := bufio.NewReader(conn)

	reader := reader.New(rd)

	handShake := Handshake{
		config:    config,
		con:       conn,
		reader:    reader,
		processor: processor,
	}

	err = handShake.SendHandshakeStagePing()
	if err != nil {
		return err
	}
	err = handShake.SendHandshakeReplcConf()
	if err != nil {
		return err
	}
	err = handShake.SendHandshakePsync()
	if err != nil {
		return err
	}
	err = handShake.AcceptRdbTransfer()
	if err != nil {
		return err
	}
	go handShake.processor.Process(conn)
	return nil
}

func (this *Handshake) SendHandshakeStagePing() error {
	logger.Logger.Info("start sending ping")
	wrt := command.GetPing().GetRawOrMarshall()
	_, err := this.con.Write(wrt)
	if err != nil {
		logger.Logger.Error("error while send handshake ping command", logger.String("error", err.Error()))
		return err
	}
	data, err := this.reader.ParseDataType()
	logger.Logger.Debug("Readed data:", logger.String("data", data.String()))
	cmd, err := command.DataTypeToCommand(data)
	if err != nil {
		logger.Logger.Error("error while parsing handshake ping command", logger.String("error", err.Error()))
		return err
	}

	if cmd.Type != command.PONG {
		return fmt.Errorf("Got unknown as a response for first stage of a handshake, needs: %v, got: %v", command.PONG, cmd.Type)
	}

	return nil
}

func (this *Handshake) SendHandshakeReplcConf() error {
	strPort := strconv.Itoa(int(this.config.GetServerPort()))
	_, err := this.con.Write(command.GetReplConf("listening-port", strPort).GetRawOrMarshall())
	if err != nil {
		logger.Logger.Error("Error writing replconf handshake", logger.String("error", err.Error()))
		return err
	}
	data, err := this.reader.ParseDataType()
	if err != nil {
		logger.Logger.Error("Error reading replconf handshake resp", logger.String("error", err.Error()))
		return err
	}

	cmd, err := command.DataTypeToCommand(data)
	if cmd.Type != command.OK {
		return fmt.Errorf("Recieve non ok response on replconf: %v", cmd.Type)
	}

	_, err = this.con.Write(command.GetReplConf("capa", "psync2").GetRawOrMarshall())
	if err != nil {
		return err
	}
	data, err = this.reader.ParseDataType()
	if err != nil {
		logger.Logger.Error("Error reading replconf handshake resp", logger.String("error", err.Error()))
		return err
	}

	cmd, err = command.DataTypeToCommand(data)
	if cmd.Type != command.OK {
		return fmt.Errorf("Recieve non ok response on replconf: %v", cmd.Type)
	}

	return nil
}

func (this *Handshake) SendHandshakePsync() error {
	_, err := this.con.Write(command.GetPsync("?", "-1").GetRawOrMarshall())
	if err != nil {
		return err
	}
	data, err := this.reader.ParseDataType()
	if err != nil {
		logger.Logger.Error("Error reading psync handshake resp", logger.String("error", err.Error()))
		return err
	}

	cmd, err := command.DataTypeToCommand(data)

	if err != nil {
		logger.Logger.Error("Error reading psync handshake resp", logger.String("error", err.Error()))
		return err
	}

	if cmd.Type != command.FULLRESYNC {
		return fmt.Errorf("Method of synchronization is not implimented yet: %v", cmd.Type)
	}

	return nil
}

func (this *Handshake) AcceptRdbTransfer() error {
	return this.reader.ReadRdb()
}
