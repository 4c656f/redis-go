package handshake

import (
	"bufio"
	"fmt"
	"net"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	replconfcommand "github.com/codecrafters-io/redis-starter-go/app/commands/repl_conf_command"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/logger"
	"github.com/codecrafters-io/redis-starter-go/app/reader"
)

type Handshake struct {
	config *config.Config
	con    net.Conn
	reader reader.Reader
}

func SendHandshake(config *config.Config) (net.Conn, *reader.Reader, error) {
	logger.Logger.Info("start sending handshake")
	slaveInfo := config.GetReplicationSlaveInfo()
	if slaveInfo == nil {
		return nil, nil, fmt.Errorf("slave info is nil")
	}
	conn, err := net.Dial("tcp", fmt.Sprintf("%v:%v", slaveInfo.GetHost(), slaveInfo.GetPort()))
	if err != nil {
		return nil, nil, err
	}

	rd := bufio.NewReader(conn)

	reader := reader.New(rd)

	handShake := Handshake{
		config: config,
		con:    conn,
		reader: reader,
	}

	err = handShake.SendHandshakeStagePing()
	if err != nil {
		return nil, nil, err
	}
	err = handShake.SendHandshakeReplcConf()
	if err != nil {
		return nil, nil, err
	}
	err = handShake.SendHandshakePsync()
	if err != nil {
		return nil, nil, err
	}
	err = handShake.AcceptRdbTransfer()
	if err != nil {
		return nil, nil, err
	}
	logger.Logger.Info("successful handshake connection with master")
	return conn, &reader, nil
}

func (this *Handshake) SendHandshakeStagePing() error {
	logger.Logger.Info("start sending ping")
	wrt := command.ConstructPing().Marshall()
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
	_, err := this.con.Write(command.ConstructReplConf(replconfcommand.ListeningPort, strPort).Marshall())
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

	_, err = this.con.Write(command.ConstructReplConf(replconfcommand.Capa, "psync2").Marshall())
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
	logger.Logger.Info("start sending psync")
	_, err := this.con.Write(command.ConstructPsync("?", "-1").Marshall())
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
	logger.Logger.Info("start accepting rdb transfer")
	return this.reader.ReadRdb()
}
