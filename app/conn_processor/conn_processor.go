package conn_processor

import (
	"bufio"
	"io"
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	"github.com/codecrafters-io/redis-starter-go/app/executor"
	"github.com/codecrafters-io/redis-starter-go/app/logger"
	"github.com/codecrafters-io/redis-starter-go/app/reader"
	"github.com/codecrafters-io/redis-starter-go/app/replicas_storage"
)

type MasterConnProcessor struct {
	replicas_storage *replicas_storage.ReplStorage
	commandExecutor  executor.CommandExecutor
}

type ReplicaConnProcessor struct {
	commandExecutor executor.CommandExecutor
	reader          *reader.Reader
}

type ConnProcessor interface {
	Process(conn net.Conn)
}

func NewMasterProcessor(replicas_storage *replicas_storage.ReplStorage, executor executor.CommandExecutor) ConnProcessor {
	return &MasterConnProcessor{
		replicas_storage: replicas_storage,
		commandExecutor:  executor,
	}
}

func NewReplicaProcessor(executor executor.CommandExecutor, reader *reader.Reader) ConnProcessor {
	return &ReplicaConnProcessor{
		commandExecutor: executor,
		reader:          reader,
	}
}

func (this *MasterConnProcessor) Process(conn net.Conn) {

	reader := reader.New(bufio.NewReader(conn))
	for {
		data, err := reader.ParseDataType()
		if err != nil {
			if err == io.EOF {
				return
			}
			logger.Logger.Error("Error parsing data", logger.String("error", err.Error()))
			break
		}
		logger.Logger.Info("Readed data:", logger.String("command:", data.String()))
		cmd, err := command.DataTypeToCommand(data)
		if err != nil {
			logger.Logger.Error("Error parsing command", logger.String("error", err.Error()))
			break
		}

		if cmd.IsNeedAddReplica() {
			this.replicas_storage.ProcessReplicaSync(conn, cmd)
			return
		}

		output := this.commandExecutor.ExecuteCmd(cmd, true)
		this.replicas_storage.PropagateCmd(cmd)
		for _, res := range output {
			logger.Logger.Debug("write res to conn", logger.String("res", string(res)))
			conn.Write(res)
			logger.Logger.Debug("end write to conn", logger.String("res", string(res)))
		}

	}
	defer conn.Close()
}

func (this *ReplicaConnProcessor) Process(conn net.Conn) {
	logger.Logger.Info("Start processing replica connection")

	for {
		data, err := this.reader.ParseDataType()
		if err != nil {
			if err == io.EOF {
				return
			}
			logger.Logger.Error("Error parsing data", logger.String("error", err.Error()))
			break
		}
		logger.Logger.Info("Readed data in replica:", logger.String("command:", data.String()))
		cmd, err := command.DataTypeToCommand(data)
		if err != nil {
			logger.Logger.Error("Error parsing command from master", logger.String("error", err.Error()))
			break
		}
		res := this.commandExecutor.ExecuteCmd(cmd, false)

		for _, r := range res {
			conn.Write(r)
		}

	}
	conn.Close()
}
