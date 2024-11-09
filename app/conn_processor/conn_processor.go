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
	executor         executor.CommandExecutor
}

type ConnProcessor interface {
	Process(conn net.Conn)
}

func New(replicas_storage *replicas_storage.ReplStorage, executor executor.CommandExecutor) ConnProcessor {
	return &MasterConnProcessor{
		replicas_storage: replicas_storage,
		executor:         executor,
	}
}

func (this *MasterConnProcessor) Process(conn net.Conn) {

	reader := reader.New(bufio.NewReader(conn))
	writer := bufio.NewWriter(conn)
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
		output := this.executor.ExecuteCmd(cmd)
		this.replicas_storage.PropagateCmd(cmd)
		for _, res := range output {
			writer.Write(res)
			writer.Flush()
		}

	}
	conn.Close()
}
