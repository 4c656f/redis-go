package offset_counter

import (
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	replconfcommand "github.com/codecrafters-io/redis-starter-go/app/commands/repl_conf_command"
	datatypes "github.com/codecrafters-io/redis-starter-go/app/data_types"
	"github.com/codecrafters-io/redis-starter-go/app/logger"
)

type Counter struct {
	bytesProcessed int
}

func New() *Counter {
	return &Counter{}
}

func (c *Counter) ProcessCmd(cmd *command.Command) (*datatypes.Data, error) {
	var out *datatypes.Data = nil
	if cmd.IsNeedToRepondeAck() {
		logger.Logger.Info("Process replconf getAck command")

		out = command.ConstructReplConf(replconfcommand.Ack, strconv.Itoa(c.bytesProcessed)).Raw
	}

	c.bytesProcessed += cmd.Raw.Len()

	return out, nil
}
