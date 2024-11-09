package offsetcounter

import (
	"github.com/codecrafters-io/redis-starter-go/app/command"
	"github.com/codecrafters-io/redis-starter-go/app/executor"
)

type Counter struct {
}

func New() executor.CommandExecutor {
	return &Counter{}
}

func (c *Counter) ExecuteCmd(cmd *command.Command) executor.ExecutionResult {
	return nil
}
