package transaction

import (
	"fmt"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	datatypes "github.com/codecrafters-io/redis-starter-go/app/data_types"
	"github.com/codecrafters-io/redis-starter-go/app/executor"
	"github.com/codecrafters-io/redis-starter-go/app/logger"
)

type ConnTransaction interface {
	ShouldConsumeCommand(cmd *command.Command) bool
	ExecuteCmd(cmd *command.Command, globalExecutor executor.CommandExecutor) *datatypes.Data
}

type ConnTransactionImpl struct {
	queued               []*command.Command
	isInTransaction bool
}

type GlobalTransaction struct {
	lock     sync.Mutex
	executor executor.CommandExecutor
}

func NewGlobalTransactionProcessor(executor executor.CommandExecutor) *GlobalTransaction {
	return &GlobalTransaction{
		executor: executor,
	}
}

func shouldLockGlobalTransaction(cmd *command.Command) bool {
	return cmd.Type == command.EXEC
}

func (t *GlobalTransaction) ExecuteCmd(cmd *command.Command, tr ConnTransaction) *datatypes.Data {
	logger.Logger.Debug("process command by global transaction", logger.String("cmd", string(cmd.Type)))
	if shouldLockGlobalTransaction(cmd) {
		t.lock.Lock()
		defer t.lock.Unlock()
	}
	logger.Logger.Debug("start processing command by conn transaction", logger.String("cmd", string(cmd.Type)))
	res := tr.ExecuteCmd(cmd, t.executor)
	return res
}

func NewConnectionTransactionProcessor() ConnTransaction {
	return &ConnTransactionImpl{}
}

var transactionalCommands = map[command.CommandEnum]bool{
	command.MULTI:   true,
	command.EXEC:    true,
	command.DISCARD: true,
}

func (t *ConnTransactionImpl) ShouldConsumeCommand(cmd *command.Command) bool {
	if t.isInTransaction {
		return true
	}
	_, ok := transactionalCommands[cmd.Type]
	return ok
}

func (t *ConnTransactionImpl) ExecuteCmd(cmd *command.Command, globalExecutor executor.CommandExecutor) *datatypes.Data {
	logger.Logger.Debug("process command by transaction", logger.String("cmd", string(cmd.Type)))
	switch cmd.Type {
	case command.MULTI:
		return t.ProcessMulti(cmd)
	case command.DISCARD:
		return t.ProcessDiscard(cmd)
	case command.EXEC:
		return t.ProcessExec(cmd, globalExecutor)
	default:
		t.queued = append(t.queued, cmd)
		return datatypes.ConstructSimpleString("QUEUED")
	}
}

func (t *ConnTransactionImpl) ProcessMulti(cmd *command.Command) *datatypes.Data {
	t.isInTransaction = true
	return datatypes.ConstructSimpleString("OK")
}

func (t *ConnTransactionImpl) ProcessDiscard(cmd *command.Command) *datatypes.Data {
	if t.isInTransaction {
		t.isInTransaction = false
		t.queued = nil
		return datatypes.ConstructSimpleString("OK")
	}
	return datatypes.ConstructSimpleError(DiscardWithoutMultiError.Error())
}

func (t *ConnTransactionImpl) ProcessExec(cmd *command.Command, globalExecutor executor.CommandExecutor) *datatypes.Data {
	if !t.isInTransaction {
		return datatypes.ConstructSimpleError(ExecWithoutMultiError.Error())
	}
	t.isInTransaction = false
	logger.Logger.Debug("start executing transaction", logger.String("queued", fmt.Sprintf("%v", t.queued)))
	n := len(t.queued)
	if n <= 0 {
		logger.Logger.Debug("return empty array from transaction")
		return datatypes.ConstructArrayFromData([]*datatypes.Data{})
	}
	results := make([]*datatypes.Data, n)
	for i := 0; i < n; i++ {
		results[i] = globalExecutor.ExecuteCmd(t.queued[i], true)
	}
	return datatypes.ConstructArrayFromData(results)
}
