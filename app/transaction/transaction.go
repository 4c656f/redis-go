package transaction

import (
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	datatypes "github.com/codecrafters-io/redis-starter-go/app/data_types"
	"github.com/codecrafters-io/redis-starter-go/app/executor"
)

type ConnTransaction interface {
	ShouldConsumeCommand(cmd *command.Command) bool
	ExecuteCmd(cmd *command.Command, globalExecutor executor.CommandExecutor) *datatypes.Data
}

type ConnTransactionImpl struct {
	queued               []*command.Command
	isTransactionStarted bool
}

type GlobalTransaction struct {
	lock     sync.Mutex
	executor executor.CommandExecutor
}

func NewGlobalTransactionProcessor(executor executor.CommandExecutor) *GlobalTransaction {
	return nil
}

func shouldLockGlobalTransaction(cmd *command.Command) bool {
	return cmd.Type == command.MULTI
}

func (t *GlobalTransaction) ExecuteCmd(cmd *command.Command, tr ConnTransaction) *datatypes.Data {
	if shouldLockGlobalTransaction(cmd) {
		t.lock.Lock()
		defer t.lock.Unlock()
	}
	return tr.ExecuteCmd(cmd, t.executor)
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
	if t.isTransactionStarted {
		return true
	}
	_, ok := transactionalCommands[cmd.Type]
	return ok
}

func (t *ConnTransactionImpl) ExecuteCmd(cmd *command.Command, globalExecutor executor.CommandExecutor) *datatypes.Data {
	switch cmd.Type{
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
	t.isTransactionStarted = true
	return datatypes.ConstructSimpleString("OK")
}

func (t *ConnTransactionImpl) ProcessDiscard(cmd *command.Command) *datatypes.Data {
	if t.isTransactionStarted {
		t.isTransactionStarted = false
		t.queued = nil
		return datatypes.ConstructSimpleString("OK")
	}
	return datatypes.ConstructSimpleError(DiscardWithoutMultiError.Error())
}

func (t *ConnTransactionImpl) ProcessExec(cmd *command.Command, globalExecutor executor.CommandExecutor) *datatypes.Data {
	if !t.isTransactionStarted {
		return datatypes.ConstructSimpleError(ExecWithoutMultiError.Error())
	}
	n := len(t.queued)
	results := make([]*datatypes.Data, n)
	for i := 0; i < n; i++ {
		results[i] = globalExecutor.ExecuteCmd(t.queued[i], true)
	}
	return datatypes.ConstructArrayFromData(results)
}
