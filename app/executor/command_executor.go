package executor

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/encoder"
	"github.com/codecrafters-io/redis-starter-go/app/storage"
)

type ExecutionResult = [][]byte

type CommandExecutor interface {
	ExecuteCmd(cmd *command.Command) ExecutionResult
}

type executor struct {
	storage storage.Storage
	config  *config.Config
}

func New(
	storage storage.Storage,
	config *config.Config,
) *executor {
	return &executor{storage, config}
}

type ExecuteFunc = func(*executor, *command.Command) (ExecutionResult, error)

var commantToExecuteMap = map[command.CommandEnum]ExecuteFunc{
	command.SET:      (*executor).ExecuteSet,
	command.INFO:     (*executor).ExecuteInfo,
	command.ECHO:     (*executor).ExecuteEcho,
	command.PING:     (*executor).ExecutePing,
	command.GET:      (*executor).ExecuteGet,
}

func (this *executor) mathcCommandToExecuteFunc(cmd *command.Command) (exec ExecuteFunc, ok bool) {
	exec, ok = commantToExecuteMap[cmd.Type]
	return exec, ok
}

func (this *executor) ExecuteCmd(cmd *command.Command) ExecutionResult {
	fmt.Printf("Execute %v command", cmd.Type)
	exec, ok := this.mathcCommandToExecuteFunc(cmd)
	if !ok {
		raw := encoder.EncodeSimpleError(fmt.Sprintf("not found command implimentation %v", cmd.Type))
		return ExecutionResult{raw}
	}
	res, err := exec(this, cmd)
	if err != nil {
		raw := encoder.EncodeSimpleError(err.Error())
		return ExecutionResult{raw}
	}
	return res
}

func (this *executor) ExecuteSet(cmd *command.Command) (ExecutionResult, error) {
	args, err := cmd.GetSetArgs()
	if err != nil {
		return nil, err
	}
	if args.Px != -1 {
		err = this.storage.SetExp(args.Key, args.Value, args.Px)
	} else {
		err = this.storage.Set(args.Key, args.Value)
	}
	if err != nil {
		return nil, err
	}
	raw := encoder.EncodeSimpleString("OK")
	return ExecutionResult{raw}, nil
}

func (this *executor) ExecuteGet(cmd *command.Command) (ExecutionResult, error) {

	getArgs, err := cmd.GetGetArgs()
	if err != nil {
		return nil, err
	}

	val, err := this.storage.Get(getArgs.Key)

	if err != nil {
		return nil, err
	}

	if val == "" {
		raw, _ := encoder.EncodeNull()
		return ExecutionResult{raw}, nil
	}
	raw := encoder.EncodeBulkString(val)
	return ExecutionResult{raw}, nil
}

func (this *executor) ExecutePing(cmd *command.Command) (ExecutionResult, error) {
	raw := encoder.EncodeSimpleString("PONG")
	return ExecutionResult{raw}, nil
}

func (this *executor) ExecuteEcho(cmd *command.Command) (ExecutionResult, error) {
	echo, err := cmd.GetEchoArgs()
	if err != nil {
		return nil, err
	}
	raw := encoder.EncodeBulkString(echo.Echo)
	return ExecutionResult{raw}, nil
}

func (this *executor) ExecuteInfo(cmd *command.Command) (ExecutionResult, error) {
	args, err := cmd.GetInfoArgs()
	if err != nil {
		return nil, err
	}
	switch args.Type {
	case command.REPLICATION:
		repInfo := this.config.GetReplicationInfo()
		raw := encoder.EncodeBulkString(encoder.EncodeKvs(repInfo))
		return ExecutionResult{raw}, nil
	case command.ALL:
		allInfo := this.config.GetAllInfo()
		raw := encoder.EncodeBulkString(encoder.EncodeKvs(allInfo))
		return ExecutionResult{raw}, nil
	}
	return nil, fmt.Errorf("executing info Error: unknown info type: %v", args.Type)
}
