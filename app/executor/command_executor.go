package executor

import (
	"errors"
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	"github.com/codecrafters-io/redis-starter-go/app/commands"
	incrcommand "github.com/codecrafters-io/redis-starter-go/app/commands/incr_command"
	infocommand "github.com/codecrafters-io/redis-starter-go/app/commands/info_command"
	replconfcommand "github.com/codecrafters-io/redis-starter-go/app/commands/repl_conf_command"
	"github.com/codecrafters-io/redis-starter-go/app/commands/type_command"
	xaddcommand "github.com/codecrafters-io/redis-starter-go/app/commands/xadd_command"
	xrangecommand "github.com/codecrafters-io/redis-starter-go/app/commands/xrange_command"
	xreadcommand "github.com/codecrafters-io/redis-starter-go/app/commands/xread_command"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	datatypes "github.com/codecrafters-io/redis-starter-go/app/data_types"
	"github.com/codecrafters-io/redis-starter-go/app/encoder"
	"github.com/codecrafters-io/redis-starter-go/app/logger"
	"github.com/codecrafters-io/redis-starter-go/app/storage"
	"github.com/codecrafters-io/redis-starter-go/app/stream"
	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type ExecutionResult = ([][]byte)

type CommandProcessor interface {
	ProcessCmd(cmd *command.Command) (*datatypes.Data, error)
}

type CommandExecutor interface {
	ExecuteCmd(cmd *command.Command, shouldRespond bool) ExecutionResult
}

type executor struct {
	counter          CommandProcessor
	replica_prosesor CommandProcessor
	storage          storage.Storage
	config           *config.Config
}

func New(
	counter CommandProcessor,
	replica_prosesor CommandProcessor,
	storage storage.Storage,
	config *config.Config,
) CommandExecutor {
	return &executor{
		counter:          counter,
		replica_prosesor: replica_prosesor,
		storage:          storage,
		config:           config,
	}
}

type ExecuteFunc = func(*executor, *command.Command) (ExecutionResult, error)

var commantToExecuteMap = map[command.CommandEnum]ExecuteFunc{
	command.SET:      (*executor).ExecuteSet,
	command.INFO:     (*executor).ExecuteInfo,
	command.CONFIG:   (*executor).ExecuteConfig,
	command.ECHO:     (*executor).ExecuteEcho,
	command.PING:     (*executor).ExecutePing,
	command.GET:      (*executor).ExecuteGet,
	command.WAIT:     (*executor).ExecuteWait,
	command.KEYS:     (*executor).ExecuteKeys,
	command.REPLCONF: (*executor).ExecuteReplConf,
	command.TYPE:     (*executor).ExecuteType,
	command.XADD:     (*executor).ExecuteXadd,
	command.XRANGE:   (*executor).ExecuteXrange,
	command.XREAD:    (*executor).ExecuteXRead,
	command.INCR:     (*executor).ExecuteIncr,
}

func (this *executor) mathcCommandToExecuteFunc(cmd *command.Command) (exec ExecuteFunc, ok bool) {
	exec, ok = commantToExecuteMap[cmd.Type]
	return exec, ok
}

func (this *executor) ExecuteCmd(cmd *command.Command, shouldRespond bool) ExecutionResult {
	logger.Logger.Info("execute command", logger.String("command", string(cmd.Type)))
	exec, ok := this.mathcCommandToExecuteFunc(cmd)
	if !ok {
		raw := encoder.EncodeSimpleError(fmt.Sprintf("not found command implimentation %v", cmd.Type))
		return ExecutionResult{raw}
	}
	counterRes, err := this.counter.ProcessCmd(cmd)
	if err != nil {
		raw := encoder.EncodeSimpleError(err.Error())
		return ExecutionResult{raw}
	}
	if counterRes != nil {
		return ExecutionResult{counterRes.Marshall()}
	}
	res, err := exec(this, cmd)
	if !shouldRespond {
		return nil
	}
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
		err = this.storage.SetExp(args.Key, storage.NewStringValue(args.Value), args.Px)
	} else {
		err = this.storage.Set(args.Key, storage.NewStringValue(args.Value))
		logger.Logger.Debug("set storageval", logger.String("key", args.Key), logger.String("value", args.Value))
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
	logger.Logger.Debug("get storageval", logger.String("key", getArgs.Key), logger.String("value", val))
	if err != nil {
		return nil, err
	}

	if val == "" {
		raw := encoder.EncodeNull()
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
	args, ok := cmd.Args.GetArgValue(infocommand.Type)
	if !ok {
		return nil, commands.GetUnknowArgError
	}
	var infoType string
	err := args.ToType(&infoType)
	if err != nil {
		return nil, fmt.Errorf("Error get info arg: %w", err)
	}
	switch infoType {
	case infocommand.REPLICATION:
		repInfo := this.config.GetReplicationInfo()
		raw := encoder.EncodeBulkString(encoder.EncodeKvs(repInfo))
		return ExecutionResult{raw}, nil
	case infocommand.ALL:
		allInfo := this.config.GetAllInfo()
		raw := encoder.EncodeBulkString(encoder.EncodeKvs(allInfo))
		return ExecutionResult{raw}, nil
	}
	return nil, fmt.Errorf("executing info Error: unknown info type: %v", infoType)
}

func (this *executor) ExecuteConfig(cmd *command.Command) (ExecutionResult, error) {
	args, err := cmd.GetConfigArgs()
	if err != nil {
		return nil, err
	}
	res := make([]string, 0, len(args.Args)*2)
	for _, arg := range args.Args {
		switch arg {
		case command.Dir:
			res = append(res, command.Dir, this.config.GetServerDbDir())
		case command.Dbfilename:
			res = append(res, command.Dbfilename, this.config.GetServerDbFileName())
		}
	}

	return ExecutionResult{encoder.EncodeArray(res)}, nil
}

func (this *executor) ExecuteWait(cmd *command.Command) (ExecutionResult, error) {
	res, err := this.replica_prosesor.ProcessCmd(cmd)

	if err != nil {
		return nil, err
	}

	return ExecutionResult{res.Marshall()}, nil
}

func (this *executor) ExecuteReplConf(cmd *command.Command) (ExecutionResult, error) {
	args, ok := cmd.Args.GetArgValue(replconfcommand.GetAck)
	if !ok {
		return nil, nil
	}
	var getAck string
	err := args.ToType(&getAck)

	if err != nil {
		return nil, fmt.Errorf("Error reading replconfArgs: %w", err)
	}

	if getAck == "" {
		return nil, errors.New("Empty getack argument")
	}

	res, err := this.counter.ProcessCmd(cmd)

	return ExecutionResult{res.Marshall()}, err
}

func (this *executor) ExecuteKeys(cmd *command.Command) (ExecutionResult, error) {
	res := this.storage.GetKeys()

	return ExecutionResult{encoder.EncodeArray(res)}, nil
}

func (this *executor) ExecuteType(cmd *command.Command) (ExecutionResult, error) {
	typeKey, ok := cmd.Args.GetArgValue(type_command.TypeKey)
	if !ok {
		return nil, commands.GetUnknowArgError
	}
	var strTypeKey string
	err := typeKey.ToType(&strTypeKey)
	if err != nil {
		return nil, fmt.Errorf("Error casting type arg to str: %w", err)
	}
	res := this.storage.GetType(strTypeKey)
	if res == "" {
		return ExecutionResult{encoder.EncodeSimpleString("none")}, nil
	}
	return ExecutionResult{encoder.EncodeSimpleString(string(res))}, nil
}

func (this *executor) ExecuteXadd(cmd *command.Command) (ExecutionResult, error) {
	logger.Logger.Debug("start executing xadd")
	streamKey, ok := cmd.Args.GetArgValue(xaddcommand.Key)
	logger.Logger.Debug("get key arg", logger.String("arg", fmt.Sprintf("%v", streamKey)))
	if !ok {
		return nil, fmt.Errorf("Error adding stream, dont have a key arg: %w", commands.GetUnknowArgError)
	}
	var streamKeyStr string
	err := streamKey.ToType(&streamKeyStr)

	if err != nil {
		return nil, fmt.Errorf("Error casting key stream arg to string: %w", err)
	}
	logger.Logger.Debug("parse stream key", logger.String("key", streamKeyStr))
	streamEntrie, ok := this.storage.GetEntrie(streamKeyStr)
	logger.Logger.Debug("get storage entries")
	var currentStream stream.Stream
	if !ok {
		s := stream.NewStream()
		this.storage.Set(streamKeyStr, storage.NewStreamValue(s))
		currentStream = s
	} else {
		storageStream, err := streamEntrie.ToStream()
		if err != nil {
			return nil, fmt.Errorf("Add stream to key of wrong datatype: %w", err)
		}
		currentStream = storageStream
	}
	logger.Logger.Debug("get active stream", logger.String("stream", fmt.Sprintf("%v", currentStream)))
	streamEntrieId, ok := cmd.Args.GetArgValue(xaddcommand.Id)
	if !ok {
		return nil, commands.GetUnknowArgError
	}
	var streamStringId string
	err = streamEntrieId.ToType(&streamStringId)
	if err != nil {
		return nil, fmt.Errorf("Error casting stream id to str: %w", err)
	}
	parsedEntrieId, mode, err := stream.ParseEntrieIdFromString(streamStringId)
	if err != nil {
		return nil, fmt.Errorf("Error parsing stream id arg: %w", err)
	}
	validEntrieid, err := currentStream.GeneratenewStreamId(*parsedEntrieId, mode)
	if err != nil {
		return nil, err
	}
	streamValues, ok := cmd.Args.GetArgValue(xaddcommand.Kv)
	if !ok {
		return nil, commands.GetUnknowArgError
	}
	var streamKvValues []types.Kv
	err = streamValues.ToType(&streamKvValues)
	if err != nil {
		return nil, fmt.Errorf("Error casting stream values to kv: %w", err)
	}
	currentStream.Add(stream.NewStreamEntrieFromKv(*validEntrieid, streamKvValues))
	logger.Logger.Debug("add entrie to stream")
	return ExecutionResult{encoder.EncodeBulkString(validEntrieid.String())}, nil
}

func (this *executor) ExecuteXrange(cmd *command.Command) (ExecutionResult, error) {
	query, err := xrangecommand.ConstructQueryFromArgs(cmd.Args)
	if err != nil {
		return nil, fmt.Errorf("Error constructing xrange query: %w", err)
	}
	streamEntrie, ok := this.storage.GetEntrie(query.Key)
	if !ok {
		return ExecutionResult{encoder.EncodeNull()}, nil
	}
	selectedStream, err := streamEntrie.ToStream()
	if err != nil {
		return nil, err
	}
	var selectedEntries []*stream.StreamEntrie
	switch query.ReadType {
	case xrangecommand.FromStart:
		selectedEntries = selectedStream.GetInRangeIncl(nil, &query.End)
	case xrangecommand.ToEnd:
		selectedEntries = selectedStream.GetInRangeIncl(&query.Start, nil)
	case xrangecommand.InRange:
		selectedEntries = selectedStream.GetInRangeIncl(&query.Start, &query.End)
	}
	encodedEntries := make([]*datatypes.Data, len(selectedEntries))

	for i, ent := range selectedEntries {
		encodedEntries[i] = ent.ToDataType()
	}

	return ExecutionResult{datatypes.ConstructArrayFromData(encodedEntries).Marshall()}, nil
}

func (this *executor) ExecuteXRead(cmd *command.Command) (ExecutionResult, error) {
	query, err := xreadcommand.ConstructQueryFromArgs(cmd.Args)
	if err != nil {
		return nil, fmt.Errorf("Error constructing xread args: %w", err)
	}
	results := make([]*datatypes.Data, len(query.Queries))
	if !query.IsBlocked {
		for i, q := range query.Queries {
			selectedStream, ok := this.storage.GetEntrie(q.Key)
			if !ok {
				results[i] = datatypes.ConstructArrayFromData([]*datatypes.Data{
					datatypes.ConstructBuldString(q.Key),
					datatypes.ConstructNull(),
				})
				continue
			}
			st, err := selectedStream.ToStream()
			if err != nil {
				return nil, fmt.Errorf("Error converting %v'th query to stream: %w", i, err)
			}
			entries := st.GetInRangeExcl(&q.Start, nil)
			encodedEntries := make([]*datatypes.Data, len(entries))
			for i, e := range entries {
				encodedEntries[i] = e.ToDataType()
			}
			results[i] = datatypes.ConstructArrayFromData([]*datatypes.Data{
				datatypes.ConstructBuldString(q.Key),
				datatypes.ConstructArrayFromData(encodedEntries),
			})
		}
		return ExecutionResult{datatypes.ConstructArrayFromData(results).Marshall()}, nil
	}
	blockedQuery := query.Queries[0]
	queryEntrie, ok := this.storage.GetEntrie(blockedQuery.Key)
	if !ok {
		return ExecutionResult{
			datatypes.ConstructArrayFromData([]*datatypes.Data{
				datatypes.ConstructBuldString(blockedQuery.Key),
				datatypes.ConstructNull(),
			}).Marshall(),
		}, nil
	}
	queryStream, err := queryEntrie.ToStream()
	if err != nil {
		return nil, fmt.Errorf("Error converting xread entrie to stream: %w", err)
	}
	if blockedQuery.ReadType == xreadcommand.OnlyNew {
		newEntrie := queryStream.BlockUntilNew(query.BlockedTimeout)
		if newEntrie == nil {
			return ExecutionResult{encoder.EncodeNull()}, nil
		}
		return ExecutionResult{
			datatypes.ConstructArrayFromData([]*datatypes.Data{
				datatypes.ConstructBuldString(blockedQuery.Key),
				datatypes.ConstructArrayFromData([]*datatypes.Data{newEntrie.ToDataType()}),
			}).Marshall(),
		}, nil
	}
	existedEnties := queryStream.GetInRangeExcl(&blockedQuery.Start, nil)
	logger.Logger.Debug("blocking stream", logger.String("entries", fmt.Sprintf("%v", existedEnties)))
	newEntrie := queryStream.BlockUntilNew(query.BlockedTimeout)
	logger.Logger.Debug("unblocking stream", logger.String("entries", fmt.Sprintf("%v", existedEnties)), logger.String("new entrie", fmt.Sprintf("%v", newEntrie)))
	if newEntrie == nil {
		logger.Logger.Debug("write nil")
		return ExecutionResult{encoder.EncodeNull()}, nil
	}
	existedEnties = append(existedEnties, newEntrie)
	encodedQueries := make([]*datatypes.Data, len(existedEnties))
	for i, e := range existedEnties {
		encodedQueries[i] = e.ToDataType()
	}
	logger.Logger.Debug("serialize entries")
	d := datatypes.ConstructArrayFromData([]*datatypes.Data{
		datatypes.ConstructBuldString(blockedQuery.Key),
		datatypes.ConstructArrayFromData(encodedQueries),
	})
	res := ExecutionResult{
		d.Marshall(),
	}
	logger.Logger.Debug("marshall res")
	return res, nil
}

func (this *executor) ExecuteIncr(cmd *command.Command) (ExecutionResult, error) {
	argVal, ok := cmd.Args.GetArgValue(incrcommand.IcrKey)
	if !ok {
		return nil, errors.New("Error IncrKey does not specified")
	}
	var strKey string
	argVal.ToType(&strKey)
	entrie, ok := this.storage.GetEntrie(strKey)
	if !ok {
		this.storage.Set(strKey, storage.NewIntValue(1))
		return ExecutionResult{datatypes.ConstructInt(1).Marshall()}, nil
	}
	if entrie.GetType() != storage.Int {
		return nil, incrcommand.IncrNotIntegerTypeError
	}
	intVal, _ := entrie.ToInt()
	this.storage.Set(strKey, storage.NewIntValue(intVal+1))
	return ExecutionResult{
		datatypes.ConstructInt(intVal + 1).Marshall(),
	}, nil
}
