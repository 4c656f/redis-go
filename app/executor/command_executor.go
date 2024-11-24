package executor

import (
	"errors"
	"fmt"
	"strconv"

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

type CommandProcessor interface {
	ProcessCmd(cmd *command.Command) (*datatypes.Data, error)
}

type CommandExecutor interface {
	ExecuteCmd(cmd *command.Command, shouldRespond bool) *datatypes.Data
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

type ExecuteFunc = func(*executor, *command.Command) (*datatypes.Data, error)

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

func (this *executor) ExecuteCmd(cmd *command.Command, shouldRespond bool) *datatypes.Data {
	logger.Logger.Info("execute command", logger.String("command", string(cmd.Type)))
	exec, ok := this.mathcCommandToExecuteFunc(cmd)
	if !ok {
		return datatypes.ConstructSimpleError(fmt.Sprintf("not found command implimentation %v", cmd.Type))
	}
	counterRes, err := this.counter.ProcessCmd(cmd)
	if err != nil {
		return datatypes.ConstructSimpleError(err.Error())
	}
	if counterRes != nil {
		return counterRes
	}
	res, err := exec(this, cmd)
	if !shouldRespond {
		return nil
	}
	if err != nil {
		return datatypes.ConstructSimpleError(err.Error())
	}
	return res
}

func (this *executor) ExecuteSet(cmd *command.Command) (*datatypes.Data, error) {
	args, err := cmd.GetSetArgs()
	if err != nil {
		return nil, err
	}

	intVal, err := strconv.Atoi(args.Value)
	var constructedEntrie storage.StorageValue
	if err != nil {
		constructedEntrie = storage.NewStringValue(args.Value)
	} else {
		constructedEntrie = storage.NewIntValue(intVal)
	}

	if args.Px != -1 {
		err = this.storage.SetExp(args.Key, constructedEntrie, args.Px)
	} else {
		err = this.storage.Set(args.Key, constructedEntrie)
		logger.Logger.Debug("set storageval", logger.String("key", args.Key), logger.String("value", args.Value))
	}

	if err != nil {
		return nil, err
	}

	return datatypes.ConstructSimpleString("OK"), nil
}

func (this *executor) ExecuteGet(cmd *command.Command) (*datatypes.Data, error) {
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
		return datatypes.ConstructNull(), nil
	}

	return datatypes.ConstructBulkString(val), nil
}

func (this *executor) ExecutePing(cmd *command.Command) (*datatypes.Data, error) {
	return datatypes.ConstructSimpleString("PONG"), nil
}

func (this *executor) ExecuteEcho(cmd *command.Command) (*datatypes.Data, error) {
	echo, err := cmd.GetEchoArgs()
	if err != nil {
		return nil, err
	}
	return datatypes.ConstructBulkString(echo.Echo), nil
}

func (this *executor) ExecuteInfo(cmd *command.Command) (*datatypes.Data, error) {
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
		return datatypes.ConstructBulkString(encoder.EncodeKvs(repInfo)), nil
	case infocommand.ALL:
		allInfo := this.config.GetAllInfo()
		return datatypes.ConstructBulkString(encoder.EncodeKvs(allInfo)), nil
	}
	return nil, fmt.Errorf("executing info Error: unknown info type: %v", infoType)
}

func (this *executor) ExecuteConfig(cmd *command.Command) (*datatypes.Data, error) {
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

	return datatypes.ConstructArray(res), nil
}

func (this *executor) ExecuteWait(cmd *command.Command) (*datatypes.Data, error) {
	res, err := this.replica_prosesor.ProcessCmd(cmd)

	if err != nil {
		return nil, err
	}

	return res, nil
}

func (this *executor) ExecuteReplConf(cmd *command.Command) (*datatypes.Data, error) {
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

	return res, err
}

func (this *executor) ExecuteKeys(cmd *command.Command) (*datatypes.Data, error) {
	res := this.storage.GetKeys()

	return datatypes.ConstructArray(res), nil
}

func (this *executor) ExecuteType(cmd *command.Command) (*datatypes.Data, error) {
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
		return datatypes.ConstructSimpleString("none"), nil
	}
	return datatypes.ConstructSimpleString(string(res)), nil
}

func (this *executor) ExecuteXadd(cmd *command.Command) (*datatypes.Data, error) {
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
	return datatypes.ConstructBulkString(validEntrieid.String()), nil
}

func (this *executor) ExecuteXrange(cmd *command.Command) (*datatypes.Data, error) {
	query, err := xrangecommand.ConstructQueryFromArgs(cmd.Args)
	if err != nil {
		return nil, fmt.Errorf("Error constructing xrange query: %w", err)
	}
	streamEntrie, ok := this.storage.GetEntrie(query.Key)
	if !ok {
		return datatypes.ConstructNull(), nil
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

	return datatypes.ConstructArrayFromData(encodedEntries), nil
}

func (this *executor) ExecuteXRead(cmd *command.Command) (*datatypes.Data, error) {
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
					datatypes.ConstructBulkString(q.Key),
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
				datatypes.ConstructBulkString(q.Key),
				datatypes.ConstructArrayFromData(encodedEntries),
			})
		}
		return datatypes.ConstructArrayFromData(results), nil
	}
	blockedQuery := query.Queries[0]
	queryEntrie, ok := this.storage.GetEntrie(blockedQuery.Key)
	if !ok {
		return datatypes.ConstructArrayFromData([]*datatypes.Data{
			datatypes.ConstructBulkString(blockedQuery.Key),
			datatypes.ConstructNull(),
		}), nil
	}
	queryStream, err := queryEntrie.ToStream()
	if err != nil {
		return nil, fmt.Errorf("Error converting xread entrie to stream: %w", err)
	}
	if blockedQuery.ReadType == xreadcommand.OnlyNew {
		newEntrie := queryStream.BlockUntilNew(query.BlockedTimeout)
		if newEntrie == nil {
			return datatypes.ConstructNull(), nil
		}
		return datatypes.ConstructArrayFromData([]*datatypes.Data{datatypes.ConstructArrayFromData([]*datatypes.Data{
			datatypes.ConstructBulkString(blockedQuery.Key),
			datatypes.ConstructArrayFromData([]*datatypes.Data{newEntrie.ToDataType()}),
		})}), nil
	}
	existedEnties := queryStream.GetInRangeExcl(&blockedQuery.Start, nil)
	logger.Logger.Debug("blocking stream", logger.String("entries", fmt.Sprintf("%v", existedEnties)))
	newEntrie := queryStream.BlockUntilNew(query.BlockedTimeout)
	logger.Logger.Debug("unblocking stream", logger.String("entries", fmt.Sprintf("%v", existedEnties)), logger.String("new entrie", fmt.Sprintf("%v", newEntrie)))
	if newEntrie == nil {
		logger.Logger.Debug("write nil")
		return datatypes.ConstructNull(), nil
	}
	existedEnties = append(existedEnties, newEntrie)
	encodedQueries := make([]*datatypes.Data, len(existedEnties))
	for i, e := range existedEnties {
		encodedQueries[i] = e.ToDataType()
	}
	logger.Logger.Debug("serialize entries")
	streamData := datatypes.ConstructArrayFromData([]*datatypes.Data{
		datatypes.ConstructBulkString(blockedQuery.Key),
		datatypes.ConstructArrayFromData(encodedQueries),
	})
	return datatypes.ConstructArrayFromData([]*datatypes.Data{
		streamData,
	}), nil
}

func (this *executor) ExecuteIncr(cmd *command.Command) (*datatypes.Data, error) {
	argVal, ok := cmd.Args.GetArgValue(incrcommand.IcrKey)
	if !ok {
		return nil, errors.New("Error IncrKey does not specified")
	}
	var strKey string
	argVal.ToType(&strKey)
	entrie, ok := this.storage.GetEntrie(strKey)
	if !ok {
		this.storage.Set(strKey, storage.NewIntValue(1))
		return datatypes.ConstructInt(1), nil
	}
	if entrie.GetType() != storage.Int {
		return nil, incrcommand.IncrNotIntegerTypeError
	}
	intVal, _ := entrie.ToInt()
	this.storage.Set(strKey, storage.NewIntValue(intVal+1))
	return datatypes.ConstructInt(intVal + 1), nil
}
