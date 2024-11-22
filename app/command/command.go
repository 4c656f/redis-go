package command

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/commands"
	incrcommand "github.com/codecrafters-io/redis-starter-go/app/commands/incr_command"
	infocommand "github.com/codecrafters-io/redis-starter-go/app/commands/info_command"
	replconfcommand "github.com/codecrafters-io/redis-starter-go/app/commands/repl_conf_command"
	"github.com/codecrafters-io/redis-starter-go/app/commands/type_command"
	xaddcommand "github.com/codecrafters-io/redis-starter-go/app/commands/xadd_command"
	xrangecommand "github.com/codecrafters-io/redis-starter-go/app/commands/xrange_command"
	xreadcommand "github.com/codecrafters-io/redis-starter-go/app/commands/xread_command"
	"github.com/codecrafters-io/redis-starter-go/app/data_types"
	"github.com/codecrafters-io/redis-starter-go/app/logger"
)

type CommandEnum string

const (
	PING       = "PING"
	PONG       = "PONG"
	ECHO       = "ECHO"
	SET        = "SET"
	GET        = "GET"
	INFO       = "INFO"
	CONFIG     = "CONFIG"
	REPLCONF   = "REPLCONF"
	OK         = "OK"
	PSYNC      = "PSYNC"
	FULLRESYNC = "FULLRESYNC"
	WAIT       = "WAIT"
	KEYS       = "KEYS"
	TYPE       = "TYPE"
	XADD       = "XADD"
	XRANGE     = "XRANGE"
	XREAD      = "XREAD"
	INCR       = "INCR"
	MULTI      = "MULTI"
	EXEC       = "EXEC"
	DISCARD    = "DISCARD"
)

type Command struct {
	Type CommandEnum
	Raw  *datatypes.Data
	Args commands.CommandArgs
}

func DataTypeToCommand(d *datatypes.Data) (cmd *Command, err error) {
	if d == nil {
		return nil, fmt.Errorf("DataTypeToCommand Error: nil data")
	}
	if d.Type == datatypes.SIMPLE_STRING {
		return HandleSimpleStringCommand(d)
	}
	if d.Type != datatypes.ARRAY {
		return nil, fmt.Errorf("DataTypeToCommand Error: wrong dataType given, expect *, given: %v", string(d.Type))
	}
	if len(d.Values) == 0 {
		return nil, fmt.Errorf("DataTypeToCommand Error: empty array data")
	}
	commandName := d.Values[0]
	commandType, ok := GetCommandAccordName(strings.ToUpper(commandName.Value))
	if !ok {
		return nil, fmt.Errorf("DataTypeToCommand Error: unknown command: %v", commandName.Value)
	}
	out := &Command{}
	out.Type = commandType
	out.Raw = d
	err = out.ParseArgs()
	if err != nil {
		logger.Logger.Error("Error parsing args", logger.String("error", err.Error()))
	}
	return out, nil
}

func HandleSimpleStringCommand(d *datatypes.Data) (cmd *Command, err error) {
	values := strings.Split(d.Value, " ")
	commandType, ok := GetCommandAccordName(strings.ToUpper(values[0]))
	if !ok {
		return nil, fmt.Errorf("DataTypeToCommand Error: unknown command: %v", d.Value)
	}
	out := &Command{}
	out.Type = commandType
	out.Raw = d
	return out, nil
}

func (this *Command) Marshall() []byte {
	return this.Raw.Marshall()
}

func (this *Command) IsWriteCommand() bool {
	return this.Type == SET
}

func (this *Command) IsNeedAddReplica() bool {
	if this.Type != REPLCONF {
		return false
	}
	arg, _ := this.Args.GetArgValue(replconfcommand.ListeningPort)
	return arg != nil
}

func (this *Command) IsNeedToRepondeAck() bool {
	if this.Type != REPLCONF {
		return false
	}
	arg, _ := this.Args.GetArgValue(replconfcommand.GetAck)
	return arg != nil
}

func ConstructSimpleOk() *Command {
	return &Command{
		Type: OK,
		Raw: &datatypes.Data{
			Type:  datatypes.SIMPLE_STRING,
			Value: "OK",
		},
	}
}

func ConstructPing() *datatypes.Data {
	return &datatypes.Data{
		Type: datatypes.ARRAY,
		Values: []*datatypes.Data{{
			Type:  datatypes.BULK_STRING,
			Value: PING,
		}},
	}
}

func ConstructPsync(replId string, offset string) *datatypes.Data {
	return &datatypes.Data{
		Type: datatypes.ARRAY,
		Values: []*datatypes.Data{{
			Type:  datatypes.BULK_STRING,
			Value: PSYNC,
		}, {
			Type:  datatypes.BULK_STRING,
			Value: replId,
		}, {
			Type:  datatypes.BULK_STRING,
			Value: offset,
		}},
	}
}

func ConstructReplConf(args ...string) *Command {
	constructedArgs := make([]*datatypes.Data, len(args)+1)
	constructedArgs[0] = &datatypes.Data{
		Type:  datatypes.BULK_STRING,
		Value: REPLCONF,
	}
	for i, a := range args {
		constructedArgs[i+1] = &datatypes.Data{
			Type:  datatypes.BULK_STRING,
			Value: a,
		}
	}
	return &Command{Type: REPLCONF, Raw: &datatypes.Data{
		Type:   datatypes.ARRAY,
		Values: constructedArgs,
	}}
}

func ConstructFullResync(id string, offset int) *Command {
	return &Command{
		Type: FULLRESYNC,
		Raw: &datatypes.Data{
			Type:  datatypes.SIMPLE_STRING,
			Value: fmt.Sprintf("%v %v %v", string(FULLRESYNC), id, strconv.Itoa(offset)),
		},
	}
}

var commandMap = map[string]CommandEnum{
	"PING":       PING,
	"PONG":       PONG,
	"ECHO":       ECHO,
	"SET":        SET,
	"GET":        GET,
	"CONFIG":     CONFIG,
	"INFO":       INFO,
	"REPLCONF":   REPLCONF,
	"OK":         OK,
	"PSYNC":      PSYNC,
	"FULLRESYNC": FULLRESYNC,
	"WAIT":       WAIT,
	"KEYS":       KEYS,
	"TYPE":       TYPE,
	"XADD":       XADD,
	"XRANGE":     XRANGE,
	"XREAD":      XREAD,
	"INCR":       INCR,
	"MULTI":      MULTI,
	"EXEC":       EXEC,
	"DISCARD":    DISCARD,
}

func GetCommandAccordName(name string) (cmd CommandEnum, ok bool) {
	cmd, ok = commandMap[name]
	return cmd, ok
}

type SetArgs struct {
	Key   string
	Value string
	Px    int
}

func (t Command) GetSetArgs() (args *SetArgs, err error) {
	if t.Raw == nil || t.Raw.Values == nil {
		return nil, fmt.Errorf("GetSetArgs Error: nil raw data")
	}
	values := t.Raw.Values
	if len(values) < 3 {
		return nil, fmt.Errorf("GetSetArgs Error: not enough values to construct set args")
	}
	out := &SetArgs{}

	setKey := values[1]
	if !setKey.IsArg() {
		return nil, fmt.Errorf("GetSetArgs Error: invalid set key argument")
	}
	out.Key = setKey.Value
	setValue := values[2]
	if !setValue.IsArg() {
		return nil, fmt.Errorf("GetSetArgs Error: invalid set value argument")
	}
	out.Value = setValue.Value
	out.Px = -1
	if len(values) > 4 {
		expKey := values[3]
		if !expKey.IsArg() {
			return nil, fmt.Errorf("GetSetArgs Error: invalid exp argument")
		}
		expValue := values[4]
		if !expValue.IsArg() {
			return nil, fmt.Errorf("GetSetArgs Error: invalid px value argument")
		}
		expAm, err := strconv.Atoi(expValue.Value)
		if err != nil {
			return nil, err
		}
		out.Px = expAm
	}

	return out, nil
}

type GetArgs struct {
	Key string
}

func (t Command) GetGetArgs() (args *GetArgs, err error) {
	if t.Raw == nil || t.Raw.Values == nil {
		return nil, fmt.Errorf("GetSetArgs Error: nil raw data")
	}
	values := t.Raw.Values
	if len(values) < 2 {
		return nil, fmt.Errorf("GetSetArgs Error: not enough values to construct set args")
	}
	out := &GetArgs{}

	getKey := values[1]
	if !getKey.IsArg() {
		return nil, fmt.Errorf("GetSetArgs Error: invalid set key argument")
	}
	out.Key = getKey.Value

	return out, nil
}

type EchoArgs struct {
	Echo string
}

func (t Command) GetEchoArgs() (args *EchoArgs, err error) {
	if t.Raw == nil || t.Raw.Values == nil {
		return nil, fmt.Errorf("GetSetArgs Error: nil raw data")
	}
	values := t.Raw.Values
	if len(values) < 2 {
		return nil, fmt.Errorf("GetSetArgs Error: not enough values to construct set args")
	}
	out := &EchoArgs{}

	echo := values[1]
	if !echo.IsArg() {
		return nil, fmt.Errorf("GetSetArgs Error: invalid set key argument")
	}
	out.Echo = echo.Value

	return out, nil
}

type WaitArgs struct {
	ReplicaCount int
	Timeout      int
}

func (t Command) GetWaitArgs() (args *WaitArgs, err error) {
	if t.Raw == nil || t.Raw.Values == nil {
		return nil, fmt.Errorf("GetSetArgs Error: nil raw data")
	}
	values := t.Raw.Values
	if len(values) < 3 {
		return nil, fmt.Errorf("GetWaitArgs Error: not enough values to construct wait args")
	}
	out := &WaitArgs{}

	replCount := values[1]
	timeoutStr := values[2]
	replCountNum, err := strconv.Atoi(replCount.Value)
	if err != nil {
		return nil, err
	}
	out.ReplicaCount = replCountNum
	timeoutInt, err := strconv.Atoi(timeoutStr.Value)
	if err != nil {
		return nil, err
	}
	out.Timeout = timeoutInt
	return out, nil
}

func (t Command) GetInfoArgs() (args commands.CommandArgs, err error) {
	if t.Raw == nil || t.Raw.Values == nil {
		return nil, fmt.Errorf("GetSetArgs Error: nil raw data")
	}
	values := t.Raw.Values
	if len(values) < 2 {
		return infocommand.NewInfoArgs(infocommand.ALL), nil
	}
	info := values[1]
	if !info.IsArg() {
		return nil, fmt.Errorf("GetSetArgs Error: invalid set key argument")
	}
	return infocommand.NewInfoArgs(infocommand.InfoEnum(info.Value)), nil
}

func (t Command) GetReplArgs() (args commands.CommandArgs, err error) {
	if t.Raw == nil || t.Raw.Values == nil {
		return nil, fmt.Errorf("GetSetArgs Error: nil raw data")
	}
	values := t.Raw.Values
	if len(values) < 3 {
		return nil, fmt.Errorf("GetReplConfPortArgs Error: not enough values to construct repl conf port args")
	}
	argsMap := make(map[replconfcommand.ReplConfArgEnum]string, len(values)-1)

	for i := 1; i < len(values)-1; i += 2 {
		name := values[i].Value
		value := values[i+1].Value
		argsMap[replconfcommand.ReplConfArgEnum(name)] = value
	}

	out, err := replconfcommand.NewReplConfArgsFromMap(argsMap)
	return out, err
}

func (t *Command) ParseArgs() error {
	switch t.Type {
	case REPLCONF:
		args, err := t.GetReplArgs()
		if err != nil {
			return fmt.Errorf("Error parsing replconf args: %w", err)
		}
		t.Args = args
	case INFO:
		args, err := t.GetInfoArgs()
		if err != nil {
			return fmt.Errorf("Error parsing info args: %w", err)
		}
		t.Args = args
	case XADD:
		args, err := xaddcommand.ParseXaddArgs(t.Raw.Values)
		if err != nil {
			return fmt.Errorf("Error parsing xadd args: %w", err)
		}
		t.Args = args
	case TYPE:
		args, err := type_command.NewTypeArgs(t.Raw.Values)
		if err != nil {
			return fmt.Errorf("Error parsing type args: %w", err)
		}
		t.Args = args
	case XRANGE:
		args, err := xrangecommand.ParseXrangeArgs(t.Raw.Values)
		if err != nil {
			return fmt.Errorf("Error parsing xrange args: %w", err)
		}
		t.Args = args
	case XREAD:
		args, err := xreadcommand.ParseXreadArgs(t.Raw.Values)
		if err != nil {
			return fmt.Errorf("Error parsing xread args: %w", err)
		}
		t.Args = args
	case INCR:
		args, err := incrcommand.ParseIncrArgs(t.Raw.Values)
		if err != nil {
			return fmt.Errorf("Error parsing incr args: %w", err)
		}
		t.Args = args
	}

	return nil
}

type ConfigArgEnum string

const (
	Dir        = "dir"
	Dbfilename = "dbfilename"
)

type ConfigArgs struct {
	Args []ConfigArgEnum
}

func (t Command) GetConfigArgs() (args *ConfigArgs, err error) {
	if t.Raw == nil || t.Raw.Values == nil {
		return nil, fmt.Errorf("GetSetArgs Error: nil raw data")
	}
	values := t.Raw.Values
	if len(values) < 3 {
		return nil, fmt.Errorf("GetReplConfPortArgs Error: not enough values to construct repl conf port args")
	}
	argsSlice := make([]ConfigArgEnum, len(values)-2)
	out := &ConfigArgs{Args: argsSlice}
	for i := 2; i < len(values); i++ {
		name := values[i].Value
		argsSlice[i-2] = ConfigArgEnum(name)
	}
	return out, nil
}
