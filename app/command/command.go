package command

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/data_types"
)

type CommandEnum string

const (
	PING       = "PING"
	PONG       = "PONG"
	ECHO       = "ECHO"
	SET        = "SET"
	GET        = "GET"
	INFO       = "INFO"
	REPLCONF   = "REPLCONF"
	OK         = "OK"
	PSYNC      = "PSYNC"
	FULLRESYNC = "FULLRESYNC"
)

type InfoEnum string

const (
	ALL         = "ALL"
	REPLICATION = "REPLICATION"
)

type Command struct {
	Type CommandEnum
	Raw  *datatypes.Data
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

func (this *Command) IsWriteCommand() bool {
	return this.Type == SET
}

func (this *Command) IsNeedAddReplica() bool {
	return this.Type == REPLCONF
}

func GetSimpleOk() *datatypes.Data {
	return &datatypes.Data{
		Type:  datatypes.SIMPLE_STRING,
		Value: "OK",
	}
}

func GetPing() *datatypes.Data {
	return &datatypes.Data{
		Type: datatypes.ARRAY,
		Values: []*datatypes.Data{{
			Type:  datatypes.BULK_STRING,
			Value: PING,
		}},
	}
}

func GetPsync(replId string, offset string) *datatypes.Data {
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

func GetReplConf(args ...string) *datatypes.Data {
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
	return &datatypes.Data{
		Type:   datatypes.ARRAY,
		Values: constructedArgs,
	}
}

func GetFullResync(id string, offset int) *datatypes.Data {
	return &datatypes.Data{
		Type:  datatypes.SIMPLE_STRING,
		Value: fmt.Sprintf("%v %v %v", string(FULLRESYNC), id, strconv.Itoa(offset)),
	}
}

var commandMap = map[string]CommandEnum{
	"PING":       PING,
	"PONG":       PONG,
	"ECHO":       ECHO,
	"SET":        SET,
	"GET":        GET,
	"INFO":       INFO,
	"REPLCONF":   REPLCONF,
	"OK":         OK,
	"PSYNC":      PSYNC,
	"FULLRESYNC": FULLRESYNC,
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

type InfoArgs struct {
	Type InfoEnum
}

func (t Command) GetInfoArgs() (args *InfoArgs, err error) {
	if t.Raw == nil || t.Raw.Values == nil {
		return nil, fmt.Errorf("GetSetArgs Error: nil raw data")
	}
	values := t.Raw.Values
	if len(values) < 2 {
		return &InfoArgs{
			Type: ALL,
		}, nil
	}
	out := &InfoArgs{}

	info := values[1]
	if !info.IsArg() {
		return nil, fmt.Errorf("GetSetArgs Error: invalid set key argument")
	}
	switch strings.ToUpper(info.Value) {
	case REPLICATION:
		out.Type = REPLICATION
	default:
		return nil, fmt.Errorf("GetInfoArgs Error: unknown info type")
	}

	return out, nil
}

type ReplConfPortArgs struct {
	Port int
}

func (t Command) GetReplConfPortArgs() (args *ReplConfPortArgs, err error) {
	if t.Raw == nil || t.Raw.Values == nil {
		return nil, fmt.Errorf("GetSetArgs Error: nil raw data")
	}
	values := t.Raw.Values
	if len(values) < 3 {
		return nil, fmt.Errorf("GetReplConfPortArgs Error: not enough values to construct repl conf port args")
	}
	out := &ReplConfPortArgs{}

	port := values[2]
	if !port.IsArg() {
		return nil, fmt.Errorf("GetReplConfPortArgs Error: invalid port argument")
	}
	nPort, err := strconv.Atoi(port.Value)
	if err != nil {
		return nil, fmt.Errorf("GetReplConfPortArgs Error: cannot convert port to int: %v", err.Error())
	}
	out.Port = nPort

	return out, nil
}
