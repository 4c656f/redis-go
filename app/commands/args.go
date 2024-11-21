package commands

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type CommandArgs interface {
	GetArgValue(argKey string) (argValue *CommandArgValue, ok bool)
	SetArgValue(argKey string, argValue *CommandArgValue)
}

type CommandArgsImpl struct {
	args map[string]*CommandArgValue
}

func (a *CommandArgsImpl) GetArgValue(argKey string) (argValue *CommandArgValue, ok bool) {
	argValue, ok = a.args[argKey]
	return
}

func (a *CommandArgsImpl) SetArgValue(argKey string, argValue *CommandArgValue) {
	a.args[argKey] = argValue
}

func NewArgs() CommandArgs {
	return &CommandArgsImpl{
		args: map[string]*CommandArgValue{},
	}
}

type CommandArgValueType = string

const (
	String   = "string"
	Int      = "int"
	KeyValue = "kv"
)

type CommandArgValue struct {
	dataType CommandArgValueType
	string   string
	num      int
	values   []types.Kv
}

func NewIntArgValue(num int) *CommandArgValue {
	return &CommandArgValue{
		dataType: Int,
		num:      num,
	}
}

func NewStringArgValue(str string) *CommandArgValue {
	return &CommandArgValue{
		dataType: String,
		string:   str,
	}
}

func NewKvArgValue(kv []types.Kv) *CommandArgValue {
	return &CommandArgValue{
		dataType: KeyValue,
		values:   kv,
	}
}

func (a CommandArgValue) ToType(typeValue any) error {
	switch val := (typeValue).(type) {
	case *[]types.Kv:
		if a.dataType != KeyValue {
			return WrongArgTypeCastError
		}
		*val = a.values
		return nil
	case *int:
		if a.dataType != Int {
			return WrongArgTypeCastError
		}
		*val = a.num
		return nil
	case *string:
		if a.dataType != String {
			return WrongArgTypeCastError
		}
		*val = a.string
		return nil
	default:
		return fmt.Errorf("Error casting arg type, this type is unsoprted: %T", typeValue)
	}
}
