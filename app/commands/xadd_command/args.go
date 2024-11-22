package xaddcommand

import (
	"github.com/codecrafters-io/redis-starter-go/app/commands"
	datatypes "github.com/codecrafters-io/redis-starter-go/app/data_types"
	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type XaddArgsEnum string

const (
	Key = "key"
	Id  = "id"
	Kv  = "kv"
)

type XaddOpts struct {
	key string
	id  string
	kv  []types.Kv
}

func NewXaddArgs(opts XaddOpts) commands.CommandArgs {
	args := commands.NewArgs()
	args.SetArgValue(Key, commands.NewStringArgValue(opts.key))
	args.SetArgValue(Id, commands.NewStringArgValue(opts.id))
	args.SetArgValue(Kv, commands.NewKvArgValue(opts.kv))
	return args
}

func ParseXaddArgs(values []*datatypes.Data) (commands.CommandArgs, error) {
	if len(values) < 3 {
		return nil, commands.NotEnoughValuesToConstructArgs
	}
	key := values[1]
	streamId := values[2]
	kv := make([]types.Kv, 0, len(values)-3/2)

	for i := 3; i < len(values)-1; i += 2 {
		key := values[i]
		value := values[i+1]
		kv = append(kv, types.Kv{key.Value, value.Value})
	}

	return NewXaddArgs(XaddOpts{
		key: key.Value,
		id:  streamId.Value,
		kv:  kv,
	}), nil
}
