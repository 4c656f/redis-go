package incrcommand

import (
	"github.com/codecrafters-io/redis-starter-go/app/commands"
	datatypes "github.com/codecrafters-io/redis-starter-go/app/data_types"
)

type IncrArgsEnum string

const (
	IcrKey = "icrKey"
)

func ParseIncrArgs(values []*datatypes.Data) (commands.CommandArgs, error) {
	if len(values) < 2 {
		return nil, commands.NotEnoughValuesToConstructArgs
	}
	keyToIncr := values[1].Value
	args := commands.NewArgs()
	args.SetArgValue(IcrKey, commands.NewStringArgValue(keyToIncr))
	return args, nil
}
