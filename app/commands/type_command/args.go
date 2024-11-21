package type_command

import (
	"errors"

	"github.com/codecrafters-io/redis-starter-go/app/commands"
	datatypes "github.com/codecrafters-io/redis-starter-go/app/data_types"
)

type TypeArgsEnum string

const (
	TypeKey = "typeKey"
)

func NewTypeArgs(values []*datatypes.Data) (commands.CommandArgs, error) {
	args := commands.NewArgs()
	err := ParseTypeArgs(args, values)
	return args, err
}

func ParseTypeArgs(args commands.CommandArgs, values []*datatypes.Data) error {
	if len(values) < 2 {
		return errors.New("Incomplite data to construct type cmd args")
	}
	key := values[1].Value
	args.SetArgValue(TypeKey, commands.NewStringArgValue(key))
	return nil
}
