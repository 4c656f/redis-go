package infocommand

import (
	"github.com/codecrafters-io/redis-starter-go/app/commands"
)

type InfoEnum string

const (
	ALL         = "all"
	REPLICATION = "replication"
)

type InfoArgsEnum string

const (
	Type = "Type"
)

func NewInfoArgs(infoType InfoEnum) commands.CommandArgs {
	args := commands.NewArgs()
	args.SetArgValue(Type, commands.NewStringArgValue(string(infoType)))
	return args
}
