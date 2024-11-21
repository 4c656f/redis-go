package replconfcommand

import (
	"fmt"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/commands"
)

type ReplConfArgEnum string

const (
	ListeningPort = "listening-port"
	GetAck        = "GETACK"
	Ack           = "ACK"
	Capa          = "capa"
)

type SetFc func(commands.CommandArgs) (key string, value string)

func NewReplConfArgsFromMap(argMap map[ReplConfArgEnum]string) (commands.CommandArgs, error) {
	args := commands.NewArgs()

	for k, v := range argMap {
		switch k {
		case ListeningPort:
			port, err := strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("Error parsing replconf port: %w", err)
			}
			args.SetArgValue(ListeningPort, commands.NewIntArgValue(port))
		case GetAck:
			args.SetArgValue(GetAck, commands.NewStringArgValue(v))
		case Ack:
			processed, err := strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("Error parsing replconf ack offset: %w", err)
			}
			args.SetArgValue(Ack, commands.NewIntArgValue(processed))
		case Capa:
			args.SetArgValue(Capa, commands.NewStringArgValue(v))
		}
	}

	return args, nil
}
