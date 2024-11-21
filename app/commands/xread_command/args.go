package xreadcommand

import (
	"errors"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/commands"
	xrangecommand "github.com/codecrafters-io/redis-starter-go/app/commands/xrange_command"
	datatypes "github.com/codecrafters-io/redis-starter-go/app/data_types"
	"github.com/codecrafters-io/redis-starter-go/app/stream"
	"github.com/codecrafters-io/redis-starter-go/app/types"
	"strconv"
)

type XrangeArgsEnum string

const (
	Block   = "block"
	Streams = "start"
)

func ParseXreadArgs(values []*datatypes.Data) (commands.CommandArgs, error) {
	if len(values) < 4 {
		return nil, errors.New("Wrong amount of values to construct xread args")
	}
	args := commands.NewArgs()
	block := values[1].Value
	if block == "block" {
		blockTimeout := values[2].Value
		blockT, err := strconv.Atoi(blockTimeout)
		if err != nil {
			return nil, fmt.Errorf("Error parsing block timeout: %w", err)
		}
		args.SetArgValue(Block, commands.NewIntArgValue(blockT))
		if values[3].Value != "streams" {
			return nil, errors.New("Keywoard streams does not specified")
		}
		selectedStreams := make([]types.Kv, 0, (len(values)-3)/2)
		left := 4
		right := ((len(values) - 4) / 2) + 4
		for right < len(values) {
			streamName := values[left]
			streamQuery := values[right]
			selectedStreams = append(selectedStreams, types.Kv{streamName.Value, streamQuery.Value})
			left++
			right++
		}
		args.SetArgValue(Streams, commands.NewKvArgValue(selectedStreams))
		return args, nil
	}
	if block != "streams" {
		return nil, errors.New("Keywoard streams does not specified")

	}
	selectedStreams := make([]types.Kv, 0, (len(values)-2)/2)
	left := 2
	right := ((len(values) - 2) / 2) + 2
	for right < len(values) {
		streamName := values[left]
		streamQuery := values[right]
		selectedStreams = append(selectedStreams, types.Kv{streamName.Value, streamQuery.Value})
		left++
		right++
	}
	args.SetArgValue(Streams, commands.NewKvArgValue(selectedStreams))

	return args, nil
}

type ReadType string

const (
	InRange = "inRange"
	OnlyNew = "onlyNew"
)

type StreamQuery struct {
	Start    stream.StreamEntrieId
	Key      string
	ReadType ReadType
}

type ConstrctedStreamsQueries struct {
	IsBlocked      bool
	BlockedTimeout int
	Queries        []StreamQuery
}

func ConstructQueryFromArgs(arg commands.CommandArgs) (ConstrctedStreamsQueries, error) {
	out := ConstrctedStreamsQueries{}
	blockedTimeout, isBlocked := arg.GetArgValue(Block)
	out.IsBlocked = isBlocked
	if isBlocked {
		var blockTieo int
		blockedTimeout.ToType(&blockTieo)
		out.BlockedTimeout = blockTieo
	}
	selectedStreams, ok := arg.GetArgValue(Streams)
	if !ok {
		return ConstrctedStreamsQueries{}, errors.New("Error cannot construct xread query, selected streams is empty")
	}
	var streams []types.Kv
	err := selectedStreams.ToType(&streams)
	if err != nil || streams == nil {
		return ConstrctedStreamsQueries{}, fmt.Errorf("Error casting xread streams to kv: %w", err)
	}
	queries := make([]StreamQuery, len(streams))
	for i, str := range streams {
		streamKey := str[0]
		streamId := str[1]
		if streamId == "$" {
			queries[i] = StreamQuery{
				Key:      streamKey,
				ReadType: OnlyNew,
			}
			continue
		}
		parsedEntrieId, err := xrangecommand.ParseEntrieIdFromString(streamId, true)
		if err != nil {
			return ConstrctedStreamsQueries{}, fmt.Errorf("Error parsing %v'th stream id: %w", i, err)
		}
		queries[i] = StreamQuery{
			Key:      streamKey,
			ReadType: InRange,
			Start:    parsedEntrieId,
		}

	}
	out.Queries = queries
	return out, nil
}
