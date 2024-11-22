package xrangecommand

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/commands"
	datatypes "github.com/codecrafters-io/redis-starter-go/app/data_types"
	"github.com/codecrafters-io/redis-starter-go/app/stream"
)

type XrangeArgsEnum string

const (
	StreamKey = "streamKey"
	Start     = "start"
	End       = "end"
)

func ParseXrangeArgs(values []*datatypes.Data) (commands.CommandArgs, error) {
	if len(values) < 4 {
		return nil, commands.NotEnoughValuesToConstructArgs
	}
	key := values[1].Value
	start := values[2].Value
	end := values[3].Value

	args := commands.NewArgs()

	args.SetArgValue(StreamKey, commands.NewStringArgValue(key))
	args.SetArgValue(Start, commands.NewStringArgValue(start))
	args.SetArgValue(End, commands.NewStringArgValue(end))

	return args, nil
}

type ReadType string

const (
	InRange   = "inRange"
	FromStart = "fromStart"
	ToEnd     = "ToEnd"
)

type ConstructedQuery struct {
	Start    stream.StreamEntrieId
	End      stream.StreamEntrieId
	Key      string
	ReadType ReadType
}

func ParseEntrieIdFromString(id string, isStart bool) (stream.StreamEntrieId, error) {
	splited := strings.Split(id, "-")
	if len(splited) == 1 {
		streamId, err := strconv.ParseInt(splited[0], 10, 64)
		if err != nil {
			return stream.StreamEntrieId{}, fmt.Errorf("Error parsing xrange interval id: %w", err)
		}
		if isStart {
			return stream.StreamEntrieId{
				Id:             streamId,
				SequenceNumber: 0,
			}, nil
		}
		return stream.StreamEntrieId{
			Id:             streamId,
			SequenceNumber: math.MaxInt,
		}, nil
	}
	if len(splited) < 2 {
		return stream.StreamEntrieId{}, errors.New("Error cannot split xrange id into two parts")
	}
	streamId, err := strconv.ParseInt(splited[0], 10, 64)
	if err != nil {
		return stream.StreamEntrieId{}, fmt.Errorf("Error parsing xrange interval id: %w", err)
	}
	sequenceNumber, err := strconv.Atoi(splited[1])
	if err != nil {
		return stream.StreamEntrieId{}, fmt.Errorf("Error parsing xrange interval seqNumber: %w", err)
	}

	return stream.StreamEntrieId{
		Id:             streamId,
		SequenceNumber: sequenceNumber,
	}, nil
}

func ConstructQueryFromArgs(arg commands.CommandArgs) (ConstructedQuery, error) {
	streamKey, ok := arg.GetArgValue(StreamKey)
	if !ok {
		return ConstructedQuery{}, errors.New("StreamKey is not specified")
	}
	var streamKeyStr string
	err := streamKey.ToType(&streamKeyStr)
	if err != nil {
		return ConstructedQuery{}, fmt.Errorf("Error castring stream key to str: %w", err)
	}
	start, ok := arg.GetArgValue(Start)
	if !ok {
		return ConstructedQuery{}, errors.New("Start of the interval is not specified")
	}
	end, ok := arg.GetArgValue(End)
	if !ok {
		return ConstructedQuery{}, errors.New("End of the interval is not specified")
	}
	var startStr string
	var endStr string
	err = start.ToType(&startStr)
	err = end.ToType(&endStr)
	if err != nil {
		return ConstructedQuery{}, fmt.Errorf("Error castring stream interval to str: %w", err)
	}
	if startStr == "-" {
		endInteval, err := ParseEntrieIdFromString(endStr, false)
		if err != nil {
			return ConstructedQuery{}, fmt.Errorf("Error parsing end id of xrange: %w", err)
		}
		return ConstructedQuery{
			ReadType: FromStart,
			End:      endInteval,
			Key:      streamKeyStr,
		}, nil
	}
	if endStr == "+" {
		startInteval, err := ParseEntrieIdFromString(startStr, true)
		if err != nil {
			return ConstructedQuery{}, fmt.Errorf("Error parsing start id of xrange: %w", err)
		}
		return ConstructedQuery{
			Start:    startInteval,
			ReadType: ToEnd,
			Key:      streamKeyStr,
		}, nil
	}
	endInteval, err := ParseEntrieIdFromString(endStr, false)
	if err != nil {
		return ConstructedQuery{}, fmt.Errorf("Error parsing end id of xrange: %w", err)
	}
	startInteval, err := ParseEntrieIdFromString(startStr, true)
	if err != nil {
		return ConstructedQuery{}, fmt.Errorf("Error parsing start id of xrange: %w", err)
	}
	return ConstructedQuery{
		Start:    startInteval,
		End:      endInteval,
		Key:      streamKeyStr,
		ReadType: InRange,
	}, nil
}
