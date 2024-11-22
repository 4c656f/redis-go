package stream

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	datatypes "github.com/codecrafters-io/redis-starter-go/app/data_types"
	"github.com/codecrafters-io/redis-starter-go/app/logger"
	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type GenerateIdMode string

const (
	Explicit                = "explicit"
	PartitialAuotoGenerated = "part"
	AuotoGenerated          = "auto"
)

type Stream interface {
	GetAll() []*StreamEntrie
	GetInRangeIncl(start *StreamEntrieId, end *StreamEntrieId) []*StreamEntrie
	GetInRangeExcl(start *StreamEntrieId, end *StreamEntrieId) []*StreamEntrie
	BlockUntilNew(timeout int) *StreamEntrie
	Add(*StreamEntrie)
	GetLast() *StreamEntrie
	GeneratenewStreamId(id StreamEntrieId, mode GenerateIdMode) (*StreamEntrieId, error)
}

type StreamImpl struct {
	entries    []*StreamEntrie
	blockChans []chan *StreamEntrie
	mut        sync.Mutex
}

func NewStream() Stream {
	return &StreamImpl{
		entries:    []*StreamEntrie{},
		blockChans: []chan *StreamEntrie{},
	}
}

func (s *StreamImpl) GetAll() []*StreamEntrie {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.entries
}

func (s *StreamImpl) GeneratenewStreamId(id StreamEntrieId, mode GenerateIdMode) (*StreamEntrieId, error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	var prevEntrieId *StreamEntrieId = nil
	if len(s.entries) > 0 {
		prevEntrieId = &s.entries[len(s.entries)-1].Id
	}
	switch mode {
	case PartitialAuotoGenerated:
		return GeneratePartitialAutoId(prevEntrieId, id.Id)
	case AuotoGenerated:
		entrieId := GenerateAutoId(prevEntrieId)
		return &entrieId, nil
	case Explicit:
		err := id.ValidateOnMinAccepted()
		if err != nil {
			return nil, err
		}
		if prevEntrieId != nil {
			logger.Logger.Debug("validate explicit id", logger.String("prev", prevEntrieId.String()), logger.String("cur", id.String()))
			err = id.Validate(*prevEntrieId)
		}
		if err != nil {
			return nil, err
		}
		return &id, nil
	}
	return nil, errors.New("Error unknown mode of generating id")
}

func (s *StreamImpl) GetInRangeIncl(start *StreamEntrieId, end *StreamEntrieId) []*StreamEntrie {
	s.mut.Lock()
	defer s.mut.Unlock()
	out := make([]*StreamEntrie, 0, len(s.entries))
	for _, e := range s.entries {
		if end != nil && e.Id.Cmp(*end) > 0 {
			break
		}
		if start != nil && e.Id.Cmp(*start) < 0 {
			continue
		}
		out = append(out, e)
	}
	return out
}

func (s *StreamImpl) GetInRangeExcl(start *StreamEntrieId, end *StreamEntrieId) []*StreamEntrie {
	s.mut.Lock()
	defer s.mut.Unlock()
	out := make([]*StreamEntrie, 0, len(s.entries))
	for _, e := range s.entries {
		if end != nil {
			cmp := e.Id.Cmp(*end)
			if cmp >= 0 {

				break
			}
		}
		if start != nil {
			cmp := e.Id.Cmp(*start)
			if cmp <= 0 {
				continue
			}
		}
		out = append(out, e)
	}
	return out
}

func (s *StreamImpl) BlockUntilNew(timeout int) *StreamEntrie {
	s.mut.Lock()
	c := make(chan *StreamEntrie, 1)
	s.blockChans = append(s.blockChans, c)
	s.mut.Unlock()
	if timeout == 0 {
		return <-c
	}
	timeoutC := time.After(time.Duration(timeout) * time.Millisecond)
	select {
	case e := <-c:
		logger.Logger.Debug("recieve entrie in block")
		return e
	case <-timeoutC:
		logger.Logger.Debug("timeout block")
		return nil
	}
}

func (s *StreamImpl) Add(e *StreamEntrie) {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.entries = append(s.entries, e)
	logger.Logger.Debug("adding new entrie to steam", logger.String("chans", fmt.Sprintf("%v", s.blockChans)))
	for _, c := range s.blockChans {
		c <- e
	}
	s.blockChans = []chan *StreamEntrie{}
}

func (s *StreamImpl) GetLast() *StreamEntrie {
	n := len(s.entries)
	if n == 0 {
		return nil
	}
	return s.entries[n-1]
}

type StreamEntrieId struct {
	Id             int64
	SequenceNumber int
}

func (e StreamEntrieId) Cmp(rhs StreamEntrieId) int {
	if e.Id == rhs.Id {
		return e.SequenceNumber - rhs.SequenceNumber
	}
	return int(e.Id - rhs.Id)
}

func (e StreamEntrieId) String() string {
	return fmt.Sprintf("%v-%v", e.Id, e.SequenceNumber)
}

func (e StreamEntrieId) Validate(prev StreamEntrieId) error {
	err := e.ValidateOnMinAccepted()
	if err != nil {
		return nil
	}
	if e.Id < prev.Id {
		return LessThenPreviousStreamEntryError
	}
	if e.Id == prev.Id && e.SequenceNumber <= prev.SequenceNumber {
		return LessThenPreviousStreamEntryError
	}
	return err
}

func (e StreamEntrieId) ValidateOnMinAccepted() error {
	if e.Id <= 0 && e.SequenceNumber <= 0 {
		return LessThenAcceptedStreamEntryError
	}
	return nil
}

func GeneratePartitialAutoId(prev *StreamEntrieId, ts int64) (*StreamEntrieId, error) {
	if prev == nil {

		if ts == 0 {
			return &StreamEntrieId{
				Id:             ts,
				SequenceNumber: 1,
			}, nil
		}

		return &StreamEntrieId{
			Id:             ts,
			SequenceNumber: 0,
		}, nil
	}
	if ts < prev.Id {
		return nil, LessThenPreviousStreamEntryError
	}
	entrieId := generatePartitialAutoIdWithPrev(*prev, ts)

	return &entrieId, nil
}

func generatePartitialAutoIdWithPrev(prev StreamEntrieId, ts int64) StreamEntrieId {
	if prev.Id == ts {
		return StreamEntrieId{
			Id:             ts,
			SequenceNumber: prev.SequenceNumber + 1,
		}
	}
	return StreamEntrieId{
		Id:             ts,
		SequenceNumber: 0,
	}
}

func GenerateAutoId(prev *StreamEntrieId) StreamEntrieId {
	ts := time.Now().UnixMilli()
	if prev != nil {
		if prev.Id == ts {
			return StreamEntrieId{
				Id:             ts,
				SequenceNumber: prev.SequenceNumber + 1,
			}
		}
		return StreamEntrieId{
			Id:             ts,
			SequenceNumber: prev.SequenceNumber,
		}
	}
	return StreamEntrieId{
		Id:             ts,
		SequenceNumber: 0,
	}
}

type StreamEntrie struct {
	Id     StreamEntrieId
	values map[string]string
}

func (e StreamEntrie) ToDataType() *datatypes.Data {
	kv := make([]string, 0, len(e.values))
	for k, v := range e.values {
		kv = append(kv, k)
		kv = append(kv, v)
	}

	encodedValues := datatypes.ConstructArray(kv)
	encodedId := datatypes.ConstructBulkString(e.Id.String())

	return datatypes.ConstructArrayFromData([]*datatypes.Data{
		encodedId,
		encodedValues,
	})
}

func NewStreamEntrieFromKv(id StreamEntrieId, kv []types.Kv) *StreamEntrie {
	out := StreamEntrie{
		Id:     id,
		values: make(map[string]string, len(kv)),
	}
	if kv == nil {
		return &out
	}
	for _, keyValue := range kv {
		key := keyValue[0]
		value := keyValue[1]
		out.values[key] = value
	}
	return &out
}

func ParseEntrieIdFromString(id string) (*StreamEntrieId, GenerateIdMode, error) {
	if id == "*" {
		return &StreamEntrieId{}, AuotoGenerated, nil
	}
	splited := strings.Split(id, "-")
	if len(splited) != 2 {
		return nil, "", WrongIdFormatError
	}
	streamId, err := strconv.ParseInt(splited[0], 10, 64)
	if err != nil {
		return nil, "", fmt.Errorf("Error converting stream id to int: %w", err)
	}
	sequenceNumber := splited[1]
	if sequenceNumber == "*" {
		return &StreamEntrieId{
			Id: streamId,
		}, PartitialAuotoGenerated, nil
	}
	sequenceNumberInt, err := strconv.Atoi(splited[1])
	if err != nil {
		return nil, "", fmt.Errorf("Error converting stream sequence number to int: %w", err)
	}
	return &StreamEntrieId{
		Id:             streamId,
		SequenceNumber: sequenceNumberInt,
	}, Explicit, nil
}
