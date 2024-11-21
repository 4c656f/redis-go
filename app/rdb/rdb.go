package rdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/logger"
	"github.com/codecrafters-io/redis-starter-go/app/storage"
)

// int encoded flags
type IntEncodedType int

const (
	//4 LSB of same byte represent an integer
	Int4bit = 0
	//4 LSB of same byte and next byte represent an integer
	Int14Bit = 1
	//next 4 bytes represent an integer
	Int32Bit = 2
	//4 LSB of same byte represent an flag for special encoded flags
	IntSpecialEncoded = 3
)

// int encoded flags
type RDBValueTypes int

const (
	StringEncoding RDBValueTypes = 0
	// Not implimented
	ListEncoding             RDBValueTypes = 1
	SetEncoding              RDBValueTypes = 2
	SortedSetEncoding        RDBValueTypes = 3
	HashEncoding             RDBValueTypes = 4
	ZipmapEncoding           RDBValueTypes = 9
	ZiplistEncoding          RDBValueTypes = 10
	IntsetEncoding           RDBValueTypes = 11
	SortedSetZiplistEncoding RDBValueTypes = 12
	HashZiplistEncoding      RDBValueTypes = 13 // Introduced in RDB version 4
	ListQuicklistEncoding    RDBValueTypes = 14 // Introduced in RDB version 7
)

// use to determine is int parser return actual int or flag for spcial type encoded data
type LengthType int

const (
	Int            = 0
	SpecialEncoded = 1
)

// string special encoded flags
type StringFormat int

const (
	Bit8Integer      = 0
	Bit16Integer     = 1
	Bit32Integer     = 2
	CompressedString = 3
)

type OpCodes byte

const (
	// EOF (0xFF) - End of the RDB file
	EOF byte = 0xFF

	// SELECTDB (0xFE) - Database Selector
	SELECTDB byte = 0xFE

	// EXPIRETIME (0xFD) - Expire time in seconds, see Key Expiry Timestamp
	EXPIRETIME byte = 0xFD

	// EXPIRETIMEMS (0xFC) - Expire time in milliseconds, see Key Expiry Timestamp
	EXPIRETIMEMS byte = 0xFC

	// RESIZEDB (0xFB) - Hash table sizes for the main keyspace and expires
	RESIZEDB byte = 0xFB

	// AUX (0xFA) - Auxiliary fields. Arbitrary key-value settings
	AUX byte = 0xFA
)

type ExpType int

const (
	MilliSeconds = 0
	Seconds      = 1
	NotExpire    = 2
)

type Parser struct {
	reader *bufio.Reader
}

func LoadRdbFromFile(
	config *config.Config,
	storage storage.Storage,
) error {
	file, err := openRdbFile(config)
	defer file.Close()
	if err != nil {
		return err
	}
	rd := bufio.NewReader(file)
	return LoadRdbFromReader(rd, storage)
}

func LoadRdbFromReader(
	rd *bufio.Reader,
	s storage.Storage,
) error {
	p := NewParser(rd)
	values, err := p.ParseRdb()

	for _, v := range values {
		for _, kv := range v {
			if kv.expType == NotExpire {
				s.Set(kv.key, storage.NewStringValue(kv.value))
				continue
			}
			switch kv.expType {
			case NotExpire:
				s.Set(kv.key, storage.NewStringValue(kv.value))
			case Seconds:
				expTime := time.Unix(kv.expValue, 0)
				currentTime := time.Now()
				diff := expTime.Sub(currentTime).Milliseconds()
				s.SetExp(kv.key, storage.NewStringValue(kv.value), int(diff))
			case MilliSeconds:
				expTime := time.UnixMilli(kv.expValue)
				currentTime := time.Now()

				diff := expTime.Sub(currentTime).Milliseconds()
				s.SetExp(kv.key, storage.NewStringValue(kv.value), int(diff))
			}
		}
	}
	return err
}

func (p *Parser) ParseRdb() ([][]ParsedKeyValue, error) {
	err := p.readMagicString()
	if err != nil {
		return nil, err
	}
	err = p.readHeader()
	if err != nil {
		return nil, err
	}
	out := make([][]ParsedKeyValue, 1)
	for {
		isEnd, err := p.isEnd()
		if err != nil {
			return nil, err
		}
		if isEnd {
			break
		}
		selector, err := p.readDbSelector()
		logger.Logger.Debug("Parse rdb selector", logger.String("selector", selector.String()))
		if err != nil {
			return nil, err
		}

		kv, err := p.readDbKeyValues()
		if err != nil {
			return nil, err
		}
		fmt.Println("kv", kv)

		out = append(out, kv)
	}
	return out, nil
}

func openRdbFile(config *config.Config) (*os.File, error) {
	dir := config.GetServerDbDir()
	name := config.GetServerDbFileName()

	rdb, err := os.Open(path.Join(dir, name))

	if err != nil {
		return nil, fmt.Errorf("Error open rdb file: %w", err)
	}

	return rdb, nil
}

func NewParser(rd *bufio.Reader) *Parser {
	return &Parser{
		reader: rd,
	}
}

func (p *Parser) isEnd() (bool, error) {
	op, err := p.reader.Peek(1)
	if err != nil {
		return false, fmt.Errorf("Error reading isEnd: %w", err)
	}

	return op[0] == EOF, nil
}
func (p *Parser) readMagicString() error {
	header := make([]byte, 9)
	_, err := p.reader.Read(header)

	if err != nil {
		return fmt.Errorf("Error reading header: %w", err)
	}

	magicString := string(header[:5])

	if magicString != "REDIS" {
		return fmt.Errorf("Wrong header provided: %v", magicString)
	}

	return nil
}

func (p *Parser) readHeader() error {
	headerFlag, err := p.reader.ReadByte()
	if err != nil {
		return fmt.Errorf("Error reading header op code: %w", err)
	}
	if headerFlag != AUX {
		p.reader.UnreadByte()
		logger.Logger.Error("Wrong op code for rdb header section", logger.String("given", string(headerFlag)), logger.String("waiting", string(AUX)))
		return nil
	}
	headerSection, err := p.reader.ReadBytes(SELECTDB)
	if err != nil {
		return fmt.Errorf("Error reading header up to selectdp opcode: %w", err)
	}
	parser := &Parser{
		reader: bufio.NewReader(bytes.NewReader(headerSection)),
	}

	for {
		key, err := parser.readString()
		if err != nil {
			break
		}
		value, err := parser.readString()
		if err != nil {
			break
		}
		logger.Logger.Debug("read aux info", logger.String("key", key.str), logger.String("value", value.str))
	}
	p.reader.UnreadByte()
	return nil
}

type DbSelector struct {
	hashTableSize    int
	hashTableExpSize int
}

func (d DbSelector) String() string {
	return fmt.Sprintf("hashTableSize: %v, hashTableExpSize: %v", d.hashTableSize, d.hashTableExpSize)
}

func (p *Parser) readDbSelector() (*DbSelector, error) {

	headerFlag, err := p.reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("Error reading header op code: %w", err)
	}
	if headerFlag != SELECTDB {
		p.reader.UnreadByte()
		return nil, errors.New("Wrong opcode for selectdb section")
	}
	// db idx
	_, err = p.reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("Error reading header op code: %w", err)
	}
	resizeDbOpCode, err := p.reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("Error reading resizedb op code: %w", err)
	}
	if resizeDbOpCode != RESIZEDB {
		return nil, errors.New("Wrong opcode for resizedb section")
	}
	hashTableSize, t, err := p.readLengthEncoded()
	if err != nil {
		return nil, fmt.Errorf("Error reading hashtable size: %w", err)
	}
	if t != Int {
		return nil, errors.New("Wrong encoded hashtable size")
	}
	hashTableExpSize, t, err := p.readLengthEncoded()
	if err != nil {
		return nil, fmt.Errorf("Error reading exp hashtable size: %w", err)
	}
	if t != Int {
		return nil, errors.New("Wrong encoded exp hashtable size")
	}

	return &DbSelector{
		hashTableSize:    hashTableSize,
		hashTableExpSize: hashTableExpSize,
	}, nil
}

type ParsedKeyValue struct {
	key      string
	value    string
	expType  ExpType
	expValue int64
	dataType RDBValueTypes
}

func (p *Parser) readDbKeyValues() ([]ParsedKeyValue, error) {
	out := make([]ParsedKeyValue, 0)

	for {
		nxt, err := p.reader.Peek(1)
		if err != nil {
			return nil, fmt.Errorf("Error reading db key values: %w", err)
		}

		if nxt[0] == SELECTDB || nxt[0] == EOF {
			break
		}
		kv, err := p.readKeyValuePair()
		if err != nil {
			return nil, fmt.Errorf("Error reading db key values: %w", err)
		}
		out = append(out, *kv)
	}

	return out, nil
}

func (p *Parser) readKeyValuePair() (*ParsedKeyValue, error) {
	op, err := p.reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("Error while reading rdb kv: %w", err)
	}
	out := &ParsedKeyValue{}
	switch op {
	case EXPIRETIME:
		var value int64
		err := binary.Read(p.reader, binary.LittleEndian, &value)
		if err != nil {
			return nil, fmt.Errorf("Error while reading exp time kv: %w", err)
		}
		out.expType = Seconds
		out.expValue = value
	case EXPIRETIMEMS:
		var value int64
		err := binary.Read(p.reader, binary.LittleEndian, &value)
		if err != nil {
			return nil, fmt.Errorf("Error while reading exp time kv: %w", err)
		}
		out.expType = MilliSeconds
		out.expValue = value
		fmt.Println("read ms exp time", value)
	default:
		p.reader.UnreadByte()
		out.expType = NotExpire
	}

	dataType, err := p.reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("Error reading kv datatype: %w", err)
	}
	switch dataType {
	case byte(StringEncoding):
		out.dataType = StringEncoding
	default:
		return nil, fmt.Errorf("Format is unsuported: %v", dataType)
	}

	key, err := p.readString()

	if err != nil {
		return nil, fmt.Errorf("Error reading key of key value pair: %w", err)
	}

	value, err := p.readString()

	if err != nil {
		return nil, fmt.Errorf("Error reading value of key value pair: %w", err)
	}

	out.key = key.str
	out.value = value.str

	return out, nil
}

func (p *Parser) readLengthEncoded() (int, LengthType, error) {
	lenByte, err := p.reader.ReadByte()

	if err != nil {
		return 0, Int, fmt.Errorf("Error reading length enoded: %w", err)
	}

	switch int(lenByte >> 4) {
	// header byte: 00 000000 header bits: 00 int bits: 000000
	case Int4bit:
		return int(lenByte), Int, nil
	// header bits: 01 int bits: 000000 additional byte fro cunstructing int: 01000000
	case Int14Bit:
		additionalByte, err := p.reader.ReadByte()
		if err != nil {
			return 0, Int, fmt.Errorf("Error reading length enoded: %w", err)
		}
		return int(int((lenByte<<2)>>2)<<8 | int(additionalByte)), Int, nil
	// header byte: 10 000000 read additional 4 bytes and form an int
	case Int32Bit:
		var num int32
		err = binary.Read(p.reader, binary.LittleEndian, &num)
		if err != nil {
			return 0, Int, fmt.Errorf("Error reading length enoded: %w", err)
		}
		return int(num), Int, nil
	// header byte: 11 000000 information about how we need to interpret next bytes is stored in 4 LSB of header byte
	case IntSpecialEncoded:
		return int((lenByte << 2) >> 2), SpecialEncoded, nil
	}

	return 0, Int, errors.New("Unknown format of encoded length")
}

type ParseStringData struct {
	str string
	num int
}

func (p *Parser) readString() (*ParseStringData, error) {
	stringL, lengthType, err := p.readLengthEncoded()
	if err != nil {
		return nil, fmt.Errorf("Error while reading string length: %w", err)
	}
	switch lengthType {
	case Int:
		strBuf := make([]byte, stringL)
		_, err := p.reader.Read(strBuf)
		if err != nil {
			return nil, fmt.Errorf("Error reading str to buf: %w", err)
		}
		return &ParseStringData{
			str: string(strBuf),
			num: -1,
		}, nil
	case SpecialEncoded:
		switch stringL {
		case Bit8Integer:
			var num int8
			err = binary.Read(p.reader, binary.LittleEndian, &num)
			if err != nil {
				return nil, fmt.Errorf("Error reading 8bit int str to buf: %w", err)
			}
			return &ParseStringData{
				num: int(num),
			}, nil
		case Bit16Integer:
			var num int16
			err = binary.Read(p.reader, binary.LittleEndian, &num)
			if err != nil {
				return nil, fmt.Errorf("Error reading 16bit int str to buf: %w", err)
			}
			return &ParseStringData{
				num: int(num),
			}, nil
		case Bit32Integer:
			var num int32
			err = binary.Read(p.reader, binary.LittleEndian, &num)
			if err != nil {
				return nil, fmt.Errorf("Error reading 32bit int str to buf: %w", err)
			}
			return &ParseStringData{
				num: int(num),
			}, nil
		case CompressedString:
			return nil, errors.New("Compressed string is unsuported")
		}
	}

	return nil, errors.New("Unknown string encoded format")
}
