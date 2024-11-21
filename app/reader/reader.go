// exmple description
package reader

import (
	"bufio"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/data_types"
	"github.com/codecrafters-io/redis-starter-go/app/logger"
	"strconv"
)

type Reader struct {
	rd *bufio.Reader
}

// creates new reader
func New(rd *bufio.Reader) Reader {
	return Reader{rd}
}

// reads and parse commands from data stream
func (this *Reader) ParseDataType() (data *datatypes.Data, err error) {
	initType, err := this.rd.ReadByte()
	if err != nil {
		return nil, err
	}

	dataType, ok := datatypes.GetDataTypeAcordType(initType)

	if !ok {
		return nil, fmt.Errorf("Unexpected commands dataType given: %v", string(initType))
	}
	data, raw, err := this.ReadStreamAcordDataType(dataType)
	data.Raw = make([]byte, len(raw)+1)
	data.Raw[0] = initType
	copy(data.Raw[1:], raw)
	return data, err
}

func (this *Reader) ReadStreamAcordDataType(dataType datatypes.DataTypeEnum) (data *datatypes.Data, raw []byte, err error) {
	switch dataType {
	case datatypes.ARRAY:
		data, raw, err = this.ReadArray()
	case datatypes.SIMPLE_ERROR:
		data, raw, err = this.ReadSimpleError()
	case datatypes.SIMPLE_STRING:
		data, raw, err = this.ReadSimpleString()
	case datatypes.BULK_STRING:
		data, raw, err = this.ReadString()
	case datatypes.INT:
		data, raw, err = this.ReadInt()
	default:
		return nil, nil, fmt.Errorf("Unknown datatype: %v", dataType)
	}
	return
}

func (this *Reader) ReadSimpleString() (data *datatypes.Data, raw []byte, err error) {
	commandRaw, _, err := this.rd.ReadLine()
	if err != nil {

		return nil, nil, err
	}
	raw = append(commandRaw, datatypes.CLFR...)
	return &datatypes.Data{
		Value: string(commandRaw),
		Type:  datatypes.SIMPLE_STRING,
	}, raw, nil
}

func (this *Reader) ReadRdb() error {
	initType, err := this.rd.ReadByte()
	dataType, _ := datatypes.GetDataTypeAcordType(initType)
	if dataType != datatypes.BULK_STRING {
		return fmt.Errorf("Wrong datatype: %v", string(dataType))
	}
	stringLen, _, err := this.ReadLen()
	if err != nil {
		return err
	}
	readed := make([]byte, stringLen)
	n, err := this.rd.Read(readed)
	if err != nil {
		return err
	}
	if n != stringLen {
		return fmt.Errorf("Read less amount of bytes from rdb transfer")
	}
	return nil
}

func (this *Reader) ReadSimpleError() (data *datatypes.Data, raw []byte, err error) {
	commandRaw, _, err := this.rd.ReadLine()

	if err != nil {
		return nil, nil, err
	}
	raw = append(commandRaw, datatypes.CLFR...)
	return &datatypes.Data{
		Value: string(commandRaw),
		Type:  datatypes.SIMPLE_ERROR,
	}, raw, nil
}

func (this *Reader) ReadLen() (len int, raw []byte, err error) {
	lenRaw, _, err := this.rd.ReadLine()

	if err != nil {
		logger.Logger.Error("Error reading line:", logger.String("error", err.Error()))
		return 0, nil, err
	}

	readedLen, err := strconv.Atoi(string(lenRaw))

	if err != nil {
		return 0, nil, err
	}

	return readedLen, lenRaw, nil
}

func (this *Reader) ReadString() (data *datatypes.Data, raw []byte, err error) {
	stringLen, rawLen, err := this.ReadLen()

	if err != nil {
		return nil, nil, err
	}

	stringRaw, _, err := this.rd.ReadLine()

	if err != nil {
		return nil, nil, err
	}
	rawRet := []byte{}
	rawRet = append(rawRet, rawLen...)
	rawRet = append(rawRet, datatypes.CLFR...)
	rawRet = append(rawRet, stringRaw...)
	rawRet = append(rawRet, datatypes.CLFR...)
	return &datatypes.Data{
		Type:  datatypes.BULK_STRING,
		Value: string(stringRaw[:stringLen]),
	}, rawRet, nil
}

func (this *Reader) ReadInt() (data *datatypes.Data, raw []byte, err error) {
	intRaw, _, err := this.rd.ReadLine()
	if err != nil {
		return nil, nil, err
	}
	rawRet := []byte{}
	rawRet = append(rawRet, intRaw...)
	rawRet = append(rawRet, datatypes.CLFR...)
	return &datatypes.Data{
		Type:  datatypes.BULK_STRING,
		Value: string(intRaw),
	}, rawRet, nil
}

func (this *Reader) ReadArray() (data *datatypes.Data, raw []byte, err error) {
	arrayLen, rawLen, err := this.ReadLen()

	if err != nil {
		return nil, nil, err
	}

	data = &datatypes.Data{
		Type:   datatypes.ARRAY,
		Values: []*datatypes.Data{},
	}
	rawRet := []byte{}
	rawRet = append(rawRet, rawLen...)
	rawRet = append(rawRet, datatypes.CLFR...)
	for i := 0; i < arrayLen; i++ {
		nxt, err := this.ParseDataType()
		if err != nil {
			return nil, nil, err
		}
		data.Values = append(data.Values, nxt)
		rawRet = append(rawRet, nxt.Raw...)
	}

	return data, rawRet, nil
}
