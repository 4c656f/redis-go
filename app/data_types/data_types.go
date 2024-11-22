package datatypes

import (
	"fmt"
	"strconv"
	"strings"
)

type DataTypeEnum byte

const (
	ARRAY         DataTypeEnum = '*'
	BULK_STRING   DataTypeEnum = '$'
	SIMPLE_STRING DataTypeEnum = '+'
	SIMPLE_ERROR  DataTypeEnum = '-'
	INT           DataTypeEnum = ':'
	NULL          DataTypeEnum = '!'
)

var commandType = map[byte]DataTypeEnum{
	'*': ARRAY,
	'$': BULK_STRING,
	'+': SIMPLE_STRING,
	'-': SIMPLE_ERROR,
	':': INT,
}

var CLFR = "\r\n"

func GetDataTypeAcordType(inputType byte) (dataType DataTypeEnum, ok bool) {
	dataType, ok = commandType[inputType]
	return
}

type Data struct {
	Type   DataTypeEnum
	Value  string
	Values []*Data
	Raw    []byte
}

func (d *Data) IsArg() bool {
	if d != nil && d.Type == BULK_STRING {
		return true
	}
	return false
}

func (d Data) CounstructMarshall() []byte {
	switch d.Type {
	case SIMPLE_ERROR:
		return d.marshallSimpleError()
	case SIMPLE_STRING:
		return d.marshallSimpleString()
	case BULK_STRING:
		return d.marshallBulkString()
	case ARRAY:
		return d.marshallArray()
	case INT:
		return d.marshallInt()
	case NULL:
		return d.marshallNull()
	}
	return nil
}

func (d Data) Marshall() []byte {
	if d.Raw != nil {
		return d.Raw
	}
	raw := d.CounstructMarshall()
	d.Raw = raw
	return raw
}

func (d Data) marshallSimpleString() []byte {
	out := make([]byte, 0)
	out = append(out, byte(SIMPLE_STRING))
	out = append(out, d.Value...)
	out = append(out, CLFR...)
	return out
}

func (d Data) marshallInt() []byte {
	out := make([]byte, 0)
	out = append(out, byte(INT))
	out = append(out, d.Value...)
	out = append(out, CLFR...)
	return out
}

func (d Data) marshallNull() []byte {
	return []byte(fmt.Sprintf("$-1\r\n"))
}

func (d Data) marshallBulkString() []byte {
	out := make([]byte, 0)
	out = append(out, byte(BULK_STRING))

	strLen := len(d.Value)
	out = append(out, strconv.Itoa(strLen)...)

	out = append(out, CLFR...)
	out = append(out, d.Value...)
	out = append(out, CLFR...)
	return out
}

func (d Data) marshallArray() []byte {
	out := make([]byte, 0)
	out = append(out, byte(ARRAY))
	strLen := len(d.Values)
	out = append(out, strconv.Itoa(strLen)...)
	out = append(out, CLFR...)
	for _, v := range d.Values {
		out = append(out, v.Marshall()...)
	}
	return out
}

func (d Data) marshallSimpleError() []byte {
	out := make([]byte, 0)
	out = append(out, byte(SIMPLE_ERROR))
	out = append(out, d.Value...)
	out = append(out, CLFR...)
	return out
}

func ConstructSimpleError(error string) *Data {
	return &Data{
		Type:  SIMPLE_ERROR,
		Value: error,
	}
}

func ConstructBulkString(value string) *Data {
	return &Data{
		Type:  BULK_STRING,
		Value: value,
	}
}

func ConstructSimpleString(value string) *Data {
	return &Data{
		Type:  SIMPLE_STRING,
		Value: value,
	}
}

func ConstructArray(arr []string) *Data {
	values := make([]*Data, len(arr))

	for i, str := range arr {
		values[i] = &Data{
			Value: str,
			Type:  BULK_STRING,
		}
	}

	data := Data{
		Type:   ARRAY,
		Value:  "",
		Values: values,
	}

	return &data
}

func ConstructArrayFromData(arr []*Data) *Data {
	data := Data{
		Type:   ARRAY,
		Value:  "",
		Values: arr,
	}
	return &data
}

func ConstructInt(num int) *Data {
	return &Data{
		Type:  INT,
		Value: strconv.Itoa(num),
	}
}

func ConstructNull() *Data {
	return &Data{
		Type: NULL,
	}
}

func (d Data) Len() int {
	if d.Raw != nil {
		return len(d.Raw)
	}
	return len(d.Marshall())
}

func (d Data) String() string {
	var builder strings.Builder

	// Print the current node's type and value
	builder.WriteString(fmt.Sprintf("Type: %v\n", string(d.Type)))

	// Print Value if it's not empty
	if d.Value != "" {
		builder.WriteString(fmt.Sprintf("Value: %v ", d.Value))
	}

	// Print Raw bytes if present
	if len(d.Raw) > 0 {
		builder.WriteString(fmt.Sprintf("Raw: %s ", string(d.Raw)))
	}

	// Recursively print child values if present
	if len(d.Values) > 0 {
		builder.WriteString("Values:\n")
		for i, val := range d.Values {
			if val != nil {
				// Indent nested values
				lines := strings.Split(val.String(), "\n")
				for _, line := range lines {
					if line != "" {
						builder.WriteString(fmt.Sprintf("  %s\n", line))
					}
				}
			} else {
				builder.WriteString(fmt.Sprintf("  [%d]: nil\n", i))
			}
		}
	}

	return strings.TrimSuffix(builder.String(), "\n")
}
