package encoder

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/data_types"
	"github.com/codecrafters-io/redis-starter-go/app/types"
)

func EncodeBulkString(str string) []byte {
	data := datatypes.Data{
		Type:  datatypes.BULK_STRING,
		Value: str,
	}
	return data.Marshall()
}

func EncodeSimpleString(str string) []byte {
	data := datatypes.Data{
		Type:  datatypes.SIMPLE_STRING,
		Value: str,
	}
	return data.Marshall()
}

func EncodeSimpleError(err string) []byte {
	data := datatypes.Data{
		Type:  datatypes.SIMPLE_ERROR,
		Value: err,
	}
	return data.Marshall()
}

func EncodeNull() []byte{
	return []byte(fmt.Sprintf("$-1\r\n"))
}

func EncodeKv(kv types.Kv) string {
	formated := fmt.Sprintf("%v:%v", kv[0], kv[1])
	return formated
}

func EncodeKvs(kvs []types.Kv) string {
	var builder strings.Builder
	builder.Grow(len(kvs) * 2)
	for _, kv := range kvs {
		builder.WriteString(EncodeKv(kv))
		builder.WriteString("\n")
	}
	return builder.String()
}

func EncodeArray(arr []string) []byte {
	return datatypes.ConstructArray(arr).Marshall()
}

var emptyRdb = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

func EncodeRDB() []byte {
	bytes, _ := hex.DecodeString(emptyRdb)
	return []byte(fmt.Sprintf("$%v\r\n%v", len(bytes), string(bytes)))
}
