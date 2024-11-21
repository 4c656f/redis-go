package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/logger"
	"github.com/codecrafters-io/redis-starter-go/app/stream"
)

type DataTypes string

const (
	String = "string"
	Stream = "stream"
)

type Storage interface {
	Get(key string) (string, error)
	GetEntrie(key string) (*StorageValue, bool)
	GetType(key string) DataTypes
	GetKeys() []string
	KeysLen() int
	Set(key string, val StorageValue) error
	SetExp(key string, val StorageValue, px int) error
	Delete(key string) error
	Lock()
	UnLock()
}

type StorageImpl struct {
	values map[string]StorageValue
	exp    map[string]StorageExpValue
	mu     sync.Mutex
}

func New() *StorageImpl {
	return &StorageImpl{
		values: make(map[string]StorageValue),
		exp:    make(map[string]StorageExpValue),
	}
}

func (this *StorageImpl) Get(key string) (string, error) {
	this.Lock()
	val, ok := this.values[key]
	expMark, expOk := this.exp[key]
	this.UnLock()
	if !ok {
		return "", nil
	}
	if expOk && expMark.CheckIsExp() {
		this.Delete(key)
		return "", nil
	}

	return val.ToString()
}

func (this *StorageImpl) GetEntrie(key string) (*StorageValue, bool) {
	this.Lock()
	val, ok := this.values[key]
	expMark, expOk := this.exp[key]
	this.UnLock()
	if !ok {
		return nil, false
	}
	if expOk && expMark.CheckIsExp() {
		this.Delete(key)
		return nil, false
	}
	return &val, true
}

func (this *StorageImpl) Set(key string, val StorageValue) error {
	this.Lock()
	this.values[key] = val
	this.UnLock()
	return nil
}

func (this *StorageImpl) Delete(key string) error {
	this.Lock()
	defer this.UnLock()
	delete(this.values, key)
	return nil
}

func (this *StorageImpl) SetExp(key string, val StorageValue, px int) error {
	this.Lock()
	defer this.UnLock()
	this.values[key] = val
	logger.Logger.Debug("set exp", logger.String("key", key), logger.Int("px", px), logger.String("time", time.Now().Add(time.Duration(px)*time.Millisecond).String()))
	this.exp[key] = StorageExpValue{
		validUntil: time.Now().Add(time.Duration(px) * time.Millisecond),
	}
	return nil
}

func (this *StorageImpl) GetType(key string) DataTypes {
	this.Lock()
	defer this.UnLock()
	v, ok := this.values[key]
	if ok {
		return v.dataType
	}
	return ""
}

func (this *StorageImpl) Lock() {
	this.mu.Lock()
}

func (this *StorageImpl) UnLock() {
	this.mu.Unlock()
}

func (this *StorageImpl) KeysLen() int {
	return len(this.values)
}

func (this *StorageImpl) GetKeys() []string {
	out := make([]string, 0, this.KeysLen())

	for k := range this.values {
		out = append(out, k)
	}

	return out
}

type StorageExpValue struct {
	validUntil time.Time
}

type StorageValue struct {
	value    string
	dataType DataTypes
	stream   stream.Stream
}

func (this *StorageValue) ToString() (string, error) {
	if this.dataType != String {
		return "", fmt.Errorf("Wrong string data type cast: current type: %v", this.dataType)
	}
	return this.value, nil
}

func (this *StorageValue) ToStream() (stream.Stream, error) {
	if this.dataType != Stream {
		return nil, fmt.Errorf("Wrong stream data type cast: current type: %v", this.dataType)
	}
	return this.stream, nil
}

func NewStringValue(v string) StorageValue {
	return StorageValue{
		value:    v,
		dataType: String,
	}
}

func NewStreamValue(s stream.Stream) StorageValue {
	return StorageValue{
		stream:   s,
		dataType: Stream,
	}
}

func (this *StorageExpValue) CheckIsExp() bool {
	logger.Logger.Debug("cmp valid", logger.Int("cmp", time.Now().Compare(this.validUntil)))
	return time.Now().Compare(this.validUntil) == 1
}
