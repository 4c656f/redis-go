package storage

import (
	"sync"
	"time"
)

type Storage interface {
	Get(key string) (string, error)
	SetExp(key string, val string, px int) error
	Delete(key string) error
	Set(key string, val string) error
	Lock()
	UnLock()
}

type StorageImpl struct {
	mappStorage map[string]StorageValue
	mu          sync.Mutex
}

func New() *StorageImpl {
	return &StorageImpl{
		mappStorage: make(map[string]StorageValue),
	}
}

func (this *StorageImpl) Get(key string) (string, error) {
	this.Lock()
	val := this.mappStorage[key]
	isExp := val.CheckIsExp()
	this.UnLock()
	if isExp {
		this.Delete(key)
		return "", nil
	}
	return val.GetVal(), nil
}

func (this *StorageImpl) Set(key string, val string) error {
	this.Lock()
	defer this.UnLock()
	this.mappStorage[key] = StorageValue{
		value:      val,
		px:         -1,
		creationTs: time.Now(),
	}
	return nil
}

func (this *StorageImpl) Delete(key string) error {
	this.Lock()
	defer this.UnLock()
	delete(this.mappStorage, key)
	return nil
}

func (this *StorageImpl) SetExp(key string, val string, px int) error {
	this.Lock()
	defer this.UnLock()
	this.mappStorage[key] = StorageValue{
		value:      val,
		px:         px,
		creationTs: time.Now(),
	}
	return nil
}

func (this *StorageImpl) Lock() {
	this.mu.Lock()
}

func (this *StorageImpl) UnLock() {
	this.mu.Unlock()
}

type StorageValue struct {
	value      string
	px         int
	creationTs time.Time
}

func (this *StorageValue) CheckIsExp() bool {
	expInterval := this.px
	if expInterval == -1 {
		return false
	}
	creationTs := this.creationTs
	cur := time.Now()
	diff := cur.Sub(creationTs)
	diffMs := int(diff.Milliseconds())
	return diffMs > expInterval
}

func (this *StorageValue) GetVal() string {

	return this.value
}
