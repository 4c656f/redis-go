package config

import (
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type RoleEnum string

const (
	MASTER RoleEnum = "master"
	SLAVE           = "slave"
)

// config will hold struct that impliments this method, when command info is executed
// we fire GetInfo method on needed parts and combine them into list
type ConfigInfoPart interface {
	GetInfo() []types.Kv
}

type ReplicationConfig interface {
	ConfigInfoPart
	GetSlaveInfo() *SlaveOf
	GetRole() RoleEnum
	GetReplId() (string, error)
}

// config impl contains all part of information from server, to replication
type Config struct {
	server      serverConfig
	replication ReplicationConfig
}

// server part of config
type serverConfig struct {
	port       uint16
	dir        string
	dbFileName string
}

func (this *serverConfig) GetServerPort() uint16 {
	return this.port
}

// slave part of replication config, contains master host and port
type SlaveOf struct {
	host string
	port uint16
}

// replication part of config, impliments part interface
type replicationConfig struct {
	role         RoleEnum
	slaveOf      *SlaveOf
	masterConfig *mastterReplicationConfig
}

// if replica is master replicationConfig will have this structure ref
type mastterReplicationConfig struct {
	master_replid string
}

func (this *replicationConfig) GetRole() RoleEnum {
	return this.role
}

func (this *replicationConfig) GetReplId() (string, error) {
	if this.GetRole() != MASTER {
		return "", fmt.Errorf("Error: get replId on nonmaster replica")
	}
	return this.masterConfig.master_replid, nil
}

// applyes masterReplication info if current replica is master, fires on server boot
func (this *replicationConfig) applyReplId() {
	if this.GetRole() != MASTER {
		return
	}
	this.masterConfig = &mastterReplicationConfig{
		master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
	}
}

// returns replication info part of config for INFO command
func (this *replicationConfig) GetInfo() []types.Kv {
	info := []types.Kv{
		{"role", string(this.GetRole())},
	}
	info = append(info, this.getMasterInfo()...)
	return info
}

func (this *replicationConfig) GetSlaveInfo() *SlaveOf {
	return this.slaveOf
}

func (this *SlaveOf) GetPort() uint16 {
	return this.port
}

func (this *SlaveOf) GetHost() string {
	return this.host
}

// returns master replication info if replica is master
func (this *replicationConfig) getMasterInfo() []types.Kv {
	info := []types.Kv{}
	if this.GetRole() != MASTER || this.masterConfig == nil {
		return nil
	}
	info = append(info, types.Kv{"master_replid", this.masterConfig.master_replid})
	info = append(info, types.Kv{"master_repl_offset", "0"})
	return info
}

// construct new config, parse flags from cli, applies replication info
func New() (*Config, error) {
	flags := NewConfigFlags()
	flag.Parse()

	role, replicaOf, err := parseReplicaOf(*flags.role)
	if err != nil {
		return nil, err
	}
	replicationConifg := replicationConfig{
		role:    role,
		slaveOf: replicaOf,
	}
	replicationConifg.applyReplId()

	config := &Config{
		server: serverConfig{
			port:       uint16(*flags.port),
			dbFileName: *flags.dbFileName,
			dir:        *flags.dir,
		},
		replication: &replicationConifg,
	}

	return config, nil
}

func (this *Config) GetServerPort() uint16 {
	return this.server.GetServerPort()
}

func (this *Config) GetServerDbDir() string {
	return this.server.dir
}

func (this *Config) GetServerDbFileName() string {
	return this.server.dbFileName
}

func (this *Config) GetAllInfo() []types.Kv {
	allInfo := []types.Kv{}
	allInfo = append(allInfo, this.replication.GetInfo()...)
	return allInfo
}

func (this *Config) GetReplId() (string, error) {
	return this.replication.GetReplId()
}

func (this *Config) GetReplicationInfo() []types.Kv {
	return this.replication.GetInfo()
}

func (this *Config) GetReplicationSlaveInfo() *SlaveOf {
	return this.replication.GetSlaveInfo()
}

func (this *Config) GetRole() RoleEnum {
	return this.replication.GetRole()
}

func (this *Config) ShouldRespondOnCommand() bool {
	return this.replication.GetRole() == MASTER
}

type ConfigFlags struct {
	port       *int
	role       *string
	dir        *string
	dbFileName *string
}

func NewConfigFlags() ConfigFlags {
	return ConfigFlags{
		port:       flag.Int("port", 6379, "defines port"),
		role:       flag.String("replicaof", "", "defines is server are replica or master"),
		dir:        flag.String("dir", "", "defines rdb file path"),
		dbFileName: flag.String("dbfilename", "", "defines rdb file name"),
	}
}

func parseMasterHostAndPort(replicaOfFlag string) (host string, port uint16, err error) {
	splited := strings.Split(replicaOfFlag, " ")
	if len(splited) != 2 {
		return "", 0, fmt.Errorf("Unexpexted format of replicaOfFlag flag")
	}
	host = splited[0]
	parsedPort, err := strconv.Atoi(splited[1])
	if err != nil {
		return "", 0, err
	}
	port = uint16(parsedPort)

	return
}

func parseReplicaOf(replicaOfFlag string) (role RoleEnum, masterInfo *SlaveOf, err error) {
	if replicaOfFlag != "" {
		host, port, err := parseMasterHostAndPort(replicaOfFlag)
		if err != nil {
			return SLAVE, nil, err
		}
		return SLAVE, &SlaveOf{host, port}, nil
	}
	return MASTER, nil, nil
}
