package replicas_storage

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	// "time"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	"github.com/codecrafters-io/redis-starter-go/app/commands"
	replconfcommand "github.com/codecrafters-io/redis-starter-go/app/commands/repl_conf_command"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	datatypes "github.com/codecrafters-io/redis-starter-go/app/data_types"
	"github.com/codecrafters-io/redis-starter-go/app/encoder"
	"github.com/codecrafters-io/redis-starter-go/app/logger"
	"github.com/codecrafters-io/redis-starter-go/app/reader"
)

type ReplStorage struct {
	repls  []*Repl
	config *config.Config
}

func New(config *config.Config) *ReplStorage {
	return &ReplStorage{
		repls:  []*Repl{},
		config: config,
	}
}

func (this *ReplStorage) ProcessReplicaSync(con net.Conn, replConf *command.Command) {
	tmpReplica := newRepl(con)

	err := this.ProcessReplConfPort(tmpReplica, replConf)
	if err != nil {
		con.Write(encoder.EncodeSimpleError(err.Error()))
		con.Close()
		return
	}
	ok := command.ConstructSimpleOk()
	con.Write(ok.Marshall())
	err = this.ProcessReplConfCapa(tmpReplica)
	if err != nil {
		con.Write(encoder.EncodeSimpleError(err.Error()))
		con.Close()
		return
	}

	con.Write(ok.Marshall())
	err = this.ProcessPsync(tmpReplica)
	if err != nil {
		con.Write(encoder.EncodeSimpleError(err.Error()))
		con.Close()
		return
	}
	masterReplId, err := this.config.GetReplId()
	if err != nil {
		con.Write(encoder.EncodeSimpleError(err.Error()))
		con.Close()
		return
	}
	con.Write(command.ConstructFullResync(masterReplId, 0).Marshall())
	con.Write(encoder.EncodeRDB())

	this.AddReplica(tmpReplica)
	go tmpReplica.StartReadingRoutine()
}

func (this *ReplStorage) ProcessReplConfPort(repl *Repl, replConf *command.Command) error {
	portArg, ok := replConf.Args.GetArgValue(replconfcommand.ListeningPort)
	if !ok {
		return fmt.Errorf("Error reading listeningport: %w", commands.GetUnknowArgError)
	}
	var port int
	err := portArg.ToType(&port)
	if err != nil {
		return err
	}
	repl.ApplyPort(port)
	return nil
}

func (this *ReplStorage) ProcessReplConfCapa(repl *Repl) error {
	_, err := repl.reader.ParseDataType()
	return err
}

func (this *ReplStorage) ProcessPsync(repl *Repl) error {
	_, err := repl.reader.ParseDataType()
	return err
}

func (this *ReplStorage) PropagateCmd(cmd *command.Command) {
	if this.config.GetRole() == config.SLAVE {
		return
	}
	if !cmd.IsWriteCommand() {
		return
	}

	for i, repl := range this.repls {
		logger.Logger.Debug("Propagate command to nth's replice", logger.Int("replica number", i), logger.String("command", string(cmd.Type)))
		repl.PropagateCmd(cmd)
	}
}

func (this *ReplStorage) ProcessCmd(cmd *command.Command) (*datatypes.Data, error) {
	if cmd.Type != command.WAIT {
		return nil, errors.New("unknown command to process by replstorage")
	}
	// base case, we dont have replicas
	if len(this.repls) == 0 {
		return datatypes.ConstructInt(0), nil
	}

	waitArgs, err := cmd.GetWaitArgs()
	if err != nil {
		return nil, err
	}

	replicaNeedCount := min(waitArgs.ReplicaCount, len(this.repls))
	counter := make(chan bool, replicaNeedCount)
	processedAmount := 0

	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < len(this.repls); i++ {
		go this.repls[i].LockReplicaProcessGetAck(i, counter, ctx)
	}
	timeout := time.After(time.Duration(waitArgs.Timeout) * time.Millisecond)

	// loop untill we ack all possible replicas or timeout, cause some replicas can be in sync state already, some are locked, and some will respond with ack after some time
outer:
	for {
		select {
		case <-counter:
			{
				processedAmount++
			}
		case <-timeout:
			{
				break outer
			}
		}
	}
	cancel()

	return datatypes.ConstructInt(processedAmount), nil
}

func (this *ReplStorage) AddReplica(repl *Repl) error {
	this.repls = append(this.repls, repl)
	return nil
}

type Repl struct {
	con                net.Conn
	port               int
	reader             reader.Reader
	mu                 sync.Mutex
	readChan           chan int
	shouldProcessBytes int
	procesedBytes      int
	firstReplConf      bool
}

func newRepl(con net.Conn) *Repl {
	return &Repl{
		con:           con,
		reader:        reader.New(bufio.NewReader(con)),
		readChan:      make(chan int),
		firstReplConf: true,
	}
}

func (this *Repl) PropagateCmd(cmd *command.Command) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	_, err := this.con.Write(cmd.Marshall())
	if err != nil {
		logger.Logger.Error("error propagate to replica", logger.String("error", err.Error()), logger.String("replica", this.String()))
		return err
	}
	this.shouldProcessBytes += cmd.Raw.Len()
	return nil
}

func (this *Repl) String() string {
	return fmt.Sprintf("port: %v, addr: %v", this.port, this.con.RemoteAddr().String())
}

func (this *Repl) IsInSync() bool {
	return this.GetShouldProcessAmount() == 0
}

func (this *Repl) AwaitProcessed() (bool, error) {
	if this.IsInSync() {
		return true, nil
	}
	this.mu.Lock()
	defer this.mu.Unlock()
	cmd := command.ConstructReplConf(replconfcommand.GetAck, "*")
	_, err := this.con.Write(cmd.Marshall())

	if !this.firstReplConf {
		this.shouldProcessBytes += cmd.Raw.Len()
	} else {
		this.firstReplConf = false
	}

	return false, err
}

func (repl *Repl) LockReplicaProcessGetAck(idx int, out chan bool, context context.Context) {
	processed, err := repl.AwaitProcessed()
	if processed {
		logger.Logger.Debug("Replica is in sync state", logger.Int("idx", idx))
		out <- true
		return
	}
	if err != nil {
		logger.Logger.Error("Replica error processed GETACK", logger.String("error", err.Error()))
		return
	}

	select {
	//replica does not respond on getack before timeout, so for preventing locking on readChan for further wait commands, we need to return and unlock readChan
	case <-context.Done():
		logger.Logger.Debug("Replica does not complete GETACK before timeout", logger.Int("idx", idx))
	//replica respond on getack before timeout
	case <-repl.readChan:
		logger.Logger.Debug("Replica processed GETACK", logger.Int("idx", idx), logger.Int("should process", repl.shouldProcessBytes), logger.Int("procesed", repl.procesedBytes))
		out <- true
	}
}

func (this *Repl) StartReadingRoutine() {
	for {
		data, err := this.reader.ParseDataType()
		if err != nil {
			logger.Logger.Error("Error reading from replica", logger.String("error", err.Error()))
			return
		}
		logger.Logger.Info("Recieve data from replica", logger.String("data", data.String()))
		cmd, err := command.DataTypeToCommand(data)
		if err != nil {
			logger.Logger.Error("Error parsing cmd from replica", logger.String("error", err.Error()))
			continue
		}
		this.AddProcessAmout(cmd)
		this.readChan <- this.procesedBytes
	}
}

func (this *Repl) AddProcessAmout(cmd *command.Command) {
	if cmd.Type != command.REPLCONF {
		return
	}
	ackArg, ok := cmd.Args.GetArgValue(replconfcommand.Ack)
	if !ok {
		return
	}
	var ackInt int
	err := ackArg.ToType(&ackInt)
	if err != nil {
		return
	}
	this.procesedBytes = ackInt
}

func (this *Repl) GetShouldProcessAmount() int {
	return this.shouldProcessBytes - this.procesedBytes
}

func (this *Repl) ApplyPort(port int) {
	this.port = port
}
