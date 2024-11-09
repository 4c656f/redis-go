package replicas_storage

import (
	"bufio"
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/encoder"
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

	con.Write(command.GetSimpleOk().GetRawOrMarshall())
	err = this.ProcessReplConfCapa(tmpReplica)
	if err != nil {
		con.Write(encoder.EncodeSimpleError(err.Error()))
		con.Close()
		return
	}

	con.Write(command.GetSimpleOk().GetRawOrMarshall())
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

	con.Write(command.GetFullResync(masterReplId, 0).GetRawOrMarshall())
	con.Write(encoder.EncodeRDB())

	this.AddReplica(tmpReplica)
}

func (this *ReplStorage) ProcessReplConfPort(repl *Repl, replConf *command.Command) error {
	portConf, err := replConf.GetReplConfPortArgs()
	if err != nil {
		return err
	}
	repl.ApplyPort(portConf.Port)
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
	for _, repl := range this.repls {
		repl.PropagateCmd(cmd)
	}
}

func (this *ReplStorage) AddReplica(repl *Repl) error {
	this.repls = append(this.repls, repl)
	return nil
}

type Repl struct {
	con    net.Conn
	port   int
	reader reader.Reader
}

func newRepl(con net.Conn) *Repl {
	return &Repl{
		con:    con,
		reader: reader.New(bufio.NewReader(con)),
	}
}

func (this *Repl) PropagateCmd(cmd *command.Command) error {
	_, err := this.con.Write(cmd.Raw.GetRawOrMarshall())
	return err
}

func (this *Repl) ApplyPort(port int) {
	this.port = port
}
