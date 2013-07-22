package gostorm

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

type mode int

const (
	spout mode = iota
	bolt
)

// BoltConn is the interface that implements the possible bolt actions
type BoltConn interface {
	Initialise(fi *os.File)
	Log(msg string)
	ReadTuple() (tuple *tupleMsg, eof bool)
	SendAck(id string)
	SendFail(id string)
	Emit(contents []interface{}, anchors []string, stream string) (taskIds []int)
	EmitDirect(contents []interface{}, anchors []string, stream string, directTask int)
}

// SpoutConn is the interface that implements the possible spout actions
type SpoutConn interface {
	Initialise(fi *os.File)
	Log(msg string)
	ReadMsg() (msg *spoutMsg, eof bool)
	SendSync()
	Emit(contents []interface{}, id string, stream string) (taskIds []int)
	EmitDirect(contents []interface{}, id string, stream string, directTask int)
}

// newStormConn creates a new generic Storm connection
// This connection must be embedded in either a spout or bolt
func newStormConn(mode mode) *stormConnImpl {
	stormConn := &stormConnImpl{
		mode: mode,
	}
	return stormConn
}

// stormConnImpl represents the common functions that both a bolt and spout are capable of
type stormConnImpl struct {
	mode   mode
	input  *os.File
	reader *bufio.Reader
	conf   *confImpl
}

// readBytes reads data from stdin into the struct provided.
func (this *stormConnImpl) readMsg(msg interface{}) (eof bool) {
	// Read a single json record from the input file
	data, err := this.reader.ReadBytes('\n')
	log.Printf("Data: %s", data)
	if err == io.EOF {
		return true
	} else if err != nil {
		panic(err)
	}

	//Read the end delimiter
	end, err := this.reader.ReadBytes('\n')
	log.Printf("End: %s", end)
	if err == io.EOF {
		eof = true
	} else if err != nil {
		panic(err)
	} else {
		eof = false
	}

	// Remove the newline character
	data = bytes.Trim(data, "\n")

	err = json.Unmarshal(data, msg)
	if err != nil {
		panic(err)
	}
	return eof
}

// sendMsg sends the contents of a known Storm message to Storm
func (this *stormConnImpl) sendMsg(msg interface{}) {
	data, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(os.Stdout, string(data))
	// Storm requires that every message be suffixed with an "end" string
	fmt.Fprintln(os.Stdout, "end")
}

type topologyContext struct {
	TaskComponent map[string]string `json:"task->component"`
	TaskId        int               `json:"taskid"`
}

//{
//    "conf": {
//        "topology.message.timeout.secs": 3,
//        // etc
//    },
//    "context": {
//        "task->component": {
//            "1": "example-spout",
//            "2": "__acker",
//            "3": "example-bolt"
//        },
//        "taskid": 3
//    },
//    "pidDir": "..."
//}
type confImpl struct {
	Conf    map[string]interface{} `json:"conf"`
	Context topologyContext        `json:"context"`
	PidDir  string                 `json:"pidDir"`
}

func (this *stormConnImpl) readConfig() (conf *confImpl) {
	conf = &confImpl{}
	this.readMsg(conf)
	return conf
}

// {"pid": 1234}
type pidMsg struct {
	Pid int `json:"pid"`
}

func (this *stormConnImpl) reportPid() {
	msg := pidMsg{
		Pid: os.Getpid(),
	}
	this.sendMsg(msg)

	pidDir := this.conf.PidDir
	if len(pidDir) > 0 {
		pidDir += "/"
	}
	// Write an empty file with the pid, which storm can use to kill our process
	pidFile, err := os.Create(pidDir + strconv.Itoa(os.Getpid()))
	if err != nil {
		panic(err)
	}
	pidFile.Close()
}

func (this *stormConnImpl) TestInput(filename string) {
	if this.conf != nil {
		panic("Cannot change input stream after Storm has already been initialised.")
	}
	fi, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	this.input = fi
}

func (this *stormConnImpl) Initialise(fi *os.File) {
	this.input = fi
	this.reader = bufio.NewReader(fi)
	// Receive the topology config for this storm connection
	this.conf = this.readConfig()

	this.reportPid()
}

//{
//	"command": "log",
//	// the message to log
//	"msg": "hello world!"
//}
type logMsg struct {
	Command string `json:"command"`
	Msg     string `json:"msg"`
}

func (this *stormConnImpl) Log(text string) {
	msg := logMsg{
		Command: "log",
		Msg:     text,
	}
	this.sendMsg(msg)
}

//-------------------------------------------------------------------
// Bolt
//-------------------------------------------------------------------

func NewBoltConn() BoltConn {
	boltConn := &boltConnImpl{
		stormConnImpl: newStormConn(bolt),
	}
	return boltConn
}

type boltConnImpl struct {
	*stormConnImpl
}

//{
//    // The tuple's id - this is a string to support languages lacking 64-bit precision
//	"id": "-6955786537413359385",
//	// The id of the component that created this tuple
//	"comp": "1",
//	// The id of the stream this tuple was emitted to
//	"stream": "1",
//	// The id of the task that created this tuple
//	"task": 9,
//	// All the values in this tuple
//	"tuple": ["snow white and the seven dwarfs", "field2", 3]
//}
type tupleMsg struct {
	Id       string        `json:"id"`
	Comp     string        `json:"comp"`
	Stream   string        `json:"stream"`
	Task     int           `json:"task"`
	Contents []interface{} `json:"tuple"`
}

func (this *boltConnImpl) ReadTuple() (tuple *tupleMsg, eof bool) {
	if this.conf == nil {
		panic("Attempting to read from uninitialised Storm connection")
	}

	tuple = &tupleMsg{}
	eof = this.readMsg(tuple)
	return tuple, eof
}

func (this *boltConnImpl) SendAck(id string) {
	msg := &spoutMsg{
		Command: "ack",
		Id:      id,
	}
	this.sendMsg(msg)
}

func (this *boltConnImpl) SendFail(id string) {
	msg := &spoutMsg{
		Command: "fail",
		Id:      id,
	}
	this.sendMsg(msg)
}

//{
//	"command": "emit",
//	// The ids of the tuples this output tuples should be anchored to
//	"anchors": ["1231231", "-234234234"],
//	// The id of the stream this tuple was emitted to. Leave this empty to emit to default stream.
//	"stream": "1",
//	// If doing an emit direct, indicate the task to send the tuple to
//	"task": 9,
//	// All the values in this tuple
//	"tuple": ["field1", 2, 3]
//}
type boltEmission struct {
	Command  string        `json:"command"`
	Anchors  []string      `json:"anchors"`
	Stream   string        `json:"stream,omitempty"`
	Contents []interface{} `json:"tuple"`
}

type boltDirectEmission struct {
	Command  string        `json:"command"`
	Anchors  []string      `json:"anchors"`
	Stream   string        `json:"stream,omitempty"`
	Task     int           `json:"task"`
	Contents []interface{} `json:"tuple"`
}

func (this *boltConnImpl) Emit(contents []interface{}, anchors []string, stream string) (taskIds []int) {
	emission := boltEmission{
		Command:  "emit",
		Anchors:  anchors,
		Stream:   stream,
		Contents: contents,
	}
	this.sendMsg(emission)

	// Bolt are asynchronous, so we might not receive a list of
	// taskIds here, but another tuple for processing instead.
	// Lets fix that when it happens
	this.readMsg(&taskIds)
	return taskIds
}

func (this *boltConnImpl) EmitDirect(contents []interface{}, anchors []string, stream string, directTask int) {
	emission := boltDirectEmission{
		Command:  "emit",
		Anchors:  anchors,
		Stream:   stream,
		Task:     directTask,
		Contents: contents,
	}
	this.sendMsg(emission)
}

//-------------------------------------------------------------------
// Spout
//-------------------------------------------------------------------

func NewSpoutConn() SpoutConn {
	spoutConn := &spoutConnImpl{
		stormConnImpl: newStormConn(spout),
	}
	return spoutConn
}

type spoutConnImpl struct {
	readyToSend bool
	tuplesSent  bool
	*stormConnImpl
}

// {"command": "next"}
// {"command": "sync"}
// {"command": "ack", "id": "1231231"}
// {"command": "fail", "id": "1231231"}
type spoutMsg struct {
	Command string `json:"command"`
	Id      string `json:"id,omitempty"`
}

func (this *spoutConnImpl) ReadMsg() (msg *spoutMsg, eof bool) {
	if this.conf == nil {
		panic("Attempting to read from uninitialised Storm connection")
	}
	this.readyToSend = true

	msg = &spoutMsg{}
	eof = this.readMsg(msg)

	if msg.Command == "next" {
		this.tuplesSent = false
	}
	return msg, eof
}

func (this *spoutConnImpl) SendSync() {
	this.readyToSend = false
	// Storm requires that a spout sleeps for "a small amount of
	// time" after receiving next, before sending sync, if no
	// tuples were emitted. We sleep for 1 millisecond, the same
	// as ISpout's default wait strategy
	if !this.tuplesSent {
		time.Sleep(time.Millisecond)
	}

	msg := &spoutMsg{
		Command: "sync",
	}
	this.sendMsg(msg)
}

//{
//	"command": "emit",
//	// The id for the tuple. Leave this out for an unreliable emit. The id can
//    // be a string or a number.
//	"id": "1231231",
//	// The id of the stream this tuple was emitted to. Leave this empty to emit to default stream.
//	"stream": "1",
//	// If doing an emit direct, indicate the task to send the tuple to
//	"task": 9,
//	// All the values in this tuple
//	"tuple": ["field1", 2, 3]
//}
type spoutEmission struct {
	Command  string        `json:"command"`
	Id       string        `json:"id"`
	Stream   string        `json:"stream,omitempty"`
	Contents []interface{} `json:"tuple"`
}

type spoutDirectEmission struct {
	Command  string        `json:"command"`
	Id       string        `json:"id"`
	Stream   string        `json:"stream,omitempty"`
	Task     int           `json:"task"`
	Contents []interface{} `json:"tuple"`
}

func (this *spoutConnImpl) Emit(contents []interface{}, id string, stream string) (taskIds []int) {
	if !this.readyToSend {
		panic("Spout not ready to send")
	}
	this.tuplesSent = true

	emission := spoutEmission{
		Command:  "emit",
		Id:       id,
		Stream:   stream,
		Contents: contents,
	}
	this.sendMsg(emission)

	// Upon an indirect emit, storm replies with a list of chosen destination task Ids
	this.readMsg(&taskIds)
	return taskIds
}

func (this *spoutConnImpl) EmitDirect(contents []interface{}, id string, stream string, directTask int) {
	if !this.readyToSend {
		panic("Spout not ready to send")
	}
	this.tuplesSent = true

	emission := spoutDirectEmission{
		Command:  "emit",
		Id:       id,
		Stream:   stream,
		Task:     directTask,
		Contents: contents,
	}
	this.sendMsg(emission)
}
