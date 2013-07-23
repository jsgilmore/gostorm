package storm

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
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
	Initialise()
	Log(msg string)
	ReadTuple(contentStructs ...interface{}) (tuple *TupleMetadata)
	ReadRawTuple() (tuple *TupleMsg)
	SendAck(id string)
	SendFail(id string)
	Emit(anchors []string, stream string, content ...interface{}) (taskIds []int)
	EmitDirect(anchors []string, stream string, directTask int, contents ...interface{})
}

// SpoutConn is the interface that implements the possible spout actions
type SpoutConn interface {
	Initialise()
	Log(msg string)
	ReadMsg() (msg *spoutMsg)
	SendSync()
	Emit(id string, stream string, contents ...interface{}) (taskIds []int)
	EmitDirect(id string, stream string, directTask int, contents ...interface{})
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
	reader *bufio.Reader
	conf   *confImpl
}

// readBytes reads data from stdin into the struct provided.
func (this *stormConnImpl) readMsg(msg interface{}) {
	// Read a single json record from the input file
	data, err := this.reader.ReadBytes('\n')
	log.Printf("Data: %s", data)
	if err != nil {
		panic(err)
	}

	//Read the end delimiter
	end, err := this.reader.ReadBytes('\n')
	log.Printf("End: %s", end)
	if err != nil {
		panic(err)
	}

	// Remove the newline character
	data = bytes.Trim(data, "\n")

	err = json.Unmarshal(data, msg)
	if err != nil {
		panic(err)
	}
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

// Initialise set the storm input reader to the specified file
// descriptor, reads the topology configuration for Storm and reports
// the pid to Storm
func (this *stormConnImpl) Initialise() {
	this.reader = bufio.NewReader(os.Stdin)
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

// Log sends a log message that will be logged by Storm
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

// NewBoltConn returns a Storm bolt connection that a Go bolt can use to communicate with Storm
func NewBoltConn() BoltConn {
	boltConn := &boltConnImpl{
		stormConnImpl: newStormConn(bolt),
	}
	return boltConn
}

type boltConnImpl struct {
	*stormConnImpl
}

type TupleMetadata struct {
	Id     string `json:"id"`
	Comp   string `json:"comp"`
	Stream string `json:"stream"`
	Task   int    `json:"task"`
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
type TupleMsg struct {
	*TupleMetadata
	Contents [][]byte `json:"tuple"`
}

// ReadTuple reads a tuple from Storm
// It ensures that Storm was first initialised. If an input file is
// used, eof might be returned, which has to be handled by the calling
// application.
func (this *boltConnImpl) ReadTuple(contentStructs ...interface{}) (metadata *TupleMetadata) {
	if this.conf == nil {
		panic("Attempting to read from uninitialised Storm connection")
	}

	tuple := &TupleMsg{}
	this.readMsg(tuple)
	if len(tuple.Contents) != len(contentStructs) {
		panic("Number of output structs does not match the number of received content objects")
	}

	for i, contentStruct := range contentStructs {
		err := json.Unmarshal(tuple.Contents[i], contentStruct)
		if err != nil {
			panic(err)
		}
	}

	return tuple.TupleMetadata
}

func (this *boltConnImpl) ReadRawTuple() (tuple *TupleMsg) {
	if this.conf == nil {
		panic("Attempting to read from uninitialised Storm connection")
	}

	tuple = &TupleMsg{}
	this.readMsg(tuple)
	return tuple
}

// SendAck acks the received message id
// SendAck has to be called after an emission anchored to the acked id,
// otherwise Storm will report an error.
func (this *boltConnImpl) SendAck(id string) {
	msg := &spoutMsg{
		Command: "ack",
		Id:      id,
	}
	this.sendMsg(msg)
}

// SendFail reports that the message with the given Id failed
// No emission should be anchored to a failed message Id
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
	Command  string   `json:"command"`
	Anchors  []string `json:"anchors"`
	Stream   string   `json:"stream,omitempty"`
	Contents [][]byte `json:"tuple"`
}

type boltDirectEmission struct {
	Command  string   `json:"command"`
	Anchors  []string `json:"anchors"`
	Stream   string   `json:"stream,omitempty"`
	Task     int      `json:"task"`
	Contents [][]byte `json:"tuple"`
}

func contentsConvert(contents ...interface{}) [][]byte {
	var contentList [][]byte
	for _, content := range contents {
		data, err := json.Marshal(content)
		if err != nil {
			panic(err)
		}
		contentList = append(contentList, data)
	}
	return contentList
}

// Emit emits a tuple with the given array of interface{}s as values,
// anchored to the given array of taskIds, sent out on the given stream.
// A stream value of "" or "default" can be used to denote the default stream
// The function returns a list of taskIds to which the message was sent.
func (this *boltConnImpl) Emit(anchors []string, stream string, contents ...interface{}) (taskIds []int) {
	emission := boltEmission{
		Command:  "emit",
		Anchors:  anchors,
		Stream:   stream,
		Contents: contentsConvert(contents...),
	}
	this.sendMsg(emission)

	// Bolt are asynchronous, so we might not receive a list of
	// taskIds here, but another tuple for processing instead.
	// Lets fix that when it happens
	this.readMsg(&taskIds)
	return taskIds
}

// EmitDirect emits a tuple with the given array of interface{}s as values,
// anchored to the given array of taskIds, sent out on the given stream,
// to the given taskId.
// The topology should have been configured for direct transmission
// for this call to work.
// A stream value of "" or "default" can be used to denote the default stream
// The function returns a list of taskIds to which the message was sent.
func (this *boltConnImpl) EmitDirect(anchors []string, stream string, directTask int, contents ...interface{}) {
	emission := boltDirectEmission{
		Command:  "emit",
		Anchors:  anchors,
		Stream:   stream,
		Task:     directTask,
		Contents: contentsConvert(contents...),
	}
	this.sendMsg(emission)
}

//-------------------------------------------------------------------
// Spout
//-------------------------------------------------------------------

// NewSpoutConn returns a Storm spout connection that a Go spout can use to communicate with Storm
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

// ReadMsg reads a message from Storm.
// The message read can be either a next, ack or fail message.
// A check is performed to verify that Storm has been initialised.
func (this *spoutConnImpl) ReadMsg() (msg *spoutMsg) {
	if this.conf == nil {
		panic("Attempting to read from uninitialised Storm connection")
	}
	this.readyToSend = true

	msg = &spoutMsg{}
	this.readMsg(msg)

	if msg.Command == "next" {
		this.tuplesSent = false
	}
	return msg
}

// SendSync sends a sync message to Storm.
// After a sync message is sent, it is not possible for a spout to
// emit a message before a ReadMsg has been performed. This is to
// enforce the synchronous behaviour of a spout as required by Storm.
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
	Command  string   `json:"command"`
	Id       string   `json:"id"`
	Stream   string   `json:"stream,omitempty"`
	Contents [][]byte `json:"tuple"`
}

type spoutDirectEmission struct {
	Command  string   `json:"command"`
	Id       string   `json:"id"`
	Stream   string   `json:"stream,omitempty"`
	Task     int      `json:"task"`
	Contents [][]byte `json:"tuple"`
}

// Emit emits a tuple with the given array of interface{}s as values,
// with the given taskId, sent out on the given stream.
// A stream value of "" or "default" can be used to denote the default stream
// The function returns a list of taskIds to which the message was sent.
func (this *spoutConnImpl) Emit(id string, stream string, contents ...interface{}) (taskIds []int) {
	if !this.readyToSend {
		panic("Spout not ready to send")
	}
	this.tuplesSent = true

	emission := spoutEmission{
		Command:  "emit",
		Id:       id,
		Stream:   stream,
		Contents: contentsConvert(contents...),
	}
	this.sendMsg(emission)

	// Upon an indirect emit, storm replies with a list of chosen destination task Ids
	this.readMsg(&taskIds)
	return taskIds
}

// EmitDirect emits a tuple with the given array of interface{}s as values,
// with the given taskId, sent out on the given stream, to the given taskId.
// The topology should have been configured for direct transmission
// for this call to work.
// A stream value of "" or "default" can be used to denote the default stream
// The function returns a list of taskIds to which the message was sent.
func (this *spoutConnImpl) EmitDirect(id string, stream string, directTask int, contents ...interface{}) {
	if !this.readyToSend {
		panic("Spout not ready to send")
	}
	this.tuplesSent = true

	emission := spoutDirectEmission{
		Command:  "emit",
		Id:       id,
		Stream:   stream,
		Task:     directTask,
		Contents: contentsConvert(contents...),
	}
	this.sendMsg(emission)
}
