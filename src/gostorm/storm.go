// GoStorm is a library that allows you to write Storm spouts and bolts in Go 
package gostorm

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	Initialise(reader io.Reader, writer io.Writer)
	Log(msg string)
	ReadTuple(contentStructs ...interface{}) (meta *TupleMetadata, err error)
	ReadRawTuple() (tuple *RawTupleMsg, err error)
	SendAck(id string)
	SendFail(id string)
	Emit(anchors []string, stream string, content ...interface{}) (taskIds []int)
	EmitDirect(anchors []string, stream string, directTask int, contents ...interface{})
}

// SpoutConn is the interface that implements the possible spout actions
type SpoutConn interface {
	Initialise(reader io.Reader, writer io.Writer)
	Log(msg string)
	ReadMsg() (msg *spoutMsg, err error)
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
	mode    mode
	reader  *bufio.Reader
	writer  io.Writer
	context *Context
}

// readBytes reads data from stdin into the struct provided.
func (this *stormConnImpl) readMsg(msg interface{}) error {
	// Read a single json record from the input file
	data, err := this.reader.ReadBytes('\n')
	if err != nil {
		panic(err)
	}

	//Read the end delimiter
	_, err = this.reader.ReadBytes('\n')
	if err != nil {
		panic(err)
	}

	// Remove the newline character
	data = bytes.Trim(data, "\n")

	err = json.Unmarshal(data, msg)
	if err != nil {
		return err
	}
	return nil
}

// sendMsg sends the contents of a known Storm message to Storm
func (this *stormConnImpl) sendMsg(msg interface{}) {
	data, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(this.writer, string(data))
	// Storm requires that every message be suffixed with an "end" string
	fmt.Fprintln(this.writer, "end")
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
type Context struct {
	Conf     map[string]interface{} `json:"conf"`
	Topology topologyContext        `json:"context"`
	PidDir   string                 `json:"pidDir"`
}

func (this *stormConnImpl) readContext() (context *Context, err error) {
	context = &Context{}
	err = this.readMsg(context)
	if err != nil {
		return nil, err
	}
	return context, nil
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

	pidDir := this.context.PidDir
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
func (this *stormConnImpl) Initialise(reader io.Reader, writer io.Writer) {
	this.reader = bufio.NewReader(reader)
	this.writer = writer
	// Receive the topology config for this storm connection
	var err error
	this.context, err = this.readContext()
	if err != nil {
		panic(fmt.Sprintf("Storm failed to initialise: %v", err))
	}

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

func newTupleMetadata(id, comp, stream string, task int) *TupleMetadata {
	meta := &TupleMetadata{
		Id:     id,
		Comp:   comp,
		Stream: stream,
		Task:   task,
	}
	return meta
}

type TupleMetadata struct {
	Id     string `json:"id"`
	Comp   string `json:"comp"`
	Stream string `json:"stream"`
	Task   int    `json:"task"`
}

func newTupleMsg(id, comp, stream string, task int) *TupleMsg {
	msg := &TupleMsg{
		TupleMetadata: newTupleMetadata(id, comp, stream, task),
	}
	return msg
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

type RawTupleMsg struct {
	*TupleMetadata
	Contents []interface{} `json:"tuple"`
}

func (this *TupleMsg) AddContent(contentStruct interface{}) {
	data, err := json.Marshal(contentStruct)
	if err != nil {
		panic(err)
	}
	this.Contents = append(this.Contents, data)
}

func (this *TupleMsg) Content(index int, contentStruct interface{}) error {
	err := json.Unmarshal(this.Contents[index], contentStruct)
	if err != nil {
		return err
	} else {
		return nil
	}
}

// ReadTuple reads a tuple from Storm
// It ensures that Storm was first initialised. If an input file is
// used, eof might be returned, which has to be handled by the calling
// application.
func (this *boltConnImpl) ReadTuple(contentStructs ...interface{}) (metadata *TupleMetadata, err error) {
	if this.context == nil {
		return nil, errors.New("Attempting to read from uninitialised Storm connection")
	}

	tuple := &TupleMsg{}
	err = this.readMsg(tuple)
	if err != nil {
		return nil, err
	}
	if len(tuple.Contents) != len(contentStructs) {
		return nil, errors.New("Number of output structs does not match the number of received content objects")
	}

	for i, contentStruct := range contentStructs {
		err = tuple.Content(i, contentStruct)
		if err != nil {
			return nil, err
		}
	}

	return tuple.TupleMetadata, nil
}

func (this *boltConnImpl) ReadRawTuple() (tuple *RawTupleMsg, err error) {
	if this.context == nil {
		return nil, errors.New("Attempting to read from uninitialised Storm connection")
	}

	tuple = &RawTupleMsg{}
	err = this.readMsg(tuple)
	if err != nil {
		return nil, err
	}
	return tuple, nil
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

func contentsAppend(contents ...interface{}) []interface{} {
	var contentList []interface{}
	for _, content := range contents {
		contentList = append(contentList, content)
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
		Contents: contentsAppend(contents...),
	}
	this.sendMsg(emission)

	// Bolt are asynchronous, so we might not receive a list of
	// taskIds here, but another tuple for processing instead.
	// Lets fix that when it happens
	err := this.readMsg(&taskIds)
	if err != nil {
		panic(err)
	}
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
		Contents: contentsAppend(contents...),
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
func (this *spoutConnImpl) ReadMsg() (msg *spoutMsg, err error) {
	if this.context == nil {
		return nil, errors.New("Attempting to read from uninitialised Storm connection")
	}
	this.readyToSend = true

	msg = &spoutMsg{}
	err = this.readMsg(msg)
	if err != nil {
		return nil, err
	}
	if msg.Command == "next" {
		this.tuplesSent = false
	}
	return msg, nil
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
		Contents: contentsAppend(contents...),
	}
	this.sendMsg(emission)

	// Upon an indirect emit, storm replies with a list of chosen destination task Ids
	err := this.readMsg(&taskIds)
	if err != nil {
		panic(err)
	}
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
		Contents: contentsAppend(contents...),
	}
	this.sendMsg(emission)
}
