#gostorm

GoStorm is a Go library that implements the communications protocol required for non-Java languages to communicate as part of a storm topology. In other words, gostorm lets you write Storm spouts and bolts in Go.

GoStorm also correctly handles the asynchronous behaviour of bolts i.t.o. task Ids that might not be delivered directly after an emission.

Currently, the main purpose of GoStorm is to act as a library for Go spouts and bolts that wish to interact with a Storm topology. The topology definition itself should still be done in Java and a shell spout or bolt is still required for each Go spout or bolt. See the storm-starter WordCountTopology on how to do this.

For testing and evaluation purposes, the GoStorm release also contains its own splitsentence.go implementation that replaces the python splitsentence.py implementation in the storm-starter WordCountTopology (https://github.com/nathanmarz/storm-starter). GoStorm also contains mock conn implementations that allows a developer to test her code code, before having to submit it as part of a Storm cluster.

GoStorm implements the complete Storm communications protocol as described in https://github.com/nathanmarz/storm/wiki/Multilang-protocol. GoStorm doesn't just implement the Storm communication protocol, but enforces it. It ensures that all function calls are valid and if invalid function calls are performed, it informs the user of why the call is invalid.

Apart from implementing the multilang JSON protocol that is used by Storm shell components, GoStorm also implements a pure protocol buffer encoding scheme for improved performance.

##Encoding
GoStorm implements various encoding schemes with varying levels of performance:
1. jsonobject
2. jsonencoded
2. hybrid
3. protobuf

The jsonobject scheme encodes all objects as JSON objects. This means that in the Storm shell component, Java serialisation is used by Kryo to serialise your objects. This scheme is pretty slow and should generally be avoided.

The jsonencoded scheme first marshals a JSON object and then sends all objects as byte slices. In the Java shell component, Kryo is able to efficiently marshall the byte slices.

The hybrid scheme also sends byte slices, but the user objects are expected to be protocol buffer objects. These protocol buffer byte slices are still sent in the Storm multilang JSON envelope, so the existing Storm shell components can be used. This scheme has the highest performance that is still compatible with the Storm shell components.

The protobuf scheme is a pure protocol buffer encoding and requires specialised Storm ProtoShell components. These ProtoShell components have already been implemented and I'll paste a link soon. The protobuf encoding is a binary encoding scheme that transmits varints followed by byte slices. No text encodings or "end" strings, which makes it more compact.

I would suggest starting with the jsonencoded scheme and benchmarking your application. If the throughput doesn't suit your needs, starts converting your project to use protocol buffers. And see which of the protocol buffers schemes meet your needs.

##Interfaces
The GoStorm library contains a spoutConn and a boltConn that you can use to implement a spout or bolt respectively. The respective interfaces are:
```go
// BoltConn is the interface that implements the possible bolt actions
type BoltConn interface {
	Initialise()
	Context() *messages.Context
	Log(msg string)
	ReadTuple(contentStructs ...interface{}) (meta *messages.TupleMetadata, err error)
	SendAck(id string)
	SendFail(id string)
	Emit(anchors []string, stream string, content ...interface{}) (taskIds []int32)
	EmitDirect(anchors []string, stream string, directTask int64, contents ...interface{})
}

// SpoutConn is the interface that implements the possible spout actions
type SpoutConn interface {
	Initialise()
	Context() *messages.Context
	Log(msg string)
	ReadSpoutMsg() (command, id string, err error)
	SendSync()
	Emit(id string, stream string, contents ...interface{}) (taskIds []int32)
	EmitDirect(id string, stream string, directTask int64, contents ...interface{})
}
```

##Creating a boltConn

Usually, you will be writing Storm bolts, so I shall focus on how to use GoStorm to write bolts. Firstly, you need to create the required boltConn. The most flexible way to do this is:
```go
import (
    "github.com/jsgilmore/gostorm"
    _ "github.com/jsgilmore/gostorm/encoding/hybrid"
    _ "github.com/jsgilmore/gostorm/encoding/json"
    _ "github.com/jsgilmore/gostorm/encoding/protobuf"
    "io"
)

func getBoltConn(encoding string, reader io.Reader, writer io.Writer) (boltConn gostorm.BoltConn) {
	boltConn = gostorm.LookupBoltConn(encoding, reader, writer)
	boltConn.Initialise()
	return
}
```

The GoStorm library makes use of a registry pattern, where each encoding registers itself with GoStorm. The storm developer can use the lookup functions to retrieve spout and bolt conns with the required encoding. It should be noted that for an encoding to be available for lookup, it still has to be imported as:  _ "github.com/jsgilmore/gostorm/encoding/json". As shown in the above example. You only have to import the encodings that you will be using.

Input encodings require an io.Reader and output encodings require an io.Writer. This is the interface over which it will try to communicate. For standard Storm functionality, these are os.Stdin and os.Stdout. Always ensure that the boltConn is Initialised, before you start using it. It will panic otherwise. During initialisation, the boltConn connects to Storm and performs various setup functions with Storm (like obtaining the topology configuration and communicating its pid).

A more explicit way to create a BoltConn is:
```go
import (
    "github.com/jsgilmore/gostorm"
    stormjson "github.com/jsgilmore/gostorm/encoding/json"
    "os"
)

func getBoltConn() (boltConn gostorm.BoltConn) {
    input := stormjson.NewJsonEncodedInput(os.Stdin)
    output := stormjson.NewJsonEncodedOutput(os.Stdout)
    boltConn := gostorm.NewBoltConn(input, output)
    boltConn.Initialise()
}
```

In this example, the encoding, input and output interfaces are fixed at compile time. Note that the spoutConn creation process is identical to the boltConn creation process.

##The Bolt Run() loop
After the boltConn is created, your bolt can start using. This is usually in the form of a Run() loop that is part of your Go "object":
```go
func (this *myBolt) Run() {
    for {
        msg1 := &MyFirstMsg{}
        msg2 := &MySecondMsg{}
        meta, err := this.boltConn.ReadTuple(msg1, msg2)
        if err != nil {
            panic(err)
        }
        this.Event(msg1, msg2)
        this.boltConn.SendAck(meta.Id)
    }
}
```

The above code shows an example of the run loop. It continuously reads messages from Storm and handles those messages. To read messages from Storm, you have to call the ReadTuple function and pass it empty non-nil structures for GoStorm to populate. Take not that you can emit and receive any number of messages.

After receiving objects from ReadTuple, the Event message handler function is called. The message handler logic is contained in the Event function. To enable you to chain Storm spout and bolts together during integration testing, your bolt should implement the Consumer interface:
```go
type Consumer interface {
    Event(msgs ...interface{})
}
```
You can then create GoStorm mock spouts and bolts and give them your bolt objects as the consumers in the chain.

##Emitting tuples
To emit tuples (objects) to another bolt, the Emit and EmitDirect functions can be used:
```go
import (
    "github.com/jsgilmore/gostorm/messages"
)

func (this *myBolt) emitRecord(meta *messages.TupleMetadata, key string, msg *myMsgType) {
	event := &myBoltEvent{
		Message: msg,
	}
	this.boltConn.Emit([]string{meta.Id}, "default", key, event)
	this.boltConn.SendAck(meta.Id)
}
```

The parameters required by the Emit function are:
1. List of tuple IDs to anchor this emission to.
2. The output stream to emit the tuple on.
3. A list of objects that should be emitted.

Tuple emissions may be anchored to received tuples. This specifies that the current emission is as a result of the earlier received tuple. An emission may be anchored to multiple received tuples (say you joined some tuples to create a compound tuple), which is why a list is required. Anchoring emissions to received tuples will have the effect that the original emission at the spout will only be acked after all resulting emissions have been acked. If any resultant emission is fail, the spout will immediately receive a failure notification from Storm. If an emission fails to be acked within some timeout period (30s default), the spout originating the emission will also receive a failure notification.

If you bolt only has one output stream, you can just use the "default" string, or the empty ("") string.

A union message type is always emitted (myBoltEvent). The union message contains pointers to all the message types that our bolt can emit. Whenever a message is emitted, it is first placed in the union message structure. This way, the received always knows what message type to cast to and can then check for a non-nil element in the union message.

The last two objects in the Emit call are the contents of the message that will be transferred to the receiving bolt in the form that they are Emitted here. It is possible to specify any number of objects. In the above example, we specified a ket and a message. The first field will be used to group tuples on as part of a fieldsGrouping that will be shown when the topology definition is shown.

To ensure the "at least once" processing semantics of Storm, every tuple that is receive should be acknowledged, either by an Ack or a Fail. This is done by the SendAck and SendFail functions that is part of the boltConn interface. To enable Storm to build up its ack directed acyclic graph (DAG): no emission may be anchored to a tuple that has already been acked. The Storm topology will panic if this occurs.

#Spout usage

SpoutConns can be created similarly to boltConns.

Since spouts are synchronous, a spout has to wait for next, ack or fail messages from Storm before it may send messages. After a spout has sent the messages it wishes to send, it has to send a sync message to Storm. The sync message signals that a spout will not send any more messages until the next "next", "ack" or "fail" message is received.

Therefore, to emit one tuple for each progress message as a custom spout (mySpoutImpl), implement the following Emit function:
```go
func (this *mySpoutImpl) Emit(msg interface{}) {
    command, id, err := this.spoutConn.ReadSpoutMsg()
    if err != nil {
        panic(err)
    }

    switch command {
    case "next":
        this.emit()
    case "ack":
        this.handleAck(id)
    case "fail":
        this.handleFail(id)
    default:
        panic("Unknown command received from Storm")
    }
    this.spoutConn.SendSync()
}
```
