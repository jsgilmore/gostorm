#gostorm

GoStorm is a Go library that implements the communications protocol required for non-Java languages to communicate as part of a storm topology. In other words, gostorm lets you write Storm spouts and bolts in Go.

GoStorm is still an alpha release under active development. That said, GoStorm implements the complete Storm communications protocol as described in https://github.com/nathanmarz/storm/wiki/Multilang-protocol. GoStorm doesn't just implement the Storm communication protocol, but enforces it. It ensures that all function calls are valid and if invalid function calls are performed, it informs the user of why the call is invalid.

GoStorm also correctly handles the asynchronous behaviour of bolts i.t.o. task Ids that might not be delivered directly after an emission.

Currently, the main purpose of GoStorm is to act as a library for Go spouts and bolts that wish to interact with a Storm topology. The topology definition itself should still be done in Java and a shell spout or bolt is still required for each Go spout or bolt. See the storm-starter WordCountTopology on how to do this.

For testing and evaluation purposes, the GoStorm release also contains its own splitsentence.go implementation that replaces the python splitsentence.py implementation in the storm-starter WordCountTopology (https://github.com/nathanmarz/storm-starter).

The GoStorm library contains a spoutConn and a boltConn that you can use to implement a spout or bolt respectively. The respective interfaces are:
```go
// BoltConn is the interface that implements the possible bolt actions
type BoltConn interface {
    Initialise()
    Context() *Context
    Log(msg string)
    ReadTuple(contentStructs ...interface{}) (meta *TupleMetadata, err error)
    SendAck(id string)
    SendFail(id string)
    Emit(anchors []string, stream string, content ...interface{}) (taskIds []int)
    EmitDirect(anchors []string, stream string, directTask int, contents ...interface{})
}
 
// SpoutConn is the interface that implements the possible spout actions
type SpoutConn interface {
    Initialise()
    Context() *Context
    Log(msg string)
    ReadSpoutMsg() (msg *spoutMsg, err error)
    SendSync()
    Emit(id string, stream string, contents ...interface{}) (taskIds []int)
    EmitDirect(id string, stream string, directTask int, contents ...interface{})
}
```

##Creating a boltConn

Usually, you will be writing Storm bolts, so I shall focus on how to use GoStorm to write bolts. Firstly, you need to create the required boltConn. The most flexible way to do this is:
```go
import (
    "github.com/jsgilmore/gostorm"
    stormenc "github.com/jsgilmore/gostorm/encoding"
    _ "github.com/jsgilmore/gostorm/encoding/json"
    "io"
    "os"
)
 
func getBoltConn(encoding string, reader io.Reader) (boltConn gostorm.BoltConn) {
    inputFactory := stormenc.LookupInput(encoding)
    outputFactory := stormenc.LookupOutput(encoding)
    input := inputFactory.NewInput(reader)
    output := outputFactory.NewOutput(os.Stdout)
    boltConn = gostorm.NewBoltConn(input, output)
    boltConn.Initialise()
}
```

The GoStorm library has implemented various encoding methods to communicate with Storm shell components. Currently implemented encodings are:

1. jsonObject - serialises emissions as json objects and serialises tuples that should be transferred as json objects. The shell component has to unmarshal the json tuple and use Java serialisation to serialise the object as a Kryo object.
2. jsonEncoding - serialises emissions as json objects, but performs an extra json marshalling step on the tuples that should be transferred. This means that tuples are transferred as byte slices and not json objects, which simplifies the process of the shell component having to unmarshal the json byte slice and having to Kryo serialise it.

The GoStorm library makes use of a registry pattern, where each encoding registers itself with GoStorm. The storm developer can then lookup the encoding by calling the stormenc lookup functions. These return factory that can be used to create the required input and output encodings. It should be noted that for an encoding to be available for lookup, it still has to be imported as:  _ "github.com/jsgilmore/gostorm/encoding/json". As shown in the above example.

Input encodings require an io.Reader and output encodings require an io.Writer. This is the interface over which it will try to communicate. For standard Storm functionality, these are os.Stdin and os.Stdout. Always ensure that the boltConn is Initialised, before you start using it. It will panic otherwise. During initialisation, the boltConn connects to Storm and performs various setup functions with Storm (like obtaining the topology configuration and communicating its pid).

The simplest way to create a boltConn is:
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
func (this *myStruct) Run() {
    for {
        var id string
        msg := &myMsgType{}
        meta, err := this.boltConn.ReadTuple(&id, msg)
        if err == io.EOF {
            this.Close()
            return
        } else if err != nil {
            panic(err)
        }
        
        this.Event(id, msg)
        this.boltConn.SendAck(meta.Id)
    }
}
```

The above code shows an example of the run loop. It continuously reads messages from Storm and handles those messages. To read messages from Storm, you have to call the ReadTuple function and pass it empty non-nil structures for GoStorm to populate. In Lines 3 and 4, these empty objects are created and then passed to ReadTuple in Line 5. The EOF error checked at Line 6 may occur if your testing Storm with some finite input buffer. You usually don't have to include this check.

After receiving objects from ReadTuple, the Event message handler function is called. The message handler logic is contained in the Event function. To enable you to chain Storm spout and bolts together during integration testing, your bolt should implement the Consumer interface:
```go
type Consumer interface {
    Event(msgs ...interface{})
}
```
You can then create GoStorm mock spouts and bolts and give them your bolt objects as the consumers in the chain.

#Spout usage

SpoutConns can be created similarly to boltConns.

Since spouts are synchronous, a spout has to wait for next, ack or fail messages from Storm before it may send messages. After a spout has sent the messages it wishes to send, it has to send a sync message to Storm. The sync message signals that a spout will not send any more messages until the next "next", "ack" or "fail" message is received.

Therefore, to emit one tuple for each progress message as a custom spout (mySpoutImpl), implement the following Emit function:
```go
func (this *mySpoutImpl) Emit(msg interface{}) {
	spoutMsg, err := this.spoutConn.ReadMsg()
	if err != nil {
		panic(err)
	}
	
	switch spoutMsg.Command {
	case "next":
		this.spoutConn.Emit(generateId(), "", msg)
  	case "ack":
    		this.spoutConn.Emit(generateId(), "", msg)
    		handleAck(spoutMsg)
  	case "fail":
    		this.spoutConn.Emit(generateId(), "", msg)
    		handleFail(spoutMsg)
	default:
		panic("Unknown command received from Storm")
	}
	this.spoutConn.SendSync()
}
```
