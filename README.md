# gostorm

[![Build Status](https://drone.io/github.com/jsgilmore/gostorm/status.png)](https://drone.io/github.com/jsgilmore/gostorm/latest)

[godocs](https://godoc.org/github.com/jsgilmore/gostorm)

GoStorm is a Go library that implements the communications protocol required for non-Java languages to communicate as part of a storm topology. In other words, gostorm lets you write Storm spouts and bolts in Go.

GoStorm also correctly handles the asynchronous behaviour of bolts i.t.o. task Ids that might not be delivered directly after an emission.

Currently, the main purpose of GoStorm is to act as a library for Go spouts and bolts that wish to interact with a Storm topology. The topology definition itself should still be done in Java and a shell spout or bolt is still required for each Go spout or bolt. See the storm-starter WordCountTopology on how to do this.

For testing and evaluation purposes, the GoStorm release also contains its own splitsentence.go implementation that replaces the python splitsentence.py implementation in the [storm-starter](https://github.com/nathanmarz/storm-starter) WordCountTopology. GoStorm also contains mock output collector implementations that allows a developer to test her code code, before having to submit it as part of a Storm cluster.

GoStorm implements (and enforces) the Storm [multilang protocol](https://github.com/nathanmarz/storm/wiki/Multilang-protocol). Apart from implementing the multilang JSON protocol that is used by Storm shell components, GoStorm also implements a protocol buffer binary encoding scheme for improved performance. The protocol buffer encoding requires the Storm [protoshell](https://github.com/jsgilmore/protoshell) multilang serialiser and Storm 0.9.2 or later.

GoStorm itself requires Storm 0.10.0 or later.

## Encoding

GoStorm implements various encoding schemes with varying levels of performance:

1. jsonobject
2. jsonencoded
2. hybrid
3. protobuf

The jsonobject scheme encodes all objects as JSON objects. This means that in the Storm shell component, Java serialisation is used by Kryo to serialise your objects. This scheme is pretty slow and should generally be avoided.

The jsonencoded scheme first marshals a JSON object and then sends all objects as byte slices. In the Java shell component, Kryo is able to efficiently marshall the byte slices.

The hybrid scheme also sends byte slices, but the user objects are expected to be protocol buffer objects. These protocol buffer byte slices are still sent in the Storm multilang JSON envelope, so the existing Storm shell components can be used. This scheme has the highest performance that is still compatible with the Storm shell components.

The protobuf scheme is a pure protocol buffer encoding and requires specialised Storm ProtoShell components. These ProtoShell components have already been implemented and I'll paste a link soon. The protobuf encoding is a binary encoding scheme that transmits varints followed by byte slices. No text encodings or "end" strings, which makes it more compact.

I would suggest starting with the jsonencoded scheme and benchmarking your application. If the throughput doesn't suit your needs, start converting your project to use protocol buffers. This allows for the hybrid scheme to be used, without requiring any changes to Storm. For best performance, the protobuf encoding can be used, but this requires some changes in the Storm cluster's configuration.

## Bolts

This section will describe how to write bolts using the GoStorm library.

### Creating a bolt

To create a bolt, you just have to create a Go "object" that implements the Bolt interface:
```go
type Bolt interface {
    FieldsFactory
    Execute(meta stormmsg.BoltMsgMeta, fields ...interface{})
    Prepare(context *stormmsg.Context, collector OutputCollector)
    Cleanup()
}

type FieldsFactory interface {
    Fields() []interface{}
}
```

Prepare can be used to setup a bolt and will be called once, before a bolt receives any messages. Prepare supplies the bolt with the topology context and the output collector, which the bolt can use to emit messages.

A bolt receives messages with the Execute method. BoltMsgMeta contains information about the received message, namely: id, comp, stream, task. The fields are the tuple fields (objects) that were emitted by the input component.

Cleanup is called if the topology completes. This will only happen during testing, for finite input streams.

The fields factory declares the message types that the bolt expects to receive. In other words, these fields must match the field types of the execute method. Specifically, GoStorm uses these empty objects to marshal received objects into.

To write a bolt, import the following:
```go
import (
    "github.com/jsgilmore/gostorm"
    stormmsg "github.com/jsgilmore/gostorm/messages"
)
```

The gostorm import contains the spout and bolt interfaces and the messages import contains the required gostorm message types.

### Running a bolt

All that remains is for you to write a main method for your bolt. The main method will run the bolt, specify the encoding that might be used and state whether destination task ids are required (more on that later). It will typically look something like this:
```go
package main

import (
    "github.com/jsgilmore/gostorm"
    _ "github.com/jsgilmore/gostorm/encodings"
)

func main() {
    encoding := "jsonEncoded"
    needTaskIds := false
    myBolt := NewMyBolt()
    gostorm.RunBolt(myBolt, encoding, needTaskIds)
}
```

The gostorm import contains the RunBolt function. The encodings import imports all GoStorm encodings and allows any of them to be specified in the RunBolt method. This also allows you to use a Go flag and specify the encoding to use at runtime.

### Emitting tuples

To emit tuples (objects) to another bolt, the bolt output collector is used:
```go
type OutputCollector interface {
    SendAck(id string)
    SendFail(id string)
    Emit(anchors []string, stream string, fields ...interface{}) (taskIds []int32)
    EmitDirect(anchors []string, stream string, directTask int64, fields ...interface{})
}
```

SendAck acks a received message. SendFail fails a received message.

Emit emits a tuple (set of fields). Emit tuple returns the destination task IDs to which the message was emitted if this option was set in the RunBolt function, otherwise it returns nil.

The parameters required by the Emit function are:
1. List of tuple IDs to anchor this emission to.
2. The output stream to emit the tuple on.
3. A list of objects that should be emitted.

Tuple emissions may be anchored to received tuples. This specifies that the current emission is as a result of the earlier received tuple. An emission may be anchored to multiple received tuples (say you joined some tuples to create a compound tuple), which is why a list is required. An emission does not have to be anchored, in which case the parameter can be set to nil. Anchoring emissions to received tuples will have the effect that the original emission at the spout will only be acked after all resulting emissions have been acked. If any resultant emission is failed, the spout will immediately receive a failure notification from Storm. If an emission fails to be acked within some timeout period (30s default), the spout originating the emission will also receive a failure notification.

If the bolt has a single output stream, the "default" or the empty ("") string can be used.

The EmitDirect function can be used to emit a tuple directly to a task.

### Message unions

A union message type is always emitted (myBoltEvent). The union message contains pointers to all the message types that our bolt can emit. Whenever a message is emitted, it is first placed in the union message structure. This way, the receiver always knows what message type to cast to and can then check for a non-nil element in the union message.

The last two objects in the Emit call are the contents of the message that will be transferred to the receiving bolt in the form that they are Emitted here. It is possible to specify any number of objects. In the above example, we specified a ket and a message. The first field will be used to group tuples on as part of a fieldsGrouping that will be shown when the topology definition is shown.

To ensure the "at least once" processing semantics of Storm, every tuple that is receive should be acknowledged, either by an Ack or a Fail. This is done by the SendAck and SendFail functions that is part of the boltConn interface. To enable Storm to build up its ack directed acyclic graph (DAG): no emission may be anchored to a tuple that has already been acked. The Storm topology will panic if this occurs.

## Spouts

This section will describe how to write spouts using the GoStorm library.

### Creating spouts

Similarly to bolts in GoStorm, to create a spout, the Spout interface should be implemented:
```go
type Spout interface {
	NextTuple()
	Acked(id string)
	Failed(id string)
	Exit()
	Open(context *stormmsg.Context, collector SpoutOutputCollector)
}
```

When a spouts starts, GoStorm will call Open on it to provide it with the Storm context and a spout output collector.

Spouts in Storm are synchronous, which means a spout has to wait for NextTuple, Acked, or Failed to be called on it, before it may emit messages. GoStorm will never call any spout (or bolt) functions concurrently.

The Acked and Failed functions inform a spout that the tuple emited with the specified ID was acked or failed respectively. When Acked or Failed is called on a spout, emissions are possible, but it should be kept in mind that always emitting tuples when Storm informs a spout of another tuples state may lead to runaway behaviour.

A spout can emit tuples when NextTuple is called on it. It may emit any number of tuples, but the developer should keep in mind that emitting multiple tuples will increase message latency in the topology.

### Running a spout

Very similarly to running bolts, a main method has to be created to run the spout, specify the encoding that might be used and state whether destination task ids are required. It will typically look something like this:
```go
package main

import (
    "github.com/jsgilmore/gostorm"
    _ "github.com/jsgilmore/gostorm/encodings"
)

func main() {
    encoding := "jsonEncoded"
    needTaskIds := false
    mySpout := NewMySpout()
    gostorm.RunSpout(mySpout, encoding, needTaskIds)
}
```

### Emitting tuples

```go
type SpoutOutputCollector interface {
    Emit(id string, stream string, fields ...interface{}) (taskIds []int32)
    EmitDirect(id string, stream string, directTask int64, fields ...interface{})
}
```

A spout can emit tuples using the Emit or EmitDirect functions of the spout output collector.

The parameters required by the Emit function are:
1. The id of the tuple to emit.
2. The output stream to emit the tuple on.
3. A list of objects that should be emitted.

The ID with which the tuple is emitted will be the ID provided in the Acked and Failed functions.

The output stream and object tuple list is the same as with bolt emissions.

## Testing without Storm

It's possible to link up GoStorm spouts and bolts using the mockOutputCollector implementations of GoStorm. This does not require a running Storm cluster or indeed anything other than the GoStorm library. Mock output collectors is a basic way of stringing some Storm components together, while manually calling Execute on a bolt to get the topology running. I am hopefull of obtaining a GoStorm local mode controbution within the next few months. The GoStorm local mode will allow spouts and bolts to be connected in a single process and acks and fails are also handled correctly.

Because mock collectors do not connect to a real Storm topology and because the mock collector implementation in GoStorm is still fairly immature, there are some important differences (and shortcomings) between mock components and real components that should be taken into account when testing:
