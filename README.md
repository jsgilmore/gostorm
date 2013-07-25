gostorm
=======

GoStorm is a Go library that implements the communications protocol required for non-Java languages to communicate as part of a storm topology. In other words, gostorm lets you write Storm spouts and bolts in Go.

GoStorm is still an alpha release under active development. That said, GoStorm implements the complete Storm communications protocol as described in https://github.com/nathanmarz/storm/wiki/Multilang-protocol. GoStorm doesn't just implement the Storm communication protocol, but enforces it. It ensures that all functions calls are valid and if invalid function calls are performed, it informs the user of why the call is invalid.

Currently, the main purpose of GoStorm is to act as a library for Go spouts and bolts that wish to interact with a Storm topology. The topology definition itself should still be done in Java and a shell spout or bolt is still required for each Go spout or bolt. See the storm-starter WordCountTopology on how to do this.

For testing and evaluation purposes, the GoStorm release also contains its own splitsentence.go implementation that replaces the python splitsentence.py implementation in the storm-starter WordCountTopology (https://github.com/nathanmarz/storm-starter).

The GoStorm API provides two types of Storm connections: SpoutConn and BoltConn. Both of the underlying structures implement some of the more finicky parts of the Storm communications protocol, so you don't have to worry about them.

Bolt usage
==========

To create your own Storm bolt connection:
```go
boltConn := storm.NewBoltConn()
spoutConn.Initialise(os.Stdin, os.Stdout)
```

The Initialise function received the configuration from storm and reports your pid to storm. Storm communicates over stdin and stdout. It is possible to redirect the input and output for testing purposes. See storm_test.go.

In a bolt, you'll usually have something like this:
```go
var msg myStruct
for {
  	meta, err := boltConn.ReadTuple(&msg)
	if err != nil {
		panic(err)
	}
	data, ok := handleMsg(msg)
	if ok {
		boltConn.Emit([]string{tuple.Id}, "", data)
		boltConn.sendAck(meta.Id)
	} else {
		boltConn.SendFail(meta.Id)
	}
}
```
  
There are two methods to read tuples from Storm. ReadTuple and ReadRawTuple. If you don't know what type of contents you expect from storm, use ReadRawTuple. If you expect a certain type of message, use ReadTuple. ReadTuple packs the contents of the tuple into the struct you give it. This is very handy and you can design your code that you always know which type of message to expect.

In a system where you're sending multiple types of messages, you can create a message that's a union of all possible message types. The union will have the format:
```go
type myMsg struct {
	Event1 *EventType1 `json:"Event1,omitempty"`
	Event2 *EventType2 `json:"Event2,omitempty"`
}
```

Now, whenever you send a msg, create a new union message and insert the message that you want to transmit. json will only encode the filled in fields. When you receive the message, you know always know which message will be received and to check which actual message was received, you just test which member is != nil.

Also bear in mind that it is possible to Emit and Read multiple message types at once, because of the ...interface{} contents parameter.

Spout usage
===========

To create your own Storm spout connection:
```go
spoutConn := storm.NewSpoutConn()
spoutConn.Initialise(os.Stdin, os.Stdout)
```

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

This function might still be integrated into the Storm library itself. I just need to think about how best to combine Emiting data with handling acks and fails.
