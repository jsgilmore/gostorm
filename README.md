gostorm
=======

GoStorm is a Go library that implements the communications protocol required for non-Java languages to communicate as part of a storm topology. In other words, gostorm lets you write Storm spouts and bolts in Go.

GoStorm is still an alpha release under active development. That said, GoStorm implements the complete Storm communications protocol as described in https://github.com/nathanmarz/storm/wiki/Multilang-protocol.

Currently, the main purpose of GoStorm is to act as a library for Go spouts and bolts that wish to interact with a Storm topology. The topology definition itself should still be done in Java and a shell spout or bolt is still required for each Go spout or bolt. See the storm-starter WordCountTopology on how to do this.

For testing and evaluation purposes, the GoStorm release also contains its own splitsentence.go implementation that replaces the python splitsentence.py implementation in the storm-starter WordCountTopology.

The GoStorm API provides two types of Storm connections: SpoutConn and BoltConn. Both of the underlying structures implement some of the more finicky parts of the Storm communications protocol, so you don't have to worry about them.

Bolt usage
==========

To create your own Storm bolt connection:
boltConn := storm.NewBoltConn()
spoutConn.Initialise(fd)

The Initialise(fd) function received the configuration from storm and reports your pid to storm. The file descriptor is the input file descriptor GoStorm should use to receive information from Storm. If you're not doing anything fancy, just use os.Stdin.

I decided to pass in the input file descriptor so you can test your Storm bolt with an input test file of Storm data.

In a bolt, you'll usually have something like this:
for {
  	tuple, eof := boltConn.ReadTuple()
		if eof {
			return
		}
    data, ok := handleTuple(tuple.Contents)
    if ok {
      boltConn.Emit([]interface{}{data}, []string{tuple.Id}, "")
      boltConn.sendAck(tuple.Id)
    } else {
		  boltConn.SendFail(tuple.Id)
    }
	}
  
Here, ReadTuple reads a single tuple. The tuple is processed in handleTuple, which takes the contents of the received tuple and produces some output data, which makes up the output of the bolt. The output data is then anchored to the received tuple's Id and emitted. After an anchored tuple has been emitted, the anchor should be acked.

The Emit function interface probably needs some cleanup. As it stands, it very literally just implements the Storm requirements.

Spout usage
===========

To create your own Storm spout connection:
spoutConn := storm.NewSpoutConn()
spoutConn.Initialise()

The Initialise() function received the configuration from storm and reports your pid to storm.

Since spouts are synchronous, a spout has to wait for next, ack or fail messages from Storm before it may send messages. After a spout has sent the messages it wishes to send, it has to send a sync message to Storm. The sync message signals that a spout will not send any more messages until the next "next", "ack" or "fail" message is received.

Therefore, to emit one tuple for each progress message as a custom spout (mySpoutImpl), implement the following Emit function:
func (this *mySpoutImpl) Emit(msg interface{}) {
  spoutMsg, _ := this.spoutConn.ReadMsg()
	switch spoutMsg.Command {
	case "next", "ack", "fail":
		this.spoutConn.Emit([]interface{}{msg}, generateId(), "")
		this.spoutConn.SendSync()
  case "ack:
    this.spoutConn.Emit([]interface{}{msg}, generateId(), "")
    handleAck(spoutMsg)
  	this.spoutConn.SendSync()
  case "fail":
    this.spoutConn.Emit([]interface{}{msg}, generateId(), "")
    handleFail(spoutMsg)
  	this.spoutConn.SendSync()
	default:
		panic("Unknown command received from Storm")
	}
}

This function might still be integrated into the Storm library itself. I just need to think about how best to combine Emiting data with handling acks and fails.
