//   Copyright 2013 Vastech SA (PTY) LTD
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

// GoStorm is a library that allows you to write Storm spouts and bolts in Go
package gostorm

import (
	"github.com/jsgilmore/gostorm/core"
	_ "github.com/jsgilmore/gostorm/encodings"
	stormmsg "github.com/jsgilmore/gostorm/messages"
	"os"
)

type Bolt interface {
	FieldsFactory
	Execute(meta stormmsg.BoltMsgMeta, fields ...interface{})
	Prepare(context *stormmsg.Context, collector OutputCollector)
	Cleanup()
}

type Spout interface {
	NextTuple()
	Acked(id string)
	Failed(id string)
	Exit()
	Open(context *stormmsg.Context, collector SpoutOutputCollector)
}

type SpoutOutputCollector interface {
	Emit(id string, stream string, fields ...interface{}) (taskIds []int32)
	EmitDirect(id string, stream string, directTask int64, fields ...interface{})
}

type OutputCollector interface {
	SendAck(id string)
	SendFail(id string)
	Emit(anchors []string, stream string, fields ...interface{}) (taskIds []int32)
	EmitDirect(anchors []string, stream string, directTask int64, fields ...interface{})
}

type FieldsFactory interface {
	Fields() []interface{}
}

func RunBolt(bolt Bolt, encoding string) {
	boltConn := core.LookupBoltConn(encoding, os.Stdin, os.Stdout)
	shellBolt := NewShellBolt(bolt)
	shellBolt.Initialise(boltConn)
	shellBolt.Go()
	shellBolt.Exit()
}

func RunSpout(spout Spout, encoding string) {
	spoutConn := core.LookupSpoutConn(encoding, os.Stdin, os.Stdout)
	shellSpout := NewShellSpout(spout)
	shellSpout.Initialise(spoutConn)
	shellSpout.Go()
	shellSpout.Exit()
}
