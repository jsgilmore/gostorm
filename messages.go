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

package gostorm

type topologyContext struct {
	TaskComponent map[string]string `json:"task->component"`
	TaskId        int               `json:"taskid"`
}

// Multilang message definition:
//  {
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
//  }
type Context struct {
	Conf     map[string]interface{} `json:"conf"`
	Topology topologyContext        `json:"context"`
	PidDir   string                 `json:"pidDir"`
}

// Multilang message definition:
// {"pid": 1234}
type pidMsg struct {
	Pid int `json:"pid"`
}

// Multilang message definition:
//  {
//	"command": "log",
//	// the message to log
//	"msg": "hello world!"
//  }
type logMsg struct {
	Command string `json:"command"`
	Msg     string `json:"msg"`
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

// Multilang message definition:
//  {
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
//  }
type TupleMsg struct {
	*TupleMetadata
	Contents []interface{} `json:"tuple"`
}

func (this *TupleMsg) AddContent(content interface{}) {
	this.Contents = append(this.Contents, content)
}

// Multilang message definition:
//  {
//	"command": "emit",
//	// The ids of the tuples this output tuples should be anchored to
//	"anchors": ["1231231", "-234234234"],
//	// The id of the stream this tuple was emitted to. Leave this empty to emit to default stream.
//	"stream": "1",
//	// If doing an emit direct, indicate the task to send the tuple to
//	"task": 9,
//	// All the values in this tuple
//	"tuple": ["field1", 2, 3]
//  }
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

// Multilang message definitions:
// {"command": "next"}
// {"command": "sync"}
// {"command": "ack", "id": "1231231"}
// {"command": "fail", "id": "1231231"}
type spoutMsg struct {
	Command string `json:"command"`
	Id      string `json:"id,omitempty"`
}

// Multilang message definition:
//  {
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
//  }
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
