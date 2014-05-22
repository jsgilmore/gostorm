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

package messages

import (
	"encoding/json"
	"fmt"
)

type topologyContextJson struct {
	TaskComponentMappings map[string]string `json:"task->component"`
	TaskId                int64             `json:"taskid"`
}

type contextJson struct {
	Conf     map[string]interface{} `json:"conf"`
	Topology *topologyContextJson   `json:"context"`
	PidDir   string                 `json:"pidDir"`
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

func (this *Context) UnmarshalJSON(data []byte) error {
	// This json unmarshaller is inefficient, but the implementation was
	// easy and it's only done once per bolt
	msg := &contextJson{}
	err := json.Unmarshal(data, msg)
	if err != nil {
		return err
	}

	this.PidDir = msg.PidDir

	// Convert the topology mapping from a map to a list
	this.Topology = &Topology{
		TaskId: msg.Topology.TaskId,
	}
	for key, value := range msg.Topology.TaskComponentMappings {
		mapping := &TaskComponentMapping{
			Task:      key,
			Component: value,
		}
		this.Topology.TaskComponentMappings = append(this.Topology.TaskComponentMappings, mapping)
	}

	// Covert the configuration from a map of interfaces to a list of key,value string pairs
	for key, value := range msg.Conf {
		conf := &Conf{
			Key:   key,
			Value: fmt.Sprintf("%v", value),
		}
		this.Confs = append(this.Confs, conf)
	}
	return nil
}

// Multilang message definition:
// {"pid": 1234}
func (this *Pid) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`{"pid": %d}`, this.Pid)), nil
}

// Multilang message definition:
//  {
//	"command": "log",
//	// the message to log
//	"msg": "hello world!"
//  }

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
type BoltMsgJson struct {
	*BoltMsgMeta
	Contents []interface{} `json:"tuple"`
}

type BoltMsg struct {
	*BoltMsgProto
	BoltMsgJson *BoltMsgJson
}

func (this *BoltMsg) MarshalJSON() ([]byte, error) {
	contents, err := json.Marshal(this.BoltMsgJson.Contents)
	if err != nil {
		panic(err)
	}
	id := this.BoltMsgJson.Id
	comp := this.BoltMsgJson.Comp
	stream := this.BoltMsgJson.Stream
	task := this.BoltMsgJson.Task
	return []byte(fmt.Sprintf(`{"id": "%s", "comp": "%s", "stream": "%s", "task": %d, "tuple": %s}`, id, comp, stream, task, contents)), nil
}

func (this *BoltMsg) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, this.BoltMsgJson)
	if err != nil {
		return err
	}
	return nil
}

// Multilang message definitions:
// {"command": "next"}
// {"command": "ack", "id": "1231231"}
// {"command": "fail", "id": "1231231"}

func (this *SpoutMsg) MarshalJSON() ([]byte, error) {
	if len(this.Id) > 0 {
		return []byte(fmt.Sprintf(`{"command": "%s", "id": "%s"}`, this.Command, this.Id)), nil
	} else {
		switch this.Command {
		case "next":
			return []byte(`{"command": "next"}`), nil
		case "sync":
			return []byte(`{"command": "sync"}`), nil
		default:
			panic("GoStorm: unknown spout command specified")
		}
	}
}

// Multilang bolt emission message definition:
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

// {"command": "sync"}

// Multilang spout emission message definition:
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

type ShellMsgJson struct {
	*ShellMsgMeta
	Contents []interface{} `json:"tuple"`
}

type ShellMsg struct {
	*ShellMsgProto
	ShellMsgJson *ShellMsgJson
}

func (this *ShellMsg) MarshalJSON() ([]byte, error) {
	command := this.ShellMsgJson.ShellMsgMeta.Command
	result := map[string]interface{}{
		"command": command,
	}
	if id := this.ShellMsgJson.ShellMsgMeta.GetId(); len(id) > 0 {
		result["id"] = id
	}
	if anchors := this.ShellMsgJson.ShellMsgMeta.GetAnchors(); len(anchors) > 0 {
		result["anchors"] = anchors
	}
	if stream := this.ShellMsgJson.ShellMsgMeta.GetStream(); len(stream) > 0 {
		result["stream"] = stream
	}
	if task := this.ShellMsgJson.ShellMsgMeta.GetTask(); task != 0 {
		result["task"] = task
	}
	if msg := this.ShellMsgJson.ShellMsgMeta.GetMsg(); len(msg) > 0 {
		result["msg"] = msg
	}
	if contents := this.ShellMsgJson.Contents; len(contents) > 0 {
		result["tuple"] = contents
	}
	if command == "emit" && !this.ShellMsgJson.ShellMsgMeta.GetNeedTaskIds() {
		result["need_task_ids"] = false
	}
	return json.Marshal(result)
}

func (this *ShellMsg) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, this.ShellMsgJson)
	if err != nil {
		return err
	}
	return nil
}
