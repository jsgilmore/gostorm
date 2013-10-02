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
	"strings"
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
type TupleJson struct {
	*TupleMetadata
	Contents []interface{} `json:"tuple"`
}

type TupleMsg struct {
	*TupleProto
	TupleJson *TupleJson
}

func (this *TupleMsg) MarshalJSON() ([]byte, error) {
	contents, err := json.Marshal(this.TupleJson.Contents)
	if err != nil {
		panic(err)
	}
	id := this.TupleJson.Id
	comp := this.TupleJson.Comp
	stream := this.TupleJson.Stream
	task := this.TupleJson.Task
	return []byte(fmt.Sprintf(`{"id": "%s", "comp": "%s", "stream": "%s", "task": %d, "tuple": %s}`, id, comp, stream, task, contents)), nil
}

func (this *TupleMsg) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, this.TupleJson)
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

type Emission struct {
	*EmissionProto
	ContentsJson []interface{} `json:"tuple"`
}

func (this *Emission) MarshalJSON() ([]byte, error) {
	var emissionStrings []string
	var emissionStr string

	emissionStr = fmt.Sprintf(`{"command": "%s"`, this.EmissionMetadata.Command)
	emissionStrings = append(emissionStrings, emissionStr)

	if len(this.EmissionMetadata.GetId()) > 0 {
		emissionStr = fmt.Sprintf(`, "id": "%s"`, this.EmissionMetadata.GetId())
		emissionStrings = append(emissionStrings, emissionStr)
	}

	if len(this.EmissionMetadata.GetAnchors()) > 0 {
		emissionStr = fmt.Sprintf(`, "anchors": ["%s"`, this.EmissionMetadata.Anchors[0])
		emissionStrings = append(emissionStrings, emissionStr)
		for i := 1; i < len(this.EmissionMetadata.Anchors); i++ {
			emissionStr = fmt.Sprintf(`, "%s"`, this.EmissionMetadata.Anchors[i])
			emissionStrings = append(emissionStrings, emissionStr)
		}
		emissionStr = `]`
		emissionStrings = append(emissionStrings, emissionStr)
	}

	if len(this.EmissionMetadata.GetStream()) > 0 {
		emissionStr = fmt.Sprintf(`, "stream": "%s"`, this.EmissionMetadata.GetStream())
		emissionStrings = append(emissionStrings, emissionStr)
	}
	if this.EmissionMetadata.GetTask() != 0 {
		emissionStr = fmt.Sprintf(`, "task": %d`, this.EmissionMetadata.GetTask())
		emissionStrings = append(emissionStrings, emissionStr)
	}
	if len(this.EmissionMetadata.GetMsg()) > 0 {
		emissionStr = fmt.Sprintf(`, "msg": "%s"`, this.EmissionMetadata.GetMsg())
		emissionStrings = append(emissionStrings, emissionStr)
	}

	if len(this.ContentsJson) > 0 {
		contents, err := json.Marshal(this.ContentsJson)
		if err != nil {
			panic(err)
		}
		emissionStr = fmt.Sprintf(`, "tuple": %s`, contents)
		emissionStrings = append(emissionStrings, emissionStr)
	}
	emissionStr = `}`
	emissionStrings = append(emissionStrings, emissionStr)

	return []byte(strings.Join(emissionStrings, "")), nil
}
