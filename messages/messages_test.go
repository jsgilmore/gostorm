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
	"testing"
)

func TestMarshalShellMsg(t *testing.T) {
	msg := getMessage()

	// Verifies that excluded fields (stream) aren't marshaled
	expected := []byte(`{"anchors":["anchor1, anchor2"],"command":"emit","id":"id","msg":"{\"hello\":\"there\"}","need_task_ids":false,"task":123}`)
	verifyJsonOutput(t, msg, expected)

	// Verfies that complex tuples are json marshaled just once.
	stream := "some_stream"
	msg.ShellMsgJson.ShellMsgMeta.Stream = &stream
	msg.ShellMsgJson.Contents = getTestTuple()
	expected = []byte(`{"anchors":["anchor1, anchor2"],"command":"emit","id":"id","msg":"{\"hello\":\"there\"}","need_task_ids":false,"stream":"some_stream","task":123,"tuple":[{"Field1":"Lorem ipsum dolor sit amet, consectetur adipisicing elit","Field2":"sed do eiusmod tempor incididunt ut labore","Field3":12345},{"Field1":"sed quia consequuntur magni dolores eos qui","Field2":"ratione voluptatem sequi nesciunt. Neque porro quisquam","Field3":654321}]}`)
	verifyJsonOutput(t, msg, expected)
}

func verifyJsonOutput(t *testing.T, msg interface{}, expected []byte) {
	marshaled, err := json.Marshal(msg)
	if err != nil {
		t.Error("Error marshaling msg: ", err)
	}
	if string(marshaled) != string(expected) {
		t.Errorf("Unexpected output from marshaling message: %+v.\n Expected: %+v", string(marshaled), string(expected))
	}
}

func BenchmarkMarshalJSON(b *testing.B) {
	msg := getMessage()
	msg.ShellMsgJson.Contents = getTestTuple()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(msg)
		if err != nil {
			b.Log("Error: ", err)
			b.Failed()
		}
	}
}

// Test helpers and miscellaneous definitions.

func getMessage() *ShellMsg {
	id, complexMsg := "id", "{\"hello\":\"there\"}"
	task := int64(123)
	needTaskIds := false
	return &ShellMsg{
		ShellMsgJson: &ShellMsgJson{
			ShellMsgMeta: &ShellMsgMeta{
				Command:     "emit",
				Anchors:     []string{"anchor1, anchor2"},
				Id:          &id,
				Stream:      nil,
				Task:        &task,
				NeedTaskIds: &needTaskIds,
				Msg:         &complexMsg,
			},
			Contents: []interface{}{},
		},
	}
}

type testTupleItem struct {
	Field1 string
	Field2 string
	Field3 int
}

func getTestTuple() []interface{} {
	return []interface{}{
		testTupleItem{
			"Lorem ipsum dolor sit amet, consectetur adipisicing elit",
			"sed do eiusmod tempor incididunt ut labore",
			12345,
		},
		testTupleItem{
			"sed quia consequuntur magni dolores eos qui",
			"ratione voluptatem sequi nesciunt. Neque porro quisquam",
			654321,
		},
	}
}
