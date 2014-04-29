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
	id, stream, complexMsg := "id", "stream", "{\"hello\":\"there\"}"
	task := int64(123)
	needTaskIds := false
	msg := &ShellMsg{
		ShellMsgJson: &ShellMsgJson{
			ShellMsgMeta: &ShellMsgMeta{
				Command:     "foo",
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

	// Verifies that excluded fields (stream) aren't marshaled
	expected := []byte(`{"anchors":["anchor1, anchor2"],"command":"foo","id":"id","msg":"{\"hello\":\"there\"}","task":123}`)
	verifyJsonOutput(t, msg, expected)

	// Verfies that complex tuples are json marshaled just once.
	msg.ShellMsgJson.Stream = &stream
	msg.ShellMsgJson.Contents = getTestTuple()
	expected = []byte(`{"anchors":["anchor1, anchor2"],"command":"foo","id":"id","msg":"{\"hello\":\"there\"}","stream":"stream","task":123,"tuple":[{"Field1":"a","Field2":"b"},{"Field1":"c","Field2":"d"}]}`)
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

type testTupleItem struct {
	Field1 string
	Field2 string
}

func getTestTuple() []interface{} {
	return []interface{}{
		testTupleItem{"a", "b"},
		testTupleItem{"c", "d"},
	}
}
