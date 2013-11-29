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

package json

import (
	"bytes"
	"fmt"
	"github.com/jsgilmore/gostorm/messages"
	"math/rand"
	"testing"
)

func TestObjectInput(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	input := NewJsonObjectInput(buffer)

	testReadMsg(buffer, input, t)
}

func TestObjectOutput(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	output := NewJsonObjectOutput(buffer)

	testSendMsg(buffer, output, t)
}

func TestObjectEmitGeneric(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	output := NewJsonObjectOutput(buffer)
	input := NewJsonObjectInput(buffer)

	for i := 0; i < 100; i++ {
		num := rand.Int63()
		numStr := fmt.Sprintf("%d", num)
		outMsg := NewTestObj(numStr, num, []byte(numStr))
		outMeta := &messages.ShellMsgMeta{
			Command: "emit",
			Anchors: []string{"1", "2"},
			Id:      &numStr,
			Stream:  &numStr,
			Task:    &num,
			Msg:     &numStr,
		}
		output.EmitGeneric(outMeta.Command, outMeta.GetId(), outMeta.GetStream(), outMeta.GetMsg(), outMeta.GetAnchors(), outMeta.GetTask(), outMsg)

		inMsg := &testObj{}
		shellMsg := &messages.ShellMsg{
			ShellMsgJson: &messages.ShellMsgJson{},
		}
		shellMsg.ShellMsgJson.Contents = append(shellMsg.ShellMsgJson.Contents, inMsg)
		err := input.ReadMsg(shellMsg)
		checkErr(err, t)

		if !shellMsg.ShellMsgJson.ShellMsgMeta.Equal(outMeta) {
			t.Fatalf("Emission metadata (%+v) does not equal read emission metadata (%+v)", outMeta, shellMsg.ShellMsgJson.ShellMsgMeta)
		}

		checkErr(err, t)
		if !inMsg.Equal(outMsg) {
			t.Fatalf("Emission data (%+v) does not equal read emission data (%+v)", outMsg, inMsg)
		}
	}
}

func TestObjectReadBoltMsg(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	output := NewJsonObjectOutput(buffer)
	input := NewJsonObjectInput(buffer)

	for i := 0; i < 100; i++ {
		num := rand.Int63()
		numStr := fmt.Sprintf("%d", num)
		outMsg := NewTestObj(numStr, num, []byte(numStr))
		outTuple := &messages.BoltMsg{
			BoltMsgJson: &messages.BoltMsgJson{
				BoltMsgMeta: &messages.BoltMsgMeta{
					Id:     numStr,
					Comp:   numStr,
					Stream: numStr,
					Task:   num,
				},
			},
		}

		outTuple.BoltMsgJson.Contents = append(outTuple.BoltMsgJson.Contents, outMsg)
		output.SendMsg(outTuple)

		inMsg := &testObj{}
		inMeta := &messages.BoltMsgMeta{}
		err := input.ReadBoltMsg(inMeta, inMsg)
		checkErr(err, t)

		if !inMeta.Equal(outTuple.BoltMsgJson.BoltMsgMeta) {
			t.Fatalf("Tuple metadata (%+v) does not equal read Tuple metadata (%+v)", outTuple.BoltMsgJson.BoltMsgMeta, inMeta)
		}
		if !inMsg.Equal(outMsg) {
			t.Fatalf("Tuple data (%+v) does not equal read tuple data (%+v)", outMsg, inMsg)
		}
	}
}
