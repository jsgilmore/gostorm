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
	"encoding/json"
	"fmt"
	"github.com/jsgilmore/gostorm/messages"
	"math/rand"
	"testing"
)

func TestEncodedInput(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	input := NewJsonEncodedInput(buffer)

	testReadMsg(buffer, input, t)
}

func TestEncodedOutput(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	output := NewJsonEncodedOutput(buffer)

	testSendMsg(buffer, output, t)
}

func TestEncodedEmitGeneric(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	output := NewJsonEncodedOutput(buffer)
	input := NewJsonEncodedInput(buffer)

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

		shellMsg := &messages.ShellMsg{
			ShellMsgJson: &messages.ShellMsgJson{},
		}
		shellMsg.ShellMsgJson.Contents = append(shellMsg.ShellMsgJson.Contents, &[]byte{})

		err := input.ReadMsg(shellMsg)
		checkErr(err, t)

		if !shellMsg.ShellMsgJson.ShellMsgMeta.Equal(outMeta) {
			t.Fatalf("Emission metadata (%+v) does not equal read emission metadata (%+v)", outMeta, shellMsg.ShellMsgJson.ShellMsgMeta)
		}

		inMsg := &testObj{}
		err = json.Unmarshal(*shellMsg.ShellMsgJson.Contents[0].(*[]byte), inMsg)
		checkErr(err, t)
		if !inMsg.Equal(outMsg) {
			t.Fatalf("Emission data (%+v) does not equal read emission data (%+v)", outMsg, inMsg)
		}
	}
}

func TestEncodedReadBoltMsg(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	output := NewJsonEncodedOutput(buffer)
	input := NewJsonEncodedInput(buffer)

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

		outJson, err := json.Marshal(outMsg)
		checkErr(err, t)
		outTuple.BoltMsgJson.Contents = append(outTuple.BoltMsgJson.Contents, outJson)
		output.SendMsg(outTuple)

		inMsg := &testObj{}
		inMeta := &messages.BoltMsgMeta{}
		err = input.ReadBoltMsg(inMeta, inMsg)
		checkErr(err, t)

		if !inMeta.Equal(outTuple.BoltMsgJson.BoltMsgMeta) {
			t.Fatalf("Tuple metadata (%+v) does not equal read Tuple metadata (%+v)", outTuple.BoltMsgJson.BoltMsgMeta, inMeta)
		}
		if !inMsg.Equal(outMsg) {
			t.Fatalf("Tuple data (%+v) does not equal read tuple data (%+v)", outMsg, inMsg)
		}
	}
}
