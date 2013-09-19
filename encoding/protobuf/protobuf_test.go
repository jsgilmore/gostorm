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

package protobuf

import (
	"bytes"
	"code.google.com/p/gogoprotobuf/proto"
	"fmt"
	"github.com/jsgilmore/gostorm/messages"
	"math/rand"
	"testing"
)

func checkErr(err error, t *testing.T) {
	if err != nil {
		t.Fatal(err)
	}
}

func newTestObj(name string, number int64, data []byte) *messages.Test {
	return &messages.Test{
		Name:   name,
		Number: number,
		Data:   data,
	}
}

func TestContSendRecv(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	output := NewProtobufOutput(buffer)
	input := NewProtobufInput(buffer)

	var outMsgStream []*messages.Test
	for i := 0; i < 100; i++ {
		num := rand.Int63()
		numStr := fmt.Sprintf("%d", num)
		outMsg := newTestObj(numStr, num, []byte(numStr))
		outMsgStream = append(outMsgStream, outMsg)
		output.SendMsg(outMsg)
	}

	for i := 0; i < 100; i++ {
		inMsg := &messages.Test{}
		err := input.ReadMsg(inMsg)
		checkErr(err, t)

		if !inMsg.Equal(outMsgStream[i]) {
			t.Fatalf("Written message (%v) does not equal read message (%v)", outMsgStream[i], inMsg)
		}
	}
}

func TestDiscSendRecv(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	output := NewProtobufOutput(buffer)
	input := NewProtobufInput(buffer)

	for i := 0; i < 100; i++ {
		num := rand.Int63()
		numStr := fmt.Sprintf("%d", num)
		outMsg := newTestObj(numStr, num, []byte(numStr))
		output.SendMsg(outMsg)

		inMsg := &messages.Test{}
		err := input.ReadMsg(inMsg)
		checkErr(err, t)

		if !inMsg.Equal(outMsg) {
			t.Fatalf("Written message (%v) does not equal read message (%v)", outMsg, inMsg)
		}
	}
}

func TestEmitGeneric(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	output := NewProtobufOutput(buffer)
	input := NewProtobufInput(buffer)

	for i := 0; i < 100; i++ {
		num := rand.Int63()
		numStr := fmt.Sprintf("%d", num)
		outMsg := newTestObj(numStr, num, []byte(numStr))
		outMeta := &messages.EmissionMetadata{
			Command: "emit",
			Anchors: []string{"1", "2"},
			Id:      &numStr,
			Stream:  &numStr,
			Task:    &num,
			Msg:     &numStr,
		}
		output.EmitGeneric(outMeta.Command, outMeta.GetId(), outMeta.GetStream(), outMeta.GetMsg(), outMeta.GetAnchors(), outMeta.GetTask(), outMsg)

		emission := &messages.Emission{
			EmissionProto: &messages.EmissionProto{},
		}
		err := input.ReadMsg(emission)
		checkErr(err, t)

		if !emission.EmissionProto.EmissionMetadata.Equal(outMeta) {
			t.Fatalf("Emission metadata (%+v) does not equal read emission metadata (%+v)", outMeta, emission.EmissionProto.EmissionMetadata)
		}

		inMsg := &messages.Test{}
		err = proto.Unmarshal(emission.EmissionProto.Contents[0], inMsg)
		checkErr(err, t)
		if !inMsg.Equal(outMsg) {
			t.Fatalf("Emission data (%+v) does not equal read emission data (%+v)", outMsg, inMsg)
		}
	}
}

func TestReadTuple(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	output := NewProtobufOutput(buffer)
	input := NewProtobufInput(buffer)

	for i := 0; i < 100; i++ {
		num := rand.Int63()
		numStr := fmt.Sprintf("%d", num)
		outMsg := newTestObj(numStr, num, []byte(numStr))
		outTuple := &messages.TupleMsg{
			TupleProto: &messages.TupleProto{
				TupleMetadata: &messages.TupleMetadata{
					Id:     numStr,
					Comp:   numStr,
					Stream: numStr,
					Task:   num,
				},
			},
		}
		outProto, err := proto.Marshal(outMsg)
		checkErr(err, t)
		outTuple.TupleProto.Contents = append(outTuple.TupleProto.Contents, outProto)
		output.SendMsg(outTuple)

		inMsg := &messages.Test{}
		inMeta, err := input.ReadTuple(inMsg)
		checkErr(err, t)

		if !inMeta.Equal(outTuple.TupleProto.TupleMetadata) {
			t.Fatalf("Tuple metadata (%+v) does not equal read Tuple metadata (%+v)", outTuple.TupleProto.TupleMetadata, inMeta)
		}
		if !inMsg.Equal(outMsg) {
			t.Fatalf("Tuple data (%+v) does not equal read tuple data (%+v)", outMsg, inMsg)
		}
	}
}
