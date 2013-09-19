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
	"github.com/jsgilmore/gostorm"
	"math/rand"
	"testing"
)

func checkErr(err error, t *testing.T) {
	if err != nil {
		t.Error(err)
	}
}

func NewTestObj(name string, number int, data []byte) *testObj {
	return &testObj{
		Name:   name,
		Number: number,
		Data:   data,
	}
}

type testObj struct {
	Name   string
	Number int
	Data   []byte
}

func (this *testObj) Equal(that *testObj) bool {
	if this.Name != that.Name {
		return false
	}
	if this.Number != that.Number {
		return false
	}
	if !bytes.Equal(this.Data, that.Data) {
		return false
	}
	return true
}

func testReadMsg(buffer *bytes.Buffer, input gostorm.Input, t *testing.T) {
	//Test sending some ints
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("%d", i)
		testObject := NewTestObj(name, i, []byte(name))
		data, err := json.Marshal(testObject)
		checkErr(err, t)
		_, err = buffer.Write(data)
		checkErr(err, t)
		buffer.WriteString("\nend\n")
	}

	for i := 0; i < 100; i++ {
		testObject := &testObj{}
		err := input.ReadMsg(testObject)
		checkErr(err, t)

		name := fmt.Sprintf("%d", i)
		expectedObj := NewTestObj(name, i, []byte(name))
		if !testObject.Equal(expectedObj) {
			t.Errorf("Objects don't match")
		}
	}
}

//func testObjectContructInput(t *testing.T) {
//	input := NewJsonObjectInput(nil)
//
//	testObjects := make([]*testObj, 5)
//	for i := 0; i < 5; i++ {
//		name := fmt.Sprintf("%d", i)
//		testObjects[i] = NewTestObj(name, i, []byte(name))
//	}
//
//	objects := input.ConstructInput(testObjects[0], testObjects[1], testObjects[2], testObjects[3], testObjects[4])
//	for i, testObject := range testObjects {
//		if !testObject.Equal(objects[i].(*testObj)) {
//			t.Errorf("Constructing inputs failed")
//		}
//	}
//}

func TestJsonObjectInput(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	input := NewJsonObjectInput(buffer)

	testReadMsg(buffer, input, t)
	//testObjectContructInput(t)
}

//func testEncodedContructInput(t *testing.T) {
//	input := NewJsonEncodedInput(nil)
//
//	testObjects := make([]*testObj, 5)
//	for i := 0; i < 5; i++ {
//		name := fmt.Sprintf("%d", i)
//		testObjects[i] = NewTestObj(name, i, []byte(name))
//	}
//
//	objects := input.ConstructInput(testObjects[0], testObjects[1], testObjects[2], testObjects[3], testObjects[4])
//
//	for _, object := range objects {
//		if !bytes.Equal(*object.(*[]byte), []byte{}) {
//			t.Errorf("Expected: %+v, received: %+v", []byte{}, *object.(*[]byte))
//		}
//	}
//}

func TestJsonEncodedInput(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	input := NewJsonEncodedInput(buffer)

	testReadMsg(buffer, input, t)
	//testEncodedContructInput(t)
}

func expect(expected string, buffer *bytes.Buffer, t *testing.T) {
	// Check whether the pid was reported
	recv, err := buffer.ReadString('\n')
	checkErr(err, t)
	if recv != expected+"\n" {
		t.Errorf("Expected: %s, received: %s", recv, expected)
	}
}

func testSendMsg(buffer *bytes.Buffer, output gostorm.Output, t *testing.T) {
	for i := 0; i < 100; i++ {
		msg := fmt.Sprintf("%d", rand.Int63())
		output.SendMsg(msg)
		msgEncoded, err := json.Marshal(msg)
		checkErr(err, t)

		expect(string(msgEncoded), buffer, t)
		expect("end", buffer, t)
	}
}

//func testConstructObjectOutput(t *testing.T) {
//	output := NewJsonObjectOutput(nil)
//
//	testObjects := make([]*testObj, 5)
//	for i := 0; i < 5; i++ {
//		name := fmt.Sprintf("%d", i)
//		testObjects[i] = NewTestObj(name, i, []byte(name))
//	}
//
//	objects := output.ConstructOutput(testObjects[0], testObjects[1], testObjects[2], testObjects[3], testObjects[4])
//	for i, testObject := range testObjects {
//		if !testObject.Equal(objects[i].(*testObj)) {
//			t.Errorf("Constructing inputs failed")
//		}
//	}
//}

func TestJsonObjectOutput(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	output := NewJsonObjectOutput(buffer)

	testSendMsg(buffer, output, t)
	//testConstructObjectOutput(t)
}

//func testConstructDecodeEncoded(t *testing.T) {
//	input := NewJsonEncodedInput(nil)
//	output := NewJsonEncodedOutput(nil)
//
//	testObjects := make([]*testObj, 5)
//	destObjects := make([]*testObj, 5)
//	for i := 0; i < 5; i++ {
//		name := fmt.Sprintf("%d", i)
//		testObjects[i] = NewTestObj(name, i, []byte(name))
//		destObjects[i] = &testObj{}
//	}
//
//	objects := output.ConstructOutput(testObjects[0], testObjects[1], testObjects[2], testObjects[3], testObjects[4])
//	input.DecodeInput(objects, destObjects[0], destObjects[1], destObjects[2], destObjects[3], destObjects[4])
//
//	for i, testObject := range testObjects {
//		if !testObject.Equal(destObjects[i]) {
//			t.Errorf("Expected: %+v, received: %+v", testObject, destObjects[i])
//		}
//	}
//}

func TestJsonEncodedOutput(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	output := NewJsonEncodedOutput(buffer)

	testSendMsg(buffer, output, t)
	//testConstructDecodeEncoded(t)
}
