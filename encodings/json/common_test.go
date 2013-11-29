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
	stormcore "github.com/jsgilmore/gostorm/core"
	"math/rand"
	"testing"
)

func checkErr(err error, t *testing.T) {
	if err != nil {
		t.Error(err)
		panic(err)
	}
}

func NewTestObj(name string, number int64, data []byte) *testObj {
	return &testObj{
		Name:   name,
		Number: number,
		Data:   data,
	}
}

type testObj struct {
	Name   string
	Number int64
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

func testReadMsg(buffer *bytes.Buffer, input stormcore.Input, t *testing.T) {
	//Test sending some ints
	for i := int64(0); i < 100; i++ {
		name := fmt.Sprintf("%d", i)
		testObject := NewTestObj(name, i, []byte(name))
		data, err := json.Marshal(testObject)
		checkErr(err, t)
		_, err = buffer.Write(data)
		checkErr(err, t)
		buffer.WriteString("\nend\n")
	}

	for i := int64(0); i < 100; i++ {
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

func expect(expected string, buffer *bytes.Buffer, t *testing.T) {
	// Check whether the pid was reported
	recv, err := buffer.ReadString('\n')
	checkErr(err, t)
	if recv != expected+"\n" {
		t.Errorf("Expected: %s, received: %s", recv, expected)
	}
}

func testSendMsg(buffer *bytes.Buffer, output stormcore.Output, t *testing.T) {
	for i := 0; i < 100; i++ {
		msg := fmt.Sprintf("%d", rand.Int63())
		output.SendMsg(msg)
		msgEncoded, err := json.Marshal(msg)
		checkErr(err, t)

		expect(string(msgEncoded), buffer, t)
		expect("end", buffer, t)
	}
}
