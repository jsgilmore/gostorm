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
	"encoding/json"
	"github.com/jsgilmore/gostorm/core"
	"github.com/jsgilmore/gostorm/messages"
	"io"
)

func NewJsonEncodedInputFactory() core.InputFactory {
	return &jsonEncodedInputFactory{}
}

type jsonEncodedInputFactory struct{}

func (this *jsonEncodedInputFactory) NewInput(reader io.Reader) core.Input {
	return NewJsonEncodedInput(reader)
}

func NewJsonEncodedInput(reader io.Reader) core.Input {
	return &jsonEncodedInput{
		jsonInput: newJsonInput(reader),
	}
}

type jsonEncodedInput struct {
	*jsonInput
}

// ConstructInput for encoded json returns a list of bytes into which marshalled json should be written
func (this *jsonEncodedInput) constructInput(contents ...interface{}) []interface{} {
	contentList := make([]interface{}, len(contents))
	for i := 0; i < len(contents); i++ {
		contentList[i] = &[]byte{}
	}
	return contentList
}

func (this *jsonEncodedInput) decodeInput(contentList []interface{}, contentStructs ...interface{}) {
	for i, content := range contentStructs {
		err := json.Unmarshal(*contentList[i].(*[]byte), content)
		if err != nil {
			panic(err)
		}
	}
}

// ReadTuple reads a tuple from Storm of which the contents are known
// and decodes the contents into the provided list of structs
func (this *jsonEncodedInput) ReadBoltMsg(metadata *messages.BoltMsgMeta, contentStructs ...interface{}) (err error) {
	boltMsg := &messages.BoltMsg{
		BoltMsgJson: &messages.BoltMsgJson{
			BoltMsgMeta: metadata,
			Contents:    this.constructInput(contentStructs...),
		},
	}
	err = this.ReadMsg(boltMsg)
	if err != nil {
		return err
	}

	this.decodeInput(boltMsg.BoltMsgJson.Contents, contentStructs...)
	return nil
}

func NewJsonEncodedOutputFactory() core.OutputFactory {
	return &jsonEncodedOutputFactory{}
}

type jsonEncodedOutputFactory struct{}

func (this *jsonEncodedOutputFactory) NewOutput(writer io.Writer) core.Output {
	return NewJsonEncodedOutput(writer)
}

func NewJsonEncodedOutput(writer io.Writer) core.Output {
	return &jsonEncodedOutput{
		jsonOutput: newJsonOutput(writer),
	}
}

type jsonEncodedOutput struct {
	*jsonOutput
}

func (this *jsonEncodedOutput) constructOutput(contents ...interface{}) []interface{} {
	contentList := make([]interface{}, len(contents))
	for i, content := range contents {
		encoded, err := json.Marshal(content)
		if err != nil {
			panic(err)
		}
		contentList[i] = &encoded
	}
	return contentList
}

func (this *jsonEncodedOutput) EmitGeneric(command, id, stream, msg string, anchors []string, directTask int64, contents ...interface{}) {
	shellMsg := &messages.ShellMsg{
		ShellMsgJson: &messages.ShellMsgJson{
			ShellMsgMeta: &messages.ShellMsgMeta{
				Command: command,
				Anchors: anchors,
				Id:      &id,
				Stream:  &stream,
				Task:    &directTask,
				Msg:     &msg,
			},
			Contents: this.constructOutput(contents...),
		},
	}
	this.SendMsg(shellMsg)
}

func init() {
	core.RegisterInput("jsonEncoded", NewJsonEncodedInputFactory())
	core.RegisterOutput("jsonEncoded", NewJsonEncodedOutputFactory())
}
