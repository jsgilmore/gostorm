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
	"github.com/jsgilmore/gostorm/core"
	"github.com/jsgilmore/gostorm/messages"
	"io"
)

func NewJsonObjectInputFactory() core.InputFactory {
	return &jsonObjectInputFactory{}
}

type jsonObjectInputFactory struct{}

func (this *jsonObjectInputFactory) NewInput(reader io.Reader) core.Input {
	return NewJsonObjectInput(reader)
}

func NewJsonObjectInput(reader io.Reader) core.Input {
	return &jsonObjectInput{
		jsonInput: newJsonInput(reader),
	}
}

type jsonObjectInput struct {
	*jsonInput
}

// ConstructInput for object json returns a list of objects into which json objects should be unmarshalled
func (this *jsonObjectInput) constructInput(contents ...interface{}) []interface{} {
	contentList := make([]interface{}, len(contents))
	for i, content := range contents {
		contentList[i] = content
	}
	return contentList
}

// ReadTuple reads a tuple from Storm of which the contents are known
// and decodes the contents into the provided list of structs
func (this *jsonObjectInput) ReadBoltMsg(metadata *messages.BoltMsgMeta, contentStructs ...interface{}) (err error) {
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

	return nil
}

func NewJsonObjectOutputFactory() core.OutputFactory {
	return &jsonObjectOutputFactory{}
}

type jsonObjectOutputFactory struct{}

func (this *jsonObjectOutputFactory) NewOutput(writer io.Writer) core.Output {
	return NewJsonObjectOutput(writer)
}

func NewJsonObjectOutput(writer io.Writer) core.Output {
	return &jsonObjectOutput{
		jsonOutput: newJsonOutput(writer),
	}
}

type jsonObjectOutput struct {
	*jsonOutput
}

func (this *jsonObjectOutput) constructOutput(contents ...interface{}) []interface{} {
	contentList := make([]interface{}, len(contents))
	for i, content := range contents {
		contentList[i] = content
	}
	return contentList
}

func (this *jsonObjectOutput) EmitGeneric(command, id, stream, msg string, anchors []string, directTask int64, contents ...interface{}) {
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
	core.RegisterInput("jsonObject", NewJsonObjectInputFactory())
	core.RegisterOutput("jsonObject", NewJsonObjectOutputFactory())
}
