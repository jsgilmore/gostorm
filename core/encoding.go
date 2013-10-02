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

package core

import (
	"fmt"
	"github.com/jsgilmore/gostorm/messages"
	"io"
)

type Input interface {
	ReadMsg(msg interface{}) (err error)
	ReadTaskIds() (taskIds []int32)
	ReadTuple(contentStructs ...interface{}) (metadata *messages.TupleMetadata, err error)
}

type Output interface {
	SendMsg(msg interface{})
	EmitGeneric(command, id, stream, msg string, anchors []string, directTask int64, contents ...interface{})
}

type InputFactory interface {
	NewInput(reader io.Reader) Input
}

type OutputFactory interface {
	NewOutput(writer io.Writer) Output
}

var (
	inputs  map[string]InputFactory  = make(map[string]InputFactory)
	outputs map[string]OutputFactory = make(map[string]OutputFactory)
)

func RegisterInput(name string, inFactory InputFactory) {
	inputFactory, ok := inputs[name]
	if !ok {
		inputs[name] = inFactory
	} else {
		panic(fmt.Sprintf("Input name already registered as type: %T", inputFactory))
	}
}

func RegisterOutput(name string, outFactory OutputFactory) {
	outputFactory, ok := outputs[name]
	if !ok {
		outputs[name] = outFactory
	} else {
		panic(fmt.Sprintf("Input name already registered as type: %T", outputFactory))
	}
}

func LookupInput(encoding string, reader io.Reader) Input {
	input, ok := inputs[encoding]
	if !ok {
		panic(fmt.Sprintf("gostorm encoding: Specified input not registered: %s", encoding))
	}
	return input.NewInput(reader)
}

func LookupOutput(encoding string, writer io.Writer) Output {
	output, ok := outputs[encoding]
	if !ok {
		panic(fmt.Sprintf("gostorm encoding: Specified output not registered: %s", encoding))
	}
	return output.NewOutput(writer)
}

func LookupBoltConn(encoding string, reader io.Reader, writer io.Writer) BoltConn {
	input := LookupInput(encoding, reader)
	output := LookupOutput(encoding, writer)
	return NewBoltConn(input, output)
}

func LookupSpoutConn(encoding string, reader io.Reader, writer io.Writer) SpoutConn {
	input := LookupInput(encoding, reader)
	output := LookupOutput(encoding, writer)
	return NewSpoutConn(input, output)
}
