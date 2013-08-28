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

package encoding

import (
	"fmt"
	"io"
)

type Input interface {
	ReadMsg(msg interface{}) (err error)
	ReadTaskIds() (taskIds []int)
	ConstructInput(contents ...interface{}) []interface{}
	DecodeInput(contentList []interface{}, contentStructs ...interface{})
}

type Output interface {
	SendMsg(msg interface{})
	SendEncoded(msg string)
	ConstructOutput(contents ...interface{}) []interface{}
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

func LookupInput(name string) InputFactory {
	input, ok := inputs[name]
	if !ok {
		panic(fmt.Sprintf("gostorm encoding: Specified input not registered: %s", name))
	}
	return input
}

func LookupOutput(name string) OutputFactory {
	output, ok := outputs[name]
	if !ok {
		panic(fmt.Sprintf("gostorm encoding: Specified output not registered: %s", name))
	}
	return output
}
