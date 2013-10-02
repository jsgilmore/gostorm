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

package gostorm

import (
	"check"
	"github.com/jsgilmore/gostorm/core"
	"io"
	"sync"
)

type ShellBolt interface {
	Go()
	Exit()
	Initialise(boltConn core.BoltConn)
}

type shellBoltImpl struct {
	sync.Mutex
	boltConn core.BoltConn
	bolt     Bolt
	cleaned  bool
	read     int
}

func NewShellBolt(bolt Bolt) ShellBolt {
	return &shellBoltImpl{
		bolt: bolt,
	}
}

func (this *shellBoltImpl) Initialise(boltConn core.BoltConn) {
	this.boltConn = boltConn
	this.boltConn.Connect()
	this.bolt.Prepare(this.boltConn.Context(), this.boltConn)
}

func (this *shellBoltImpl) Go() {
	for {
		fields := this.bolt.Fields()
		meta, err := this.boltConn.ReadTuple(fields...)
		if err == io.EOF {
			this.Exit()
			return
		}
		check.Error(err)
		this.bolt.Execute(meta, fields...)
	}
}

func (this *shellBoltImpl) Exit() {
	this.Lock()
	defer this.Unlock()
	if !this.cleaned {
		this.bolt.Cleanup()
		this.cleaned = true
	}
}
