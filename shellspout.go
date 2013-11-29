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
	"fmt"
	"github.com/jsgilmore/gostorm/core"
	"io"
	"sync"
)

type ShellSpout interface {
	Go()
	Exit()
	Initialise(spoutConn core.SpoutConn)
}

type shellSpoutImpl struct {
	sync.Mutex
	spoutConn core.SpoutConn
	spout     Spout
	cleaned   bool
}

func NewShellSpout(spout Spout) ShellSpout {
	return &shellSpoutImpl{
		spout: spout,
	}
}

func (this *shellSpoutImpl) Initialise(spoutConn core.SpoutConn) {
	this.spoutConn = spoutConn
	this.spoutConn.Connect()
	this.spout.Open(this.spoutConn.Context(), this.spoutConn)
}

func (this *shellSpoutImpl) Go() {
	for {
		// This lock prevents the spout exit function being called
		// concurrently with another function. The lock is above the
		// ReadSpoutMsg to ensure that any messages read from the spout
		// in channel will make it to the spout before exit is called.
		command, id, err := this.spoutConn.ReadSpoutMsg()
		if err == io.EOF {
			this.Exit()
			return
		}
		if err != nil {
			panic(err)
		}
		this.Lock()
		if this.cleaned && command != "next" {
			panic(fmt.Sprintf("ShellSpout: %s message sent to cleaned up spout", command))
		}

		switch command {
		case "next":
			this.spout.NextTuple()
		case "ack":
			this.spout.Acked(id)
		case "fail":
			this.spout.Failed(id)
		default:
			panic(fmt.Sprintf("ShellSpout: Unknown command received from Storm: %s", command))
		}
		this.spoutConn.SendSync()
		this.Unlock()
	}
}

func (this *shellSpoutImpl) Exit() {
	this.Lock()
	defer this.Unlock()
	if !this.cleaned {
		this.spout.Exit()
		this.cleaned = true
	}
}
