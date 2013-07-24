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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"
)

var (
	conf = []byte("{\"pidDir\":\"\",\"context\":{\"task->component\":{\"1\":\"__acker\",\"2\":\"count\",\"3\":\"split\",\"4\":\"spout\"}},\"conf\":{\"storm.id\":\"word-count-1-1374054737\",\"dev.zookeeper.path\":\"\\/tmp\\/dev-storm-zookeeper\",\"topology.tick.tuple.freq.secs\":null,\"topology.fall.back.on.java.serialization\":true,\"topology.max.error.report.per.interval\":5,\"zmq.linger.millis\":0,\"topology.skip.missing.kryo.registrations\":true,\"ui.childopts\":\"-Xmx768m\",\"storm.zookeeper.session.timeout\":20000,\"nimbus.reassign\":true,\"topology.trident.batch.emit.interval.millis\":50,\"nimbus.monitor.freq.secs\":10,\"java.library.path\":\"\\/usr\\/local\\/lib:\\/opt\\/local\\/lib:\\/usr\\/lib\",\"topology.executor.send.buffer.size\":1024,\"storm.local.dir\":\"\\/var\\/folders\\/cq\\/bfb6h3xd52dds5gq_8l0xn3r0000gn\\/T\\/\\/1e32acfa-e4e9-4b74-a879-0eef9fd94032\",\"supervisor.worker.start.timeout.secs\":120,\"topology.enable.message.timeouts\":true,\"nimbus.cleanup.inbox.freq.secs\":600,\"nimbus.inbox.jar.expiration.secs\":3600,\"drpc.worker.threads\":64,\"topology.worker.shared.thread.pool.size\":4,\"nimbus.host\":\"localhost\",\"storm.zookeeper.port\":2000,\"transactional.zookeeper.port\":null,\"topology.executor.receive.buffer.size\":1024,\"transactional.zookeeper.servers\":null,\"storm.zookeeper.root\":\"\\/storm\",\"supervisor.enable\":true,\"storm.zookeeper.servers\":[\"localhost\"],\"transactional.zookeeper.root\":\"\\/transactional\",\"topology.acker.executors\":1,\"topology.kryo.decorators\":[],\"topology.name\":\"word-count\",\"topology.transfer.buffer.size\":1024,\"topology.worker.childopts\":null,\"drpc.queue.size\":128,\"worker.childopts\":\"-Xmx768m\",\"supervisor.heartbeat.frequency.secs\":5,\"topology.error.throttle.interval.secs\":10,\"zmq.hwm\":0,\"drpc.port\":3772,\"supervisor.monitor.frequency.secs\":3,\"topology.receiver.buffer.size\":8,\"task.heartbeat.frequency.secs\":3,\"topology.tasks\":null,\"topology.spout.wait.strategy\":\"backtype.storm.spout.SleepSpoutWaitStrategy\",\"topology.max.spout.pending\":null,\"storm.zookeeper.retry.interval\":1000,\"topology.sleep.spout.wait.strategy.time.ms\":1,\"nimbus.topology.validator\":\"backtype.storm.nimbus.DefaultTopologyValidator\",\"supervisor.slots.ports\":[1,2,3],\"topology.debug\":true,\"nimbus.task.launch.secs\":120,\"nimbus.supervisor.timeout.secs\":60,\"topology.kryo.register\":null,\"topology.message.timeout.secs\":30,\"task.refresh.poll.secs\":10,\"topology.workers\":1,\"supervisor.childopts\":\"-Xmx256m\",\"nimbus.thrift.port\":6627,\"topology.stats.sample.rate\":0.05,\"worker.heartbeat.frequency.secs\":1,\"topology.acker.tasks\":null,\"topology.disruptor.wait.strategy\":\"com.lmax.disruptor.BlockingWaitStrategy\",\"nimbus.task.timeout.secs\":30,\"storm.zookeeper.connection.timeout\":15000,\"topology.kryo.factory\":\"backtype.storm.serialization.DefaultKryoFactory\",\"drpc.invocations.port\":3773,\"zmq.threads\":1,\"storm.zookeeper.retry.times\":5,\"topology.state.synchronization.timeout.secs\":60,\"supervisor.worker.timeout.secs\":30,\"nimbus.file.copy.expiration.secs\":600,\"drpc.request.timeout.secs\":600,\"storm.local.mode.zmq\":false,\"ui.port\":8080,\"nimbus.childopts\":\"-Xmx1024m\",\"storm.cluster.mode\":\"local\",\"topology.optimize\":true,\"topology.max.task.parallelism\":1}}\nend\n")

	contents = []string{"Later on he will understand how some men so loved her, that they did dare much for her sake.",
		"It was the best of times, it was the worst of times",
		"Call me Ishmael",
		"'To be born again,' sang Gibreel Farishta tumbling from the heavens, 'first you have to die.",
		"In the beginning God created the heavens and the earth.",
		"Behind every man now alive stand thirty ghosts, for that is the ratio by which the dead outnumber the living.",
	}

	ids = []string{"6537357682049040408",
		"-4692846741051456627",
		"-98981468164054242",
		"1517917499117655786",
		"7079856853154658789",
		"-5121255599131402952",
	}
)

func genMsg(id string, content interface{}) *TupleMsg {
	msg := newTupleMsg(id, "spout", "default", 4)
	msg.AddContent(content)
	return msg
}

func testMsg(index int) *TupleMsg {
	return genMsg(ids[index], contents[index])
}

func writeMsg(msg *TupleMsg, writer io.Writer) {
	data, err := json.Marshal(msg)
	checkErr(err)
	_, err = writer.Write(data)
	checkErr(err)
	_, err = writer.Write([]byte("\nend\n"))
	checkErr(err)
}

func feedConf(buffer *bytes.Buffer) {
	_, err := buffer.Write(conf)
	checkErr(err)
}

func feedBolt(buffer *bytes.Buffer) {
	feedConf(buffer)
	writeMsg(testMsg(0), buffer)
	writeMsg(testMsg(1), buffer)
	writeMsg(testMsg(2), buffer)
	writeMsg(testMsg(3), buffer)
	writeMsg(testMsg(4), buffer)
	writeMsg(testMsg(5), buffer)
}

func msgCheck(given, expected string, t *testing.T) {
	if given != expected {
		t.Errorf("Bolt failed to read msg (expected: %s, received: %s)", expected, given)
	}
}

func metaTest(given *TupleMetadata, index int, t *testing.T) {
	expected := &TupleMetadata{
		Id:     ids[index],
		Stream: "default",
		Comp:   "spout",
		Task:   4,
	}
	metaCheck(given, expected, t)
}

func metaCheck(given, expected *TupleMetadata, t *testing.T) {
	if given.Id != expected.Id {
		t.Error("Tuple metadata Ids don't match")
	}
	if given.Comp != expected.Comp {
		t.Error("Tuple metadata components don't match")
	}
	if given.Stream != expected.Stream {
		t.Error("Tuple metadata streams don't match")
	}
	if given.Task != expected.Task {
		t.Error("Tuple metadata task Ids don't match")
	}
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func checkPidFile(t *testing.T) {
	pidFilename := fmt.Sprintf("%d", os.Getpid())
	err := os.Remove(pidFilename)
	if err != nil {
		t.Error(err)
	}
}

func TestInit(t *testing.T) {
	inBuffer := bytes.NewBuffer(nil)
	feedConf(inBuffer)

	outBuffer := bytes.NewBuffer(nil)

	boltConn := NewBoltConn()
	boltConn.Initialise(inBuffer, outBuffer)

	// Check whether the pid was reported
	pid, err := outBuffer.ReadString('\n')
	if err != nil {
		panic(err)
	}
	if pid != fmt.Sprintf("{\"pid\":%d}\n", os.Getpid()) {
		t.Error("Pid not correctly reported")
	}

	// Check whether "end" was appended to the message
	end, err := outBuffer.ReadString('\n')
	if err != nil {
		panic(err)
	}
	if end != "end\n" {
		t.Error("End not correctly appended")
	}

	// Check whether the pid file was created
	checkPidFile(t)
}

func TestReadTuple(t *testing.T) {

	buffer := bytes.NewBuffer(nil)
	feedBolt(buffer)

	boltConn := NewBoltConn()
	boltConn.Initialise(buffer, os.Stdout)

	var msg string
	for i := 0; i < 6; i++ {
		meta, err := boltConn.ReadTuple(&msg)
		checkErr(err)
		msgCheck(msg, contents[i], t)
		metaTest(meta, i, t)
	}

	checkPidFile(t)
}

func TestSpout(t *testing.T) {

}
