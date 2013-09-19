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

package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/jsgilmore/gostorm"
	stormenc "github.com/jsgilmore/gostorm/encoding/json"
	"github.com/jsgilmore/gostorm/messages"
	"io"
	"math/rand"
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

func newJsonTupleMsg(id, comp, stream string, task int64) *messages.TupleMsg {
	msg := &messages.TupleMsg{
		TupleJson: &messages.TupleJson{
			TupleMetadata: &messages.TupleMetadata{
				Id:     id,
				Comp:   comp,
				Stream: stream,
				Task:   task,
			},
		},
	}
	return msg
}

func genTupleMsg(id string, content interface{}) *messages.TupleMsg {
	msg := newJsonTupleMsg(id, "spout", "default", 4)
	msg.TupleJson.Contents = append(msg.TupleJson.Contents, content)
	return msg
}

func testTupleMsg(index int) *messages.TupleMsg {
	return genTupleMsg(ids[index], contents[index])
}

func genTaskIdsMsg() (taskIds []int32) {
	for i := 0; i < rand.Intn(10)+1; i++ {
		taskIds = append(taskIds, rand.Int31()+1)
	}
	return taskIds
}

func writeMsg(msg interface{}, writer io.Writer, t *testing.T) {
	data, err := json.Marshal(msg)
	checkErr(err, t)
	_, err = writer.Write(data)
	checkErr(err, t)
	_, err = writer.Write([]byte("\nend\n"))
	checkErr(err, t)
}

func feedConf(buffer io.Writer, t *testing.T) {
	_, err := buffer.Write(conf)
	checkErr(err, t)
}

func msgCheck(given, expected string, t *testing.T) {
	if given != expected {
		t.Fatalf("Bolt failed to read msg (expected: %s, received: %s)", expected, given)
	}
}

func metaTest(given *messages.TupleMetadata, index int, t *testing.T) {
	expected := &messages.TupleMetadata{
		Id:     ids[index],
		Stream: "default",
		Comp:   "spout",
		Task:   4,
	}
	if !given.Equal(expected) {
		t.Fatal("Tuple metadata not as expected")
	}
}

func checkErr(err error, t *testing.T) {
	if err != nil {
		t.Fatal(err)
	}
}

func checkPidFile(t *testing.T) {
	pidFilename := fmt.Sprintf("%d", os.Getpid())
	err := os.Remove(pidFilename)
	checkErr(err, t)
}

func expect(expected string, buffer *bytes.Buffer, t *testing.T) {
	// Check whether the pid was reported
	recv, err := buffer.ReadString('\n')
	checkErr(err, t)
	if recv != expected+"\n" {
		t.Fatalf("Expected: %s, received: %s", expected, recv)
	}
}

func expectPid(outBuffer *bytes.Buffer, t *testing.T) {
	expected := fmt.Sprintf(`{"pid":%d}`, os.Getpid())
	expect(expected, outBuffer, t)
	expect("end", outBuffer, t)
}

func TestInit(t *testing.T) {
	inBuffer := bytes.NewBuffer(nil)
	feedConf(inBuffer, t)

	outBuffer := bytes.NewBuffer(nil)

	input := stormenc.NewJsonObjectInput(inBuffer)
	output := stormenc.NewJsonObjectOutput(outBuffer)
	boltConn := gostorm.NewBoltConn(input, output)
	boltConn.Initialise()

	expectPid(outBuffer, t)

	// Check whether the pid file was created
	checkPidFile(t)
}

func TestLog(t *testing.T) {
	inBuffer := bytes.NewBuffer(nil)
	feedConf(inBuffer, t)
	outBuffer := bytes.NewBuffer(nil)
	input := stormenc.NewJsonObjectInput(inBuffer)
	output := stormenc.NewJsonObjectOutput(outBuffer)
	spoutConn := gostorm.NewSpoutConn(input, output)
	spoutConn.Initialise()

	expectPid(outBuffer, t)

	for i := 0; i < 6; i++ {
		msg := fmt.Sprintf("%d", rand.Int63())
		spoutConn.Log(msg)
		// Expect emission
		expect(fmt.Sprintf(`{"command":"log","msg":"%s"}`, msg), outBuffer, t)
		expect("end", outBuffer, t)
	}

	checkPidFile(t)
}

func feedReadTuple(buffer io.Writer, t *testing.T) {
	feedConf(buffer, t)
	writeMsg(testTupleMsg(0), buffer, t)
	writeMsg(testTupleMsg(1), buffer, t)
	writeMsg(testTupleMsg(2), buffer, t)
	writeMsg(testTupleMsg(3), buffer, t)
	writeMsg(testTupleMsg(4), buffer, t)
	writeMsg(testTupleMsg(5), buffer, t)
}

func TestReadTuple(t *testing.T) {
	buffer := bytes.NewBuffer(nil)

	feedReadTuple(buffer, t)

	input := stormenc.NewJsonObjectInput(buffer)
	output := stormenc.NewJsonObjectOutput(os.Stdout)
	boltConn := gostorm.NewBoltConn(input, output)
	boltConn.Initialise()

	var msg string
	for i := 0; i < 6; i++ {
		meta, err := boltConn.ReadTuple(&msg)
		checkErr(err, t)
		msgCheck(msg, contents[i], t)
		metaTest(meta, i, t)
	}

	checkPidFile(t)
}

func TestSendAck(t *testing.T) {
	inBuffer := bytes.NewBuffer(nil)
	feedConf(inBuffer, t)

	outBuffer := bytes.NewBuffer(nil)

	input := stormenc.NewJsonObjectInput(inBuffer)
	output := stormenc.NewJsonObjectOutput(outBuffer)
	boltConn := gostorm.NewBoltConn(input, output)
	boltConn.Initialise()

	var ids []string
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("%d", rand.Int63())
		ids = append(ids, id)

		boltConn.SendAck(id)
	}

	expect(fmt.Sprintf(`{"pid":%d}`, os.Getpid()), outBuffer, t)
	expect("end", outBuffer, t)

	for i := 0; i < 1000; i++ {
		expect(fmt.Sprintf(`{"command":"ack","id":"%s"}`, ids[i]), outBuffer, t)
		expect("end", outBuffer, t)
	}
	checkPidFile(t)
}

func TestSendFail(t *testing.T) {
	inBuffer := bytes.NewBuffer(nil)
	feedConf(inBuffer, t)

	outBuffer := bytes.NewBuffer(nil)

	input := stormenc.NewJsonObjectInput(inBuffer)
	output := stormenc.NewJsonObjectOutput(outBuffer)
	boltConn := gostorm.NewBoltConn(input, output)
	boltConn.Initialise()

	var ids []string
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("%d", rand.Int63())
		ids = append(ids, id)

		boltConn.SendFail(id)
	}

	expect(fmt.Sprintf(`{"pid":%d}`, os.Getpid()), outBuffer, t)
	expect("end", outBuffer, t)

	for i := 0; i < 1000; i++ {
		expect(fmt.Sprintf(`{"command":"fail","id":"%s"}`, ids[i]), outBuffer, t)
		expect("end", outBuffer, t)
	}

	checkPidFile(t)
}

func feedBoltSync(inBuffer io.Writer, t *testing.T) (taskIdsList [][]int32) {
	feedConf(inBuffer, t)
	writeMsg(testTupleMsg(0), inBuffer, t)
	taskIds := genTaskIdsMsg()
	taskIdsList = append(taskIdsList, taskIds)
	writeMsg(taskIds, inBuffer, t)

	writeMsg(testTupleMsg(1), inBuffer, t)
	taskIds = genTaskIdsMsg()
	taskIdsList = append(taskIdsList, taskIds)
	writeMsg(taskIds, inBuffer, t)

	writeMsg(testTupleMsg(2), inBuffer, t)
	taskIds = genTaskIdsMsg()
	taskIdsList = append(taskIdsList, taskIds)
	writeMsg(taskIds, inBuffer, t)

	writeMsg(testTupleMsg(3), inBuffer, t)
	taskIds = genTaskIdsMsg()
	taskIdsList = append(taskIdsList, taskIds)
	writeMsg(taskIds, inBuffer, t)

	writeMsg(testTupleMsg(4), inBuffer, t)
	taskIds = genTaskIdsMsg()
	taskIdsList = append(taskIdsList, taskIds)
	writeMsg(taskIds, inBuffer, t)

	writeMsg(testTupleMsg(5), inBuffer, t)
	taskIds = genTaskIdsMsg()
	taskIdsList = append(taskIdsList, taskIds)
	writeMsg(taskIds, inBuffer, t)
	return
}

func testBoltEmit(taskIdsList [][]int32, inBuffer io.Reader, t *testing.T) {
	outBuffer := bytes.NewBuffer(nil)
	input := stormenc.NewJsonObjectInput(inBuffer)
	output := stormenc.NewJsonObjectOutput(outBuffer)
	boltConn := gostorm.NewBoltConn(input, output)
	boltConn.Initialise()

	expectPid(outBuffer, t)

	var msg string
	for i := 0; i < 6; i++ {
		meta, err := boltConn.ReadTuple(&msg)
		checkErr(err, t)
		msgCheck(msg, contents[i], t)
		metaTest(meta, i, t)

		taskIds := boltConn.Emit([]string{}, "", fmt.Sprintf("Msg%d", i))

		// Expect task Ids
		if len(taskIds) != len(taskIdsList[i]) {
			t.Error("Task id list is not of expected length")
		}
		for j := 0; j < len(taskIdsList[i]); j++ {
			if taskIds[j] != taskIdsList[i][j] {
				t.Error("Returned task Ids do not match expected")
			}
		}

		// Expect emission
		expect(fmt.Sprintf(`{"command":"emit","tuple":["Msg%d"]}`, i), outBuffer, t)
		expect("end", outBuffer, t)
	}

	checkPidFile(t)
}

func TestBoltSyncEmit(t *testing.T) {
	inBuffer := bytes.NewBuffer(nil)
	taskIdsList := feedBoltSync(inBuffer, t)
	testBoltEmit(taskIdsList, inBuffer, t)
}

func feedBoltAsync(inBuffer io.Writer, t *testing.T) (taskIdsList [][]int32) {
	// Send messages and task Ids in an asynchronous way
	feedConf(inBuffer, t)
	writeMsg(testTupleMsg(0), inBuffer, t)
	writeMsg(testTupleMsg(1), inBuffer, t)

	taskIds := genTaskIdsMsg()
	taskIdsList = append(taskIdsList, taskIds)
	writeMsg(taskIds, inBuffer, t)
	taskIds = genTaskIdsMsg()
	taskIdsList = append(taskIdsList, taskIds)
	writeMsg(taskIds, inBuffer, t)

	writeMsg(testTupleMsg(2), inBuffer, t)
	writeMsg(testTupleMsg(3), inBuffer, t)
	writeMsg(testTupleMsg(4), inBuffer, t)
	writeMsg(testTupleMsg(5), inBuffer, t)

	taskIds = genTaskIdsMsg()
	taskIdsList = append(taskIdsList, taskIds)
	writeMsg(taskIds, inBuffer, t)
	taskIds = genTaskIdsMsg()
	taskIdsList = append(taskIdsList, taskIds)
	writeMsg(taskIds, inBuffer, t)
	taskIds = genTaskIdsMsg()
	taskIdsList = append(taskIdsList, taskIds)
	writeMsg(taskIds, inBuffer, t)
	taskIds = genTaskIdsMsg()
	taskIdsList = append(taskIdsList, taskIds)
	writeMsg(taskIds, inBuffer, t)
	return
}

func TestBoltAsyncEmit(t *testing.T) {
	inBuffer := bytes.NewBuffer(nil)
	taskIdsList := feedBoltAsync(inBuffer, t)
	testBoltEmit(taskIdsList, inBuffer, t)
}

func newSpoutMsg(command, id string) *messages.SpoutMsg {
	msg := &messages.SpoutMsg{
		Command: command,
		Id:      id,
	}
	return msg
}

func feedReadMsg(buffer io.Writer, t *testing.T) []*messages.SpoutMsg {
	var spoutMsgs []*messages.SpoutMsg
	feedConf(buffer, t)
	spoutMsg := newSpoutMsg("next", "")
	spoutMsgs = append(spoutMsgs, spoutMsg)
	writeMsg(spoutMsg, buffer, t)
	spoutMsg = newSpoutMsg("ack", "0")
	spoutMsgs = append(spoutMsgs, spoutMsg)
	writeMsg(spoutMsg, buffer, t)
	spoutMsg = newSpoutMsg("ack", "puppy")
	spoutMsgs = append(spoutMsgs, spoutMsg)
	writeMsg(spoutMsg, buffer, t)
	spoutMsg = newSpoutMsg("ack", "3231puppy")
	spoutMsgs = append(spoutMsgs, spoutMsg)
	writeMsg(spoutMsg, buffer, t)
	spoutMsg = newSpoutMsg("fail", "1")
	spoutMsgs = append(spoutMsgs, spoutMsg)
	writeMsg(spoutMsg, buffer, t)
	spoutMsg = newSpoutMsg("fail", "kitten")
	spoutMsgs = append(spoutMsgs, spoutMsg)
	writeMsg(spoutMsg, buffer, t)
	return spoutMsgs
}

func TestReadMsg(t *testing.T) {
	buffer := bytes.NewBuffer(nil)

	spoutMsgs := feedReadMsg(buffer, t)

	input := stormenc.NewJsonObjectInput(buffer)
	output := stormenc.NewJsonObjectOutput(os.Stdout)
	spoutConn := gostorm.NewSpoutConn(input, output)
	spoutConn.Initialise()

	for i := 0; i < 6; i++ {
		command, id, err := spoutConn.ReadSpoutMsg()
		checkErr(err, t)
		if command != spoutMsgs[i].Command {
			t.Fatalf(fmt.Sprintf("Incorrect command received: expected: %s, received: %s", spoutMsgs[i].Command, command))
		}
		if id != spoutMsgs[i].Id {
			t.Fatalf(fmt.Sprintf("Incorrect id received: expected: %s, received: %s", spoutMsgs[i].Id, id))
		}
	}

	checkPidFile(t)
}

func TestSendSync(t *testing.T) {
	inBuffer := bytes.NewBuffer(nil)
	feedConf(inBuffer, t)
	outBuffer := bytes.NewBuffer(nil)
	input := stormenc.NewJsonObjectInput(inBuffer)
	output := stormenc.NewJsonObjectOutput(outBuffer)
	spoutConn := gostorm.NewSpoutConn(input, output)
	spoutConn.Initialise()

	expectPid(outBuffer, t)

	for i := 0; i < 6; i++ {
		spoutConn.SendSync()
		// Expect emission
		expect(`{"command":"sync"}`, outBuffer, t)
		expect("end", outBuffer, t)
	}

	checkPidFile(t)
}
