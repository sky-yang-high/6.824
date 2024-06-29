package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.

	// * 实现思路：for 轮询，有空时，则发送心跳给 Coordinator，等待其分配任务
	// * 收到任务后，执行任务，结果写入文件
	// * 任务完成后，上报任务完成，等待下一轮任务分配

	WorkNotDone := true
	for WorkNotDone {
		reply := heartBeat()

		switch reply.Type {
		case MapTask:
			doMapTask(&reply.Task, mapf)
		case ReduceTask:
			doReduceTask(&reply.Task, reducef)
		case WaitTask:
			doWaitTask()
		case ExitTask:
			doExitTask()
			WorkNotDone = false
		}
	}
}

// 心跳，返回 Coordinator 分配的任务
func heartBeat() *HeartReply {
	hreq, hreply := &HeartRequset{}, &HeartReply{}
	ok := call("Coordinator.HandleHeartBeat", hreq, hreply)
	if !ok {
		hreply.Type = WaitTask //something wrong, just wait
		log.Println("[HeartBeat]: something Wrong")
	}
	return hreply
}

// 进行 map 任务
func doMapTask(t *Task, mapf func(string, string) []KeyValue) {
	// * 实现思路：读取 file，进行 map，获得 中间结果
	// * 把中间结果，根据 ihash(key) % NReduce，写入不同的文件中，以 json 格式
	// * 最后上报给 Coordinator

	log.Printf("[Mapping]: task-%d, file-%s\n", t.TaskId, t.FileName)

	file, err := os.Open(t.FileName)
	if err != nil {
		log.Fatalln("[Mapping]: fail to open file", err)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalln("[Mapping]: fail to read file", err)
	}

	intermediate := mapf(t.FileName, string(content))
	HashedKV := make([][]KeyValue, t.NReduce)
	for _, kv := range intermediate {
		index := ihash(kv.Key) % t.NReduce
		HashedKV[index] = append(HashedKV[index], kv)
	}

	for i := 0; i < t.NReduce; i++ {
		ofName := "mr-tmp-" + strconv.Itoa(t.TaskId) + "-" + strconv.Itoa(i) + ".txt"
		of, err := os.OpenFile(ofName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
		if err != nil {
			log.Fatalln("[Mapping]: fail to write to file: ", ofName, err)
		}
		enc := json.NewEncoder(of)
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		of.Close()
	}

	doReport(t)
}

// todo
// 进行 reduce 任务
func doReduceTask(t *Task, reducef func(string, []string) string) {

	doReport(t)
}

// do Map/Reduce 之后，进行上报
func doReport(t *Task) {
	rreq, rreply := &ReportRequest{t.TaskId}, &ReportReply{}
	ok := call("Coordinator.HandleReport", rreq, rreply)
	if !ok {
		log.Fatal("[Reporting]: fail to call Coordinator.HandleReport")
	}
}

// 等待，暂定为 1s
func doWaitTask() {
	log.Println("[Waiting]: all tasks working, wait for a while...")
	time.Sleep(1 * time.Second)
}

// todo
// 暂时不需要内容，后续可能可以添加实现收尾
func doExitTask() {

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
