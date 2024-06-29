package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
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

	log.Printf("[Mapping] task-%d, %s\n", t.TaskId, t.FileNames[0])

	file, err := os.Open(t.FileNames[0]) //对于 map 任务，file只有一个文件
	if err != nil {
		log.Fatalln("[Mapping]: fail to open file", err)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalln("[Mapping]: fail to read file", err)
	}

	kvs := mapf(t.FileNames[0], string(content))
	HashedKV := make([][]KeyValue, t.NReduce)
	for _, kv := range kvs {
		index := ihash(kv.Key) % t.NReduce
		HashedKV[index] = append(HashedKV[index], kv)
	}

	for i := 0; i < t.NReduce; i++ {
		ofName := "mr-tmp-" + strconv.Itoa(t.TaskId) + "-" + strconv.Itoa(i) + ".txt"
		of, err := os.OpenFile(ofName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
		if err != nil {
			log.Fatalln("[Mapping] fail to write to file: ", ofName, err)
		}
		enc := json.NewEncoder(of)
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		of.Close()
	}

	doReport(t)
}

// 进行 reduce 任务
func doReduceTask(t *Task, reducef func(string, []string) string) {
	// * 实现思路：读取 task 中的几个文件，用json 解码得到很多 kv对
	// * 然后排序，参照 mrsequential.go 中的思路，批量处理相同的 key，结果重新写入文件中(或许这里可以不需要重新hash)
	// * 最后上报给 Coordinator 即可
	log.Printf("[Reducing] task-%d, %v", t.TaskId, t.FileNames)
	var kvs []KeyValue
	kv := KeyValue{}
	for _, fn := range t.FileNames {
		f, err := os.Open(fn)
		if err != nil {
			log.Fatalln("[Reducing] fail to open file: ", fn)
		}
		dec := json.NewDecoder(f)
		for err := dec.Decode(&kv); err == nil; err = dec.Decode(&kv) {
			kvs = append(kvs, kv)
		}
		f.Close()
	}

	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})

	//理论上，这些 key 的 hash % nreduce 都是相同的，因此取第一个来创建文件即可
	oname := "mr-out-" + strconv.Itoa(ihash(kvs[0].Key)%t.NReduce) + ".txt"
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalln("[Reducing] fail to create file: ", err)
	}

	//from mrsequential.go
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)

		i = j
	}
	ofile.Close()

	doReport(t)
}

// do Map/Reduce 之后，进行上报
func doReport(t *Task) {
	rreq, rreply := &ReportRequest{t.TaskId}, &ReportReply{}
	ok := call("Coordinator.HandleReport", rreq, rreply)
	if !ok {
		log.Fatal("[Reporting] fail to call Coordinator.HandleReport")
	}
}

// 等待，暂定为 1s
func doWaitTask() {
	log.Println("[Waiting] all tasks working, wait for a while...")
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
