package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

// Add your RPC definitions here.

type TaskType int

const (
	MapTask    TaskType = iota //task 类型为 map
	ReduceTask                 //task 类型为 reduce
	WaitTask                   //task 类型为 wait
	ExitTask                   //task 类型为 exit
)

// 具体的 task 定义
type Task struct {
	Type      TaskType  //任务类型
	TaskId    int       //task 的 id
	NReduce   int       //用于 hash
	FileNames []string  //task 的文件
	StartTime time.Time //上一次被分配的时间，用于超时重分配
}

// 心跳请求，在 worker 有空时发送
type HeartRequset struct{}

// 心跳响应，在 worker 有空时发送，Coordinator填充数据
type HeartReply struct {
	Task
}

// 上报请求，worker 完成 task 时发送
type ReportRequest struct {
	TaskId int //完成的 task 的 id
}

// 上报响应，worker 完成 task 时发送
type ReportReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
