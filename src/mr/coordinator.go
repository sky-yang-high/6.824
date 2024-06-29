package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type PhaseType int

const (
	Mapping PhaseType = iota
	Reducing
	Exitting
)

// Coordinator 的定义
type Coordinator struct {
	files   []string      //需要进行 map 的 files
	nReduce int           //用于 map 的 hash
	done    chan struct{} //是否(map 和 reduce)任务都完成了
	phase   PhaseType     //当前处于哪个阶段

	bitm *bitmap //维护 task 的完成情况，和tasks应该同步，
	// 即若id处于tasks中，则在bitm 中置为一定为0
	// 另外，我们这里的 tasks 不超过 max(len(files),nReduce)，因此 bitm 只需要一个 uint32
	tasks        map[int]*Task  //需要处理和正在处理的tasks
	nextTaskid   int            //下一个task创建时分配的id
	nextAssignid int            //下一个分配给 worker 的 task  id
	heartCh      chan heartMsg  //worker 心跳 的 chan
	reportCh     chan reportMsg // worker report 的 chan
}

type heartMsg struct {
	hreply *HeartReply   // heartBeatReply，需要填充数据，返回给 worker
	ok     chan struct{} // 数据是否填充完毕
}

type reportMsg struct {
	rreq *ReportRequest // 读取 task id，标记为已完成
	ok   chan struct{}  //是否标记完成
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:   files,
		nReduce: nReduce,
		done:    make(chan struct{}),
		phase:   Mapping,

		bitm:     &bitmap{}, //todo，改为设置初始大小和容量
		tasks:    map[int]*Task{},
		heartCh:  make(chan heartMsg),
		reportCh: make(chan reportMsg),
	}

	// 启动两个后台协程，一个接收 rpc 请求，一个处理任务
	go c.server()
	go c.Schedule()
	return &c
}

// Your code here -- RPC handlers for the worker to call.

// * called by ./worker.go: heartBeat()
// 分配任务返回
func (c *Coordinator) HandleHeartBeat(hreq *HeartRequset, hreply *HeartReply) error {
	msg := heartMsg{hreply: hreply, ok: make(chan struct{})}

	c.heartCh <- msg
	<-msg.ok

	return nil
}

// * called by ./worker.go: doReport()
// 确认被完成的任务
func (c *Coordinator) HandleReport(rreq *ReportRequest, rreply *ReportReply) error {
	msg := reportMsg{rreq: rreq, ok: make(chan struct{})}

	c.reportCh <- msg
	<-msg.ok

	return nil
}

// 处理来自 HandleHeartBeat 和 HandleReport 的 reply 和 request
func (c *Coordinator) Schedule() {
	c.initMapPhase()

	log.Println("[Schedule]: assigning tasks")
	for {
		select {
		case hmsg := <-c.heartCh:
			c.AssignTask(hmsg.hreply)
			hmsg.ok <- struct{}{}
		case rmsg := <-c.reportCh:
			c.AcceptReport(rmsg.rreq)
			rmsg.ok <- struct{}{}
		}
		// 检查 c.bitm 是否全为1，若是，则表示当前阶段结束，转下一阶段
		if c.bitm.isAllSet() {
			switch c.phase {
			case Mapping:
				c.initExitPhase() //现在只实现 map
				//c.initReducePhase()
			case Reducing:
				c.initExitPhase()
			case Exitting:
				// ! 不会执行到这里，因为initExit 时应该吧 bitm clear
			}
		}
	}

}

// 初始化为 mapPhase，把 files 创建为 task，
func (c *Coordinator) initMapPhase() {
	log.Println("[initMap] initializing....")
	for i := 0; i < len(c.files); i++ {
		c.nextTaskid++
		t := &Task{
			Type:     MapTask,
			TaskId:   c.nextTaskid,
			NReduce:  c.nReduce,
			FileName: c.files[i],
		}
		c.tasks[c.nextTaskid] = t
	}
}

// todo: 初始化为 reducePhase
func (c *Coordinator) initReducePhase() {

}

// todo: 初始化为 exitPhase
func (c *Coordinator) initExitPhase() {
	// 启动一个定时器，时间到达后，c.schedule 退出
}

// 为心跳请求分配 task
func (c *Coordinator) AssignTask(hreply *HeartReply) {
	// * 实现思路：所有的 task 放置在一个环上，每次把环上当前位置的 task 分配出去
	// * 然后，跳转到下一个任务的位置，如果有任务完成，则从环上移走该任务
	// * 可以发现，如果某个任务第一次被分配出去后，worker 挂了，会在下一轮重新分配给其他 worker
	// * 难点在于跳转到下一个环的位置，要求 bitmap 提供接口
	defer log.Println("[Assign] assigned task: ", hreply.FileName)

	// 告知每个 worker exit
	if c.phase == Exitting {
		hreply.Type = ExitTask
		return
	}

	id := c.bitm.findFirstZeroAfter(c.nextAssignid)
	c.nextAssignid = id + 1
	t := c.tasks[id]

	hreply.Task = *t //? 或许这里应该减少一次拷贝?
}

// 接收上报信息
func (c *Coordinator) AcceptReport(rreq *ReportRequest) {
	// * 实现思路：把 bitmap 中对应位置1，并从 tasks 中移除对应的 task
	log.Println("[Accept] accept task id: ", rreq.TaskId)

	c.bitm.set(rreq.TaskId, 1)
	delete(c.tasks, rreq.TaskId)
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	<-c.done //阻塞，直到 c.done 获得数据

	return true
}
