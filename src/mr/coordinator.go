package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

type PhaseType int

const (
	Mapping PhaseType = iota
	Reducing
	Exitting
)

var (
	defaultTimeout = 6.0 //jobcout(见jobcount.go) 的最大时延是5s，超时时间比它长一点即可
)

// Coordinator 的定义
type Coordinator struct {
	files   []string      //需要进行 map 的 files
	nReduce int           //用于 map 和 reduce 的 hash 模数
	done    chan struct{} //是否(map 和 reduce)任务都完成了
	phase   PhaseType     //当前处于哪个阶段

	bitm *bitmap //维护 task 的完成情况，和tasks应该同步，
	// 即若id处于tasks中，则在bitm 中置为一定为0
	tasks        map[int]*Task  //需要处理和正在处理的tasks
	nextTaskid   int            //下一个task创建时分配的id
	nextAssignid int            //下一个分配给worker的task的id
	heartCh      chan heartMsg  //worker 心跳 的 chan
	reportCh     chan reportMsg //worker report 的 chan
	exitch       chan struct{}  //用于终止退出的 chan
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

		bitm:     NewBitMap(uint(len(files))),
		tasks:    map[int]*Task{},
		heartCh:  make(chan heartMsg),
		reportCh: make(chan reportMsg),
		exitch:   make(chan struct{}),
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

	//log.Println("[Schedule] assigning tasks")
	for {
		select {
		case hmsg := <-c.heartCh:
			c.AssignTask(hmsg.hreply)
			hmsg.ok <- struct{}{}
		case rmsg := <-c.reportCh:
			c.AcceptReport(rmsg.rreq)
			rmsg.ok <- struct{}{}
		case <-c.exitch:
			//log.Println("[Schedule] Coordinator successfully exit ")
			return
		}
		// 检查 c.bitm 是否全为1，若是，则表示当前阶段结束，转下一阶段
		if c.bitm.isAllSet() {
			switch c.phase {
			case Mapping:
				c.initReducePhase()
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
	//log.Println("[initMap] initializing....")
	for i := 0; i < len(c.files); i++ {
		t := &Task{
			Type:      MapTask,
			TaskId:    c.nextTaskid,
			NReduce:   c.nReduce,
			FileNames: []string{c.files[i]},
		}
		c.tasks[c.nextTaskid] = t
		c.nextTaskid++
	}

	// //把 bitmap 中额外的位置置为1
	// for pos := len(c.files); pos < defaultBitSize*8; pos++ {
	// 	c.bitm.set(uint(pos))
	// }
}

// 初始化为 reducePhase，把相同 hash 后缀的 file 创建为一个 task(即-0.txt为一个task，-1.txt为另一个)
func (c *Coordinator) initReducePhase() {
	//log.Println("[initRedice] initializing...")
	c.phase = Reducing
	fgroup := selectReduceFiles(c.nReduce)

	// 重置 tasks
	c.tasks = make(map[int]*Task)
	c.nextAssignid = 0
	c.nextTaskid = 0
	for i := 0; i < len(fgroup); i++ {
		t := &Task{
			Type:      ReduceTask,
			TaskId:    c.nextTaskid,
			NReduce:   c.nReduce,
			FileNames: fgroup[i],
		}
		c.tasks[c.nextTaskid] = t
		c.nextTaskid++
	}

	c.bitm = NewBitMap(uint(c.nReduce))

	// //重置 bitmap
	// c.bitm.clear()
	// for i := c.nReduce; i < defaultBitSize*8; i++ {
	// 	c.bitm.set(uint(i))
	// }
}

// 把所有以 mr-tmp-x-y.txt 的文件名，按 y 汇合为 nreduce 组
func selectReduceFiles(nReduce int) [][]string {
	fgroup := make([][]string, nReduce)
	pwd, _ := os.Getwd()
	files, _ := os.ReadDir(pwd)
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		fName := f.Name()
		if strings.HasPrefix(fName, "mr-tmp-") {
			sepIdx := strings.LastIndex(fName, "-")
			pointIdx := strings.LastIndex(fName, ".")
			idx, err := strconv.Atoi(fName[sepIdx+1 : pointIdx])
			if err != nil {
				log.Fatalln("[selectReduce] fail to convert string to index")
			}
			fgroup[idx] = append(fgroup[idx], fName)
		}
	}
	return fgroup
}

func (c *Coordinator) initExitPhase() {
	c.phase = Exitting
	c.bitm.clear()
	go func() {
		// 启动一个定时器，时间到达后，c.schedule 退出
		time.Sleep(2 * time.Second)
		c.exitch <- struct{}{}
		c.done <- struct{}{}
	}()
}

// 为心跳请求分配 task
func (c *Coordinator) AssignTask(hreply *HeartReply) {
	// * 实现思路：所有的 task 放置在一个环上，每次把环上当前位置的 task 分配出去
	// * 然后，跳转到下一个任务的位置，如果有任务完成，则从环上移走该任务
	// * 可以发现，如果某个任务第一次被分配出去后，worker 挂了，会在下一轮重新分配给其他 worker
	// * 难点在于跳转到下一个环的位置，要求 bitmap 提供接口

	// 告知每个 worker exit
	if c.phase == Exitting || len(c.tasks) == 0 {
		hreply.Type = ExitTask
		return
	}

	id := c.bitm.findFirstZeroAfter(c.nextAssignid)

	// 没有要分配的任务
	if id == -1 {
		hreply.Type = WaitTask
		return
	}

	//第一次被分配
	if c.tasks[id].StartTime.IsZero() {
		c.tasks[id].StartTime = time.Now()
	} else if time.Since(c.tasks[id].StartTime).Seconds() > defaultTimeout {
		c.tasks[id].StartTime = time.Now() //超时了，重新分配
	} else {
		hreply.Type = WaitTask //还没超时，不需要重新分配
		return
	}

	c.nextAssignid = id + 1
	t := c.tasks[id]

	hreply.Task = *t //? 或许这里应该减少一次拷贝?
	//log.Println("[Assign] assigned task: ", hreply.FileNames)
}

// 接收上报信息
func (c *Coordinator) AcceptReport(rreq *ReportRequest) {
	// * 实现思路：把 bitmap 中对应位置1，并从 tasks 中移除对应的 task
	//log.Println("[Accept] accept task id: ", rreq.TaskId)

	c.bitm.set(uint(rreq.TaskId))
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
