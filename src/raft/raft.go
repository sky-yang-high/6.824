package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
	"k8s.io/klog/v2"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type ElectionState int

const (
	Follower  ElectionState = iota
	Candidate ElectionState = iota
	Leader    ElectionState = iota
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers, peers 中包含自己
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 持久性状态
	currentTerm int       // 节点已知的最新的任期 (初始化时为0，单调递增)
	votedFor    *int      // 当前任期内投给票的candidateId，如果没有则为空(用指针是因为节点编号从0开始)
	logs        []RaftLog // 日志条目，每个条目包含命令和leader收到该条目的任期

	// 易矢性状态
	commitIndex int // 已知已提交的最高的日志条目的索引 (初值为0，单调递增)
	lastApplied int // 已知被应用到状态机的最高的日志条目的索引 (初值为0，单调递增)

	// leader 的易矢性状态，每次选举后重新初始化
	nextIndex   []int         // 对每个节点，发送到该节点的下一日志条目的索引
	matchIndex  []int         // 对于每个，已知的已经复制到该节点的最高日志条目的索引
	heartBeatCh chan struct{} // 控制定时心跳的channel，退出leader状态时，关闭，进入leader状态时，重新初始化

	// 其他添加的状态
	electionState ElectionState // 当前本节点的状态(三个枚举值，Follower, Candidate, Leader)
	voteRPCFlag   bool          // ticker过程收到投票rpc的标志
	appendRPCFlag bool          // ticker过程收到日志rpc的标志
}

type RaftLog struct {
	Operation string // 日志条目的命令，若为空则表示心跳信息
	Term      int    // 该日志被leader接收时的任期
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// (2A)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.electionState == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// 2A
	Term         int // 发起投票的 candidate
	CandidateId  int // 发起投票的 candidate 的 id
	LastLogIndex int // 发起投票的 candidate 的最后日志条目的索引值
	LastLogTerm  int // 发起投票的 candidate 的最后日志条目的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	// 2A
	Term        int  // 投票者的任期号
	VoteGranted bool // 是否投给该 candidate
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 2A
	defer func() {
		reply.Term = rf.currentTerm
	}()

	rf.mu.Lock()
	rf.voteRPCFlag = true
	rf.mu.Unlock()

	rf.mu.Lock()
	if rf.currentTerm < args.Term {
		// 更新任期，并重置投票
		rf.currentTerm = args.Term
		rf.votedFor = nil
		if rf.electionState != Follower {
			rf.mu.Unlock()
			rf.changeState(Follower)
		} else {
			rf.mu.Unlock()
		}
	} else {
		rf.mu.Unlock()
	}

	// 自己任期号更大
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		return
	}

	// 已投票
	if rf.votedFor != nil {
		reply.VoteGranted = false
		return
	}

	// 日志比candidate更新
	if rf.logs[len(rf.logs)-1].Term > args.LastLogTerm {
		reply.VoteGranted = false
		return
	}
	if (len(rf.logs) - 1) > args.LastLogIndex {
		reply.VoteGranted = false
		return
	}

	// 同意投票
	klog.V(2).Infof("[Vote] server %d term %d vote for candidate %d term %d", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	rf.mu.Lock()
	votedFor := args.CandidateId
	rf.votedFor = &votedFor
	rf.mu.Unlock()

	reply.VoteGranted = true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries RPC, 也被当做心跳使用
type AppendEntriesArgs struct {
	Term         int       // leader 的 term，只有心跳的 term 为 0
	LeaderId     int       // leader id, 便于让客户端重定向
	PrevLogIndex int       // 新日志条目的上一个日志条目的索引
	PrevLogTerm  int       // 新日志条目的上一个日志条目的索引
	Entries      []RaftLog // 日志条目，可能会有多个条目来提高效率
	LeaderCommit int       // leader 已知的已提交的最高日志条目的索引
}

type AppendEntriesReply struct {
	Term    int  // follower 的 term
	Success bool // todo: 2A 默认返回 true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 2A
	defer func() {
		reply.Term = rf.currentTerm
	}()

	rf.mu.Lock()
	rf.appendRPCFlag = true
	rf.mu.Unlock()

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		// 当前不是 follower，且收到更高任期的信号，则转为 follower
		if rf.electionState != Follower {
			rf.changeState(Follower)
		}
	}

	// todo: 2A中只有心跳信号，因此不检验日志，留给2B完成
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// 开始超时选举前，重置标志位
		// todo: 考虑用 ch 实现，感觉会更好，不过多个flag的情况又怎么处理呢
		rf.mu.Lock()
		rf.voteRPCFlag = false
		rf.appendRPCFlag = false
		rf.mu.Unlock()

		// todo: 超时选举时间，后面需要调整
		sleepDuration := 100 + rand.Int31()%100
		time.Sleep(time.Duration(sleepDuration) * time.Millisecond)

		// 如果中间收到过 投票rpc/日志rpc，重新下一轮ticker
		rf.mu.Lock()
		if rf.voteRPCFlag || rf.appendRPCFlag {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()

		// 没有收到, 变更状态为 candidate，然后发起投票
		// todo: 开始选举后，也要重置超时计数器，，避免选举过程超时，即下面的过程也应该并发进行
		klog.V(2).Infof("[Vote] follower %d, term %d, turn into candidate, start request vote", rf.me, rf.currentTerm)
		rf.changeState(Candidate)
		candidateRequestVote(rf)
	}
}

// 候选人请求投票，不需要一直等待，获得多数选票即可
func candidateRequestVote(rf *Raft) {
	var mu sync.Mutex
	var voteCountReached bool
	ch := make(chan *RequestVoteReply, len(rf.peers))

	for i := 0; i < len(rf.peers); i++ {
		req := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
			LastLogIndex: len(rf.logs) - 1,
		}

		go func(i int) {
			// 不需要跳过自己，投票自己肯定投给自己
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, req, reply)

			if !ok {
				reply.Term = rf.currentTerm
				reply.VoteGranted = false
			}

			mu.Lock()
			if !voteCountReached {
				ch <- reply
			}
			mu.Unlock()
		}(i)
	}

	// 处理投票结果
	// 如果未获得半数以上票，则重新等待下一轮选举
	// 否则，成为 leader，更新 leader 状态并广播心跳
	var reply *RequestVoteReply
	voteCount, totalCount := 0, 0

	for {
		select {
		case reply = <-ch:
			totalCount++
			if reply.VoteGranted {
				voteCount++
				if voteCount >= (len(rf.peers)+1)/2 {
					mu.Lock()
					voteCountReached = true
					// 确保后续不会在发送给ch
					close(ch)
					mu.Unlock()

					klog.V(2).Infof("[Vote] candidate %d got %d vote, turn into leader", rf.me, voteCount)
					rf.changeState(Leader)
					// 假定成为 leader 过程不会被打断
					for {
						if rf.heartBeatCh == nil {
							continue
						}

						// 阻塞 leader 自己的 ticker
						<-rf.heartBeatCh
						break
					}
					return
				}
			}
		default:
			if totalCount >= len(rf.peers) {
				// 未获得半数以上选票, 退回 follower, 重启下一轮 ticker
				klog.V(2).Infof("[Vote] candidate %d got %d vote, fail to turn into leader", rf.me, voteCount)
				rf.changeState(Follower)
				return
			}
		}
	}
}

// 变更状态为预期状态
func (rf *Raft) changeState(expectedState ElectionState) {
	// follower -> candidate
	// candidate -> follower
	// candidate -> leader
	// leader -> follower
	switch expectedState {
	case Follower:
		changeStateToFollower(rf)
	case Candidate:
		changeStateToCandidate(rf)
	case Leader:
		changeStateToLeader(rf)
	}
}

// 初始化，或者收到更高的 term 的 appendRPC
func changeStateToFollower(rf *Raft) {
	rf.mu.Lock()
	oldState := rf.electionState
	rf.electionState = Follower
	if rf.heartBeatCh != nil {
		close(rf.heartBeatCh)
	}
	klog.V(2).Infof("[StateChange] server %d term %d, oldState %d, change state to <Follower>", rf.me, rf.currentTerm, oldState)
	rf.mu.Unlock()
}

// 选举超时时间到
func changeStateToCandidate(rf *Raft) {
	rf.mu.Lock()
	oldState := rf.electionState
	rf.currentTerm += 1
	rf.votedFor = nil
	rf.electionState = Candidate
	klog.V(2).Infof("[StateChange] server %d term %d, oldState %d, change state to <Candidate>", rf.me, rf.currentTerm, oldState)
	rf.mu.Unlock()
}

// 获得多数选票
func changeStateToLeader(rf *Raft) {
	rf.mu.Lock()
	oldState := rf.electionState
	rf.electionState = Leader
	rf.heartBeatCh = make(chan struct{})
	klog.V(2).Infof("[StateChange] server %d term %d, oldState %d, change state to <Leader>", rf.me, rf.currentTerm, oldState)
	rf.mu.Unlock()

	// 立即广播心跳一次
	heartBeatArgs := &AppendEntriesArgs{
		Term:     0,
		LeaderId: rf.me,
	}
	Broadcast(rf, heartBeatArgs)

	// 后续定期广播心跳
	go rf.HeartBeatOnTime()
}

func (rf *Raft) HeartBeatOnTime() {
	if rf.electionState != Leader {
		klog.V(1).Infof("[Warning] server %d term %d is not leader, but try to heartbeat", rf.me, rf.currentTerm)
		return
	}

	heartBeatArgs := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	for {
		// 定时心跳
		// todo: 心跳间隔需要修改
		heartBeatDuration := 50 + rand.Int31()%50
		time.Sleep(time.Duration(heartBeatDuration) * time.Millisecond)

		select {
		case <-rf.heartBeatCh:
			rf.heartBeatCh = nil
			return
		default:
			Broadcast(rf, heartBeatArgs)
		}
	}
}

// todo: 待完善，需要处理 reply。特别是更新 term
// ! 暂时先不考虑接收 reply，后面需要改
func Broadcast(rf *Raft, args *AppendEntriesArgs) []*AppendEntriesReply {
	klog.V(3).Infof("[HeartBeat] leader %d, term %d broadcast heartBeat", rf.me, rf.currentTerm)
	replys := make([]*AppendEntriesReply, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		reply := &AppendEntriesReply{}
		go func(i int) {
			// 最多 retry 3 次
			for j := 0; j < 3; j++ {
				ok := rf.sendAppendEntries(i, args, reply)
				if ok {
					break
				}
			}
		}(i)
		replys[i] = reply
	}

	var success, fail []int
	// for i := 0; i < len(rf.peers); i++ {
	// 	if replys[i] == nil {
	// 		if i == rf.me {
	// 			continue
	// 		}
	// 		fail = append(fail, i)
	// 		continue
	// 	}

	// 	if !replys[i].Success {
	// 		fail = append(fail, i)
	// 		continue
	// 	}
	// 	success = append(success, i)
	// }

	klog.V(3).Infof("[HeartBeat] leader %d, term %d broadcast heartBeat result is: success %v, fail %v", rf.me, rf.currentTerm, success, fail)
	return replys
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	rf.currentTerm = 0
	rf.votedFor = nil
	// log索引从1开始，所以第0位填充一个term为0的无效log
	rf.logs = append(rf.logs, RaftLog{"", 0})

	rf.electionState = Follower
	rf.changeState(Follower)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
