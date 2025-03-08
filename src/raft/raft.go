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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 持久性状态
	currentTerm int        // 节点当前任期
	votedFor    int        // 当前任期内投票给谁，未投票置为-1
	logs        []LogEntry // 日志条目

	// 易矢性状态
	commitIndex int // 最大的已提交的日志条目索引
	lastApplied int // 最大的已应用到状态机的日志条目索引

	// leader 的易矢性状态，每次选举后重新初始化
	nextIndex    []int        // 对每个节点，发送到该节点的下一个日志条目索引
	matchIndex   []int        // 对每个节点，最大的已复制到该节点的日志条目索引，用于更新 commitIndex
	matchCount   map[int]int  // 记录对于index i，matchIndex中超过i的节点个数
	hasCommitted map[int]bool // 记录对于 index i，是否已commit

	// 其他
	leaderId        int           // 记录 leader id
	electionState   ElectionState // 节点当前状态
	electionTicker  *time.Ticker  // 选举超时定时器
	heartbeatTicker *time.Ticker  // 心跳定时器
	applyCh         chan ApplyMsg // apply 日志条目的通道，把已提交的日志条目发到这里来模拟在物理机上 apply 该 cmd
}

type LogEntry struct {
	Term    int         // leader 收到该时 log 的任期
	Command interface{} // 命令
}

type ElectionState int

const (
	StateFollower  ElectionState = iota
	StateCandidate ElectionState = iota
	StateLeader    ElectionState = iota
)

var (
	StateMap map[ElectionState]string = map[ElectionState]string{
		StateFollower:  "follower",
		StateCandidate: "candidate",
		StateLeader:    "leader",
	}
)

func randomElectionOutTime() time.Duration {
	return time.Millisecond * time.Duration((300 + rand.Int31()%100))
}

func randomHeartbeatTime() time.Duration {
	return time.Millisecond * time.Duration((150 + rand.Int31()%50))
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.electionState == StateLeader
}

// 节点切换状态，保证只在加锁的内部被调用，因此这里不加锁
func (rf *Raft) changeState(expectedState ElectionState) {
	// follower -> candidate
	// candidate -> follower
	// candidate -> leader
	// leader -> follower

	klog.V(1).Infof("{s%d t%d} [state] %s -> %s", rf.me, rf.currentTerm, StateMap[rf.electionState], StateMap[expectedState])

	switch expectedState {
	case StateFollower:
		if rf.electionState == StateLeader {
			rf.heartbeatTicker.Stop()
		}
		rf.electionState = StateFollower
	case StateCandidate:
		rf.electionState = StateCandidate
		rf.currentTerm++
		rf.votedFor = rf.me
	case StateLeader:
		rf.electionState = StateLeader
		rf.electionTicker.Stop()
		rf.heartbeatTicker.Reset(randomHeartbeatTime())
		// 初始化日志相关数据
		leaderInit(rf)
	}
}

func leaderInit(rf *Raft) {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.matchCount = make(map[int]int)
	rf.hasCommitted = make(map[int]bool)

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.logs)
	}
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

type AppendEntriesArgs struct {
	// 2A
	Term     int // leader 的任期号
	LeaderId int // leader id
	// 2B
	PrevLogIndex int        // 前一个日志条目的索引
	PrevLogTerm  int        // 前一个日志条目的任期
	Entries      []LogEntry // 发送的日志条目，一次可发送多个来提高效率。心跳则为空
	LeaderCommit int        // leader 的 commitIndex
}
type AppendEntriesReply struct {
	Term    int  // 节点的任期号
	Success bool // 日志追加是否成功
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 注意用闭包而不是直接调用，不然Success的值一直都是false
	defer func() {
		if len(args.Entries) == 0 {
			klog.V(3).Infof("{s%d t%d} [rcv/heart] ld%d t%d: %t; commitIndex %d", rf.me, rf.currentTerm, args.LeaderId, args.Term, reply.Success, rf.commitIndex)
			return
		}
		klog.V(3).Infof("{s%d t%d} [rcv/log] ld%d t%d: %t; commitIndex %d; prevIndex %d, entryCnt %d", rf.me, rf.currentTerm, args.LeaderId, args.Term, reply.Success, rf.commitIndex, args.PrevLogIndex, len(args.Entries))
	}()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1 // 想了一下是否要置为leader id，觉得不需要
		rf.changeState(StateFollower)
	}
	rf.electionTicker.Reset(randomElectionOutTime())
	reply.Term = rf.currentTerm

	// 处理日志逻辑

	// 非心跳心跳
	if len(args.Entries) != 0 {
		ok, index := existMatchedEntry(rf, args.PrevLogIndex, args.PrevLogTerm)
		if !ok {
			return
		}
		// 删除冲突的条目
		rf.logs = rf.logs[:index+1]
		// 追加新条目
		rf.logs = append(rf.logs, args.Entries...)
	}

	// 更新 commitIndex
	if args.LeaderCommit > rf.commitIndex {
		prevIndex := len(rf.logs) - 1
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = args.LeaderCommit
		if rf.commitIndex > prevIndex {
			rf.commitIndex = prevIndex
		}
		// apply 已提交的日志条目
		go rf.applyLogEntries(oldCommitIndex, rf.commitIndex)
	}

	reply.Success = true
}

// 查找是否有日志 index 和 term 都匹配
// 没有返回 false 和 -1; 有则返回 true 和对应的 index
func existMatchedEntry(rf *Raft, index, term int) (bool, int) {
	if len(rf.logs)-1 < index {
		return false, -1
	}

	if rf.logs[index].Term != term {
		return false, -1
	}

	return true, index
}

func (rf *Raft) applyLogEntries(oldCommitIndex, commitIndex int) {
	if oldCommitIndex >= commitIndex {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	klog.V(3).Infof("{s%d t%d} [apply] apply logs, old %d, new %d", rf.me, rf.currentTerm, oldCommitIndex, commitIndex)

	for i := oldCommitIndex + 1; i <= commitIndex; i++ {
		msg := ApplyMsg{
			CommandValid: true,
			CommandIndex: i,
			Command:      rf.logs[i].Command,
		}
		rf.applyCh <- msg
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	// Your code here (2B).
	// 2B
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.electionState != StateLeader {
		return -1, -1, false
	}

	// 追加到自己的 logs 中，并向其他节点复制
	rf.logs = append(rf.logs, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})

	// todo: 重构，现在这样感觉不好看
	klog.V(2).Infof("{s%d t%d} [req/log] append log index %d", rf.me, rf.currentTerm, len(rf.logs)-1)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		nextIndex := rf.nextIndex[i]
		//lastLogIndex := len(rf.logs) - 1
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  rf.logs[nextIndex-1].Term,
			Entries:      rf.logs[nextIndex:],
			LeaderCommit: rf.commitIndex,
		}

		go func(i int) {
			for {
				rf.mu.Lock()
				if rf.electionState != StateLeader {
					rf.mu.Unlock()
					return
				}

				// 不发送重复的 log entry，应对同时来多个cmd的情况
				// 例如 已经发送了 log[1:3]，不需要重复发log[1:2]
				if nextIndex < rf.nextIndex[i] {
					rf.mu.Unlock()
					return
				}

				rf.mu.Unlock()

				reply := &AppendEntriesReply{}

				ok := rf.sendAppendEntries(i, args, reply)
				if !ok {
					rf.mu.Lock()
					klog.V(1).Infof("{s%d t%d} [req/log] s%d disconnected", rf.me, rf.currentTerm, i)
					rf.mu.Unlock()
					return
				}

				rf.mu.Lock()
				klog.V(2).Infof("{s%d t%d} [rcv/reply] handle server %d reply, prevIndex %d, entryCnt %d", rf.me, rf.currentTerm, i, args.PrevLogIndex, len(args.Entries))
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.changeState(StateFollower)
					// todo: 考虑是否要把重置超时时间写入 change 为 follower 中
					rf.electionTicker.Reset(randomElectionOutTime())
					rf.mu.Unlock()
					return
				}

				if reply.Success {
					// 不需要重复处理已发送的log
					// 例如先发了log[1:2],然后发log[1:3],但是log[1:3]先被reply，则log[1:2]的reply时无需重复处理
					if (nextIndex + len(args.Entries)) < rf.nextIndex[i] {
						rf.mu.Unlock()
						return
					}

					rf.nextIndex[i] = nextIndex + len(args.Entries)
					newMatchIndex := rf.nextIndex[i] - 1
					rf.matchIndex[i] = newMatchIndex

					rf.matchCount[newMatchIndex]++
					if !rf.hasCommitted[newMatchIndex] && (rf.matchCount[newMatchIndex]+1) >= (len(rf.peers)+1)/2 {
						rf.hasCommitted[newMatchIndex] = true
						oldCommitIndex := rf.commitIndex
						rf.commitIndex = newMatchIndex
						klog.V(2).Infof("{s%d t%d} [commit] update commitIndex, old %d, new %d", rf.me, rf.currentTerm, oldCommitIndex, rf.commitIndex)
						go rf.applyLogEntries(oldCommitIndex, newMatchIndex)
					}

					rf.mu.Unlock()
					return
				}

				// 不成功，则发送的日志往前移一个
				klog.V(2).Infof("{s%d t%d} [req/log] server %d nextIndex %d failed, retry a low index", rf.me, rf.currentTerm, i, nextIndex)
				nextIndex--
				args.Entries = rf.logs[nextIndex:]
				args.PrevLogIndex = args.PrevLogIndex - 1
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
				args.LeaderCommit = rf.commitIndex
				rf.mu.Unlock()
			}
		}(i)
	}

	rf.heartbeatTicker.Reset(randomHeartbeatTime())
	return len(rf.logs) - 1, rf.currentTerm, true
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

		select {
		case <-rf.electionTicker.C:
			// 超时，转为 candidate 并发起选举，并重置 ticker，避免选举过程超时卡住
			rf.electionTicker.Reset(randomElectionOutTime())
			rf.mu.Lock()
			rf.changeState(StateCandidate)
			rf.mu.Unlock()
			tryRequestVote(rf)
		case <-rf.heartbeatTicker.C:
			broadcastHeartbeat(rf)
			rf.heartbeatTicker.Reset(randomHeartbeatTime())
		}
	}
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
	rf.logs = []LogEntry{{Term: 0}} // 让 log entry 的初始索引为1
	rf.electionState = StateFollower
	rf.electionTicker = time.NewTicker(randomElectionOutTime())
	rf.heartbeatTicker = time.NewTicker(randomHeartbeatTime())
	// 一开始不需要心跳计时
	rf.heartbeatTicker.Stop()

	// 2B
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
