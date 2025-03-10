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
	// todo: 使用 RW 锁来进一步控制粒度
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

	// leader 的易矢性状态，每次选举后重新初始化
	nextIndex  []int // 对每个节点，发送到该节点的下一个日志条目索引
	matchIndex []int // 对每个节点，最大的已复制到该节点的日志条目索引，用于更新 commitIndex

	replicatorCond []*sync.Cond // 使用条件变量来控制日志复制

	// 其他
	electionState   ElectionState // 节点当前状态
	electionTicker  *time.Ticker  // 选举超时定时器
	heartbeatTicker *time.Ticker  // 心跳定时器

	applyCh chan ApplyMsg // apply 日志到状态机的通道
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
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.logs)
		}
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
			rf.broadcast(true)
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
	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{
		mu:        sync.Mutex{},
		peers:     peers,
		persister: persister,
		me:        me,

		currentTerm: 0,
		votedFor:    -1,
		logs:        []LogEntry{{Term: 0}},

		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		replicatorCond: make([]*sync.Cond, len(peers)),

		electionState:   StateFollower,
		electionTicker:  time.NewTicker(randomElectionOutTime()),
		heartbeatTicker: time.NewTicker(randomHeartbeatTime()),

		applyCh: applyCh,
	}

	// 一开始不需要心跳计时
	rf.heartbeatTicker.Stop()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.matchIndex[i], rf.nextIndex[i] = 0, 1
		rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
		go rf.replicator(i)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
