package raft

import (
	"k8s.io/klog/v2"
)

type LogEntry struct {
	Term    int         // leader 收到该时 log 的任期
	Command interface{} // 命令
}

type AppendEntriesArgs struct {
	Term     int // leader 的任期号
	LeaderId int // leader id

	// 2B
	PrevLogIndex int        // 上一日志条目的索引
	PrevLogTerm  int        // 上一个日志条目的任期号
	Entries      []LogEntry // 日志条目
	LeaderCommit int        // leader 已提交的日志索引
}
type AppendEntriesReply struct {
	Term    int  // 节点的任期号
	Success bool // 日志追加是否成功
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.electionState != StateLeader {
		return -1, -1, false
	}

	rf.logs = append(rf.logs, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
	klog.V(2).Infof("{s%d t%d} [rcv/log] leader append log %d", rf.me, rf.currentTerm, len(rf.logs)-1)

	go rf.broadcast(false)
	return len(rf.logs) - 1, rf.currentTerm, true
}

// 广播，包括心跳和日志复制
func (rf *Raft) broadcast(isHeart bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.electionState != StateLeader {
		return
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		if isHeart {
			go rf.trySendAppendEntries(i, isHeart)
		} else {
			rf.replicatorCond[i].Signal() // 和上一版的关键区别
		}
	}
}

// 使用条件变量来发送日志
func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()

	for !rf.killed() {
		// 需要复制，则直接进行复制，否则Start等待唤醒
		for !rf.needReplicating() {
			rf.replicatorCond[peer].Wait()
		}

		rf.trySendAppendEntries(peer, false)
	}
}

// todo: 判断是否需要复制日志
func (rf *Raft) needReplicating() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return false
}

// todo: 实现具体的 leader-follower 直接日志复制的逻辑
func (rf *Raft) trySendAppendEntries(peer int, isHeart bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果处理某个响应时状态变更了
	if rf.electionState != StateLeader {
		return
	}
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[peer].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	klog.V(3).Infof("{s%d t%d} [rcv/log] ld%d t%d", rf.me, rf.currentTerm, args.LeaderId, args.Term)

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.changeState(StateFollower)
		rf.electionTicker.Reset(randomElectionOutTime())
		reply.Term = rf.currentTerm
		return
	}

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}

	// 2A 不考虑日志，只看心跳
	rf.electionTicker.Reset(randomElectionOutTime())
	reply.Term = rf.currentTerm
	reply.Success = true
}
