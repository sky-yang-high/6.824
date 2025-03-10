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
			go rf.trySendAppendEntries(i, true)
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
// ? 感觉心跳信号不需要传下来，因为走日志复制的逻辑的话，传出去的 entry 也是空的
func (rf *Raft) trySendAppendEntries(peer int, isHeart bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果处理某个响应时状态变更了
	if rf.electionState != StateLeader {
		return
	}

	// 不应该 >, 最多只能 =
	if rf.nextIndex[peer] > len(rf.logs) {
		klog.V(1).Infof("{s%d t%d} [send/log] p%d nextIdx %d > logLength %d", rf.me, rf.currentTerm, peer, rf.nextIndex[peer], len(rf.logs))
		return
	}

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: rf.nextIndex[peer] - 1,
		PrevLogTerm:  rf.logs[rf.nextIndex[peer]-1].Term,
		Entries:      rf.logs[rf.nextIndex[peer]:],
	}
	reply := &AppendEntriesReply{}

	klog.V(3).Infof("{s%d t%d} [send/log] p%d, isHeart %t, prev %d, logLength %d", rf.me, rf.currentTerm, peer, isHeart, args.PrevLogIndex, len(args.Entries))
	rf.mu.Unlock()

	ok := rf.sendAppendEntries(peer, args, reply)

	rf.mu.Lock()
	if !ok {
		klog.V(1).Infof("{s%d t%d} [send/log] p%d timeout", rf.me, rf.currentTerm, peer)
		return
	}

	if rf.currentTerm < reply.Term {
		klog.V(1).Infof("{s%d t%d} [rcv/reply] p%d send a higher t%d, backward to follower", rf.me, rf.currentTerm, peer, reply.Term)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.changeState(StateFollower)
		rf.electionTicker.Reset(randomElectionOutTime())
		return
	}

	klog.V(3).Infof("{s%d t%d} [rcv/reply] p%d, isHeart %t, prev %d, logLength %d, result: %t", rf.me, rf.currentTerm, peer, isHeart, args.PrevLogIndex, len(args.Entries), reply.Success)

	// 不成功，递减 nextIdx，等下次调用
	if !reply.Success {
		// todo: 优化，reply 中直接告诉 next 应该是几
		rf.nextIndex[peer]--
		return
	}

	// 成功，更新相关字段
	rf.nextIndex[peer] = len(args.Entries) + args.PrevLogIndex + 1
	rf.matchIndex[peer] = rf.nextIndex[peer] - 1
	// todo: 调用 apply 过程
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[peer].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	klog.V(3).Infof("{s%d t%d} [rcv/log] ld%d t%d, prev %d, logLength %d", rf.me, rf.currentTerm, args.LeaderId, args.Term, args.PrevLogIndex, len(args.Entries))

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.changeState(StateFollower)
	}

	rf.electionTicker.Reset(randomElectionOutTime())
	reply.Term = rf.currentTerm

	// 比较日志
	if len(args.Entries) != 0 {
		if !rf.hasMatchEntry(args.PrevLogIndex, args.PrevLogTerm) {
			return
		}
		// 删除冲突的条目
		rf.logs = rf.logs[:args.PrevLogIndex]
		// 追加新条目
		rf.logs = append(rf.logs, args.Entries...)

	}

	// 更新 commitIndex
	if args.LeaderCommit > rf.commitIndex {
		oldCommit := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
		klog.V(2).Infof("{s%d t%d} [rcv/log] ldCommit %d, commit %d -> %d", rf.me, rf.currentTerm, args.LeaderCommit, oldCommit, rf.commitIndex)
	}
	reply.Success = true
}

// 只在 lock 中调用，无需加锁
func (rf *Raft) hasMatchEntry(prevIndex, prevTerm int) bool {
	if len(rf.logs)-1 < prevIndex {
		return false
	}
	if rf.logs[prevIndex].Term != prevTerm {
		return false
	}
	return true
}
