package raft

// log 格式: klog.V(X).Infof("{s1,t1} [(req/rcv/state)/(vote/log)] xxx")
// 锁: 先粒度粗一点，后面有需要再调细
// 超时选举时间: 200+(0-100) 心跳时间: 100+(0-50)，大约两个心跳时间都没有收到就尝试选举

import "k8s.io/klog/v2"

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 2A
	Term        int // candidate 的任期号
	CandidateId int // candidate 的 id

	// 2B
	LastLogIndex int // 最新的日志条目的索引
	LastLogTerm  int // 最新的日志条目的任期号
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //节点的任期号
	VoteGranted bool // 是否投给该 candidate
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		klog.V(3).Infof("{s%d t%d} [rcv/vote] cd%d t%d: %t", rf.me, rf.currentTerm, args.CandidateId, args.Term, reply.VoteGranted)
	}()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.changeState(StateFollower)
	}

	// 添加投票限制: 日志至少一样新才投给它
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogIndex := len(rf.logs) - 1
		lastLogTerm := rf.logs[lastLogIndex].Term
		if lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.electionTicker.Reset(randomElectionOutTime())
		}
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 尝试发起投票，一旦获得超过半数选票，成为 leader
func tryRequestVote(rf *Raft) {
	vote := 1     //投给自己
	done := false // 是否获得多数选票

	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logs) - 1,
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(i int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, args, reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 自己的 term 小了，退回 follower
			if rf.currentTerm < reply.Term {
				klog.V(1).Infof("{s%d t%d} [vote] s%d send a higher t%d, backward to follower", rf.me, rf.currentTerm, i, reply.Term)
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.changeState(StateFollower)
				rf.electionTicker.Reset(randomElectionOutTime())
				return
			}
			if !done && rf.currentTerm == reply.Term && reply.VoteGranted {
				vote++
				if vote >= (len(rf.peers)+1)/2 {
					done = true
					klog.V(1).Infof("{s%d t%d} [vote] got %d votes", rf.me, rf.currentTerm, vote)
					// 获得半数以上选票，成为 leader
					rf.changeState(StateLeader)
					go rf.broadcast(true)
				}
			}
			// 还有一种 rf.currentTerm > reply.Term 的情况，不应当出现，
			// 因为收到 RPC 的节点需要先更新自己的 term，然后再返回
		}(i)
	}
	// ? 这里如果没有获得多数选票，考虑是否要退回 follower
	// 想了一下不退也可以，因为如果之后收到 RPC，follower/candidate处理没有差别
	// 而如果没有收到，则在下次超时时还是进入 candidate 状态
}
