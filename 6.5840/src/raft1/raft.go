package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	state       int // follower is 0, candidater is 1 and leader is 2
	nextIndex   []int
	matchIndex  []int
	log         []LogEntry
	commitIndex int
	lastApplied int
	currentTime time.Time
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry // nil for heartbeats
	LeaderCommit int
}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (3A).

	term = rf.currentTerm
	isleader = rf.state == 2

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Terms        int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTime = time.Now()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Terms > rf.currentTerm {
		rf.currentTerm = args.Terms
		reply.Term = rf.currentTerm
		rf.state = 0
		rf.votedFor = -1
	}

	if args.Terms < rf.currentTerm {
		return
	}

	// 检查投票状态
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}

	// 添加日志比较逻辑
	lastLogIndex := len(rf.log) - 1
	var lastLogTerm int
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	} else {
		lastLogTerm = 0
	}

	// 检查候选人的日志是否至少一样新
	if args.LastLogTerm < lastLogTerm {
		return
	}
	if args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex {
		return
	}

	rf.votedFor = args.CandidateId
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

	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	if rf.state != 2 {
		return index, term, false
	}

	// Leader接受命令
	term = rf.currentTerm
	index = len(rf.log)

	// 将命令添加到日志
	rf.log = append(rf.log, LogEntry{
		Term:    term,
		Command: command,
	})

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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTime = time.Now()
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = 0
		rf.votedFor = -1
	}

	// 检查prevLogIndex边界
	if args.PrevLogIndex >= 0 {
		if args.PrevLogIndex >= len(rf.log) {
			return
		}
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			return
		}
	}

	// 修复：直接使用args.Entries，不需要转换
	if args.Entries != nil && len(args.Entries) > 0 {
		for i, newEntry := range args.Entries {
			currentIndex := args.PrevLogIndex + 1 + i

			if currentIndex >= len(rf.log) {
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}

			if rf.log[currentIndex].Term != newEntry.Term {
				rf.log = rf.log[:currentIndex]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		lastLogIndex := len(rf.log) - 1
		rf.commitIndex = min(args.LeaderCommit, lastLogIndex)
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()

	if rf.state != 2 {
		rf.mu.Unlock()
		return
	}

	// 在锁内准备所有需要的数据
	currentTerm := rf.currentTerm
	leaderId := rf.me
	prevLogIndex := len(rf.log) - 1
	var prevLogTerm int
	if prevLogIndex >= 0 {
		prevLogTerm = rf.log[prevLogIndex].Term
	}
	leaderCommit := rf.commitIndex

	// 释放锁后再发送RPC
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer != rf.me {
			go func(server int) {
				// 使用之前捕获的值，不需要重新加锁
				args := AppendEntriesArgs{
					Term:         currentTerm,
					LeaderId:     leaderId,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					LeaderCommit: leaderCommit,
					Entries:      nil,
				}
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(server, &args, &reply)
			}(peer)
		}
	}
}

func (rf *Raft) becomeLeader() {
	// 调用此函数时必须已经持有锁
	rf.state = 2
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	lastLogIndex := len(rf.log)
	for i := range rf.nextIndex {
		rf.nextIndex[i] = lastLogIndex
		rf.matchIndex[i] = 0
	}

	// 不在这里调用sendHeartbeat，而是让ticker处理
}

func (rf *Raft) startElection(args *RequestVoteArgs) {
	votes := &atomic.Int32{} // 使用新的atomic类型
	votes.Store(1)

	for peer := range rf.peers {
		if peer != rf.me {
			go func(server int) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(server, args, &reply)
				if ok {
					rf.processVoteReply(server, args, &reply, votes)
				}
			}(peer)
		}
	}
}

func (rf *Raft) processVoteReply(server int, args *RequestVoteArgs, reply *RequestVoteReply, votes *atomic.Int32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = 0
		rf.votedFor = -1
		return
	}

	if rf.state != 1 || rf.currentTerm != args.Terms {
		return
	}

	if reply.VoteGranted {
		newVotes := votes.Add(1)
		if int(newVotes) > len(rf.peers)/2 {
			rf.becomeLeader()
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		if state == 2 {
			// Leader: 发送心跳
			rf.sendHeartbeat()
			time.Sleep(100 * time.Millisecond)
		} else {
			// Follower 或 Candidate: 检查选举超时
			rf.mu.Lock()
			electionTimeout := time.Duration(150+(rand.Int63()%150)) * time.Millisecond
			timeSinceLastUpdate := time.Since(rf.currentTime)
			shouldStartElection := timeSinceLastUpdate > electionTimeout

			if shouldStartElection {
				// 在锁内创建选举参数的副本
				electionArgs := RequestVoteArgs{
					Terms:        rf.currentTerm + 1,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.log) - 1,
					LastLogTerm:  0,
				}
				if electionArgs.LastLogIndex >= 0 {
					electionArgs.LastLogTerm = rf.log[electionArgs.LastLogIndex].Term
				}

				// 更新状态
				rf.state = 1
				rf.currentTerm = electionArgs.Terms
				rf.votedFor = rf.me
				rf.currentTime = time.Now()

				rf.mu.Unlock()
				rf.startElection(&electionArgs)
			} else {
				rf.mu.Unlock()
			}

			time.Sleep(50 * time.Millisecond)
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = 0 // follower
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.currentTime = time.Now()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
