package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.

type Entry struct {
	Term    int
	Command interface{}
}

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
	state       int // 1 is leader， 2 is candidate
	voteForId   int

	lastElectionReset time.Time
	electionTimeout   time.Duration

	log         []Entry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	// channel to send entry
	applyCh chan raftapi.ApplyMsg

	snapShot          []byte // 快照
	lastIncludedIndex int    // 日志中的最高索引
	lastIncludedTerm  int    // 日志中的最高Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = (rf.state == 1)
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteForId)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapShot)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var log []Entry
	var currentTerm int
	var voteForId int
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteForId) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {

	} else {
		rf.currentTerm = currentTerm
		rf.voteForId = voteForId
		rf.log = log

		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm

		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Offset           int
	Data             []byte
	Done             bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = 0
		rf.voteForId = -1
		// 持久化 term/vote 变化
		rf.persist()
	}

	if args.Offset == 0 {
		rf.snapShot = make([]byte, 0)
	}
	// write data from offset

	if len(rf.snapShot) < args.Offset+len(args.Data) {
		newSnapShot := make([]byte, args.Offset+len(args.Data))
		copy(newSnapShot, rf.snapShot)
		rf.snapShot = newSnapShot
	}
	copy(rf.snapShot[args.Offset:], args.Data)

	if args.Done {
		if args.LastIncludeIndex <= rf.lastIncludedIndex {
			rf.mu.Unlock()
			return
		}
		oldLastIncludedIndex := rf.lastIncludedIndex
		rf.lastIncludedIndex = args.LastIncludeIndex
		rf.lastIncludedTerm = args.LastIncludeTerm
		// 截断日志
		newLog := make([]Entry, 1)
		newLog[0] = Entry{Term: rf.lastIncludedTerm, Command: nil}
		for i := 0; i < len(rf.log); i++ {
			realIndex := oldLastIncludedIndex + i
			if realIndex > args.LastIncludeIndex {
				newLog = append(newLog, rf.log[i])
			}
		}
		rf.log = newLog

		// 更新索引
		if rf.commitIndex < args.LastIncludeIndex {
			rf.commitIndex = args.LastIncludeIndex
		}
		if rf.lastApplied < args.LastIncludeIndex {
			rf.lastApplied = args.LastIncludeIndex
		}
		rf.persist()

		snap := make([]byte, len(rf.snapShot))
		copy(snap, rf.snapShot)
		lastIndex := rf.lastIncludedIndex
		lastTerm := rf.lastIncludedTerm

		rf.mu.Unlock()

		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      snap,
			SnapshotIndex: lastIndex,
			SnapshotTerm:  lastTerm,
		}
		return
	}

	rf.mu.Unlock()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}

	rf.snapShot = snapshot
	relativeIndex := index - rf.lastIncludedIndex
	rf.lastIncludedTerm = rf.log[relativeIndex].Term

	// 保留 dummy entry 和 index 之后的日志
	newLog := make([]Entry, 1)
	newLog[0] = Entry{Term: rf.lastIncludedTerm, Command: nil}
	newLog = append(newLog, rf.log[relativeIndex+1:]...)
	rf.log = newLog

	rf.lastIncludedIndex = index

	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
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
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = 0
		rf.voteForId = -1
		rf.lastElectionReset = time.Now()
		rf.persist()
	}
	lastLogIndex := len(rf.log) - 1 + rf.lastIncludedIndex
	lastLogTerm := rf.lastIncludedTerm
	if len(rf.log) > 1 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}

	logIsUpToDate := false

	if args.LastLogTerm > lastLogTerm {
		logIsUpToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		logIsUpToDate = true
	}

	if (rf.voteForId == -1 || rf.voteForId == args.CandidateId) && logIsUpToDate {
		rf.voteForId = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.lastElectionReset = time.Now()
		rf.persist()
	}
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
	index := -1
	term := rf.currentTerm
	isLeader := (rf.state == 1)

	// Your code here (3B).

	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	entry := Entry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	rf.persist()
	index = len(rf.log) + rf.lastIncludedIndex - 1
	prevLogIndex := index - 1
	prevLogTerm := 0

	if prevLogIndex == rf.lastIncludedIndex {
		prevLogTerm = rf.lastIncludedTerm
	} else if prevLogIndex > rf.lastIncludedIndex {
		relativeIndex := prevLogIndex - rf.lastIncludedIndex
		if relativeIndex < len(rf.log) {
			prevLogTerm = rf.log[relativeIndex].Term
		}
	}

	args := AppendEntriesArgs{
		Term:         term,
		Leaderid:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      []Entry{entry},
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			var reply AppendEntriesReply
			ok := rf.sendAppendEntries(server, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 检查 term 是否过期
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = 0
				rf.voteForId = -1
				rf.lastElectionReset = time.Now()
				return
			}

			// 确保还是 leader 且 term 没变
			if rf.state != 1 || rf.currentTerm != term {
				return
			}

			if reply.Success {
				rf.matchIndex[server] = index
				rf.nextIndex[server] = index + 1

				// 尝试推进 commitIndex，使用绝对索引
				for N := len(rf.log) - 1 + rf.lastIncludedIndex; N > rf.commitIndex; N-- {
					if N <= rf.lastIncludedIndex {
						break
					}
					relativeN := N - rf.lastIncludedIndex
					if relativeN < 1 || relativeN >= len(rf.log) {
						continue
					}
					count := 1
					for i := range rf.peers {
						if i != rf.me && rf.matchIndex[i] >= N {
							count++
						}
					}
					// 超过半数且该 entry 是当前 term 的
					if count > len(rf.peers)/2 && rf.log[relativeN].Term == rf.currentTerm {
						rf.commitIndex = N
						break
					}
				}
			} else {
				if reply.ConflictTerm == -1 {
					rf.nextIndex[server] = reply.ConflictIndex
				} else {
					found := false
					for i := len(rf.log) - 1; i > 0; i-- {
						if rf.log[i].Term == reply.ConflictTerm {
							rf.nextIndex[server] = i + 1
							found = true
							break
						}
					}
					if !found {
						rf.nextIndex[server] = reply.ConflictIndex
					}
				}
				if rf.nextIndex[server] < 1 {
					rf.nextIndex[server] = 1
				}
			}
		}(i)
	}

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

type AppendEntriesArgs struct {
	Term     int
	Leaderid int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictTerm  int // the term of conflict
	ConflictIndex int // the first log of conflict logs
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = 0
		rf.voteForId = -1
		rf.persist()
	}

	rf.lastElectionReset = time.Now()

	relativePrevIndex := args.PrevLogIndex - rf.lastIncludedIndex

	// 如果 prevLogIndex 已经被快照了，直接成功
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = true
		reply.Term = rf.currentTerm
		return
	}

	// 如果 prevLogIndex 超出了当前日志范围
	if relativePrevIndex >= len(rf.log) {
		reply.ConflictIndex = len(rf.log) + rf.lastIncludedIndex
		reply.ConflictTerm = -1
		return
	}

	// 检查 prevLogIndex 处的 term 是否匹配
	if rf.log[relativePrevIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[relativePrevIndex].Term
		reply.ConflictIndex = args.PrevLogIndex
		for i := relativePrevIndex; i >= 1; i-- {
			if rf.log[i].Term != reply.ConflictTerm {
				reply.ConflictIndex = rf.lastIncludedIndex + i + 1
				break
			}
			if i == 1 {
				reply.ConflictIndex = rf.lastIncludedIndex + 1
			}
		}
		return
	}

	startAppendIndex := 0
	if len(args.Entries) > 0 {
		for i, entry := range args.Entries {
			realIndex := args.PrevLogIndex + 1 + i
			relativeIndex := realIndex - rf.lastIncludedIndex

			if relativeIndex < len(rf.log) {
				if rf.log[relativeIndex].Term != entry.Term {
					rf.log = rf.log[:relativeIndex]
					startAppendIndex = i
					break
				}
				startAppendIndex = i + 1
			} else {
				startAppendIndex = i
				break
			}
		}
		rf.log = append(rf.log, args.Entries[startAppendIndex:]...)
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		rf.commitIndex = min(args.LeaderCommit, lastNewEntryIndex)
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	term := rf.currentTerm
	rf.state = 2
	rf.voteForId = rf.me

	rf.persist()

	lastLogIndex := rf.lastIncludedIndex + len(rf.log) - 1
	lastLogTerm := rf.lastIncludedTerm
	if len(rf.log) > 1 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	rf.mu.Unlock()
	count := 1
	var mu sync.Mutex

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			voteArgs := RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			var voteReply RequestVoteReply

			ok := rf.sendRequestVote(server, &voteArgs, &voteReply)
			if !ok {
				return
			}

			rf.mu.Lock()
			if voteReply.Term > rf.currentTerm {
				rf.currentTerm = voteReply.Term
				rf.state = 0
				rf.voteForId = -1
				rf.lastElectionReset = time.Now()
				rf.persist()
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			if voteReply.VoteGranted {
				mu.Lock()
				count++
				if count >= (len(rf.peers)/2 + 1) {
					rf.mu.Lock()
					if rf.currentTerm == term && rf.state == 2 {
						rf.state = 1
						for i := range rf.peers {
							rf.nextIndex[i] = len(rf.log) + rf.lastIncludedIndex
							rf.matchIndex[i] = 0
						}
						rf.lastElectionReset = time.Now()
					}
					rf.mu.Unlock()
				}
				mu.Unlock()
			}

		}(i)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(10 * time.Millisecond)

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		state := rf.state
		lastReset := rf.lastElectionReset
		timeout := rf.electionTimeout
		rf.mu.Unlock()
		// 检测有没有收到心跳
		if state == 1 {
			rf.mu.Lock()

			for i := range rf.peers {
				if i == rf.me {
					continue
				}

				// 如果 nextIndex 已经被 snapshot 了，发送 InstallSnapshot
				if rf.nextIndex[i] <= rf.lastIncludedIndex {
					args := InstallSnapshotArgs{
						Term:             rf.currentTerm,
						LeaderId:         rf.me,
						LastIncludeIndex: rf.lastIncludedIndex,
						LastIncludeTerm:  rf.lastIncludedTerm,
						Offset:           0,
						Data:             rf.snapShot,
						Done:             true,
					}
					currentTerm := rf.currentTerm

					go func(server int, args InstallSnapshotArgs) {
						var reply InstallSnapshotReply
						ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
						if !ok {
							return
						}

						rf.mu.Lock()
						defer rf.mu.Unlock()

						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.state = 0
							rf.voteForId = -1
							rf.lastElectionReset = time.Now()
							rf.persist()
							return
						}

						if rf.state != 1 || rf.currentTerm != currentTerm {
							return
						}

						rf.nextIndex[server] = args.LastIncludeIndex + 1
						rf.matchIndex[server] = args.LastIncludeIndex
					}(i, args)
					continue
				}

				prevLogIndex := rf.nextIndex[i] - 1
				prevLogTerm := 0

				if prevLogIndex == rf.lastIncludedIndex {
					prevLogTerm = rf.lastIncludedTerm
				} else if prevLogIndex > rf.lastIncludedIndex {
					relativeIndex := prevLogIndex - rf.lastIncludedIndex
					if relativeIndex < len(rf.log) {
						prevLogTerm = rf.log[relativeIndex].Term
					}
				}

				var entries []Entry
				relativeNextIndex := rf.nextIndex[i] - rf.lastIncludedIndex
				if relativeNextIndex < len(rf.log) {
					// 有缺失的日志，发送从 nextIndex[i] 开始的所有日志
					entries = make([]Entry, len(rf.log[relativeNextIndex:]))
					copy(entries, rf.log[relativeNextIndex:])
				}

				heartBeatArgs := AppendEntriesArgs{
					Term:         rf.currentTerm,
					Leaderid:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				currentTerm := rf.currentTerm

				go func(server int, args AppendEntriesArgs) {
					var heartBeatReply AppendEntriesReply
					ok := rf.sendAppendEntries(server, &args, &heartBeatReply)
					if !ok {
						return
					}

					rf.mu.Lock()
					defer rf.mu.Unlock()

					if heartBeatReply.Term > rf.currentTerm {
						rf.currentTerm = heartBeatReply.Term
						rf.state = 0
						rf.voteForId = -1
						rf.lastElectionReset = time.Now()
						rf.persist()
						return
					}

					// 确保还是 leader 且 term 没变
					if rf.state != 1 || rf.currentTerm != currentTerm {
						return
					}

					if heartBeatReply.Success {
						// 更新 matchIndex 和 nextIndex
						if len(args.Entries) > 0 {
							newMatchIndex := args.PrevLogIndex + len(args.Entries)
							if newMatchIndex > rf.matchIndex[server] {
								rf.matchIndex[server] = newMatchIndex
								rf.nextIndex[server] = newMatchIndex + 1
							}
						}

						// 尝试推进 commitIndex，使用绝对索引
						for N := len(rf.log) - 1 + rf.lastIncludedIndex; N > rf.commitIndex; N-- {
							if N <= rf.lastIncludedIndex {
								break
							}
							relativeN := N - rf.lastIncludedIndex
							if relativeN < 1 || relativeN >= len(rf.log) {
								continue
							}
							count := 1 // leader 自己
							for i := range rf.peers {
								if i != rf.me && rf.matchIndex[i] >= N {
									count++
								}
							}
							// 超过半数且该 entry 是当前 term 的
							if count > len(rf.peers)/2 && rf.log[relativeN].Term == rf.currentTerm {
								rf.commitIndex = N
								break
							}
						}
					} else {
						if heartBeatReply.ConflictTerm == -1 {
							rf.nextIndex[server] = heartBeatReply.ConflictIndex
						} else {
							found := false
							for i := len(rf.log) - 1; i > 0; i-- {
								if rf.log[i].Term == heartBeatReply.ConflictTerm {
									rf.nextIndex[server] = i + 1
									found = true
									break
								}
							}
							if !found {
								rf.nextIndex[server] = heartBeatReply.ConflictIndex
							}
						}

						if rf.nextIndex[server] < 1 {
							rf.nextIndex[server] = 1
						}
					}
				}(i, heartBeatArgs)
			}
			rf.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		// 如果有的话继续下一轮随机时间，如果没有的话开始选举
		if time.Since(lastReset) >= timeout {
			rf.startElection()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()

		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			relativeIndex := rf.lastApplied - rf.lastIncludedIndex

			if relativeIndex < 1 || relativeIndex >= len(rf.log) {
				continue
			}

			// log[applyIndex] 对应索引 applyIndex 的 entry
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[relativeIndex].Command,
				CommandIndex: rf.lastApplied,
			}

			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		}

		rf.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
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
	rf.voteForId = -1
	rf.state = 0
	rf.currentTerm = 0
	rf.lastElectionReset = time.Now()
	rf.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond

	rf.commitIndex = 0
	rf.lastApplied = 0

	// 初始化 log，log[0] 是 dummy entry（索引从 1 开始）
	rf.log = make([]Entry, 1)
	rf.log[0] = Entry{Term: 0, Command: nil}

	// 保存 applyCh 通道
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapShot = persister.ReadSnapshot()

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range peers {
		rf.nextIndex[i] = len(rf.log) + rf.lastIncludedIndex
		rf.matchIndex[i] = 0
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
