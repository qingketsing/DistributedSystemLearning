package rsm

import (
	"math/rand"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	SeqNum   int64 // to detect duplicate requests
	Req      any   // the actual request (Inc, Get, Put, etc.)
}

type OpReply struct {
	Value any // can be string, *IncRep, *NullRep, etc.
	Err   rpc.Err
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	alive            bool // check if server is alive
	lastAppliedIndex int  // index of last applied command
	kvStore          map[string]string
	clientSeq        map[int64]int64      // clientID -> last seqNum
	notifyChans      map[int]chan OpReply // log index -> chan to notify waiting RPC handler

}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		alive:        true,
		notifyChans:  make(map[int]chan OpReply),
		clientSeq:    make(map[int64]int64),
		kvStore:      make(map[string]string, 0),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	go rsm.applier()
	return rsm
}

func (rsm *RSM) applier() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			op, ok := msg.Command.(Op)
			if !ok {
				continue
			}

			var opReply OpReply
			opReply.Err = rpc.OK

			rsm.mu.Lock()
			lastSeq, seen := rsm.clientSeq[op.ClientID]
			if seen && op.SeqNum <= lastSeq {
				// duplicate request - for now, still execute it (proper implementation should cache result)
				res := rsm.sm.DoOp(op.Req)
				opReply.Value = res
			} else {
				// Pass the actual request (op.Req) to StateMachine, not the Op wrapper
				res := rsm.sm.DoOp(op.Req)
				// Store the result directly (can be *IncRep, string, etc.)
				opReply.Value = res
				rsm.clientSeq[op.ClientID] = op.SeqNum
			}
			rsm.lastAppliedIndex = msg.CommandIndex

			if ch, ok := rsm.notifyChans[msg.CommandIndex]; ok {
				select {
				case ch <- opReply:
				default:
				}
				delete(rsm.notifyChans, msg.CommandIndex)
			}
			rsm.mu.Unlock()
		} else if msg.SnapshotValid {
			rsm.mu.Lock()
			rsm.sm.Restore(msg.Snapshot)
			rsm.mu.Unlock()
		}
	}
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	if !rsm.alive {
		return rpc.ErrWrongLeader, nil
	}

	// Generate unique ID for this operation
	// Use nano timestamp + random to ensure uniqueness
	op := Op{
		ClientID: int64(rsm.me*1000000 + rand.Intn(1000000)), // unique per submit
		SeqNum:   time.Now().UnixNano(),
		Req:      req,
	}

	if rsm.rf == nil {
		return rpc.ErrWrongLeader, nil
	}

	index, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	ch := make(chan OpReply, 1)
	rsm.mu.Lock()
	rsm.notifyChans[index] = ch
	rsm.mu.Unlock()

	select {
	case reply := <-ch:
		// Verify we're still leader in the same term
		rsm.mu.Lock()
		currentTerm, stillLeader := rsm.rf.GetState()
		rsm.mu.Unlock()

		if !stillLeader || currentTerm != term {
			return rpc.ErrWrongLeader, nil
		}

		return reply.Err, reply.Value
	case <-time.After(2000 * time.Millisecond): // 增加超时时间
		rsm.mu.Lock()
		delete(rsm.notifyChans, index)
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
}
