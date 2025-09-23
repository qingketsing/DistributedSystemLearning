package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.Mutex
	data    map[string]string
	version map[string]rpc.Tversion
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.data = make(map[string]string)
	kv.version = make(map[string]rpc.Tversion)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if value, exists := kv.data[args.Key]; exists {
		reply.Value = value
		reply.Version = kv.version[args.Key]
		reply.Err = rpc.OK
		return
	}

	reply.Err = rpc.ErrNoKey
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, exists := kv.data[args.Key]; exists {
		if kv.version[args.Key] != args.Version {
			reply.Err = rpc.ErrVersion
			return
		}
		kv.data[args.Key] = args.Value // 写入新值
		kv.version[args.Key] = args.Version + 1
		reply.Err = rpc.OK
		return
	} else {
		if args.Version == 0 {
			kv.data[args.Key] = args.Value
			kv.version[args.Key] = 1
			reply.Err = rpc.OK
			return
		} else {
			reply.Err = rpc.ErrNoKey
			return
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
