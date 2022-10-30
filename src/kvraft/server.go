package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key     string
	Value   string
	Command string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dataSource map[string]string
}

func (kv *KVServer) Start(args Op) Op {
	kv.rf.Start(args)
	applyMsg := <-kv.applyCh
	return applyMsg.Command.(Op)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// step 0 : check kv is leader
	_, IsLeader := kv.rf.GetState()
	if !IsLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// step 1 : start a command, wait raft to commit command
	Command := kv.Start(Op{
		Key:     args.Key,
		Command: "Get",
	})

	// step 2 : parse op struct
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = OK
	_, found := kv.dataSource[Command.Key]
	if !found {
		reply.Err = ErrNoKey
		return
	} else {
		reply.Value = kv.dataSource[Command.Key]
		DPrintf("[KVServer-%d] Get %s is %s", kv.me, Command.Key, reply.Value)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// step 0 : check kv is leader
	_, IsLeader := kv.rf.GetState()
	if !IsLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// step 1 : start a command, wait raft to commit command
	Command := kv.Start(Op{
		Key:     args.Key,
		Value:   args.Value,
		Command: args.Op,
	})

	// step 2 : parse op struct
	reply.Err = OK
	if Command.Command == "Put" {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		_, found := kv.dataSource[Command.Key]
		if !found {
			reply.Err = ErrNoKey
			return
		} else {
			kv.dataSource[Command.Key] = Command.Value
			DPrintf("[KVServer-%d] Put %s to %s", kv.me, Command.Key, Command.Value)
		}
	} else if Command.Command == "Append" {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.dataSource[Command.Key] += Command.Value
		DPrintf("[KVServer-%d] Append %s to %s", kv.me, Command.Key, Command.Value)
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	DPrintf("%d Received Kill Command", kv.me)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.dataSource = make(map[string]string)
	return kv
}
