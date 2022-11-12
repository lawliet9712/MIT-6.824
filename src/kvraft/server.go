package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
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
	ClerkId int64
	SeqId int
	Server int
	MsgUniqueId int64 // Msg unique id, for rpc reply
}

type ClerkOps struct {
	seqId int // clerk current seq id
	getCh    chan Op
	putAppendCh chan Op
	msgUniqueId int64
	mu *sync.Mutex
}

func (ck *ClerkOps) setMsgUniqueId(uid int64) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.msgUniqueId = uid
}

func (ck *ClerkOps) getMsgUniqueId() int64 {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.msgUniqueId
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister
	dataSource map[string]string
	messageMap map[int64]*ClerkOps // clerk id to ClerkOps struct
	// safe timestamp
	safeTimestamp int64
}

func (kv *KVServer) WaitApplyMsgByCh (ch chan Op, ck *ClerkOps) (Op, Err) {
	startTerm, _ := kv.rf.GetState()
	timer := time.NewTimer(1000 * time.Millisecond)
	for {
		select {
		case Msg := <-ch:
			return Msg, OK
		case <- timer.C:
			curTerm, isLeader := kv.rf.GetState()
			if curTerm != startTerm || !isLeader {
				ck.setMsgUniqueId(0)
				return Op{}, ErrWrongLeader
			}
			timer.Reset(1000 * time.Millisecond)
		}
	}
}

func (kv *KVServer) GetCk(ckId int64) *ClerkOps {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ck, found :=  kv.messageMap[ckId]
	if !found {
		ck = new(ClerkOps)
		ck.seqId = 0
		ck.getCh = make(chan Op)
		ck.putAppendCh = make(chan Op)
		ck.mu = &kv.mu
		kv.messageMap[ckId] = ck
		DPrintf("[KVServer-%d] Init ck %d getCh=%v, putAppendCh=%v", kv.me ,ckId, ck.getCh, ck.putAppendCh)
	}
	return kv.messageMap[ckId]
}

func (kv *KVServer) GenMsgUniqueId() int64 {
	return time.Now().UnixNano()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// step 1 : start a command, check kv is leader, wait raft to commit command
	//_, isLeader := kv.rf.GetState()
	ck := kv.GetCk(args.ClerkId)
	// check msg 
	kv.mu.Lock()
	// already process
	if ck.seqId > args.SeqId {
		reply.Err = OK
		_, foundData := kv.dataSource[args.Key]
		if !foundData {
			reply.Err = ErrNoKey
		} else {
			reply.Value = kv.dataSource[args.Key]
		}
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// start a command
	msgUniqueId := kv.GenMsgUniqueId() 
	ck.setMsgUniqueId(msgUniqueId)
	_, _, isLeader := kv.rf.Start(Op{
		Key:     args.Key,
		Command: "Get",
		ClerkId : args.ClerkId,
		SeqId : args.SeqId,
		Server : kv.me,
		MsgUniqueId : msgUniqueId,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[KVServer-%d] Received Req Get %v", kv.me, args)
	// step 2 : parse op struct
	getMsg, err := kv.WaitApplyMsgByCh(ck.getCh, ck)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[KVServer-%d] Received Msg [Get] 	 args=%v, SeqId=%d, Msg=%v", kv.me, args, args.SeqId, getMsg)
	reply.Err = err
	if err != OK{
		// leadership change, return ErrWrongLeader
		return
	}

	kv.SaveState()
	_, foundData := kv.dataSource[getMsg.Key]
	if !foundData {
		reply.Err = ErrNoKey
		return
	} else {
		reply.Value = kv.dataSource[getMsg.Key]
		DPrintf("[KVServer-%d] Excute Get %s is %s", kv.me, getMsg.Key, reply.Value)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// step 1 : start a command, wait raft to commit command
	// not found then init
	ck := kv.GetCk(args.ClerkId)
	// check msg 
	kv.mu.Lock()
	// already process
	if ck.seqId > args.SeqId {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	kv.mu.Unlock()
	
	// start a command
	msgUniqueId := kv.GenMsgUniqueId()
	ck.setMsgUniqueId(msgUniqueId)
	_, _, isLeader := kv.rf.Start(Op{
		Key:     args.Key,
		Value:   args.Value,
		Command: args.Op,
		ClerkId : args.ClerkId,
		SeqId : args.SeqId,
		Server : kv.me,
		MsgUniqueId : msgUniqueId,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[KVServer-%d] Received Req PutAppend %v, SeqId=%d", kv.me, args, args.SeqId)
	// step 2 : wait the channel
	reply.Err = OK
	Msg, err := kv.WaitApplyMsgByCh(ck.putAppendCh, ck)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.SaveState()
	DPrintf("[KVServer-%d] Received Msg [PutAppend] from ck.putAppendCh args=%v, SeqId=%d, Msg=%v", kv.me, args, args.SeqId, Msg)
	reply.Err = err
	if err != OK {
		DPrintf("[KVServer-%d] leader change args=%v, SeqId=%d", kv.me, args, args.SeqId)
		return
	}
}

func (kv *KVServer) SortMsg() {
	for {
		//kv.mu.Lock()
		//kv.safeTimestamp = time.Now().UnixNano()
		//kv.mu.Unlock()
		applyMsg := <-kv.applyCh
		Msg := applyMsg.Command.(Op)
		DPrintf("[KVServer-%d] Received Msg from channel. Msg=%v", kv.me, Msg)
		ck := kv.GetCk(Msg.ClerkId)

		var ch chan Op
		_, isLeader := kv.rf.GetState()
		switch Msg.Command {
		case "Put":
			ch = ck.putAppendCh
		case "Append":
			ch = ck.putAppendCh
		case "Get" :
			ch = ck.getCh
		default:
			DPrintf("[KVServer-%d] Error !!!! Msg=%v", kv.me, Msg)
		}
		
		// cache msg, because we lost the middle request
		// process msg
		needNotify := ck.getMsgUniqueId() == Msg.MsgUniqueId
		if Msg.Server == kv.me && isLeader && needNotify {
			// notify channel and reset timestamp
			ck.setMsgUniqueId(0)
			DPrintf("[KVServer-%d] Process Msg %v finish, ready send to ck.Ch, SeqId=%d isLeader=%v", kv.me, Msg, ck.seqId, isLeader)
			ch <- Msg
			DPrintf("[KVServer-%d] Process Msg %v Send to Rpc handler finish SeqId=%d isLeader=%v", kv.me, Msg, ck.seqId, isLeader)
		}

		// already process this request, so ignore
		if Msg.SeqId < ck.seqId {
			DPrintf("[KVServer-%d] Get old Msg %v, SeqId=%d, continue", kv.me, Msg, ck.seqId)
			continue
		} 

		// do logic
		kv.mu.Lock()
		ck.seqId = Msg.SeqId + 1
		switch Msg.Command {
		case "Put":
			kv.dataSource[Msg.Key] = Msg.Value
			kv.SaveState()
			DPrintf("[KVServer-%d] Excute Put key=%s value=%s", kv.me, Msg.Key, Msg.Value)
		case "Append":
			DPrintf("[KVServer-%d] Excute Append key=%s value=%s", kv.me, Msg.Key, Msg.Value)
			kv.dataSource[Msg.Key] += Msg.Value
			kv.SaveState()
		}
		kv.mu.Unlock()
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

func (kv *KVServer) healthCheck() {
	// 
	for {
		currentTs := time.Now().UnixNano()
		_, leader := kv.rf.GetState()
		output := fmt.Sprintf("consume time out , isleader=%v", leader)
		kv.mu.Lock()
		if (currentTs - kv.safeTimestamp) > (int64)(6000 * time.Millisecond) {
			panic(output)
		}
		kv.mu.Unlock()
	}
}

// save kv state and raft snapshot
func (kv *KVServer) SaveState() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	messageMap := make(map[int64]ClerkOps)
	for ckId, ck := range kv.messageMap {
		messageMap[ckId] = *ck
	}
	e.Encode(messageMap)
	e.Encode(kv.dataSource)

	kvstate := w.Bytes()
	kv.persister.SaveStateAndSnapshot(kvstate, kv.rf.GetRaftState())
}

func (kv *KVServer) readKVState(data []byte) {
	// Your code here (2C).
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	messageMap := make(map[int64]ClerkOps)
	dataSource := make(map[string]string)
	//var commitIndex int
	if d.Decode(&messageMap) != nil ||
		d.Decode(&dataSource) != nil {
		DPrintf("[readKVState] decode failed ...")
	} else {
		for ckId, ck := range messageMap {
			kv.messageMap[ckId] = &ck
		}
		kv.dataSource = dataSource
		DPrintf("[readKVState] messageMap=%v dataSource=%v" , messageMap, dataSource)
	}
}

func (kv *KVServer) readRaftState(data []byte) {
	kv.rf.ReadPersist(data)
}

// read kv state and raft snapshot
func (kv *KVServer) readState() {
	kv.readKVState(kv.persister.ReadRaftState())
	kv.readRaftState(kv.persister.ReadSnapshot())
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
	kv.applyCh = make(chan raft.ApplyMsg, 1000) // for test3A TestSpeed
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.safeTimestamp = time.Now().UnixNano()

	DPrintf("Start KVServer-%d", me)
	// You may need initialization code here.
	kv.dataSource = make(map[string]string)
	kv.messageMap = make(map[int64]*ClerkOps)
	kv.persister = persister
	kv.readState()
	go kv.SortMsg()
	//go kv.healthCheck()
	return kv
}
