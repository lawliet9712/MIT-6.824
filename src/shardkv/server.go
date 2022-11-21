package shardkv


import "6.824/labrpc"
import "6.824/raft"
import "sync"
import "log"
import "time"
import "6.824/labgob"

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
	Key         string
	Value       string
	Command     string
	ClerkId     int64
	SeqId       int
}

type ShardKVClerk struct {
	seqId 	  int
	messageCh chan Op
	msgUniqueId int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dataSource  map[string]string // db
	shardkvClerks map[int64]*ShardKVClerk  // ckid to ck
}

func (kv *ShardKV) WaitApplyMsgByCh(ch chan Op, ck *ShardKVClerk) (Op, Err) {
	startTerm, _ := kv.rf.GetState()
	timer := time.NewTimer(500 * time.Millisecond)
	for {
		select {
		case Msg := <-ch:
			return Msg, OK
		case <-timer.C:
			curTerm, isLeader := kv.rf.GetState()
			if curTerm != startTerm || !isLeader {
				kv.mu.Lock()
				ck.msgUniqueId = 0
				kv.mu.Unlock()
				return Op{}, ErrWrongLeader
			}
			timer.Reset(500 * time.Millisecond)
		}
	}
}

func (kv *ShardKV) NotifyApplyMsgByCh(ch chan Op, Msg Op) {
	// we wait 200ms 
	// if notify timeout, then we ignore, because client probably send request to anthor server
	timer := time.NewTimer(200 * time.Millisecond)
	select {
	case ch <- Msg:
		return
	case <-timer.C:
		DPrintf("[ShardKV-%d] NotifyApplyMsgByCh Msg=%v, timeout", kv.me, Msg)
		return
	}
}

func (kv *ShardKV) GetCk(ckId int64) *ShardKVClerk {
	ck, found := kv.shardkvClerks[ckId]
	if !found {
		ck = new(ShardKVClerk)
		ck.seqId = 0
		ck.messageCh = make(chan Op)
		kv.shardkvClerks[ckId] = ck
		DPrintf("[ShardKV-%d] Init ck %d", kv.me, ckId)
	}
	return kv.shardkvClerks[ckId]
}

/*
	@note : check the request is valid ?
	@retval : true mean message valid 
*/
func (kv *ShardKV) isRequestVaild() bool {
	return true
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	DPrintf("[ShardKV-%d] Received Req Get %v", kv.me, args)
	// start a command
	ck := kv.GetCk(args.ClerkId)
	logIndex, _, isLeader := kv.rf.Start(Op{
		Key:     args.Key,
		Command: "Get",
		ClerkId: args.ClerkId,
		SeqId:   args.SeqId,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		ck.msgUniqueId = 0
		kv.mu.Unlock()
		return
	}
	DPrintf("[ShardKV-%d] Received Req Get %v, waiting logIndex=%d", kv.me, args, logIndex)
	ck.msgUniqueId = logIndex
	kv.mu.Unlock()
	// step 2 : parse op struct
	getMsg, err := kv.WaitApplyMsgByCh(ck.messageCh, ck)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[ShardKV-%d] Received Msg [Get] args=%v, SeqId=%d, Msg=%v", kv.me, args, args.SeqId, getMsg)
	reply.Err = err
	if err != OK {
		// leadership change, return ErrWrongLeader
		return
	}

	_, foundData := kv.dataSource[getMsg.Key]
	if !foundData {
		reply.Err = ErrNoKey
	} else {
		reply.Value = kv.dataSource[getMsg.Key]
		DPrintf("[ShardKV-%d] Excute Get %s is %s", kv.me, getMsg.Key, reply.Value)
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	DPrintf("[ShardKV-%d] Received Req PutAppend %v, SeqId=%d ", kv.me, args, args.SeqId)
	// start a command
	logIndex, _, isLeader := kv.rf.Start(Op{
		Key:     args.Key,
		Value:   args.Value,
		Command: args.Op,
		ClerkId: args.ClerkId,
		SeqId:   args.SeqId,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	ck := kv.GetCk(args.ClerkId)
	ck.msgUniqueId = logIndex
	DPrintf("[ShardKV-%d] Received Req PutAppend %v, waiting logIndex=%d", kv.me, args, logIndex)
	kv.mu.Unlock()
	// step 2 : wait the channel
	reply.Err = OK
	Msg, err := kv.WaitApplyMsgByCh(ck.messageCh, ck)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[ShardKV-%d] Recived Msg [PutAppend] from ck.putAppendCh args=%v, SeqId=%d, Msg=%v", kv.me, args, args.SeqId, Msg)
	reply.Err = err
	if err != OK {
		DPrintf("[ShardKV-%d] leader change args=%v, SeqId=%d", kv.me, args, args.SeqId)
		return
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) processMsg() {
	for {
		applyMsg := <-kv.applyCh
		Msg := applyMsg.Command.(Op)
		DPrintf("[ShardKV-%d] Received Msg from channel. Msg=%v", kv.me, applyMsg)

		kv.mu.Lock()
		ck := kv.GetCk(Msg.ClerkId)

		_, isLeader := kv.rf.GetState()
		needNotify := ck.msgUniqueId == applyMsg.CommandIndex
		if isLeader && needNotify {
			ck.msgUniqueId = 0
			DPrintf("[ShardKV-%d] Process Msg %v finish, ready send to ck.Ch, SeqId=%d isLeader=%v", kv.me, applyMsg, ck.seqId, isLeader)
			kv.NotifyApplyMsgByCh(ck.messageCh, Msg)
			DPrintf("[ShardKV-%d] Process Msg %v Send to Rpc handler finish SeqId=%d isLeader=%v", kv.me, applyMsg, ck.seqId, isLeader)
		}

		if Msg.SeqId < ck.seqId {
			DPrintf("[ShardKV-%d] Ignore Msg %v,  Msg.SeqId < ck.seqId", kv.me, applyMsg)
			kv.mu.Unlock()
			continue
		}

		switch Msg.Command {
		case "Put":
			kv.dataSource[Msg.Key] = Msg.Value
			DPrintf("[ShardKV-%d] Excute CkId=%d Put Msg=%v, kvdata=%v", kv.me, Msg.ClerkId, applyMsg, kv.dataSource)
		case "Append":
			DPrintf("[ShardKV-%d] Excute CkId=%d Append Msg=%v kvdata=%v", kv.me, Msg.ClerkId, applyMsg, kv.dataSource)
			kv.dataSource[Msg.Key] += Msg.Value
		case "Get":
			DPrintf("[ShardKV-%d] Excute CkId=%d Get Msg=%v kvdata=%v", kv.me, Msg.ClerkId, applyMsg, kv.dataSource)
		}
		ck.seqId = Msg.SeqId + 1
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) queryForever() {
	queryInterval := 100 * time.Millisecond
	for {
		time.Sleep(queryInterval)
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.dataSource = make(map[string]string)
	kv.shardkvClerks = make(map[int64]*ShardKVClerk)

	go kv.processMsg()
	return kv
}
