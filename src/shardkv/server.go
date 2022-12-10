package shardkv

import (
	"bytes"
	"log"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const Debug = false

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
	SeqId   int
	LeaveOp ShardChangeOp
	JoinOp  ShardJoinOp
	InitOp  ShardInitOp
	ConfOp  ShardConfOp
}

type ShardKVClerk struct {
	seqId       int
	messageCh   chan Notify
	msgUniqueId int
}

type Notify struct {
	Msg    Op
	Result Err
}

type ShardContainer struct {
	RetainShards   []int
	TransferShards []int
	WaitJoinShards []int
	ConfigNum      int
	QueryDone      bool
	ShardReqSeqId  int   // for local start raft command
	ShardReqId     int64 // for local start a op
}

type ShardChangeOp struct {
	LeaveShards    map[int][]int //
	WaitJoinShards []int
	Servers        map[int][]string
	ConfigNum      int
}

type ShardJoinOp struct {
	Shards     []int // move shard rpc
	ShardData  map[string]string
	ConfigNum  int
	RequestMap map[int64]int
}

type ShardInitOp struct {
	Shards    []int // init shards
	ConfigNum int
}

type ShardConfOp struct {
	ConfigNum int
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
	dataSource    map[string]string       // db
	shardkvClerks map[int64]*ShardKVClerk // ckid to ck
	mck           *shardctrler.Clerk      // clerk
	shards        ShardContainer          // this group hold shards
	persister     *raft.Persister
}

func (kv *ShardKV) WaitApplyMsgByCh(ch chan Notify, ck *ShardKVClerk) Notify {
	startTerm, _ := kv.rf.GetState()
	timer := time.NewTimer(500 * time.Millisecond)
	for {
		select {
		case notify := <-ch:
			return notify
		case <-timer.C:
			curTerm, isLeader := kv.rf.GetState()
			if curTerm != startTerm || !isLeader {
				kv.mu.Lock()
				ck.msgUniqueId = 0
				kv.mu.Unlock()
				return Notify{Result: ErrWrongLeader}
			}
			timer.Reset(500 * time.Millisecond)
		}
	}
}

func (kv *ShardKV) NotifyApplyMsgByCh(ch chan Notify, Msg Op) {
	// we wait 200ms
	// if notify timeout, then we ignore, because client probably send request to anthor server
	result := OK
	// check shard is already move ?
	if Msg.Command == "Get" || Msg.Command == "Put" || Msg.Command == "Append" {
		keyShard := key2shard(Msg.Key)
		if !isShardInGroup(keyShard, kv.shards.RetainShards) || isShardInGroup(keyShard, kv.shards.TransferShards) {
			result = ErrWrongGroup
		}
	}
	notify := Notify{
		Result: Err(result),
		Msg:    Msg,
	}
	timer := time.NewTimer(200 * time.Millisecond)
	select {
	case ch <- notify:
		DPrintf("[ShardKV-%d-%d] NotifyApplyMsgByCh kv.shards=%v, key2shard(Msg.Key)=%d Notify=%v", kv.gid, kv.me, kv.shards, key2shard(Msg.Key), notify)
		return
	case <-timer.C:
		DPrintf("[ShardKV-%d-%d] NotifyApplyMsgByCh Notify=%v, timeout", kv.gid, kv.me, notify)
		return
	}
}

func (kv *ShardKV) GetCk(ckId int64) *ShardKVClerk {
	ck, found := kv.shardkvClerks[ckId]
	if !found {
		ck = new(ShardKVClerk)
		ck.seqId = 0
		ck.messageCh = make(chan Notify)
		kv.shardkvClerks[ckId] = ck
		DPrintf("[ShardKV-%d-%d] Init ck %d", kv.gid, kv.me, ckId)
	}
	return kv.shardkvClerks[ckId]
}

/*
@note : check the request key is correct  ?
@retval : true mean message valid
*/
func (kv *ShardKV) isRequestKeyCorrect(key string) bool {
	// check key belong shard still hold in this group ?
	keyShard := key2shard(key)
	// check shard is transfering
	isShardTransfering := isShardInGroup(keyShard, kv.shards.TransferShards)
	return isShardInGroup(keyShard, kv.shards.RetainShards) && !isShardTransfering
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("[ShardKV-%d-%d] Received Req Get %v", kv.gid, kv.me, args)
	kv.mu.Lock()
	if !kv.isRequestKeyCorrect(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	DPrintf("[ShardKV-%d-%d] Received Req Get Begin %v", kv.gid, kv.me, args)
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
	DPrintf("[ShardKV-%d-%d] Received Req Get %v, waiting logIndex=%d", kv.gid, kv.me, args, logIndex)
	ck.msgUniqueId = logIndex
	kv.mu.Unlock()
	// step 2 : parse op struct
	notify := kv.WaitApplyMsgByCh(ck.messageCh, ck)
	getMsg := notify.Msg
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[ShardKV-%d-%d] Received Msg [Get] args=%v, SeqId=%d, Msg=%v", kv.gid, kv.me, args, args.SeqId, getMsg)
	reply.Err = notify.Result
	if reply.Err != OK {
		// leadership change, return ErrWrongLeader
		return
	}

	_, foundData := kv.dataSource[getMsg.Key]
	if !foundData {
		reply.Err = ErrNoKey
	} else {
		reply.Value = kv.dataSource[getMsg.Key]
		DPrintf("[ShardKV-%d-%d] Excute Get %s is %s", kv.gid, kv.me, getMsg.Key, reply.Value)
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("[ShardKV-%d-%d] Received Req PutAppend %v, SeqId=%d ", kv.gid, kv.me, args, args.SeqId)
	kv.mu.Lock()
	if !kv.isRequestKeyCorrect(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

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
	DPrintf("[ShardKV-%d-%d] Received Req PutAppend %v, waiting logIndex=%d", kv.gid, kv.me, args, logIndex)
	kv.mu.Unlock()
	// step 2 : wait the channel
	reply.Err = OK
	notify := kv.WaitApplyMsgByCh(ck.messageCh, ck)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[ShardKV-%d-%d] Recived Msg [PutAppend] from ck.putAppendCh args=%v, SeqId=%d, Msg=%v", kv.gid, kv.me, args, args.SeqId, notify.Msg)
	reply.Err = notify.Result
	if reply.Err != OK {
		DPrintf("[ShardKV-%d-%d] leader change args=%v, SeqId=%d", kv.gid, kv.me, args, args.SeqId)
		return
	}
}

func (kv *ShardKV) readKVState(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	DPrintf("[ShardKV-%d-%d] read size=%d", kv.gid, kv.me, len(data))
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	cks := make(map[int64]int)
	dataSource := make(map[string]string)
	shards := ShardContainer{
		TransferShards: make([]int, 0),
		RetainShards:   make([]int, 0),
		ConfigNum:      1,
	}
	//var commitIndex int
	if d.Decode(&cks) != nil ||
		d.Decode(&dataSource) != nil ||
		d.Decode(&shards) != nil {
		DPrintf("[readKVState] decode failed ...")
	} else {
		for ckId, seqId := range cks {
			kv.mu.Lock()
			ck := kv.GetCk(ckId)
			ck.seqId = seqId
			kv.mu.Unlock()
		}
		kv.mu.Lock()
		kv.dataSource = dataSource
		kv.shards = shards
		DPrintf("[ShardKV-%d-%d] readKVState kv.shards=%v messageMap=%v dataSource=%v", kv.gid, kv.me, shards, kv.shardkvClerks, kv.dataSource)
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) saveKVState(index int) {
	if kv.maxraftstate == -1 {
		return
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	cks := make(map[int64]int)
	for ckId, ck := range kv.shardkvClerks {
		cks[ckId] = ck.seqId
	}
	e.Encode(cks)
	e.Encode(kv.dataSource)
	e.Encode(kv.shards)
	kv.rf.Snapshot(index, w.Bytes())
	DPrintf("[ShardKV-%d-%d] Size=%d, Shards=%v", kv.gid, kv.me, kv.persister.RaftStateSize(), kv.shards)
}

func (kv *ShardKV) persist() {
	if kv.maxraftstate == -1 {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	cks := make(map[int64]int)
	for ckId, ck := range kv.shardkvClerks {
		cks[ckId] = ck.seqId
	}
	e.Encode(cks)
	e.Encode(kv.dataSource)
	e.Encode(kv.shards)
	kv.persister.SaveSnapshot(w.Bytes())
}

// read kv state and raft snapshot
func (kv *ShardKV) readState() {
	kv.readKVState(kv.persister.ReadSnapshot())
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[ShardKV-%d-%d] Received Kill Command, Shards=%v logsize=%d, kv data=%v", kv.gid, kv.me, kv.shards, kv.persister.RaftStateSize(), kv.dataSource)
}

func (kv *ShardKV) recvMsg() {
	for {
		applyMsg := <-kv.applyCh
		kv.processMsg(applyMsg)
	}
}

func (kv *ShardKV) processMsg(applyMsg raft.ApplyMsg) {
	if applyMsg.SnapshotValid {
		kv.readKVState(applyMsg.Snapshot)
		return
	}
	Msg := applyMsg.Command.(Op)
	DPrintf("[ShardKV-%d-%d] Received Msg from channel. Msg=%v", kv.gid, kv.me, applyMsg)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	if kv.needSnapshot() {
		DPrintf("[ShardKV-%d-%d] size=%d, maxsize=%d, DoSnapshot %v", kv.gid, kv.me, kv.persister.RaftStateSize(), kv.maxraftstate, applyMsg)
		kv.saveKVState(applyMsg.CommandIndex - 1)
	}

	ck := kv.GetCk(Msg.ClerkId)
	needNotify := ck.msgUniqueId == applyMsg.CommandIndex
	if isLeader && needNotify {
		ck.msgUniqueId = 0
		DPrintf("[ShardKV-%d-%d] Process Msg %v finish, ready send to ck.Ch, SeqId=%d isLeader=%v", kv.gid, kv.me, applyMsg, ck.seqId, isLeader)
		kv.NotifyApplyMsgByCh(ck.messageCh, Msg)
		DPrintf("[ShardKV-%d-%d] Process Msg %v Send to Rpc handler finish SeqId=%d isLeader=%v", kv.gid, kv.me, applyMsg, ck.seqId, isLeader)
	}

	if Msg.SeqId < ck.seqId {
		DPrintf("[ShardKV-%d-%d] Ignore Msg %v,  Msg.SeqId < ck.seqId=%d", kv.gid, kv.me, applyMsg, ck.seqId)
		return
	}

	DPrintf("[ShardKV-%d-%d] Excute CkId=%d %s Msg=%v, ck.SeqId=%d, kvdata=%v", kv.gid, kv.me, Msg.ClerkId, Msg.Command, applyMsg, ck.seqId, kv.dataSource)
	succ := true
	switch Msg.Command {
	case "Put":
		if kv.isRequestKeyCorrect(Msg.Key) {
			kv.dataSource[Msg.Key] = Msg.Value
		} else {
			succ = false
		}
	case "Append":
		if kv.isRequestKeyCorrect(Msg.Key) {
			kv.dataSource[Msg.Key] += Msg.Value
		} else {
			succ = false
		}
	case "ShardJoin":
		kv.shardJoin(Msg.JoinOp)
	case "ShardConf":
		kv.shards.ConfigNum = Msg.ConfOp.ConfigNum
		kv.shards.ShardReqSeqId = Msg.SeqId + 1
		kv.shards.QueryDone = false
	case "ShardChange":
		kv.shardChange(Msg.LeaveOp, Msg.SeqId)
		kv.shards.ShardReqSeqId = Msg.SeqId + 1
	case "ShardLeave":
		kv.shardLeave(Msg.LeaveOp, Msg.SeqId)
		kv.shards.ShardReqSeqId = Msg.SeqId + 1
	case "ShardInit":
		if kv.shards.ConfigNum > 1 {
			DPrintf("[ShardKV-%d-%d] already ShardInit, kv.shards=%v, Msg=%v", kv.gid, kv.me, kv.shards, Msg)
		} else {
			DPrintf("[ShardKV-%d-%d] ShardChange, ShardInit, kv.shards before=%v, after=%v", kv.gid, kv.me, kv.shards, Msg)
			kv.shards.ShardReqId = Msg.ClerkId
			kv.shards.ShardReqSeqId = Msg.SeqId + 1
			kv.shards.RetainShards = Msg.InitOp.Shards
			kv.shards.ConfigNum = Msg.InitOp.ConfigNum + 1
			kv.shards.QueryDone = false
		}
	}
	if succ {
		DPrintf("[ShardKV-%d-%d] update seqid ck.seqId=%d -> Msg.seqId=%d ", kv.gid, kv.me, ck.seqId, Msg.SeqId+1)
		ck.seqId = Msg.SeqId + 1
	}
	kv.persist()
}

func (kv *ShardKV) shardJoin(joinOp ShardJoinOp) {
	if joinOp.ConfigNum != kv.shards.ConfigNum {
		DPrintf("[ShardKV-%d-%d] ignore ShardJoinOp old config, kv.shards=%v, leaveOp=%v", kv.gid, kv.me, kv.shards, joinOp)
		return
	}
	// just put it in
	DPrintf("[ShardKV-%d-%d] Excute shardJoin, old shards=%v, ShardData=%v", kv.gid, kv.me, kv.shards, joinOp.ShardData)
	for key, value := range joinOp.ShardData {
		kv.dataSource[key] = value
	}
	// update request seq id
	for clerkId, seqId := range joinOp.RequestMap {
		ck := kv.GetCk(clerkId)
		if ck.seqId < seqId {
			DPrintf("[ShardKV-%d-%d] Update RequestSeqId, ck=%d update seqid=%d to %d", kv.gid, kv.me, clerkId, ck.seqId, seqId)
			ck.seqId = seqId
		}
	}
	// add shard config
	for _, shard := range joinOp.Shards {
		kv.addShard(shard, &kv.shards.RetainShards)
	}
	joinAfterShards := make([]int, 0)
	for _, shard := range kv.shards.WaitJoinShards {
		if !isShardInGroup(shard, kv.shards.RetainShards) {
			joinAfterShards = append(joinAfterShards, shard)
		}
	}
	kv.shards.WaitJoinShards = joinAfterShards
	if (len(kv.shards.TransferShards) == 0) && len(kv.shards.WaitJoinShards) == 0 {
		kv.shards.ConfigNum = joinOp.ConfigNum + 1
		kv.shards.QueryDone = false
	}
	DPrintf("[ShardKV-%d-%d] ShardChange ShardJoin addShards shards=%v, kv.shards=%v", kv.gid, kv.me, joinOp.Shards, kv.shards)
}

func (kv *ShardKV) transferShardsToGroup(shards []int, servers []string, configNum int) bool {
	kv.mu.Lock()
	data := make(map[string]string)
	for _, moveShard := range shards {
		for key, value := range kv.dataSource {
			if key2shard(key) != moveShard {
				continue
			}
			data[key] = value
		}
	}
	DPrintf("[ShardKV-%d-%d] invokeMoveShard shards=%v, data=%v", kv.gid, kv.me, shards, data)
	// notify shard new owner
	args := RequestMoveShard{
		Data:      data,
		SeqId:     kv.shards.ShardReqSeqId,
		ClerkId:   kv.shards.ShardReqId,
		Shards:    shards,
		ConfigNum: configNum,
	}
	args.RequestMap = make(map[int64]int)
	for clerkId, clerk := range kv.shardkvClerks {
		if clerkId == kv.shards.ShardReqId {
			continue
		}
		args.RequestMap[clerkId] = clerk.seqId
	}
	kv.mu.Unlock()
	reply := ReplyMoveShard{}
	// todo when all server access fail
	for {
		for _, servername := range servers {
			DPrintf("[ShardKV-%d-%d] start move shard args=%v", kv.gid, kv.me, args)
			ok := kv.sendRequestMoveShard(servername, &args, &reply)
			if ok && reply.Err == OK {
				DPrintf("[ShardKV-%d-%d] move shard finish ...", kv.gid, kv.me)
				return true
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (kv *ShardKV) shardChange(leaveOp ShardChangeOp, seqId int) {
	if leaveOp.ConfigNum != kv.shards.ConfigNum {
		DPrintf("[ShardKV-%d-%d] ignore beginShardLeave old config, kv.shards=%v, leaveOp=%v", kv.gid, kv.me, kv.shards, leaveOp)
		return
	}

	for _, shards := range leaveOp.LeaveShards {
		for _, shard := range shards {
			kv.addShard(shard, &kv.shards.TransferShards)
		}
	}
	kv.shards.WaitJoinShards = leaveOp.WaitJoinShards
	DPrintf("[ShardKV-%d-%d] ShardChange ShardLeave, transferShards=%v kv.shards=%v", kv.gid, kv.me, leaveOp.LeaveShards, kv.shards)
	kv.shards.QueryDone = true

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DPrintf("[ShardKV-%d-%d] not leader, ignore leaveOp %v", kv.gid, kv.me, leaveOp)
		return
	}
	if len(leaveOp.LeaveShards) != 0 {
		go func(shardReqId int64, op ShardChangeOp, reqSeqId int) {
			for gid, shards := range leaveOp.LeaveShards {
				servers := leaveOp.Servers[gid]
				ok := kv.transferShardsToGroup(shards, servers, leaveOp.ConfigNum)
				kv.mu.Lock()
				if ok {
					DPrintf("[ShardKV-%d-%d] beginShardLeave to %d succ, kv.shards=%v", kv.gid, kv.me, gid, kv.shards)
				} else {
					DPrintf("[ShardKV-%d-%d] beginShardLeave to %d failed", kv.gid, kv.me, gid)
				}
				kv.mu.Unlock()
			}
			kv.mu.Lock()
			kv.rf.Start(Op{
				Command: "ShardLeave",
				ClerkId: shardReqId,
				SeqId:   kv.shards.ShardReqSeqId,
				LeaveOp: op,
			})
			kv.mu.Unlock()
		}(kv.shards.ShardReqId, leaveOp, seqId)
	}
}

func (kv *ShardKV) shardLeave(leaveOp ShardChangeOp, seqId int) {
	if leaveOp.ConfigNum < kv.shards.ConfigNum {
		DPrintf("[ShardKV-%d-%d] ignore beginShardLeave old config, kv.shards=%v, leaveOp=%v", kv.gid, kv.me, kv.shards, leaveOp)
		return
	}
	// update shards, only update the leave shard , the join shard need to update by shardjoin msg from the channel
	if len(kv.shards.WaitJoinShards) == 0 {
		kv.shards.ConfigNum = leaveOp.ConfigNum + 1
		kv.shards.QueryDone = false
	}
	afterShards := make([]int, 0)
	for _, shard := range kv.shards.RetainShards {
		if isShardInGroup(shard, kv.shards.TransferShards) {
			continue
		}
		afterShards = append(afterShards, shard)
	}
	// delete data
	deleteKeys := make([]string, 0)
	for key := range kv.dataSource {
		if isShardInGroup(key2shard(key), kv.shards.TransferShards) {
			deleteKeys = append(deleteKeys, key)
		}
	}
	for _, deleteKey := range deleteKeys {
		delete(kv.dataSource, deleteKey)
	}
	kv.shards.RetainShards = afterShards
	DPrintf("[ShardKV-%d-%d] ShardChange shardLeave finish, transferShards=%v, RetainShards=%v, kv.shards=%v", kv.gid, kv.me, leaveOp.LeaveShards, afterShards, kv.shards)
	kv.shards.TransferShards = make([]int, 0)
	DPrintf("[ShardKV-%d-%d] Excute ShardLeave finish, leave shards=%v, kv.Shards=%v", kv.gid, kv.me, leaveOp, kv.shards)
}

/*
	type Config struct {
		Num    int              // config number
		Shards [NShards]int     // shard -> gid
		Groups map[int][]string // gid -> servers[]
	}
*/
func isShardInGroup(shard int, dstShardGroup []int) bool {
	for _, dstShard := range dstShardGroup {
		if dstShard == shard {
			return true
		}
	}
	return false
}

func (kv *ShardKV) needSnapshot() bool {
	return kv.persister.RaftStateSize()/4 >= kv.maxraftstate && kv.maxraftstate != -1
}

/*
rpc handler
*/
func (kv *ShardKV) RequestMoveShard(args *RequestMoveShard, reply *ReplyMoveShard) {
	kv.mu.Lock()
	DPrintf("[ShardKV-%d-%d] Received Req MoveShard %v, SeqId=%d kv.shards=%v", kv.gid, kv.me, args, args.SeqId, kv.shards)
	reply.Err = OK
	// check request repeat ?
	ck := kv.GetCk(args.ClerkId)
	if ck.seqId > args.SeqId || kv.shards.ConfigNum != args.ConfigNum {
		DPrintf("[ShardKV-%d-%d] Received Req MoveShard %v, SeqId=%d, ck.seqId=%d, already process request", kv.gid, kv.me, args, args.SeqId, ck.seqId)
		// not update to this config, wait, set errwrongleader to let them retry
		if kv.shards.ConfigNum < args.ConfigNum {
			reply.Err = ErrWrongLeader
		}
		kv.mu.Unlock()
		return
	}

	// already add shard, but config num not change (sometime a config can trigger multi shard add from two group)
	alreadyAdd := true
	for _, shard := range args.Shards {
		if !isShardInGroup(shard, kv.shards.RetainShards) {
			alreadyAdd = false
			break
		}
	}
	if alreadyAdd {
		kv.mu.Unlock()
		return
	}

	// config not query finish, waiting, let them retry
	if !kv.shards.QueryDone {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	// start a command
	shardJoinOp := ShardJoinOp{
		Shards:     args.Shards,
		ShardData:  args.Data,
		ConfigNum:  args.ConfigNum,
		RequestMap: args.RequestMap,
	}
	logIndex, _, isLeader := kv.rf.Start(Op{
		Command: "ShardJoin",
		ClerkId: args.ClerkId, // special clerk id for indicate move shard
		SeqId:   args.SeqId,
		JoinOp:  shardJoinOp,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	ck.msgUniqueId = logIndex
	DPrintf("[ShardKV-%d-%d] Received Req MoveShard %v, waiting logIndex=%d", kv.gid, kv.me, args, logIndex)
	kv.mu.Unlock()
	// step 2 : wait the channel
	reply.Err = OK
	notify := kv.WaitApplyMsgByCh(ck.messageCh, ck)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[ShardKV-%d-%d] Recived Msg [MoveShard] from ck.channel args=%v, SeqId=%d, Msg=%v", kv.gid, kv.me, args, args.SeqId, notify.Msg)
	reply.Err = notify.Result
	if reply.Err != OK {
		DPrintf("[ShardKV-%d-%d] leader change args=%v, SeqId=%d", kv.gid, kv.me, args, args.SeqId)
		return
	}
}

func (kv *ShardKV) addShard(shard int, shards *[]int) {
	alreadExist := false
	for _, curShard := range *shards {
		if curShard == shard {
			alreadExist = true
		}
	}
	if !alreadExist {
		*shards = append(*shards, shard)
	} else {
		DPrintf("[ShardKV-%d-%d] add shard failed, shard=%d alreadyExsit in %v", kv.gid, kv.me, shard, kv.shards)
	}
}

func (kv *ShardKV) checkConfig(config shardctrler.Config) {
	if config.Num < kv.shards.ConfigNum || config.Num == 0 {
		DPrintf("[ShardKV-%d-%d] checkConfig not change, config=%v, kv.shards=%v", kv.gid, kv.me, config, kv.shards)
		return
	}

	kvShards := make([]int, 0)
	waitJoinShards := make([]int, 0)
	// collect group shards
	for index, gid := range config.Shards {
		if gid == kv.gid {
			kvShards = append(kvShards, index)
			if !isShardInGroup(index, kv.shards.RetainShards) {
				waitJoinShards = append(waitJoinShards, index)
			}
		}
	}
	// find the shard that leave this group
	leaveShards := make(map[int][]int) // shard idx to new gid
	for _, shard := range kv.shards.RetainShards {
		if !isShardInGroup(shard, kvShards) {
			shardNewGroup := config.Shards[shard]
			if _, found := leaveShards[shardNewGroup]; !found {
				leaveShards[shardNewGroup] = make([]int, 0)
			}
			leaveShards[shardNewGroup] = append(leaveShards[shardNewGroup], shard)
		}
	}

	DPrintf("[ShardKV-%d-%d] groupNewShards=%v, OldShards=%v, leaveShards=%v query config=%v", kv.gid, kv.me, kvShards, kv.shards, leaveShards, config)

	// init config
	if config.Num == 1 {
		kv.rf.Start(Op{
			Command: "ShardInit",
			ClerkId: nrand(),
			SeqId:   kv.shards.ShardReqSeqId,
			InitOp: ShardInitOp{
				Shards:    kvShards,
				ConfigNum: config.Num,
			},
		})
		return
	}

	// only incr config num
	if len(waitJoinShards) == 0 && len(leaveShards) == 0 {
		kv.rf.Start(Op{
			Command: "ShardConf",
			ClerkId: kv.shards.ShardReqId,
			SeqId:   kv.shards.ShardReqSeqId,
			ConfOp: ShardConfOp{
				ConfigNum: config.Num + 1,
			},
		})
		return
	}

	if len(leaveShards) != 0 || len(waitJoinShards) != 0 {
		op := Op{
			Command: "ShardChange",
			ClerkId: kv.shards.ShardReqId,
			SeqId:   kv.shards.ShardReqSeqId,
			LeaveOp: ShardChangeOp{
				LeaveShards:    leaveShards,
				Servers:        config.Groups,
				ConfigNum:      config.Num,
				WaitJoinShards: waitJoinShards,
			},
		}
		DPrintf("[ShardKV-%d-%d] ConfigChange, ShardChange=%v, kv.shards=%v, config=%v", kv.gid, kv.me, op, kv.shards, config)
		kv.rf.Start(op)
	} else {
		kv.shards.QueryDone = true
	}
}

func (kv *ShardKV) getNextQueryConfigNum() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.shards.ConfigNum
}

func (kv *ShardKV) intervalQueryConfig() {
	queryInterval := 100 * time.Millisecond
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
			config := kv.mck.Query(kv.getNextQueryConfigNum())
			kv.mu.Lock()
			kv.checkConfig(config)
			kv.mu.Unlock()
		}
		time.Sleep(queryInterval)
	}
}

func (kv *ShardKV) sendRequestMoveShard(servername string, args *RequestMoveShard, reply *ReplyMoveShard) bool {
	srv := kv.make_end(servername)
	ok := srv.Call("ShardKV.RequestMoveShard", args, reply)
	return ok
}

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
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.dataSource = make(map[string]string)
	kv.shardkvClerks = make(map[int64]*ShardKVClerk)
	kv.shards = ShardContainer{
		RetainShards:   make([]int, 0),
		TransferShards: make([]int, 0),
		ShardReqSeqId:  1,
		ConfigNum:      1,
	}
	kv.persister = persister

	DPrintf("[ShardKV-%d-%d] Starting ... Shards=%v", kv.gid, kv.me, kv.shards)
	kv.readState()
	go kv.recvMsg()
	go kv.intervalQueryConfig()
	return kv
}
