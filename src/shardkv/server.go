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
	Key       string
	Value     string
	Command   string
	ClerkId   int64
	SeqId     int
	Shards    []int // move shard rpc
	ShardData map[string]string
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
	retainShards   []int
	transferShards []int
	configNum 	   int
}

type BeginMoveShard struct {
	Shards []int // group to shards
	Gid int
	ConfigNum int
}

type EndMoveShard struct {
	Shards []int // group to shards
	Gid int
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
	shards        ShardContainer                   // this group hold shards
	clerkId       int64                   // uid for move shard
	seqId         int                     // seq id for prevent repeat move shard
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
		if !isShardInGroup(key2shard(Msg.Key), kv.shards.retainShards) {
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
	/*

		config := kv.mck.Query(-1)
		for shard, gid := range config.Shards {
			if shard == keyShard && gid != kv.gid {
				DPrintf("[ShardKV-%d-%d] keyShard=%d not in this group, config=%v", kv.gid, kv.me, keyShard, config)
				return false
			}
		}

		// check shard in current config ?
		kv.shardCond.L.Lock()
		for {
			kv.mu.Lock()
			if isShardInGroup(keyShard, kv.shards) {
				kv.mu.Unlock()
				break
			}
			DPrintf("[ShardKV-%d-%d] not found keyshard=%d in kv.shards=%v", kv.gid, kv.me, keyShard, kv.shards)
			kv.mu.Unlock()
			kv.shardCond.Wait()
		}
		kv.shardCond.L.Unlock()
	*/
	kv.mu.Lock()
	defer kv.mu.Unlock()
	keyShard := key2shard(key)
	// check shard is transfering 
	isShardTransfering := isShardInGroup(keyShard, kv.shards.transferShards)
	return isShardInGroup(keyShard, kv.shards.retainShards) && !isShardTransfering
	//return true
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("[ShardKV-%d-%d] Received Req Get %v", kv.gid, kv.me, args)
	if !kv.isRequestKeyCorrect(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}

	// start a command
	kv.mu.Lock()
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
	if !kv.isRequestKeyCorrect(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}

	// start a command
	kv.mu.Lock()
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
	shards := ShardContainer {
		transferShards: make([]int, 0),
		retainShards: make([]int, 0),
		configNum: -1,
	}
	var clerkId int64
	var seqId int
	//var commitIndex int
	if d.Decode(&cks) != nil ||
		d.Decode(&dataSource) != nil ||
		d.Decode(&clerkId) != nil ||
		d.Decode(&seqId) != nil ||
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
		kv.clerkId = clerkId
		kv.seqId = seqId
		kv.shards = shards
		DPrintf("[ShardKV-%d-%d] readKVState seqId=%d kv.shards=%v messageMap=%v dataSource=%v", kv.gid, kv.me, kv.seqId, shards, kv.shardkvClerks, kv.dataSource)
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
	e.Encode(kv.clerkId)
	e.Encode(kv.seqId)
	e.Encode(kv.shards)
	kv.rf.Snapshot(index, w.Bytes())
	DPrintf("[ShardKV-%d-%d] seqId=%d, Size=%d, Shards=%v", kv.gid, kv.me, kv.seqId, kv.persister.RaftStateSize(), kv.shards)
}

func (kv *ShardKV) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	cks := make(map[int64]int)
	for ckId, ck := range kv.shardkvClerks {
		cks[ckId] = ck.seqId
	}
	e.Encode(cks)
	e.Encode(kv.dataSource)
	e.Encode(kv.clerkId)
	e.Encode(kv.seqId)
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
	if kv.needSnapshot() {
		DPrintf("[ShardKV-%d-%d] size=%d, maxsize=%d, DoSnapshot %v", kv.gid, kv.me, kv.persister.RaftStateSize(), kv.maxraftstate, applyMsg)
		kv.saveKVState(applyMsg.CommandIndex - 1)
	}

	ck := kv.GetCk(Msg.ClerkId)
	_, isLeader := kv.rf.GetState()
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

	switch Msg.Command {
	case "Put":
		kv.dataSource[Msg.Key] = Msg.Value
		DPrintf("[ShardKV-%d-%d] Excute CkId=%d Put Msg=%v, kvdata=%v", kv.gid, kv.me, Msg.ClerkId, applyMsg, kv.dataSource)
	case "Append":
		DPrintf("[ShardKV-%d-%d] Excute CkId=%d Append Msg=%v kvdata=%v", kv.gid, kv.me, Msg.ClerkId, applyMsg, kv.dataSource)
		kv.dataSource[Msg.Key] += Msg.Value
	case "Get":
		DPrintf("[ShardKV-%d-%d] Excute CkId=%d Get Msg=%v kvdata=%v", kv.gid, kv.me, Msg.ClerkId, applyMsg, kv.dataSource)
	case "ShardMove":
		DPrintf("[ShardKV-%d-%d] Excute CkId=%d ShardJoin Msg=%v kvdata=%v", kv.gid, kv.me, Msg.ClerkId, applyMsg, kv.dataSource)
		kv.shardJoin(Msg.Shards, Msg.ShardData)
	case "ShardLeave":
		DPrintf("[ShardKV-%d-%d] Excute CkId=%d ShardLeave Msg=%v kvdata=%v", kv.gid, kv.me, Msg.ClerkId, applyMsg, kv.dataSource)
		kv.shardLeave(Msg.Shards, Msg.SeqId)
	}
	ck.seqId = Msg.SeqId + 1
	kv.persist()
}

func (kv *ShardKV) shardJoin(shards []int, shardData map[string]string) {
	// just put it in
	DPrintf("[ShardKV-%d-%d] Excute shardJoin, new shards=%v, ShardData=%v", kv.gid, kv.me, shards, shardData)
	for key, value := range shardData {
		kv.dataSource[key] = value
	}
	// add shard config'
	for _, shard := range shards {
		kv.addShard(shard)
	}
}

func (kv *ShardKV) shardLeave(shards []int, seqId int) {
	if kv.seqId == seqId+1 {
		return
	}
	// update seqid meaning this shard leave finish
	kv.seqId = seqId + 1
	// update shards, only update the leave shard , the join shard need to update by msg from the channel
	for _, shard := range shards {
		kv.removeShard(shard)
	}
	DPrintf("[ShardKV-%d-%d] Excute ShardLeave, leave shards=%v, kv.Shards=%v", kv.gid, kv.me, shards, kv.shards)
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
	DPrintf("[ShardKV-%d-%d] Received Req MoveShard %v, SeqId=%d ", kv.gid, kv.me, args, args.SeqId)
	// start a command
	logIndex, _, isLeader := kv.rf.Start(Op{
		ShardData: args.Data,
		Command:   "ShardMove",
		ClerkId:   args.ClerkId, // special clerk id for indicate move shard
		SeqId:     args.SeqId,
		Shards:    args.Shards,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	ck := kv.GetCk(args.ClerkId)
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

func (kv *ShardKV) invokeMoveShard(shards []int, servers []string) {
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
		Data:    data,
		SeqId:   kv.seqId,
		ClerkId: kv.clerkId,
		Shards:  shards,
	}
	reply := ReplyMoveShard{}
	// todo when all server access fail
	for {
		for _, servername := range servers {
			DPrintf("[ShardKV-%d-%d] start move shard args=%v", kv.gid, kv.me, args)
			ok := kv.sendRequestMoveShard(servername, &args, &reply)
			if ok && reply.Err == OK {
				DPrintf("[ShardKV-%d-%d] move shard finish ...", kv.gid, kv.me)
				kv.rf.Start(Op{
					Command: "ShardLeave",
					SeqId:   args.SeqId,
					Shards:  args.Shards,
					ClerkId: args.ClerkId,
				})
				kv.shardLeave(shards, kv.seqId)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) addShard(shard int) {
	alreadExist := false
	for _, curShard := range kv.shards.retainShards {
		if curShard == shard {
			alreadExist = true
		}
	}
	if !alreadExist {
		kv.shards.retainShards = append(kv.shards.retainShards, shard)
	} else {
		DPrintf("[ShardKV-%d-%d] add shard failed, shard=%d alreadyExsit in %v", kv.gid, kv.me, shard, kv.shards)
	}
}

func (kv *ShardKV) removeShard(shard int) {
	for i := 0; i < len(kv.shards.retainShards); i++ {
		if kv.shards.retainShards[i] == shard {
			kv.shards.retainShards = append(kv.shards.retainShards[:i], kv.shards.retainShards[i+1:]...)
			i-- // maintain the correct index
		}
	}
}

func (kv *ShardKV) intervalQueryConfig() {
	queryInterval := 100 * time.Millisecond
	for {
		config := kv.mck.Query(-1)
		kv.mu.Lock()
		if config.Num == kv.shards.configNum {
			kv.mu.Unlock()
			continue
		}

		kv.shards.configNum = config.Num
		kvShards := make([]int, 0)
		// collect group shards
		for index, gid := range config.Shards {
			if gid == kv.gid {
				kvShards = append(kvShards, index)
			}
		}
		// find the shard that leave this group
		leaveShards := make(map[int][]int) // shard idx to new gid
		for _, shard := range kv.shards.retainShards {
			if !isShardInGroup(shard, kvShards) {
				shardNewGroup := config.Shards[shard]
				if _, found := leaveShards[shardNewGroup]; !found {
					leaveShards[shardNewGroup] = make([]int, 0)
				}
				leaveShards[shardNewGroup] = append(leaveShards[shardNewGroup], shard)
			}
		}
		DPrintf("[ShardKV-%d-%d] groupNewShards=%v, OldShards=%v, leaveShards=%v query config=%v", kv.gid, kv.me, kvShards, kv.shards, leaveShards, config)

		_, isLeader := kv.rf.GetState()
		if isLeader {
			// move the shard, just notify, current not need to change self anything
			for gid, shards := range leaveShards {
				kv.invokeMoveShard(shards, config.Groups[gid])
			}
		}

		kv.mu.Unlock()
		time.Sleep(queryInterval)
	}
}

func (kv *ShardKV) sendRequestMoveShard(servername string, args *RequestMoveShard, reply *ReplyMoveShard) bool {
	srv := kv.make_end(servername)
	ok := srv.Call("ShardKV.RequestMoveShard", args, reply)
	return ok
}

// when shard kv restart , clean channel remain msg then start
func (kv *ShardKV) cleanChannelRemainMsg() {
	timer := time.NewTimer(1000 * time.Millisecond)
	for {
		select {
		case applyMsg := <-kv.applyCh:
			timer.Reset(1000 * time.Millisecond)
			kv.processMsg(applyMsg)
		case <-timer.C:
			return
		}

	}
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
		retainShards: make([]int, 0),
		transferShards: make([]int, 0),
		configNum: -1,
	}
	kv.clerkId = nrand()
	kv.persister = persister

	DPrintf("[ShardKV-%d-%d] Starting ... ClerkId=%d", kv.gid, kv.me, kv.clerkId)
	kv.readState()
	kv.cleanChannelRemainMsg()
	DPrintf("[ShardKV-%d-%d] clean channel msg finish ...", kv.gid, kv.me)
	go kv.recvMsg()
	go kv.intervalQueryConfig()
	return kv
}
