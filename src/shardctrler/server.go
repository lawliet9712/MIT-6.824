package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	requestMap map[int64]*ShardClerk // ckid to seqid for filter repeat request
	configs []Config // indexed by config num
}

type ShardClerk struct {
	seqId       int
	ckId  	    int64
	msgUniqueId int
	messageCh   chan Op
}

const (
	T_Join = "Join"
	T_Leave = "Leave"
	T_Move = "Move"
	T_Query = "Query"
)

type T_Op string

// we gather all request data to here
type Op struct {
	// Your data here.
	Command T_Op
	SeqId int
	Servers map[int][]string // T_Join
	GIDs []int // T_Remove
	Shard int // T_Move
	GID int // T_Move
}

func (sc *ShardCtrler) GetCk(ckId int64) *ShardClerk {
	ck, found := sc.requestMap[ckId]
	if !found {
		ck = new(ClerkOps)
		ck.messageCh = make(chan Op)
		ck.mu = &sc.mu
		sc.requestMap[ckId] = ck
		DPrintf("[ShardCtrler-%d] Init ck %d", sc.me, ckId)
	}
	return sc.
}

func (sc *ShardCtrler) WaitApplyMsgByCh(ck *ShardClerk) (Op, Err) {
	startTerm, _ := sc.rf.GetState()
	timer := time.NewTimer(1000 * time.Millisecond)
	for {
		select {
		case Msg := <-ch:
			return Msg, true
		case <-timer.C:
			curTerm, isLeader := sc.rf.GetState()
			if curTerm != startTerm || !isLeader {
				sc.mu.Lock()
				ck.msgUniqueId = 0
				sc.mu.Unlock()
				return Op{}, false
			}
			timer.Reset(1000 * time.Millisecond)
		}
	}
}

func (sc *ShardCtrler) NotifyApplyMsgByCh(ch chan Op, Msg Op) {
	// we wait 200ms 
	// if notify timeout, then we ignore, because client probably send request to anthor server
	timer := time.NewTimer(200 * time.Millisecond)
	select {
	case ch <- Msg:
		DPrintf("[ShardCtrler-%d] NotifyApplyMsgByCh finish , Msg=%v", sc.me, Msg)
		return
	case <-timer.C:
		DPrintf("[ShardCtrler-%d] NotifyApplyMsgByCh Msg=%v, timeout", sc.me, Msg)
		return
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	kv.mu.Lock()
	logIndex, _, isLeader := sc.rf.Start(Op{
		Servers : args.Servers,
		SeqId : args.seqId,
		Command : T_Join,
	})
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}

	ck := sc.GetCk(args.CkId)
	ck.msgUniqueId = logIndex
	kv.mu.Unlock()

	Msg, WrongLeader := sc.WaitApplyMsgByCh(ck)
	reply.WrongLeader = WrongLeader
	return
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	kv.mu.Lock()
	logIndex, _, isLeader := sc.rf.Start(Op{
		GIDs : args.GIDs,
		SeqId : args.seqId,
		Command : T_Leave,
	})
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}

	ck := sc.GetCk(args.CkId)
	ck.msgUniqueId = logIndex
	kv.mu.Unlock()

	Msg, WrongLeader := sc.WaitApplyMsgByCh(ck)
	reply.WrongLeader = WrongLeader
	return
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	kv.mu.Lock()
	logIndex, _, isLeader := sc.rf.Start(Op{
		Shard : args.Shard,
		GID : args.GID,
		SeqId : args.seqId,
		Command : T_Move,
	})
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}

	ck := sc.GetCk(args.CkId)
	ck.msgUniqueId = logIndex
	kv.mu.Unlock()

	Msg, WrongLeader := sc.WaitApplyMsgByCh(ck)
	reply.WrongLeader = WrongLeader
	return
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	logIndex, _, isLeader := sc.rf.Start(Op{
		SeqId : args.seqId,
		Command : T_Query,
	})
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	ck := sc.GetCk(args.CkId)
	ck.msgUniqueId = logIndex
	sc.mu.Unlock()

	Msg, WrongLeader := sc.WaitApplyMsgByCh(ck)
	sc.mu.Lock()
	defer sc.mu.Unlock()
	reply.WrongLeader = WrongLeader
	reply.Config = sc.getConfig(args.Num)
	DPrintf("[ShardCtrler-%d] %d Clerk-%d Do [Query] Reply Config=%v", sc.me, args.CkId, reply.Config)
	return
}

func (sc *ShardCtrler) getGIDs() []int {
	conf := sc.getConfig(-1)
	gids := make([]int)
	for gid, _ := range(conf.Groups) {
		gids = append(gids, gid)
	}
	return gids
}

func (sc *ShardCtrler) getConfig(confNumber int) Config {
	if confNumber == -1 || confNumber >= len(sc.configs) {
		return sc.configs[len(sc.configs) - 1]
	}
	return sc.configs[confNumber]
}

func (sc *ShardCtrler) rebalance() {
	// rebalance shard to groups
}

func (sc *ShardCtrler) invokeMsg(Msg Op) {
	switch Msg.Command {
	case T_Join: // add a set of groups
		DPrintf("[ShardCtrler-%d] Do T_Join, Msg=%v", sc.me, Msg)
		latestConf = sc.getConfig(-1))
		newGroups := make(map[int][]string)
		// merge new group
		for gid, server := range(Msg.Servers) {
			newGroups[gid] = servers
		}
		// merge old group
		for gid, server := range(latestConf.Groups) {
			newGroups[gid] := servers
		}
		// append new config
		config := Config{
			Num : len(sc.configs),
			Groups : Msg.Servers,
			Shards : latestConf.Shards,
		}
		sc.configs = append(sc.configs, config)
		// maybe need rebalance now
		sc.rebalance()
	case T_Leave:  // delete a set of groups
		DPrintf("[ShardCtrler-%d] Do T_Leave, Msg=%v", sc.me, Msg)
		latestConf := sc.getConfig(-1)
		newGroups := make(map[int][]string)
		for gid, servers range (latestConf.Groups) {
			// not in the remove gids, then append to new config
			if !xIsInGroup(gid, Msg.GIDs) {
				newGroups[gid] = servers
			}
		}
		// append new config
		config := Config{
			Num : len(sc.configs),
			Groups : Msg.Servers,
			Shards : latestConf.Shards,
		}
		sc.configs = append(sc.configs, config)
		sc.rebalance()
	case T_Move:
		DPrintf("[ShardCtrler-%d] Do T_Move, Msg=%v", sc.me, Msg)
		latestConf := sc.getConfig(-1)
		config := Config{
			Num : len(sc.configs),
			Groups : latestConf.Groups,
			Shards : latestConf.Shards,
		}
		config.Shards[Msg.Shard] = Msg.GID // no need rebalance
	case T_Query:
		DPrintf("[ShardCtrler-%d] Do T_Query, Msg=%v", sc.me, Msg)
		// nothing to do
	default:
		DPrintf("[ShardCtrler-%d] Do Op Error, not found type, Msg=%v", sc.me, Msg)
	}
}

func (sc *ShardCtrler) processMsg() {
	for {
		applyMsg := <-sc.applyCh
		opMsg := applyMsg.Command.(Op)
		sc.mu.Lock()
		ck := sc.GetCk(opMsg.CkId)
		// already process
		DPrintf("[ShardCtrler-%d] Received Msg %v", sc.me, applyMsg)
		if opMsg.seqId < ck.seqId {
			sc.mu.Unlock()
			continue
		}
		
		_, isLeader = sc.rf.GetState()
		if applyMsg.CommandIndex == ck.msgUniqueId && isLeader {
			sc.NotifyApplyMsgByCh(ck.messageCh, opMsg)
			ck.msgUniqueId = 0
		}

		sc.invokeMsg(opMsg)
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.requestMap = make(map[int64]int)
	go sc.processMsg()
	return sc
}
