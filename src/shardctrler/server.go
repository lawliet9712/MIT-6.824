package shardctrler

import (
	"log"
	"sync"
	"time"

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

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	requestMap map[int64]*ShardClerk // ckid to seqid for filter repeat request
	configs    []Config              // indexed by config num
}

type ShardClerk struct {
	seqId       int
	msgUniqueId int
	messageCh   chan Op
}

const (
	T_Join  = "Join"
	T_Leave = "Leave"
	T_Move  = "Move"
	T_Query = "Query"
)

type T_Op string

// we gather all request data to here
type Op struct {
	// Your data here.
	Command T_Op
	SeqId   int
	Servers map[int][]string // T_Join
	GIDs    []int            // T_Remove
	Shard   int              // T_Move
	GID     int              // T_Move
	CkId    int64
}

func (sc *ShardCtrler) GetCk(ckId int64) *ShardClerk {
	ck, found := sc.requestMap[ckId]
	if !found {
		ck = new(ShardClerk)
		ck.messageCh = make(chan Op)
		sc.requestMap[ckId] = ck
		DPrintf("[ShardCtrler-%d] Init ck %d", sc.me, ckId)
	}
	return sc.requestMap[ckId]
}

func (sc *ShardCtrler) WaitApplyMsgByCh(ck *ShardClerk) (Op, bool) {
	startTerm, _ := sc.rf.GetState()
	timer := time.NewTimer(1000 * time.Millisecond)
	for {
		select {
		case Msg := <-ck.messageCh:
			return Msg, false
		case <-timer.C:
			curTerm, isLeader := sc.rf.GetState()
			if curTerm != startTerm || !isLeader {
				sc.mu.Lock()
				ck.msgUniqueId = 0
				sc.mu.Unlock()
				return Op{}, true
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
	sc.mu.Lock()
	DPrintf("[ShardCtrler-%d] Received Req [Join] args=%v", sc.me, args)
	logIndex, _, isLeader := sc.rf.Start(Op{
		Servers: args.Servers,
		SeqId:   args.SeqId,
		Command: T_Join,
		CkId:    args.CkId,
	})
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	ck := sc.GetCk(args.CkId)
	ck.msgUniqueId = logIndex
	sc.mu.Unlock()

	_, WrongLeader := sc.WaitApplyMsgByCh(ck)
	reply.WrongLeader = WrongLeader
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	DPrintf("[ShardCtrler-%d] Received Req [Leave] args=%v", sc.me, args)
	logIndex, _, isLeader := sc.rf.Start(Op{
		GIDs:    args.GIDs,
		SeqId:   args.SeqId,
		Command: T_Leave,
		CkId:    args.CkId,
	})
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	ck := sc.GetCk(args.CkId)
	ck.msgUniqueId = logIndex
	sc.mu.Unlock()

	_, WrongLeader := sc.WaitApplyMsgByCh(ck)
	reply.WrongLeader = WrongLeader
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	DPrintf("[ShardCtrler-%d] Received Req [Move] args=%v", sc.me, args)
	logIndex, _, isLeader := sc.rf.Start(Op{
		Shard:   args.Shard,
		GID:     args.GID,
		SeqId:   args.SeqId,
		Command: T_Move,
		CkId:    args.CkId,
	})
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	ck := sc.GetCk(args.CkId)
	ck.msgUniqueId = logIndex
	sc.mu.Unlock()

	_, WrongLeader := sc.WaitApplyMsgByCh(ck)
	reply.WrongLeader = WrongLeader
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	DPrintf("[ShardCtrler-%d] Received Req [Query] args=%v", sc.me, args)
	logIndex, _, isLeader := sc.rf.Start(Op{
		SeqId:   args.SeqId,
		Command: T_Query,
		CkId:    args.CkId,
	})
	if !isLeader {
		reply.WrongLeader = true
		DPrintf("[ShardCtrler-%d] not leader,  Req [Move] args=%v", sc.me, args)
		sc.mu.Unlock()
		return
	}

	ck := sc.GetCk(args.CkId)
	ck.msgUniqueId = logIndex
	sc.mu.Unlock()

	_, WrongLeader := sc.WaitApplyMsgByCh(ck)
	sc.mu.Lock()
	defer sc.mu.Unlock()
	reply.WrongLeader = WrongLeader
	reply.Config = sc.getConfig(args.Num)
	DPrintf("[ShardCtrler-%d] Clerk-%d Do [Query] Reply Config=%v", sc.me, args.CkId, reply)
}

func (sc *ShardCtrler) getGIDs() []int {
	conf := sc.getConfig(-1)
	gids := make([]int, 0)
	for gid, _ := range conf.Groups {
		gids = append(gids, gid)
	}
	return gids
}

func (sc *ShardCtrler) getConfig(confNumber int) Config {
	if confNumber == -1 || confNumber >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
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
		latestConf := sc.getConfig(-1)
		newGroups := make(map[int][]string)
		// merge new group
		for gid, servers := range Msg.Servers {
			newGroups[gid] = servers
		}
		// merge old group
		for gid, servers := range latestConf.Groups {
			newGroups[gid] = servers
		}
		// append new config
		config := Config{
			Num:    len(sc.configs),
			Groups: Msg.Servers,
			Shards: latestConf.Shards,
		}
		sc.configs = append(sc.configs, config)
		// maybe need rebalance now
		sc.rebalance()
	case T_Leave: // delete a set of groups
		DPrintf("[ShardCtrler-%d] Do T_Leave, Msg=%v", sc.me, Msg)
		latestConf := sc.getConfig(-1)
		newGroups := make(map[int][]string)
		for gid, servers := range latestConf.Groups {
			// not in the remove gids, then append to new config
			if !xIsInGroup(gid, Msg.GIDs) {
				newGroups[gid] = servers
			}
		}
		// append new config
		config := Config{
			Num:    len(sc.configs),
			Groups: Msg.Servers,
			Shards: latestConf.Shards,
		}
		sc.configs = append(sc.configs, config)
		sc.rebalance()
	case T_Move:
		DPrintf("[ShardCtrler-%d] Do T_Move, Msg=%v", sc.me, Msg)
		latestConf := sc.getConfig(-1)
		config := Config{
			Num:    len(sc.configs),
			Groups: latestConf.Groups,
			Shards: latestConf.Shards,
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
		if opMsg.SeqId < ck.seqId {
			DPrintf("[ShardCtrler-%d] already process Msg %v finish ... ", sc.me, applyMsg)
			sc.mu.Unlock()
			continue
		}

		_, isLeader := sc.rf.GetState()
		if applyMsg.CommandIndex == ck.msgUniqueId && isLeader {
			DPrintf("[ShardCtrler-%d] Ready Notify To %d Msg %v, msgUniqueId=%d", sc.me, opMsg.CkId, applyMsg, ck.msgUniqueId)
			sc.NotifyApplyMsgByCh(ck.messageCh, opMsg)
			DPrintf("[ShardCtrler-%d] Notify To %d Msg %v finish ... ", sc.me, opMsg.CkId, applyMsg)
			ck.msgUniqueId = 0
		}

		ck.seqId = opMsg.SeqId + 1
		sc.invokeMsg(opMsg)
		sc.mu.Unlock()
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.requestMap = make(map[int64]*ShardClerk)
	go sc.processMsg()
	return sc
}
