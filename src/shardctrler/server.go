package shardctrler

import (
	"log"
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false
const InvalidGroup = 0

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
	timer := time.NewTimer(120 * time.Millisecond)
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
			timer.Reset(120 * time.Millisecond)
		}
	}
}

func (sc *ShardCtrler) NotifyApplyMsgByCh(ch chan Op, Msg Op) {
	// we wait 200ms
	// if notify timeout, then we ignore, because client probably send request to anthor server
	timer := time.NewTimer(120 * time.Millisecond)
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
		DPrintf("[ShardCtrler-%d] Received Req [Join] args=%v, not leader, return", sc.me, args)
		sc.mu.Unlock()
		return
	}

	ck := sc.GetCk(args.CkId)
	ck.msgUniqueId = logIndex

	DPrintf("[ShardCtrler-%d] Wait Req [Join] args=%v, ck.msgUniqueId = %d", sc.me, args, ck.msgUniqueId)
	sc.mu.Unlock()
	_, WrongLeader := sc.WaitApplyMsgByCh(ck)
	DPrintf("[ShardCtrler-%d] Wait Req [Join] Result=%v", sc.me, WrongLeader)
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
		DPrintf("[ShardCtrler-%d] Received Req [Leave] args=%v, not leader, return", sc.me, args)
		sc.mu.Unlock()
		return
	}

	ck := sc.GetCk(args.CkId)
	ck.msgUniqueId = logIndex
	DPrintf("[ShardCtrler-%d] Wait Req [Leave] args=%v ck.msgUniqueId=%d", sc.me, args, ck.msgUniqueId)
	sc.mu.Unlock()

	_, WrongLeader := sc.WaitApplyMsgByCh(ck)
	reply.WrongLeader = WrongLeader
	DPrintf("[ShardCtrler-%d] Wait Req [Leave] Result=%v", sc.me, WrongLeader)
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
	sort.Ints(gids)
	return gids
}

func (sc *ShardCtrler) getGroupShards(gid int) []int {
	conf := sc.getConfig(-1)
	shards := make([]int, 0)
	for shard, shardGid := range conf.Shards {
		if gid == shardGid {
			shards = append(shards, shard)
		}
	}
	sort.Ints(shards)
	return shards
}

func (sc *ShardCtrler) getConfig(confNumber int) Config {
	if confNumber == -1 || confNumber >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	}
	return sc.configs[confNumber]
}

/*
@note "rich shard" meaning the group have shard count more then avgShard

	"poor groups" meaing the group have shard count small than avgshard

@retval int[] rich shard, array element is shard index in the config

	map[int]int[] poor groups, key is gid, value is the group have shard count
*/
func (sc *ShardCtrler) collectRichShardsAndPoorGroups(gids []int, avgShard int) ([]int, map[int]int) {
	richShards := make([]int, 0)
	poorGroups := make(map[int]int)
	for _, gid := range gids {
		groupShards := sc.getGroupShards(gid)
		DPrintf("[ShardCtrler-%d] rebalance groupShards=%v, avgShard=%d, gids=%v", sc.me, groupShards, avgShard, gids)
		overShards := len(groupShards) - avgShard
		for i := 0; i < overShards; i++ {
			richShards = append(richShards, groupShards[i])
		}
		if overShards < 0 {
			poorGroups[gid] = len(groupShards)
		}
	}
	return richShards, poorGroups
}

func (sc *ShardCtrler) rebalance() {
	// rebalance shard to groups
	latestConf := sc.getConfig(-1)
	// if all groups leave, reset all shards
	if len(latestConf.Groups) == 0 {
		for index, _ := range latestConf.Shards {
			latestConf.Shards[index] = InvalidGroup
		}
		DPrintf("[ShardCtrler-%d] not groups, rebalance result=%v, sc.config=%v", sc.me, latestConf.Shards, sc.configs)
		return
	}

	// step 1 : collect invalid shard
	gids := sc.getGIDs()
	idleShards := make([]int, 0)
	// 1st loop collect not distribute shard
	for index, belongGroup := range latestConf.Shards {
		// not alloc shard or shard belong group already leave
		if belongGroup == InvalidGroup || !xIsInGroup(belongGroup, gids) {
			idleShards = append(idleShards, index)
		}
	}
	// 2nd loop collect rich groups
	avgShard := (len(latestConf.Shards) / len(gids))
	richShards, poorGroups := sc.collectRichShardsAndPoorGroups(gids, avgShard)
	DPrintf("[ShardCtrler-%d] rebalance avgShard=%d idleShards=%v, richShards=%v, poorGroups=%v latestConf=%v", sc.me, avgShard, idleShards, richShards, poorGroups, latestConf)
	idleShards = append(idleShards, richShards...)
	sort.Ints(idleShards)
	allocIndex, i := 0, 0
	// To prevent differnt server have diff result, sort it
	poorGIDs := make([]int, 0)
	for gid := range poorGroups {
		poorGIDs = append(poorGIDs, gid)
	}
	sort.Ints(poorGIDs)
	for _, gid := range poorGIDs {
		groupShardsNum := poorGroups[gid]
		for i = allocIndex; i < len(idleShards); i++ {
			groupShardsNum += 1
			latestConf.Shards[idleShards[i]] = gid
			if groupShardsNum > avgShard {
				break
			}
		}
		allocIndex = i
	}
	// 3rd alloc left shard
	for ; allocIndex < len(idleShards); allocIndex++ {
		i = allocIndex % len(gids)
		latestConf.Shards[idleShards[allocIndex]] = gids[i]
	}
	sc.configs[len(sc.configs)-1] = latestConf
	DPrintf("[ShardCtrler-%d] rebalance result=%v, sc.config=%v", sc.me, latestConf.Shards, sc.getConfig(-1))
}

func (sc *ShardCtrler) invokeMsg(Msg Op) {
	DPrintf("[ShardCtrler-%d] Do %s, Msg=%v, configs=%v", sc.me, Msg.Command, Msg, sc.getConfig(-1))
	switch Msg.Command {
	case T_Join: // add a set of groups
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
			Groups: newGroups,
			Shards: latestConf.Shards,
		}
		sc.configs = append(sc.configs, config)
		// maybe need rebalance now
		sc.rebalance()
	case T_Leave: // delete a set of groups
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
			Groups: newGroups,
			Shards: latestConf.Shards,
		}
		sc.configs = append(sc.configs, config)
		sc.rebalance()
	case T_Move:
		latestConf := sc.getConfig(-1)
		config := Config{
			Num:    len(sc.configs),
			Groups: latestConf.Groups,
			Shards: latestConf.Shards,
		}
		config.Shards[Msg.Shard] = Msg.GID // no need rebalance
		sc.configs = append(sc.configs, config)
	case T_Query:
		// nothing to do
	default:
		DPrintf("[ShardCtrler-%d] Do Op Error, not found type, Msg=%v", sc.me, Msg)
	}
}

func (sc *ShardCtrler) processMsg() {
	for {
		applyMsg := <-sc.applyCh
		opMsg := applyMsg.Command.(Op)
		_, isLeader := sc.rf.GetState()
		sc.mu.Lock()
		ck := sc.GetCk(opMsg.CkId)
		// already process
		DPrintf("[ShardCtrler-%d] Received Msg %v, Isleader=%v", sc.me, applyMsg, isLeader)
		if applyMsg.CommandIndex == ck.msgUniqueId && isLeader {
			DPrintf("[ShardCtrler-%d] Ready Notify To %d Msg %v, msgUniqueId=%d", sc.me, opMsg.CkId, applyMsg, ck.msgUniqueId)
			sc.NotifyApplyMsgByCh(ck.messageCh, opMsg)
			DPrintf("[ShardCtrler-%d] Notify To %d Msg %v finish ... ", sc.me, opMsg.CkId, applyMsg)
			ck.msgUniqueId = 0
		}
		
		if opMsg.SeqId < ck.seqId {
			DPrintf("[ShardCtrler-%d] already process Msg %v finish ... ", sc.me, applyMsg)
			sc.mu.Unlock()
			continue
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
	sc.applyCh = make(chan raft.ApplyMsg, 1000)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.requestMap = make(map[int64]*ShardClerk)
	go sc.processMsg()
	return sc
}
