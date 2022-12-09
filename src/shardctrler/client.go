package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	ckId  int64
	seqId int
	mu    sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) allocSeqId() int {
	ck.seqId += 1
	return ck.seqId
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.ckId = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	ck.mu.Lock()
	args.Num = num
	args.CkId = ck.ckId
	args.SeqId = ck.allocSeqId()
	DPrintf("[Clerk-%d] call [Query], args=%v", ck.ckId, args)
	ck.mu.Unlock()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				DPrintf("[Clerk-%d] call [Query] finish, args=%v", ck.ckId, args)
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	ck.mu.Lock()
	args.Servers = servers
	args.CkId = ck.ckId
	args.SeqId = ck.allocSeqId()
	DPrintf("[Clerk-%d] call [Join], args=%v", ck.ckId, args)
	ck.mu.Unlock()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			DPrintf("[Clerk-%d] call [Join] to %v, args=%v", ck.ckId, srv, args)
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				DPrintf("[Clerk-%d] call [Join] finish, args=%v", ck.ckId, args)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	ck.mu.Lock()
	args.GIDs = gids
	args.SeqId = ck.allocSeqId()
	args.CkId = ck.ckId
	DPrintf("[Clerk-%d] call [Leave], args=%v", ck.ckId, args)
	ck.mu.Unlock()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				DPrintf("[Clerk-%d] call [Leave] finish, args=%v", ck.ckId, args)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	ck.mu.Lock()
	args.Shard = shard
	args.GID = gid
	args.SeqId = ck.allocSeqId()
	args.CkId = ck.ckId
	ck.mu.Unlock()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
