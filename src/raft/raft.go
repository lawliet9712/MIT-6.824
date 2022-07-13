package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type ServerRole int

const (
	ROLE_Follwer   ServerRole = 1
	ROLE_Candidate ServerRole = 2
	ROLE_Leader    ServerRole = 3
)

// election time out
const randomRegionBegin int64 = 150000000 // nano second
const randomRegionEnd int64 = 300000000   // nano second

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm     int
	votedFor        int
	currentRole     ServerRole
	electionTimeout int64 // 状态超时时间
	votedCnt        int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	term := rf.currentTerm
	isleader := rf.currentRole == ROLE_Leader
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } rf.xxelse {
	//   x = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // candidate's term
	CandidateId int // candidate global only id
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // Term id
	VoteGranted bool // true 表示拿到票了
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	fmt.Printf("id=%d role=%d term=%d recived vote request \n", rf.me, rf.currentRole, rf.currentTerm)
	switch rf.currentRole {
	case ROLE_Follwer:
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
		rf.electionTimeout = getRandomElectionTimeout()
	case ROLE_Candidate, ROLE_Leader:
		reply.VoteGranted = false
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			rf.SwitchRole(ROLE_Follwer)
			reply.VoteGranted = true
		}
	}

	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	time.Sleep(time.Duration(rand.Intn(500)+1000) * time.Millisecond)
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		currentRole := rf.currentRole
		sleepTime := rf.electionTimeout
		rf.mu.Unlock()
		switch currentRole {
		case ROLE_Leader:
			rf.mu.Lock()
			rf.SendHeartbeat()
			rf.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		case ROLE_Follwer:
			rf.mu.Lock()
			rf.CheckHeartbeat()
			rf.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
		case ROLE_Candidate:
			rf.mu.Lock()
			rf.CollectionVote()
			rf.mu.Unlock()
			time.Sleep(time.Duration(sleepTime))
		}

	}
}

func getRandomElectionTimeout() int64 {
	// 150 ~ 300 ms 的误差
	return (rand.Int63n(150) + 150) * 1000000
}

func getCurrentTime() int64 {
	return time.Now().UnixNano()
}

/*
     1. heart beat
	 2. log replication
*/
type RequestAppendEntriesArgs struct {
	Term int
}

type RequestAppendEntriesReply struct {
}

func (rf *Raft) SwitchRole(role ServerRole) {
	fmt.Printf("id=%d role=%d, term=%d change role to %d\n", rf.me, rf.currentRole, rf.currentTerm, role)
	rf.currentRole = role
}

func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	// [candidate -> follwer] 1. 有其他 leader 出现
	fmt.Printf("[RequestAppendEntries] id=%d term=%d role=%d recived heartbeat ...\n", rf.me, rf.currentTerm, rf.currentRole)
	rf.mu.Lock()
	if rf.currentTerm < args.Term {
		rf.SwitchRole(ROLE_Follwer)
		rf.currentTerm = args.Term
	}

	// 正常情况下，重置 election time out 时间即可
	rf.electionTimeout = getRandomElectionTimeout()
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}

func (rf *Raft) SendHeartbeat() {
	ok := false
	for server, _ := range rf.peers {
		args := RequestAppendEntriesArgs{
			Term: rf.currentTerm,
		}
		reply := RequestAppendEntriesReply{}
		if server == rf.me {
			continue
		}
		ok = rf.sendRequestAppendEntries(server, &args, &reply)
		if !ok {
			fmt.Printf("id=%d role=%d term=%d send heartbeat to %d failed \n", rf.me, rf.currentRole, rf.currentTerm, server)
		} else {
			//fmt.Printf("%d send heartbeat to %d succ \n", rf.me, server)
		}
	}
}

func (rf *Raft) CheckHeartbeat() {
	// 指定时间没有收到 Heartbeat
	timeout := rf.electionTimeout
	if getCurrentTime() > timeout {
		// 开始新的 election, 切换状态
		// [follwer -> candidate] 1. 心跳超时，进入 election
		rf.currentTerm += 1
		rf.currentRole = ROLE_Candidate
		fmt.Printf("[follwer -> candidate] [%d-%d-%d]\n", rf.me, rf.currentRole, rf.currentTerm)
	}
}

func (rf *Raft) CollectionVote() {
	/* 每一个 election time 收集一次 vote，直到：
	1. leader 出现，heart beat 会切换当前状态
	2. 自己成为 leader
	*/
	fmt.Printf("id=%d role=%d term=%d collection vote ... \n", rf.me, rf.currentRole, rf.currentTerm)
	// 集票阶段
	rf.votedCnt = 1
	rf.currentTerm = rf.currentTerm + 1
	for server, _ := range rf.peers {
		args := RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
		}
		reply := RequestVoteReply{}
		if server == rf.me {
			continue
		}
		ok := rf.sendRequestVote(server, &args, &reply)
		if !ok {
			fmt.Printf("id=%d role=%d term=%d  request %d vote failed ...\n", rf.me, rf.currentRole, rf.currentTerm, server)
			continue
		}
		if reply.VoteGranted {
			rf.votedCnt += 1
		}
	}
	fmt.Printf("[candidate] id=%d term=%d got vote %d... \n", rf.me, rf.currentTerm, rf.votedCnt)

	// [candidate -> leader] 2. 自身成为 leader
	if rf.votedCnt*2 >= len(rf.peers) {
		fmt.Printf("term=%d server %d election succ \n", rf.currentTerm, rf.me)
		rf.currentRole = ROLE_Leader
		//rf.SendHeartbeat() // 先主动 send heart beat 一次
	}

	// 重置票数和超时时间
	rf.electionTimeout = getRandomElectionTimeout()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	rf.currentTerm = 1
	rf.currentRole = ROLE_Follwer
	rf.electionTimeout = getRandomElectionTimeout()
	rf.mu.Unlock()

	fmt.Printf("starting ... %d \n", me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
