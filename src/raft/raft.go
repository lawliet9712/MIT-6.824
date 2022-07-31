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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"bytes"

	"6.824/labgob"
	"6.824/labrpc"
	
)

func GoID() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}

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

type LogEntry struct {
	Term    int
	Command interface{}
	Index   int
}

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
	currentTerm    int
	votedFor       int
	currentRole    ServerRole
	votedCnt       int
	log            map[int]LogEntry
	commitIndex    int   // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied    int   // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	nextIndex      []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex     []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	applyCh        chan ApplyMsg
	heartbeatTimer *time.Timer
	electionTimer  *time.Timer
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentRole)
	e.Encode(rf.votedCnt)
	e.Encode(rf.commitIndex)
	e.Encode(rf.nextIndex)
	e.Encode(rf.log)
	e.Encode(rf.matchIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	Term         int // candidate's term
	CandidateId  int // candidate global only id
	LastLogIndex int
	LastLogTerm  int
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
	defer rf.mu.Unlock()
	//fmt.Printf("id=%d role=%d term=%d recived vote request %v\n", rf.me, rf.currentRole, rf.currentTerm, args)

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term ||
		(args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.VoteGranted = false
		return
	}

	rf.electionTimer.Reset(getRandomTimeout())
	// 新的任期，重置下投票权
	if rf.currentTerm < args.Term {
		rf.SwitchRole(ROLE_Follwer)
		rf.currentTerm = args.Term
	}

	// 2B Leader restriction，拒绝比较旧的投票(优先看任期)
	// 1. 任期号不同，则任期号大的比较新
	// 2. 任期号相同，索引值大的（日志较长的）比较新
	lastLog := rf.log[len(rf.log)]
	if (args.LastLogIndex < lastLog.Index && args.LastLogTerm == lastLog.Term) || args.LastLogTerm < lastLog.Term {
		//fmt.Printf("[RequestVote] %v not vaild, %d reject vote request\n", args, rf.me)
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
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

	// Your code here (2B).
	index := -1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.currentRole == ROLE_Leader
	term := rf.currentTerm
	if !isLeader {
		return index, term, isLeader
	}

	// record in local log
	index = len(rf.log) + 1
	rf.log[index] = LogEntry{Term: term, Command: command, Index: index}
	//fmt.Printf("[Start] %d Add Log Index=%d Term=%d\n", rf.me, rf.log[index].Index, rf.log[index].Term)
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
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.currentRole == ROLE_Leader {
				rf.SendAppendEntries()
				rf.heartbeatTimer.Reset(100 * time.Millisecond)
			}
			rf.mu.Unlock()

		case <-rf.electionTimer.C:
			rf.mu.Lock()
			switch rf.currentRole {
			case ROLE_Candidate:
				rf.StartElection()
			case ROLE_Follwer:
				// 2B 这里直接进行选举，防止出现：
				/* leader 1 follwer 2 follwer 3
				1. follwer 3 长期 disconnect， term 一直自增进行 election
				2. leader 1 和 follower 2 一直在同步 log
				3. 由于 leader restriction， leader 1 和 follwer 2 的 log index 比 3 要长
				4. 此时 follwer 3 reconnect，leader 1 和 follwer 2 都转为 follwer，然后由于一直没有 leader，会心跳超时，转为 candidate
				5. 此时会出现如下情况：
					5.1 [3] 发送 vote rpc 给 [1] 和 [2]
					5.2 [1] 和 [2] 发现 term 比自己的要高，先转换为 follwer，并修改 term，等待 election timeout 后开始 election
					5.3 [3] 发送完之后发现失败了，等待 election timeout 后再重新进行 election
					5.4 此时 [3] 会比 [1] 和 [2] 更早进入 election（[2]和[3]要接收到 rpc 并且处理完才会等待 eletcion，而 [1] 基本发出去之后就进行等待了）
				*/
				rf.SwitchRole(ROLE_Candidate)
				rf.StartElection()
			}
			rf.mu.Unlock()
		}
	}
}

func getRandomTimeout() time.Duration {
	// 150 ~ 300 ms 的误差
	return time.Duration(300+rand.Intn(150)) * time.Millisecond
}

func getCurrentTime() int64 {
	return time.Now().UnixNano()
}

func (rf *Raft) SwitchRole(role ServerRole) {
	if role == rf.currentRole {
		if role == ROLE_Follwer {
			rf.votedFor = -1
		}
		return
	}
	//fmt.Printf("[SwitchRole]%v  id=%d role=%d term=%d change to %d \n", getCurrentTime(), rf.me, rf.currentRole, rf.currentTerm, role)
	rf.currentRole = role
	switch role {
	case ROLE_Follwer:
		rf.votedFor = -1
	case ROLE_Leader:
		// init leader data
		rf.heartbeatTimer.Reset(100 * time.Millisecond)
		for i := range rf.peers {
			rf.matchIndex[i] = 0
			rf.nextIndex[i] = len(rf.log) + 1
		}
	}
}

/*
     1. heart beat
	 2. log replication
*/
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	/*
		part 2A 处理心跳
		0. 优先处理
		如果 args.term > currentTerm ，则直接转为 follwer, 更新当前 currentTerm = args.term
		1. candidate
		无需处理
		2. follwer
		需要更新 election time out
		3. leader
		无需处理
		part 2B 处理日志复制
		1. [先检查之前的]先获取 local log[args.PrevLogIndex] 的 term , 检查是否与 args.PrevLogTerm 相同，不同表示有冲突，直接返回失败
		2. [在检查当前的]遍历 args.Entries，检查 3 种情况
			a. 当前是否已经有了该日志，如果有了该日志，且一致，检查下一个日志
			b. 当前是否与该日志冲突，有冲突，则从冲突位置开始，删除 local log [conflict ~ end] 的 日志
			c. 如果没有日志，则直接追加

	*/
	// 1. Prev Check
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Success = false
		return
	}

	if rf.currentTerm < args.Term {
		rf.SwitchRole(ROLE_Follwer)
		rf.currentTerm = args.Term
	}
	////fmt.Printf("[ReciveAppendEntires] %d electionTimer reset %v\n", rf.me, getCurrentTime())
	rf.electionTimer.Reset(getRandomTimeout())
	reply.Term = rf.currentTerm
	// 1. [先检查之前的]先获取 local log[args.PrevLogIndex] 的 term , 检查是否与 args.PrevLogTerm 相同，不同表示有冲突，直接返回失败
	/* 有 3 种可能：
	a. 找不到 PrevLog ，直接返回失败
	b. 找到 PrevLog, 但是冲突，直接返回失败
	c. 找到 PrevLog，不冲突，进行下一步同步日志
	*/
	prevLog, found := rf.log[args.PrevLogIndex]
	if args.PrevLogIndex != 0 && (!found || prevLog.Term != args.PrevLogTerm) {
		reply.Success = false
		return
	}

	// 2. 如果检查通过了，说明 ： 之前的 Log 与 Leader 都是一致的（需要确保这个 Ffeature）, 接下来只要强制覆盖后续的 log 即可
	for i := 0; i < len(args.Entries); i++ {
		index := args.Entries[i].Index
		rf.log[index] = args.Entries[i]
	}

	// 3. 持久化提交
	if args.LeaderCommit > rf.commitIndex {
		for i := rf.commitIndex + 1; i <= args.LeaderCommit; i++ {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
		}
		rf.commitIndex = args.LeaderCommit
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) SendAppendEntries() {
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			args := AppendEntriesArgs{}
			reply := AppendEntriesReply{}

			// 1. check if need replicate log
			rf.mu.Lock()
			args.Term = rf.currentTerm
			args.LeaderCommit = rf.commitIndex
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[server] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			// 意味着有日志还没被 commit
			if len(rf.log) != rf.matchIndex[server] {
				for i := rf.nextIndex[server]; i <= len(rf.log); i++ { // log 的 index 从 1 开始
					args.Entries = append(args.Entries, rf.log[i])
				}
			}
			rf.mu.Unlock()
			////fmt.Printf("%d send heartbeat %d , matchIndex=%v nextIndex=%v\n", rf.me, server, rf.matchIndex, rf.nextIndex)
			ok := rf.sendAppendEntries(server, &args, &reply)
			if !ok {
				////fmt.Printf("[SendAppendEntries] id=%d send heartbeat to %d failed \n", rf.me, server)
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > args.Term {
				rf.SwitchRole(ROLE_Follwer)
				rf.currentTerm = reply.Term
				return
			}

			// 如果同步日志失败，则将 nextIndex - 1，下次心跳重试
			if !reply.Success {
				rf.nextIndex[server] -= 1
			} else {
				// 1. 如果同步日志成功，则增加 nextIndex && matchIndex
				rf.nextIndex[server] += len(args.Entries)
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				////fmt.Printf("%d replicate log to %d succ , matchIndex=%v nextIndex=%v\n", rf.me, server, rf.matchIndex, rf.nextIndex)
				// 2. 检查是否可以提交，检查 rf.commitIndex
				for commitIndex := rf.commitIndex + 1; commitIndex <= rf.matchIndex[server]; commitIndex++ {
					matchCnt := 1
					for j := 0; j < len(rf.matchIndex); j++ {
						if rf.matchIndex[j] >= commitIndex {
							matchCnt += 1
						}
					}
					//fmt.Printf("%d matchCnt=%d\n", rf.me, matchCnt)
					// a. 票数 > 1/2 则能够提交
					if matchCnt*2 > len(rf.matchIndex) {
						rf.commitIndex = commitIndex
						rf.applyCh <- ApplyMsg{
							CommandValid: true,
							Command:      rf.log[commitIndex].Command,
							CommandIndex: commitIndex,
						}
						//fmt.Printf("Leader commit succ Index=%d Term=%d CommitIndex=%d\n", rf.log[commitIndex].Index, rf.log[commitIndex].Term, commitIndex)
					} else {
						// b. 如果票数不够，后续的也不需要检查了，直接退出,只能 commit 到这里辣
						break
					}
				}
			}

		}(server)
	}
}

func (rf *Raft) StartElection() {
	/* 每一个 election time 收集一次 vote，直到：
	1. leader 出现，heart beat 会切换当前状态
	2. 自己成为 leader
	*/
	// 重置票数和超时时间
	rf.currentTerm += 1
	rf.votedCnt = 1
	rf.electionTimer.Reset(getRandomTimeout())
	rf.votedFor = rf.me
	//fmt.Printf("[StartElection]id=%d role=%d term=%d Start Election ... \n", rf.me, rf.currentRole, rf.currentTerm)
	// 集票阶段
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}

		// 由于 sendRpc 会阻塞，所以这里选择新启动 goroutine 去 sendRPC，不阻塞当前协程
		go func(server int) {
			rf.mu.Lock()
			lastLog := rf.log[len(rf.log)]
			////fmt.Printf("[StartElection] id %d role %d term %d send vote req to %d\n", rf.me, rf.currentRole, rf.currentTerm, server)
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogTerm:  lastLog.Term,
				LastLogIndex: lastLog.Index,
			}
			reply := RequestVoteReply{}
			rf.mu.Unlock()
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				////fmt.Printf("[StartElection] id=%d request %d vote failed ...\n", rf.me, server)
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.SwitchRole(ROLE_Follwer)
				rf.currentTerm = reply.Term
				return
			}

			if reply.VoteGranted {
				rf.votedCnt = rf.votedCnt + 1
			}

			if rf.votedCnt*2 > len(rf.peers) {
				// 这里有可能处理 rpc 的时候，收到 rpc，变成了 follower，所以再校验一遍
				if rf.currentRole == ROLE_Candidate {
					//fmt.Printf("[StartElection] id=%d election succ, votecnt %d \n", rf.me, rf.votedCnt)
					rf.SwitchRole(ROLE_Leader)
					rf.SendAppendEntries()
				}
			}
		}(server)
	}
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
	rf.votedFor = -1
	rf.currentRole = ROLE_Follwer
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.log = make(map[int]LogEntry)
	rf.heartbeatTimer = time.NewTimer(100 * time.Millisecond)
	rf.electionTimer = time.NewTimer(getRandomTimeout())
	rf.applyCh = applyCh
	for i := range rf.peers {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.log) + 1
	}
	//fmt.Printf("match index %v nextIndex %v \n", rf.matchIndex, rf.nextIndex)
	rf.mu.Unlock()

	//fmt.Printf("starting ... %d \n", me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
