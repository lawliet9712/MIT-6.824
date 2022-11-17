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
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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
}

type Snapshot struct {
	lastIncludedTerm  int
	lastIncludedIndex int
	data              []byte // tmp snapshot
}

// A Go object implementing a single Raft peer.
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
	log            []LogEntry
	commitIndex    int   // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied    int   // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	nextIndex      []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex     []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	applyCh        chan ApplyMsg
	heartbeatTimer *time.Timer
	electionTimer  *time.Timer
	snapshot       Snapshot
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) GetRaftState() []byte {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	//e.Encode(rf.commitIndex)
	e.Encode(rf.log)
	e.Encode(rf.snapshot.lastIncludedIndex)
	e.Encode(rf.snapshot.lastIncludedTerm)

	return w.Bytes()
}

func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	//e.Encode(rf.commitIndex)
	e.Encode(rf.log)
	e.Encode(rf.snapshot.lastIncludedIndex)
	e.Encode(rf.snapshot.lastIncludedTerm)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var snapshot Snapshot
	//var commitIndex int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		//d.Decode(&commitIndex) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&snapshot.lastIncludedIndex) != nil ||
		d.Decode(&snapshot.lastIncludedTerm) != nil {
		DPrintf("[readPersist] decode failed ...")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.snapshot = snapshot
		rf.lastApplied = snapshot.lastIncludedIndex - 1
		rf.commitIndex = snapshot.lastIncludedIndex - 1
		DPrintf("[readPersist] Term=%d VotedFor=%d, Log=%v ...", rf.currentTerm, rf.votedFor, rf.log)
	}
}

func (rf *Raft) ReadPersist(data []byte) {
	rf.readPersist(data)
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

type InstallSnapshotArgs struct {
	// Your data here (2A, 2B).
	Term              int    // Leader's term
	LeaderId          int    // so follwer can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // Term of lastIncludeIndex
	Offset            int    // byte offset where chunk is positioned in the snapshot file
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
	Done              bool   // true if this is the last chunk
}

func (rf *Raft) GetLog() []LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) SendInstallSnapshot(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LastIncludedIndex: rf.snapshot.lastIncludedIndex,
		LastIncludedTerm:  rf.snapshot.lastIncludedTerm,
		// hint: Send the entire snapshot in a single InstallSnapshot RPC.
		// Don't implement Figure 13's offset mechanism for splitting up the snapshot.
		Data: rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()
	ok := rf.sendInstallSnapshot(server, &args, &reply)
	if ok {
		// check reply term
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentRole != ROLE_Leader || rf.currentTerm != args.Term {
			return
		}

		if reply.Term > args.Term {
			DPrintf("[SendInstallSnapshot] %v to %d failed because reply.Term > args.Term, reply=%v\n", rf.role_info(), server, reply)
			rf.SwitchRole(ROLE_Follwer)
			rf.currentTerm = reply.Term
			rf.persist()
			return
		}

		// update nextIndex and matchIndex
		rf.nextIndex[server] = args.LastIncludedIndex
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		DPrintf("[SendInstallSnapshot] %s to %d nextIndex=%v, matchIndex=%v", rf.role_info(), server, rf.nextIndex, rf.matchIndex)
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	// 1. Reply immediately if term < currentTerm
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.snapshot.lastIncludedIndex {
		DPrintf("[InstallSnapshot] %s reject Install Snapshot args=%v, rf.lastIncludeIndex=%d", rf.role_info(), args, rf.snapshot.lastIncludedIndex)
		return
	}
	DPrintf("[InstallSnapshot] %s recive InstallSnapshot rpc %v", rf.role_info(), args)
	defer rf.persist()
	if rf.currentTerm < args.Term {
		rf.SwitchRole(ROLE_Follwer)
		rf.currentTerm = args.Term
	}
	rf.electionTimer.Reset(getRandomTimeout())

	rf.snapshot.data = args.Data
	rf.commitIndex = args.LastIncludedIndex - 1
	rf.lastApplied = args.LastIncludedIndex - 1
	realIndex := rf.logicIndexToRealIndex(args.LastIncludedIndex) - 1
	DPrintf("[InstallSnapshot] %s commitIndex=%d, Log=%v", rf.role_info(), rf.commitIndex, rf.log)
	if rf.getLogLogicSize() <= args.LastIncludedIndex {
		rf.log = []LogEntry{}
	} else {
		rf.log = append([]LogEntry{}, rf.log[realIndex+1:]...)
	}
	rf.snapshot.lastIncludedIndex = args.LastIncludedIndex
	rf.snapshot.lastIncludedTerm = args.LastIncludedTerm
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), rf.snapshot.data)
	}()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.snapshot.lastIncludedIndex {
		return
	}
	defer rf.persist()
	// get real index
	realIndex := rf.logicIndexToRealIndex(index) - 1
	// save snapshot
	rf.snapshot.data = snapshot //append(rf.snapshot.data, snapshot...)
	rf.snapshot.lastIncludedTerm = rf.log[realIndex].Term
	// discard before index log
	if rf.getLogLogicSize() <= index {
		rf.log = []LogEntry{}
	} else {
		rf.log = append([]LogEntry{}, rf.log[realIndex+1:]...)
	}
	rf.snapshot.lastIncludedIndex = index
	rf.lastApplied = rf.snapshot.lastIncludedIndex - 1
	DPrintf("[Snapshot] %s do snapshot, index = %d, lastApplied=%d, rf.log=%v", rf.role_info(), index, rf.lastApplied, rf.log)
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), rf.snapshot.data)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate global only id
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // Term id
	VoteGranted bool // true 表示拿到票了
}

var roleName [4]string = [4]string{"None", "Follwer", "Candidate", "Leader"}

func (rf *Raft) role_info() string {
	output := fmt.Sprintf("[%s-%d] Term=%d, VotedFor=%d", roleName[rf.currentRole], rf.me, rf.currentTerm, rf.votedFor)
	return output
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("[RequestVote] %s recived vote request %v\n", rf.role_info(), args)

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term ||
		(args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.VoteGranted = false
		return
	}

	// 新的任期，重置下投票权
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.SwitchRole(ROLE_Follwer)
	}

	// 2B Leader restriction，拒绝比较旧的投票(优先看任期)
	// 1. 任期号不同，则任期号大的比较新
	// 2. 任期号相同，索引值大的（日志较长的）比较新
	lastLogIndex := rf.getLastLogLogicIndex()
	lastLogTerm := rf.snapshot.lastIncludedTerm
	if len(rf.log) != 0 {
		lastLogTerm = rf.log[rf.getLastLogRealIndex()].Term
	}
	DPrintf("[RequestVote] %s lastLogIndex=%d, lastLogTerm=%d\n", rf.role_info(), lastLogIndex, lastLogTerm)
	if (args.LastLogIndex < lastLogIndex && args.LastLogTerm == lastLogTerm) || args.LastLogTerm < lastLogTerm {
		DPrintf("[RequestVote] %v not vaild, %d reject vote request, lastLogIndex=%d, lastLogTerm=%d\n", args, rf.me, lastLogIndex, lastLogTerm)
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.electionTimer.Reset(getRandomTimeout())
	DPrintf("[RequestVote] %s vote for %d\n", rf.role_info(), rf.votedFor)
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
	index = len(rf.log)
	rf.log = append(rf.log, LogEntry{Term: term, Command: command})
	//rf.SendAppendEntries() // for lab3a TestSpeed
	rf.heartbeatTimer.Reset(10 * time.Millisecond)
	rf.persist()
	DPrintf("[Start] %s Add Log Index=%d Term=%d Command=%v\n", rf.role_info(), rf.getLogLogicSize(), rf.log[index].Term, rf.log[index].Command)
	return rf.getLogLogicSize(), term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
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
	// 300 ~ 450 ms 的误差
	return time.Duration(300+rand.Intn(150)) * time.Millisecond
}

func (rf *Raft) SwitchRole(role ServerRole) {
	if role == rf.currentRole {
		if role == ROLE_Follwer {
			rf.votedFor = -1
		}
		return
	}
	DPrintf("[SwitchRole]%s change to %s \n", rf.role_info(), roleName[role])
	rf.currentRole = role
	switch role {
	case ROLE_Follwer:
		rf.votedFor = -1
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(getRandomTimeout())
	case ROLE_Candidate:
		rf.heartbeatTimer.Stop()
	case ROLE_Leader:
		// init leader data
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(100 * time.Millisecond)
		for i := range rf.peers {
			rf.matchIndex[i] = -1
			rf.nextIndex[i] = rf.getLogLogicSize() // 由于数组下标从 0 开始
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
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) getLastLogLogicIndex() int {
	return len(rf.log) - 1 + rf.snapshot.lastIncludedIndex
}

func (rf *Raft) getLastLogRealIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLogLogicSize() int {
	return len(rf.log) + rf.snapshot.lastIncludedIndex
}

func (rf *Raft) getLogRealSize() int {
	return len(rf.log)
}

func (rf *Raft) getLogEntry(index int) LogEntry {
	if index < 0 || index >= len(rf.log) {
		return LogEntry{}
	} else {
		return rf.log[index]
	}
}

func (rf *Raft) sliceLog(startIdx int, endIdx int) {
	if len(rf.log) == 0 || endIdx < 0 {
		return
	}

	if startIdx < 0 {
		startIdx = 0
	}

	if endIdx > len(rf.log) {
		endIdx = len(rf.log)
	}

	rf.log = rf.log[startIdx:endIdx]
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
	DPrintf("[AppendEntries] %s recive AppendEntries rpc %v", rf.role_info(), args)
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		DPrintf("[AppendEntries] return because rf.currentTerm > args.Term , %s", rf.role_info())
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
	// a
	lastLogIndex := rf.getLastLogLogicIndex()
	if lastLogIndex < args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		// optimistically thinks receiver's log matches with Leader's as a subset
		reply.ConflictIndex = lastLogIndex + 1
		// no conflict term
		reply.ConflictTerm = -1
		DPrintf("[AppendEntries] %s failed. return because log less than leader expect", rf.role_info())
		return
	}

	// b. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	realPrevLogIndex := rf.logicIndexToRealIndex(args.PrevLogIndex)
	if realPrevLogIndex >= 0 && rf.log[realPrevLogIndex].Term != args.PrevLogTerm {
		realPrevLogTerm := rf.log[realPrevLogIndex].Term
		reply.Success = false
		reply.Term = rf.currentTerm
		// receiver's log in certain term unmatches Leader's log
		reply.ConflictTerm = realPrevLogTerm

		// expecting Leader to check the former term
		// so set ConflictIndex to the first one of entries in ConflictTerm
		conflictIndex := realPrevLogIndex
		// apparently, since rf.log[0] are ensured to match among all servers
		// ConflictIndex must be > 0, safe to minus 1
		for conflictIndex-1 >= 0 && rf.log[conflictIndex-1].Term == reply.ConflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = rf.realIndexToLogicIndex(conflictIndex)

		DPrintf("[AppendEntries] failed. return because prev log conflict index=%d, args.Term=%d, rf.log.Term=%d", args.PrevLogIndex, args.Term, realPrevLogTerm)
		return
	}

	// c. Append any new entries not already in the log
	// compare from rf.log[args.PrevLogIndex + 1]
	unmatch_idx := -1
	for i := 0; i < len(args.Entries); i++ {
		index := args.PrevLogIndex + 1 + i // Entries 从 PrevLogIndex + 1 的位置开始
		entry := rf.getLogEntry(rf.logicIndexToRealIndex(index))
		if rf.getLogLogicSize() < index+1 || entry.Term != args.Entries[i].Term {
			unmatch_idx = i
			break
		}
	}

	if unmatch_idx != -1 {
		// there are unmatch entries
		// truncate unmatch Follower entries, and apply Leader entries
		// 1. append leader 的 Entry
		append_entries := make([]LogEntry, len(args.Entries)-unmatch_idx)
		copy(append_entries, args.Entries[unmatch_idx:]) // 防止 race，因为切片还是引用
		rf.sliceLog(0, unmatch_idx+rf.logicIndexToRealIndex(args.PrevLogIndex)+1)
		// rf.log = rf.log[:unmatch_idx + rf.logicIndexToRealIndex(args.PrevLogIndex) + 1] // 切片到 endIndex - 1 的位置，所以要 +1
		rf.log = append(rf.log, append_entries...)
		DPrintf("[AppendEntries] %s Add Log %v", rf.role_info(), append_entries)
	}

	// 3. 持久化提交
	if args.LeaderCommit > rf.commitIndex {
		commitIndex := args.LeaderCommit
		if commitIndex > rf.getLastLogLogicIndex() {
			commitIndex = rf.getLastLogLogicIndex()
		}
		rf.setCommitIndex(commitIndex)
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) logicIndexToRealIndex(logicIndex int) int {
	return logicIndex - rf.snapshot.lastIncludedIndex
}

func (rf *Raft) realIndexToLogicIndex(realIndex int) int {
	return realIndex + rf.snapshot.lastIncludedIndex
}

func (rf *Raft) setCommitIndex(commitIndex int) {
	rf.commitIndex = commitIndex
	DPrintf("[setCommitIndex] %s commit index = %d, last applied = %d, snapIndex=%d", rf.role_info(), commitIndex, rf.lastApplied, rf.snapshot.lastIncludedIndex)
	if rf.commitIndex > rf.lastApplied {
		commitIndex = rf.logicIndexToRealIndex(commitIndex)
		lastApplied := rf.logicIndexToRealIndex(rf.lastApplied)
		// apply all entries between lastApplied and committed
		// should be called after commitIndex updated
		go func(commandBaseIndex int, applyEntry []LogEntry) {
			for index, entry := range applyEntry {
				// lab3b 由于 提交和 Snapshot 可能在不同 goroutine 同时执行，因此需要再检查一下 lastApplied
				var msg ApplyMsg
				msg.CommandValid = true
				msg.Command = entry.Command
				msg.CommandIndex = index + commandBaseIndex + 1 // command index require start at 1
				rf.applyCh <- msg
				rf.mu.Lock()
				if rf.lastApplied < msg.CommandIndex-1 {
					rf.lastApplied = msg.CommandIndex - 1
				}
				DPrintf("[setCommitIndex] %s commit msg %v", rf.role_info(), msg)
				rf.mu.Unlock()
			}
		}(rf.lastApplied+1, rf.log[lastApplied+1:commitIndex+1])
	}
}

func (rf *Raft) SendAppendEntries() {
	alreadyCommit := false
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			args := AppendEntriesArgs{}
			reply := AppendEntriesReply{}

			// 1. check if need replicate log
			rf.mu.Lock()
			if rf.currentRole != ROLE_Leader {
				rf.mu.Unlock()
				return
			}
			args.Term = rf.currentTerm
			args.LeaderCommit = rf.commitIndex
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[server] - 1
			realPrevLogIndex := rf.logicIndexToRealIndex(args.PrevLogIndex)
			if realPrevLogIndex >= 0 && rf.getLogRealSize() != 0 {
				args.PrevLogTerm = rf.log[realPrevLogIndex].Term
			}
			// 意味着有日志还没被 commit
			if rf.getLastLogLogicIndex() != rf.matchIndex[server] {
				// hint: have the leader send an InstallSnapshot RPC if it doesn't have the log entries required to bring a follower up to date.
				if rf.nextIndex[server] < rf.snapshot.lastIncludedIndex {
					DPrintf("%s SendInstallSnapshot to %d, rf.nextIndex=%v, snapshotIndex=%d", rf.role_info(), server, rf.nextIndex, rf.snapshot.lastIncludedIndex)
					rf.mu.Unlock()
					go rf.SendInstallSnapshot(server)
					return
				} else {
					startIdx := rf.logicIndexToRealIndex(rf.nextIndex[server])
					DPrintf("[SendAppendEntries] %s to %d startIdx=%d, log=%v, snapshotIndex=%d", rf.role_info(), server, startIdx, rf.log, rf.snapshot.lastIncludedIndex)
					if startIdx < rf.getLogRealSize() {
						args.Entries = make([]LogEntry, len(rf.log)-startIdx)
						copy(args.Entries, rf.log[startIdx:]) // 防止 race，因为切片还是引用
						//args.Entries = rf.log[startIdx:] 不能直接切片，会 data race
					}
				}
			}

			DPrintf("[SendAppendEntries] %s replicate log to %d, log range [%d - %d] \n", rf.role_info(), server, rf.nextIndex[server], len(rf.log))
			rf.mu.Unlock()
			ok := rf.sendAppendEntries(server, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentRole != ROLE_Leader || rf.currentTerm != args.Term {
				return
			}

			if reply.Term > args.Term {
				DPrintf("[SendAppendEntries] %v to %d failed because reply.Term > args.Term, reply=%v\n", rf.role_info(), server, reply)
				rf.SwitchRole(ROLE_Follwer)
				rf.currentTerm = reply.Term
				rf.persist()
				return
			}

			// 如果同步日志失败，fast forward 一下, 从 PrevLogIndex - 1 开始找
			if !reply.Success {
				DPrintf("[SendAppendEntries] %s replicate log to %d failed , change nextIndex from %d to %d\n", rf.role_info(), server, rf.nextIndex[server], reply.ConflictIndex)
				rf.nextIndex[server] = reply.ConflictIndex

				// if term found, override it to
				// the first entry after entries in ConflictTerm
				if reply.ConflictTerm != -1 {
					for i := rf.logicIndexToRealIndex(args.PrevLogIndex) - 1; i >= 0; i-- {
						if rf.log[i].Term == reply.ConflictTerm {
							// in next trial, check if log entries in ConflictTerm matches
							rf.nextIndex[server] = rf.realIndexToLogicIndex(i)
							break
						}
					}
				}
			} else {
				// 1. 如果同步日志成功，则增加 nextIndex && matchIndex
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				DPrintf("[SendAppendEntries] %s replicate log to %d succ , matchIndex=%v nextIndex=%v\n", rf.role_info(), server, rf.matchIndex, rf.nextIndex)
				// 2. 检查是否可以提交，检查 rf.commitIndex
				for N := rf.getLastLogRealIndex(); N > rf.logicIndexToRealIndex(rf.commitIndex); N-- {
					if rf.log[N].Term != rf.currentTerm {
						continue
					}
					DPrintf("[SendApendEntries] %s N=%d, lastLogIndex=%d", rf.role_info(), N, rf.snapshot.lastIncludedIndex)
					matchCnt := 1
					for j := 0; j < len(rf.matchIndex); j++ {
						if rf.logicIndexToRealIndex(rf.matchIndex[j]) >= N {
							matchCnt += 1
						}
					}
					//fmt.Printf("%d matchCnt=%d\n", rf.me, matchCnt)
					// a. 票数 > 1/2 则能够提交
					if matchCnt*2 > len(rf.matchIndex) && !alreadyCommit {
						alreadyCommit = true
						rf.setCommitIndex(rf.realIndexToLogicIndex(N))
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
	rf.persist()
	//DPrintf("[StartElection] %s send vote req to %d\n", rf.role_info(), server)
	DPrintf("[StartElection] %s Start Election ... \n", rf.role_info())
	// 集票阶段
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		// 由于 sendRpc 会阻塞，所以这里选择新启动 goroutine 去 sendRPC，不阻塞当前协程
		go func(server int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}
			args.LastLogIndex = rf.getLastLogLogicIndex()
			args.LastLogTerm = rf.snapshot.lastIncludedTerm
			if len(rf.log) != 0 {
				args.LastLogTerm = rf.log[rf.getLastLogRealIndex()].Term
			}
			reply := RequestVoteReply{}
			DPrintf("[StartElection] %s send requst vote %v", rf.role_info(), args)
			rf.mu.Unlock()
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				DPrintf("[StartElection] id=%d request %d vote failed ...\n", rf.me, server)
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentRole != ROLE_Candidate || rf.currentTerm != args.Term {
				DPrintf("[StartElection] %s failed rf.currentRole != ROLE_Candidate || rf.currentTerm != args.Term, args=%v", rf.role_info(), args)
				return
			}

			if reply.Term > rf.currentTerm {
				rf.SwitchRole(ROLE_Follwer)
				rf.currentTerm = reply.Term
				rf.persist()
				DPrintf("[StartElection] %s failed reply.Term > rf.currentTerm, args=%v", rf.role_info(), args)
				return
			}

			if reply.VoteGranted {
				DPrintf("[StartElection] %s get VoteGranted from %d \n", rf.role_info(), server)
				rf.votedCnt = rf.votedCnt + 1
			}

			if rf.votedCnt*2 > len(rf.peers) {
				// 这里有可能处理 rpc 的时候，收到 rpc，变成了 follower，所以再校验一遍
				if rf.currentRole == ROLE_Candidate {
					DPrintf("[StartElection] %s election succ, votecnt %d \n", rf.role_info(), rf.votedCnt)
					rf.SwitchRole(ROLE_Leader)
					rf.SendAppendEntries()
				}
			}
		}(server)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	rf.currentTerm = 1
	rf.commitIndex = -1
	rf.votedFor = -1
	rf.lastApplied = -1
	rf.currentRole = ROLE_Follwer
	rf.log = make([]LogEntry, 0)
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.heartbeatTimer = time.NewTimer(100 * time.Millisecond)
	rf.electionTimer = time.NewTimer(getRandomTimeout())
	rf.applyCh = applyCh
	for i := range rf.peers {
		rf.matchIndex[i] = -1
		rf.nextIndex[i] = rf.getLogLogicSize()
	}

	rf.mu.Unlock()
	DPrintf("starting ... %d \n", me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
