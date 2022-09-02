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
	log            []LogEntry
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	//e.Encode(rf.commitIndex)
	e.Encode(rf.log)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	//var commitIndex int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		//d.Decode(&commitIndex) != nil ||
		d.Decode(&log) != nil {
		DPrintf("[readPersist] decode failed ...")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		DPrintf("[readPersist] Term=%d VotedFor=%d, Log=%v ...", rf.currentTerm, rf.votedFor, rf.log)
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	// 1. Reply immediately if term < currentTerm
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
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

var roleName [4]string = [4]string{"None", "Follwer", "Candidate", "Leader"}

func (rf *Raft) to_simple_string() string {
	output := fmt.Sprintf("[%s-%d] Term=%d, VotedFor=%d", roleName[rf.currentRole], rf.me, rf.currentTerm, rf.votedFor)
	return output
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("[RequestVote] %s recived vote request %v\n", rf.to_simple_string(), args)

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
	lastLogIndex := len(rf.log) - 1
	lastLog := rf.log[lastLogIndex]
	if (args.LastLogIndex < lastLogIndex && args.LastLogTerm == lastLog.Term) || args.LastLogTerm < lastLog.Term {
		DPrintf("[RequestVote] %v not vaild, %d reject vote request\n", args, rf.me)
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.electionTimer.Reset(getRandomTimeout())
	DPrintf("[RequestVote] %s vote for %d\n", rf.to_simple_string(), rf.votedFor)
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
	index = len(rf.log)
	rf.log = append(rf.log, LogEntry{Term: term, Command: command, Index: index})
	rf.persist()
	DPrintf("[Start] %s Add Log Index=%d Term=%d Command=%v\n", rf.to_simple_string(), rf.log[index].Index, rf.log[index].Term, rf.log[index].Command)
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
	// 300 ~ 450 ms 的误差
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
	DPrintf("[SwitchRole]%s change to %s \n", rf.to_simple_string(), roleName[role])
	rf.currentRole = role
	switch role {
	case ROLE_Follwer:
		rf.votedFor = -1
	case ROLE_Leader:
		// init leader dafta
		rf.heartbeatTimer.Reset(100 * time.Millisecond)
		for i := range rf.peers {
			rf.matchIndex[i] = 0
			rf.nextIndex[i] = len(rf.log)  // 下标从 1 开始的
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
	//DPrintf("[AppendEntries] %s recive AppendEntries rpc %v", rf.to_simple_string(), args)
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		DPrintf("[AppendEntries] return because rf.currentTerm > args.Term , %s", rf.to_simple_string())
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
	lastLogIndex := len(rf.log) - 1
	if lastLogIndex < args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		// optimistically thinks receiver's log matches with Leader's as a subset
		reply.ConflictIndex = len(rf.log)
		// no conflict term
		reply.ConflictTerm = -1
		DPrintf("[AppendEntries] %s failed. return because log less than leader expect", rf.to_simple_string())
		return
	}

	// b. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	if rf.log[(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		// receiver's log in certain term unmatches Leader's log
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term

		// expecting Leader to check the former term
		// so set ConflictIndex to the first one of entries in ConflictTerm
		conflictIndex := args.PrevLogIndex
		// apparently, since rf.log[0] are ensured to match among all servers
		// ConflictIndex must be > 0, safe to minus 1
		for rf.log[conflictIndex-1].Term == reply.ConflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex

		DPrintf("[AppendEntries] failed. return because prev log conflict index=%d, args.Term=%d, rf.log.Term=%d", args.PrevLogIndex, args.Term, rf.log[(args.PrevLogIndex)].Term)
		return
	}

	// c. Append any new entries not already in the log
	// compare from rf.log[args.PrevLogIndex + 1]
	unmatch_idx := -1
	for i := 0; i < len(args.Entries); i++ {
		index := args.PrevLogIndex + 1 + i // Entries 从 PrevLogIndex + 1 的位置开始
		if len(rf.log) < index + 1 || rf.log[index].Term != args.Entries[i].Term {
			unmatch_idx = i
			break
		}
	}

	if unmatch_idx != -1 {
		// there are unmatch entries
		// truncate unmatch Follower entries, and apply Leader entries
		// 1. append leader 的 Entry
		append_entries := args.Entries[unmatch_idx:]
		rf.log = rf.log[:unmatch_idx + args.PrevLogIndex + 1] // 切片到 endIndex - 1 的位置，所以要 +1
		for _, entry := range append_entries {
			rf.log = append(rf.log, entry)
		}
		DPrintf("[AppendEntries] %s Add Log %v", rf.to_simple_string(), append_entries)
	}

	// 3. 持久化提交
	if args.LeaderCommit > rf.commitIndex {
		commitIndex := args.LeaderCommit
		if commitIndex > len(rf.log) - 1 {
			commitIndex = len(rf.log) - 1
		}
		rf.setCommitIndex(commitIndex)
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) setCommitIndex(commitIndex int) {
	rf.commitIndex = commitIndex
	// apply all entries between lastApplied and committed
	// should be called after commitIndex updated
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.commitIndex > rf.lastApplied {
			DPrintf("[setCommitIndex] commitIndex %d, lastApplied %d", commitIndex, rf.lastApplied)
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				var msg ApplyMsg
				msg.CommandValid = true
				msg.Command = rf.log[i].Command
				msg.CommandIndex = i // command index require start at 1
				rf.applyCh <- msg
				rf.lastApplied = i
			}
		}
	}()
}

func (rf *Raft) SendAppendEntries() {
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
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			// 意味着有日志还没被 commit
			if len(rf.log) != rf.matchIndex[server] {
				for i := rf.nextIndex[server]; i < len(rf.log); i++ { // log 的 index 从 1 开始，数组下标从 0 开始，所以 -1
					args.Entries = append(args.Entries, rf.log[i])
				}
			}
			if len(args.Entries) != 0 {
				DPrintf("[SendAppendEntries] %s replicate log to %d, log range [%d - %d] \n", rf.to_simple_string(), server, rf.nextIndex[server], len(rf.log))
			}
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
				DPrintf("[SendAppendEntries] %v to %d failed because reply.Term > args.Term, reply=%v\n", rf.to_simple_string(), server, reply)
				rf.SwitchRole(ROLE_Follwer)
				rf.currentTerm = reply.Term
				rf.persist()
				return
			}

			// 如果同步日志失败，fast forward 一下, 从 PrevLogIndex - 1 开始找
			if !reply.Success {
				DPrintf("[SendAppendEntries] %s replicate log to %d failed , change nextIndex from %d to %d\n", rf.to_simple_string(), server, rf.nextIndex[server], reply.ConflictIndex)
				rf.nextIndex[server] = reply.ConflictIndex

				// if term found, override it to
				// the first entry after entries in ConflictTerm
				if reply.ConflictTerm != -1 {
					for i := args.PrevLogIndex - 1; i >= 1; i-- {
						if rf.log[i].Term == reply.ConflictTerm {
							// in next trial, check if log entries in ConflictTerm matches
							rf.nextIndex[server] = i
							break
						}
					}
				}
			} else {
				// 1. 如果同步日志成功，则增加 nextIndex && matchIndex
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				DPrintf("[SendAppendEntries] %s replicate log to %d succ , matchIndex=%v nextIndex=%v\n", rf.to_simple_string(), server, rf.matchIndex, rf.nextIndex)
				// 2. 检查是否可以提交，检查 rf.commitIndex
				for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
					if rf.log[N].Term != rf.currentTerm {
						continue
					}

					matchCnt := 1
					for j := 0; j < len(rf.matchIndex); j++ {
						if rf.matchIndex[j] >= N {
							matchCnt += 1
						}
					}
					//fmt.Printf("%d matchCnt=%d\n", rf.me, matchCnt)
					// a. 票数 > 1/2 则能够提交
					if matchCnt*2 > len(rf.matchIndex) {
						rf.setCommitIndex(N)
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
	//DPrintf("[StartElection] %s send vote req to %d\n", rf.to_simple_string(), server)
	DPrintf("[StartElection] %s Start Election ... \n", rf.to_simple_string())
	// 集票阶段
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		// 由于 sendRpc 会阻塞，所以这里选择新启动 goroutine 去 sendRPC，不阻塞当前协程
		go func(server int) {
			rf.mu.Lock()
			lastLogIndex := len(rf.log) - 1
			lastLog := rf.log[lastLogIndex]
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogTerm:  lastLog.Term,
				LastLogIndex: lastLogIndex,
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
			if rf.currentRole != ROLE_Candidate || rf.currentTerm != args.Term {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.SwitchRole(ROLE_Follwer)
				rf.currentTerm = reply.Term
				rf.persist()
				return
			}

			if reply.VoteGranted {
				fmt.Printf("[StartElection] %s get VoteGranted from %d \n", rf.to_simple_string(), server)
				rf.votedCnt = rf.votedCnt + 1
			}

			if rf.votedCnt*2 > len(rf.peers) {
				// 这里有可能处理 rpc 的时候，收到 rpc，变成了 follower，所以再校验一遍
				if rf.currentRole == ROLE_Candidate {
					fmt.Printf("[StartElection] %s election succ, votecnt %d \n", rf.to_simple_string(), rf.votedCnt)
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
	rf.log = make([]LogEntry, 1)
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.heartbeatTimer = time.NewTimer(100 * time.Millisecond)
	rf.electionTimer = time.NewTimer(getRandomTimeout())
	rf.applyCh = applyCh
	for i := range rf.peers {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.log)
	}

	rf.mu.Unlock()
	DPrintf("starting ... %d \n", me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
