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

import "sync"
import (
	"labrpc"
	"math/rand"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Raft_Role string

const RAFT_ROLE_FOLLOWER = Raft_Role("Follower")
const RAFT_ROLE_LEADER = Raft_Role("Leader")
const RAFT_ROLE_CANDIDATE = Raft_Role("Candidate")

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        Raft_Role
	currentTerm int
	votedFor    int
	log         []int
	commitIndex int
	lastApplied int

	granted_votes_count int

	timeoutChan chan bool
	loopChan    chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = rf.role == RAFT_ROLE_LEADER
	// Your code here (2A).
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

type AppendEntries struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, entries *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", entries, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	reply.Success = false
	reply.Term = rf.currentTerm
	if rf.currentTerm < args.Term {
		reply.Success = true
		reply.Term = args.Term
		rf.setFollower(-1)
	} else if rf.currentTerm == args.Term && args.LeaderId != -1 {
		rf.setFollower(args.LeaderId)
	}
	//DPrintf("%d Received a heartbeat from %d", rf.me, args.LeaderId)
	rf.resetTimeout()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
	//LastLogIndex int
	//LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term      int
	VoteGrant bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	originVote := rf.votedFor
	originTerm := rf.currentTerm

	reply.VoteGrant = false
	reply.Term = args.Term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
	} else if args.Term == rf.currentTerm {
		if rf.votedFor == -1 {
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
		}
		reply.VoteGrant = rf.votedFor == args.CandidateId
	} else {
		rf.currentTerm = args.Term
		rf.role = RAFT_ROLE_FOLLOWER
		rf.votedFor = args.CandidateId
		reply.VoteGrant = true
	}
	if reply.VoteGrant {
		rf.resetTimeout()
	}
	DPrintf("(id:%d, originTerm:%d, originVote:%d) recived a vote ask from %++v, reply is %++v", rf.me, originTerm, originVote, args, reply)
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
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) Loop() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == RAFT_ROLE_CANDIDATE {
		rf.dealCandidate()
	} else if rf.role == RAFT_ROLE_LEADER {
		rf.dealLeader()
	} else if rf.role == RAFT_ROLE_FOLLOWER {
		rf.dealFollower()
	}
}

func (rf *Raft) refresh() {
	go func() {
		if rf.loopChan != nil && len(rf.loopChan) == 0 {
			rf.loopChan <- true
		}
	}()
}

func (rf *Raft) dealLeader() {
	var wg sync.WaitGroup
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(index int) {
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(index, &AppendEntries{
				LeaderId: rf.me,
				Term:     rf.currentTerm,
			}, &reply)
			//DPrintf("%d send heartbeat to %d", rf.me, index)
			if reply.Term > rf.currentTerm {
				rf.setFollower(-1)
			}
			wg.Done()
		}(i)
	}
	var heartBeatTimeout = make(chan bool, 1)
	var heartBeatFinish = make(chan bool, 1)
	go func() {
		wg.Wait()
		heartBeatFinish <- true
	}()
	go func() {
		time.Sleep(100 * time.Microsecond)
		heartBeatTimeout <- true
	}()
	select {
	case <-heartBeatTimeout:
	case <-heartBeatFinish:
	}
	//return 70
}

func (rf *Raft) dealFollower() {
	randCount := rand.Intn(150) + 150
	rf.setTimeout(randCount)
	select {
	case isTimeout := <-rf.timeoutChan:
		if isTimeout {
			DPrintf("%d is wait for heartbeat %d timeout and become a candidate", rf.me, randCount)
			rf.setCandidate()
		}
	}
}

/**
候选者会不断发起选举， 以下三个情况会终止候选者的行为
1. 获得多数的选票， 候选者成为Leader
2. 有更Term更高的选举请求， 变为该发起者的Follower
3. 选举超时

目前实现第1,2种情况
*/
func (rf *Raft) dealCandidate() {
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	rf.granted_votes_count = 1
	var wg sync.WaitGroup
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(index int) {
			reply := &RequestVoteReply{}
			rf.sendRequestVote(index, &RequestVoteArgs{}, reply)
			if reply.VoteGrant {
				DPrintf("%d get a vote from %d, currentTerm: %d, result is %++v", rf.me, index, rf.currentTerm, reply)
			}
			rf.handlerVoteReply(reply)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func (rf *Raft) handlerVoteReply(reply *RequestVoteReply) {
	if rf.currentTerm > reply.Term {
		return
	} else if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.setFollower(-1)
	} else {
		if rf.role == RAFT_ROLE_CANDIDATE {
			if reply.VoteGrant {
				rf.granted_votes_count++
			}
			if rf.granted_votes_count*2 > len(rf.peers) {
				DPrintf("================ get a leader =======================")
				rf.setLeader()
			}
		}
	}
}

func (rf *Raft) changeRoleLog(targetRole Raft_Role) {
	if rf.role != targetRole {
		DPrintf("%d Become to %s from %s, currentTerm: %d", rf.me, targetRole, rf.role, rf.currentTerm)
	}
}

func (rf *Raft) setFollower(leaderId int) {
	rf.changeRoleLog(RAFT_ROLE_FOLLOWER)
	rf.role = RAFT_ROLE_FOLLOWER
	rf.votedFor = leaderId
	//rf.refresh()
}

func (rf *Raft) setLeader() {
	rf.changeRoleLog(RAFT_ROLE_LEADER)
	rf.role = RAFT_ROLE_LEADER
	rf.votedFor = rf.me
	rf.refresh()
}

func (rf *Raft) setCandidate() {
	rf.changeRoleLog(RAFT_ROLE_CANDIDATE)
	rf.role = RAFT_ROLE_CANDIDATE
	rf.votedFor = rf.me
	rf.refresh()
}

func (rf *Raft) resetTimeout() {
	if rf.timeoutChan != nil && len(rf.timeoutChan) == 0 {
		rf.timeoutChan <- false
		//close(rf.timeoutChan)
		rf.timeoutChan = nil
	} else {
		rf.timeoutChan = nil
	}

}

func (rf *Raft) setTimeout(timeout int) {
	rf.resetTimeout()
	rf.timeoutChan = make(chan bool, 1)
	go func(timeoutChan chan bool) {
		time.Sleep(time.Duration(timeout) * time.Microsecond) // sleep one second
		if timeoutChan != nil && len(timeoutChan) == 0 {
			timeoutChan <- true
		}
	}(rf.timeoutChan)
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
	rf.votedFor = -1
	rf.role = RAFT_ROLE_FOLLOWER
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go func() {
		for {
			interval := 10
			rf.Loop()
			rf.loopChan = make(chan bool)
			go func() {
				time.Sleep(time.Duration(interval) * time.Microsecond)
				if rf.loopChan != nil && len(rf.loopChan) == 0 {
					rf.loopChan <- true
				}
			}()
			<-rf.loopChan
			rf.loopChan = nil
		}
	}()
	return rf
}
