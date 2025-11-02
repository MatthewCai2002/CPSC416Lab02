package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"cpsc416-2025w1/labgob"
	"cpsc416-2025w1/labrpc"
	"cpsc416-2025w1/raftapi"
	"cpsc416-2025w1/tester1"
)


// Server state constants
const (
	Follower = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	
	// Persistent state on all servers (Figure 2)
	currentTerm int // latest term server has seen
	votedFor     int // candidateId that received vote in current term (or -1 if none)
	log          []LogEntry // log entries; first entry is dummy at index 0
	
	// Volatile state on all servers
	state       int // Follower, Candidate, or Leader
	commitIndex int // index of highest log entry known to be committed
	
	// Volatile state on leaders (reinitialized after election)
	nextIndex  []int // for each server, index of next log entry to send
	matchIndex []int // for each server, index of highest log entry known to be replicated
	
	// Election and heartbeat timing
	lastHeartbeat time.Time // last time we received a heartbeat or sent one
	electionTimeout time.Duration // randomized election timeout
}

// Log entry structure
type LogEntry struct {
	Term    int         // term when entry was received by leader
	Command interface{} // command for state machine
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	// Initialize reply
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	
	// If term is outdated, reject
	if args.Term < rf.currentTerm {
		return
	}
	
	// If we see a higher term, become follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}
	
	// Check if we can vote for this candidate
	// Rule: vote if (votedFor is null or candidateId) AND candidate's log is at least as up-to-date
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}
	
	canVote := (rf.votedFor == -1 || rf.votedFor == args.CandidateId)
	logUpToDate := (args.LastLogTerm > lastLogTerm) || 
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	
	if canVote && logUpToDate {
		rf.votedFor = args.CandidateId
		rf.lastHeartbeat = time.Now()
		rf.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond
		reply.VoteGranted = true
	}
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

// AppendEntries RPC structures
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	reply.Term = rf.currentTerm
	reply.Success = false
	
	// If term is outdated, reject
	if args.Term < rf.currentTerm {
		return
	}
	
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}
	
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond // longer because of readme
	
	// For Part A, we just need heartbeats (empty entries)
	// In Part B, we'll check PrevLogIndex/PrevLogTerm and append entries
	if len(args.Entries) == 0 {
		// This is a heartbeat
		reply.Success = true
	}
}

// Send AppendEntries RPC to a server
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).


	return index, term, isLeader
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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
        // Check if a leader election should be started.
		rf.mu.Lock()
		state := rf.state
		lastHeartbeat := rf.lastHeartbeat
		electionTimeout := rf.electionTimeout
		rf.mu.Unlock()

		// Check if a leader election should be started (for followers/candidates)
		if state == Follower || state == Candidate {
			if time.Since(lastHeartbeat) > electionTimeout {
				rf.startElection()
				rf.mu.Lock()
				rf.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond
				rf.lastHeartbeat = time.Now()
				rf.mu.Unlock()
			}
		} else if state == Leader {
			rf.sendHeartbeats()
		}

		// pause for a short time to avoid busy waiting
		time.Sleep(50 * time.Millisecond)
	}
}

// Start an election (called when election timeout expires)
func (rf *Raft) startElection() {
	rf.mu.Lock()
	
	if rf.state != Follower && rf.state != Candidate {
		rf.mu.Unlock()
		return
	}
	
	// Become candidate
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}
	
	currentTerm := rf.currentTerm
	votesNeeded := len(rf.peers)/2 + 1
	
	args := RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	
	rf.mu.Unlock()
	
	// Send RequestVote to all peers
	votes := int32(1) // vote for self 
	
	var wg sync.WaitGroup
	
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		
		wg.Add(1)
		go func(server int) {
			defer wg.Done()
			
			reply := RequestVoteReply{}
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond
					rf.lastHeartbeat = time.Now()
				} else if rf.state == Candidate && currentTerm == rf.currentTerm && reply.VoteGranted {
					newVotes := atomic.AddInt32(&votes, 1)
					if int(newVotes) >= votesNeeded && rf.state == Candidate {
						rf.state = Leader
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						for i := range rf.peers {
							rf.nextIndex[i] = len(rf.log)
							rf.matchIndex[i] = 0
						}
						rf.lastHeartbeat = time.Now()
					}
				}
			}
		}(i)
	}
	
	wg.Wait()
}

// Send heartbeats to all followers
func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	
	if rf.state != Leader || time.Since(rf.lastHeartbeat) < 100*time.Millisecond {
		rf.mu.Unlock()
		return
	}
	
	rf.lastHeartbeat = time.Now()
	
	term := rf.currentTerm
	leaderId := rf.me
	
	rf.mu.Unlock()
	
	// Send AppendEntries (heartbeat) to all followers
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		
		go func(server int) {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:         term,
				LeaderId:     leaderId,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []LogEntry{},
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			
			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(server, &args, &reply) {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
				}
				rf.mu.Unlock()
			}
		}(i)
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	
	// Initialize state (will be overwritten by readPersist if there's persisted state)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{Term: 0, Command: nil}} // dummy entry at index 0
	rf.state = Follower
	rf.commitIndex = 0
	
	// Randomize election timeout (between 300ms and 600ms)
	rf.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond
	rf.lastHeartbeat = time.Now()

	rf.readPersist(persister.ReadRaftState())
	go rf.ticker()

	return rf
}
