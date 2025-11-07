package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	// "fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"cpsc416-2025w1/labgob"
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
	lastApplied int // index of highest log entry known to be applied to state machine
	
	// Volatile state on leaders (reinitialized after election)
	nextIndex  []int // for each server, index of next log entry to send
	matchIndex []int // for each server, index of highest log entry known to be replicated
	
	// Election and heartbeat timing
	lastHeartbeat time.Time // last time we received a heartbeat or sent one
	electionTimeout time.Duration // randomized election timeout

	// Snapshot state (3D)
	lastIncludedIndex int // index of the last entry included in the snapshot
	lastIncludedTerm  int // term of the last entry included in the snapshot
	applyCh           chan raftapi.ApplyMsg // channel to deliver ApplyMsg (including snapshots)
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
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)

    // encode persistent state
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.log)
    e.Encode(rf.lastIncludedIndex)
    e.Encode(rf.lastIncludedTerm)

    raftstate := w.Bytes()
 
    snapshot := rf.persister.ReadSnapshot() // save with current snapshot (if any)
    rf.persister.Save(raftstate, snapshot) 
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

	r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)

    var currentTerm int
    var votedFor int
    var log []LogEntry
    var lastIncludedIndex int
    var lastIncludedTerm int

    if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
        panic("failed to decode persisted Raft state")
    } else {
        rf.mu.Lock()
        rf.currentTerm = currentTerm
        rf.votedFor = votedFor
        rf.log = log
        rf.lastIncludedIndex = lastIncludedIndex
        rf.lastIncludedTerm = lastIncludedTerm
        if len(rf.log) == 0 {
            rf.log = []LogEntry{{Term: rf.lastIncludedTerm}}
        }
        // Initialize commitIndex and lastApplied to at least lastIncludedIndex
        if rf.commitIndex < rf.lastIncludedIndex {
            rf.commitIndex = rf.lastIncludedIndex
        }
        if rf.lastApplied < rf.lastIncludedIndex {
            rf.lastApplied = rf.lastIncludedIndex
        }
        rf.mu.Unlock()
    }

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
    rf.mu.Lock()
    defer rf.mu.Unlock()

    // ignore if index is not ahead
    if index <= rf.lastIncludedIndex {
        return
    }

    // cannot snapshot beyond the last log index
    lastIndex := rf.lastIncludedIndex + len(rf.log) - 1
    if index > lastIndex {
        index = lastIndex
    }

    // fetch term at snapshot index
    termAtIndex := rf.getTermAtIndex(index)

    // create new log with a dummy at base (index, term)
    // keep entries after index
    sliceIdx := index - rf.lastIncludedIndex
    newLog := make([]LogEntry, 1)
    newLog[0] = LogEntry{Term: termAtIndex}
    if sliceIdx+1 < len(rf.log) {
        newLog = append(newLog, rf.log[sliceIdx+1:]...)
    }
    rf.log = newLog
    rf.lastIncludedIndex = index
    rf.lastIncludedTerm = termAtIndex

    if rf.commitIndex < rf.lastIncludedIndex {
        rf.commitIndex = rf.lastIncludedIndex
    }
    if rf.lastApplied < rf.lastIncludedIndex {
        rf.lastApplied = rf.lastIncludedIndex
    }

    // persist raft state together with snapshot
    // build raftstate
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.log)
    e.Encode(rf.lastIncludedIndex)
    e.Encode(rf.lastIncludedTerm)
    raftstate := w.Bytes()
    rf.persister.Save(raftstate, snapshot)
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
		rf.persist()
	}
	
	// Check if we can vote for this candidate
	// Rule: vote if (votedFor is null or candidateId) AND candidate's log is at least as up-to-date
    lastLogIndex := rf.getLastIndex()
    lastLogTerm := rf.getTermAtIndex(lastLogIndex)
	
    canVote := (rf.votedFor == -1 || rf.votedFor == args.CandidateId)
    logUpToDate := (args.LastLogTerm > lastLogTerm) || 
        (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	
	if canVote && logUpToDate {
		rf.votedFor = args.CandidateId
		rf.lastHeartbeat = time.Now()
		rf.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond
		reply.VoteGranted = true
		rf.persist()
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
	ConflictTerm int
	ConflictIndex int
}

// InstallSnapshot RPC (3D)
type InstallSnapshotArgs struct {
    Term              int
    LeaderId          int
    LastIncludedIndex int
    LastIncludedTerm  int
    Data              []byte
}

type InstallSnapshotReply struct {
    Term int
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	reply.Success = false
	reply.ConflictTerm = -1
    reply.ConflictIndex = 0

	// If term is outdated, reject
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}
	reply.Term = rf.currentTerm

	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond // longer because of readme

    // Consistency check with snapshot-aware indices
    // if follower does not have entry at prevLogIndex -> conflict
    if args.PrevLogIndex < rf.lastIncludedIndex {
        // leader is behind our snapshot, ask it to advance nextIndex to lastIncludedIndex+1
        reply.ConflictTerm = -1
        reply.ConflictIndex = rf.lastIncludedIndex + 1
        return
    }

    // Special case: if prevLogIndex equals lastIncludedIndex, verify term matches
    if args.PrevLogIndex == rf.lastIncludedIndex {
        if args.PrevLogTerm != rf.lastIncludedTerm {
            // Terms don't match at snapshot boundary
            reply.ConflictTerm = rf.lastIncludedTerm
            reply.ConflictIndex = rf.lastIncludedIndex
            return
        }
    }

    // translate absolute index to slice index
    if args.PrevLogIndex > rf.getLastIndex() {
        // follower's log is shorter than leader expects
		reply.ConflictTerm = -1
        reply.ConflictIndex = rf.getLastIndex() + 1
		return
	}

    // if follower does have entry but does not match leader -> conflict
    if rf.getTermAtIndex(args.PrevLogIndex) != args.PrevLogTerm {
		// get conflicting term and idx of first log entry for term
        reply.ConflictTerm = rf.getTermAtIndex(args.PrevLogIndex)

        idx := args.PrevLogIndex
        for idx > rf.lastIncludedIndex && rf.getTermAtIndex(idx-1) == reply.ConflictTerm {
            idx--
        }
        reply.ConflictIndex = idx
		return
	}

	// logs are consistent, overwrite any conflict entries with entries from leader
	i := 0
    start := args.PrevLogIndex + 1
	for ; i < len(args.Entries); i++ {
        if start + i > rf.getLastIndex() {
			break
		}
        if rf.getTermAtIndex(start + i) != args.Entries[i].Term {
            // cut log up to slice index of (start+i)
            cut := (start + i) - rf.lastIncludedIndex
            rf.log = rf.log[:cut]
			break
		}
	}

	// append any remaining entries
	if i < len(args.Entries) {
        rf.log = append(rf.log, args.Entries[i:]...)
		rf.persist()
	}

	reply.Success = true

	// update commit index of follower
    if args.LeaderCommit > rf.commitIndex {
        lastNewIndex := rf.getLastIndex()
        if args.LeaderCommit < lastNewIndex {
            rf.commitIndex = args.LeaderCommit
        } else {
            rf.commitIndex = lastNewIndex
        }
	}
}

// Send AppendEntries RPC to a server
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Send InstallSnapshot RPC to a server
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
    ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// check if leader, return if not
	isLeader = rf.state == Leader
	if !isLeader {
		return index, term, isLeader
	}

    // create new log entry for command
	newEntry := LogEntry{
		Term: rf.currentTerm,
		Command: command,
	}

	// append entry to leader log
	rf.log = append(rf.log, newEntry)
	rf.persist()

    // set return values (absolute index)
    index = rf.getLastIndex()
    term = rf.currentTerm
	
	return index, term, isLeader
}

// indefinitely loops replicating leader's log to peers
// reads leader's log each iteration, no need to call in start()
// just need to update leader's log in start()
func (rf *Raft) replicateToPeer(peerId int) {
	for {
		rf.mu.Lock()
		// stop if we're not the leader anymore or killed
		if rf.state != Leader || rf.killed() {
			rf.mu.Unlock()
			return
		}

		// get the next log entry to send to peer
		nextIdx := rf.nextIndex[peerId]
		if nextIdx <= 0 {
			nextIdx = 1
		}

        // if follower is too far behind our snapshot, install snapshot
        if nextIdx <= rf.lastIncludedIndex {
            snap := rf.persister.ReadSnapshot()
            argsSnap := InstallSnapshotArgs{
                Term:              rf.currentTerm,
                LeaderId:          rf.me,
                LastIncludedIndex: rf.lastIncludedIndex,
                LastIncludedTerm:  rf.lastIncludedTerm,
                Data:              snap,
            }
            rf.mu.Unlock()

            var replySnap InstallSnapshotReply
            ok := rf.sendInstallSnapshot(peerId, &argsSnap, &replySnap)
            if !ok {
                time.Sleep(50 * time.Millisecond)
                continue
            }
            rf.mu.Lock()
            if replySnap.Term > rf.currentTerm {
                rf.currentTerm = replySnap.Term
                rf.state = Follower
                rf.votedFor = -1
                rf.persist()
                rf.mu.Unlock()
                return
            } else if replySnap.Term == rf.currentTerm {
                rf.lastHeartbeat = time.Now()
            }
            // after installing snapshot, set nextIndex just after snapshot
            rf.nextIndex[peerId] = rf.lastIncludedIndex + 1
            rf.matchIndex[peerId] = rf.lastIncludedIndex
            rf.mu.Unlock()
            time.Sleep(10 * time.Millisecond)
            continue
        }

        prevLogIndex := nextIdx - 1
        prevLogTerm := rf.getTermAtIndex(prevLogIndex)
        // copy entries from nextIdx to end (translate slice indices)
        start := nextIdx - rf.lastIncludedIndex
        entries := append([]LogEntry(nil), rf.log[start:]...)
        args := AppendEntriesArgs{
            Term:         rf.currentTerm,
            LeaderId:     rf.me,
            PrevLogIndex: prevLogIndex,
            PrevLogTerm:  prevLogTerm,
            Entries:      entries,
            LeaderCommit: rf.commitIndex,
        }
		rf.mu.Unlock()

        reply := AppendEntriesReply{}

		ok := rf.sendAppendEntries(peerId, &args, &reply)
		if !ok {
			// network failure or crash, wait a little bit then try again
			time.Sleep(50 * time.Millisecond)
			continue 
		}

		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			rf.mu.Unlock()
			return
		}

		if rf.state != Leader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			// Update follower’s matchIndex and nextIndex
			matchIdx := args.PrevLogIndex + len(args.Entries)
			rf.matchIndex[peerId] = matchIdx
			rf.nextIndex[peerId] = matchIdx + 1
			
			rf.updateCommitIndex()
			rf.mu.Unlock()

			time.Sleep(50 * time.Millisecond)
            continue
		} else {

			// follower has conflict with entry at prevLogIndex
			if (reply.ConflictTerm == -1) {
				// follower log is shorter than leader log
				rf.nextIndex[peerId] = reply.ConflictIndex
			} else {
				// find the first entry in leader log with conflicting term
				// this will be the first index we need to send to follower

				// possible for conflicting term to not be in leader log
				idx := -1
				for i := len(rf.log) - 1; i >= 0; i-- {
					if rf.log[i].Term == reply.ConflictTerm {
						idx = i
						break
					}
				}
				
				if idx != -1 {
					// found first entry of conflicting term in leader log, convert to absolute index
					rf.nextIndex[peerId] = rf.lastIncludedIndex + idx + 1
				} else {
					// did not find entry
					rf.nextIndex[peerId] = reply.ConflictIndex
				}
			}
	
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// InstallSnapshot RPC handler (3D)
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    reply.Term = rf.currentTerm
    if args.Term < rf.currentTerm {
        return
    }
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.votedFor = -1
        rf.state = Follower
        rf.persist()
    }
    reply.Term = rf.currentTerm

    // If snapshot is older than current snapshot, ignore
    if args.LastIncludedIndex <= rf.lastIncludedIndex {
        return
    }

    // Discard log up to snapshot
    // Find position where args.LastIncludedIndex falls in current log
    if args.LastIncludedIndex <= rf.getLastIndex() {
        // Check if term matches at snapshot index
        termAtIdx := rf.getTermAtIndex(args.LastIncludedIndex)
        if termAtIdx == args.LastIncludedTerm {
            // Terms match, keep entries after snapshot index
            sliceIdx := args.LastIncludedIndex - rf.lastIncludedIndex
            newLog := make([]LogEntry, 1)
            newLog[0] = LogEntry{Term: args.LastIncludedTerm}
            if sliceIdx+1 < len(rf.log) {
                newLog = append(newLog, rf.log[sliceIdx+1:]...)
            }
            rf.log = newLog
        } else {
            // Terms don't match, discard all entries and start fresh
            rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
        }
    } else {
        // snapshot goes beyond our log; keep only dummy
        rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
    }

    rf.lastIncludedIndex = args.LastIncludedIndex
    rf.lastIncludedTerm = args.LastIncludedTerm
    if rf.commitIndex < rf.lastIncludedIndex {
        rf.commitIndex = rf.lastIncludedIndex
    }
    if rf.lastApplied < rf.lastIncludedIndex {
        rf.lastApplied = rf.lastIncludedIndex
    }
    
    // Update heartbeat to prevent election timeout
    // This is critical: InstallSnapshot is a form of leader communication
    // Without this, followers might start elections even after receiving snapshots
    rf.lastHeartbeat = time.Now()
    rf.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond
	
    // Persist state and snapshot
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.log)
    e.Encode(rf.lastIncludedIndex)
    e.Encode(rf.lastIncludedTerm)
    raftstate := w.Bytes()
    rf.persister.Save(raftstate, args.Data)

    // Deliver snapshot to service
    if rf.applyCh != nil {
        snapMsg := raftapi.ApplyMsg{
            SnapshotValid: true,
            Snapshot:      args.Data,
            SnapshotTerm:  args.LastIncludedTerm,
            SnapshotIndex: args.LastIncludedIndex,
        }
        // avoid holding lock while sending
        rf.mu.Unlock()
        rf.applyCh <- snapMsg
        rf.mu.Lock()
    }
}

func (rf *Raft) updateCommitIndex() {
	// only leader executes this
    for N := rf.getLastIndex(); N > rf.commitIndex; N-- {
		count := 1 
		for i := range rf.peers {
            if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
        if count > len(rf.peers) / 2 && rf.getTermAtIndex(N) == rf.currentTerm {
			rf.commitIndex = N
			// fmt.Printf("updated commit index N: %d, count: %d \n", N, count)
			break
		}
	}
}

func (rf *Raft) applyLoop(applyCh chan raftapi.ApplyMsg) {
	for {
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		// Skip entries that are already in snapshot
		if rf.lastApplied < rf.lastIncludedIndex {
			rf.lastApplied = rf.lastIncludedIndex
		}

		// apply all entries between lastApplied and commitIndex
        for rf.lastApplied < rf.commitIndex {
            rf.lastApplied++
            entry := rf.getEntryAtIndex(rf.lastApplied)

            applyMsg := raftapi.ApplyMsg{
                CommandValid: true,
                Command:      entry.Command,
                CommandIndex: rf.lastApplied,
            }

			// send the apply message to the service
			rf.mu.Unlock()
			applyCh <- applyMsg
			rf.mu.Lock()
		}

		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
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

// Helper: get absolute last index in the log (including snapshot base)
func (rf *Raft) getLastIndex() int {
    return rf.lastIncludedIndex + len(rf.log) - 1
}

// Helper: get term at absolute index (handles snapshot base)
func (rf *Raft) getTermAtIndex(index int) int {
    if index == rf.lastIncludedIndex {
        return rf.lastIncludedTerm
    }
    if index < rf.lastIncludedIndex {
        return -1
    }
    sliceIdx := index - rf.lastIncludedIndex
    if sliceIdx >= 0 && sliceIdx < len(rf.log) {
        return rf.log[sliceIdx].Term
    }
    return -1
}

// Helper: get entry at absolute index
func (rf *Raft) getEntryAtIndex(index int) LogEntry {
    if index == rf.lastIncludedIndex {
        return LogEntry{Term: rf.lastIncludedTerm}
    }
    if index < rf.lastIncludedIndex {
        panic("getEntryAtIndex: index < lastIncludedIndex")
    }
    sliceIdx := index - rf.lastIncludedIndex
    if sliceIdx < 0 || sliceIdx >= len(rf.log) {
        panic("getEntryAtIndex: index out of range")
    }
    return rf.log[sliceIdx]
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
            // If leader hasn't heard back from any peers for a while, step down.
            if time.Since(lastHeartbeat) > 2*electionTimeout {
                rf.mu.Lock()
                rf.state = Follower
                rf.votedFor = -1
                rf.mu.Unlock()
            } else {
                rf.sendHeartbeats()
            }
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
	rf.persist()

    lastLogIndex := rf.getLastIndex()
    lastLogTerm := rf.getTermAtIndex(lastLogIndex)
	
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
					rf.persist()
				} else if rf.state == Candidate && currentTerm == rf.currentTerm && reply.VoteGranted {
					newVotes := atomic.AddInt32(&votes, 1)
					if int(newVotes) >= votesNeeded && rf.state == Candidate {
						rf.state = Leader
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						lastIndex := rf.getLastIndex()
						for i := range rf.peers {
							rf.nextIndex[i] = lastIndex + 1
							rf.matchIndex[i] = 0
							// once leader is elected start go routines that continuously
							// replicate leader's log, 1 for each peer
							go rf.replicateToPeer(i)
						}
						rf.matchIndex[rf.me] = lastIndex
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
    rf.mu.Unlock()
    
    // Send AppendEntries (heartbeat) to all followers using per-follower nextIndex
    for i := range rf.peers {
        if i == rf.me {
            continue
        }
        go func(server int) {
            rf.mu.Lock()
            if rf.state != Leader {
                rf.mu.Unlock()
                return
            }
            prevIdx := rf.nextIndex[server] - 1
            if prevIdx < rf.lastIncludedIndex {
                // follower too far behind, let replicate loop handle snapshot
                rf.mu.Unlock()
                return
            }
            args := AppendEntriesArgs{
                Term:         rf.currentTerm,
                LeaderId:     rf.me,
                PrevLogIndex: prevIdx,
                PrevLogTerm:  rf.getTermAtIndex(prevIdx),
                Entries:      []LogEntry{},
                LeaderCommit: rf.commitIndex,
            }
            rf.mu.Unlock()
            var reply AppendEntriesReply
            if rf.sendAppendEntries(server, &args, &reply) {
                rf.mu.Lock()
                if reply.Term > rf.currentTerm {
                    rf.currentTerm = reply.Term
                    rf.state = Follower
                    rf.votedFor = -1
                    rf.persist()
                } else if reply.Term == rf.currentTerm {
                    rf.lastHeartbeat = time.Now()
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
	rf.lastApplied = 0
    rf.lastIncludedIndex = 0
    rf.lastIncludedTerm = 0
    rf.applyCh = applyCh
	
	// Randomize election timeout (between 300ms and 600ms)
	rf.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond
	rf.lastHeartbeat = time.Now()

	rf.readPersist(persister.ReadRaftState())
	go rf.ticker()
	go rf.applyLoop(applyCh)

	return rf
}
