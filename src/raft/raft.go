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
import "labrpc"
import "time"
import "math/rand"

// import "bytes"
// import "labgob"

const Heartbeat = 150 // ms

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
    CommandValid bool
    Command      interface{}
    CommandIndex int
}

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
    applyCh chan ApplyMsg

    currentTerm int
    votedFor    *int
    status      int // 0 follower, 1 leader, 2 candidate

    electionTimeout *time.Timer
    heartbeatTimeout *time.Timer

    log         []LogEntry

    commitIndex int
    lastApplied int

    nextIndex   []int
    matchIndex  []int

    stepDownTerm int

}

type LogEntry struct {
    Command interface{}
    Term    int
    Index   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

    // var term int
    // var isleader bool
    // Your code here (2A).
    return rf.currentTerm, rf.status == 1
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
    // } else {
    //   rf.xxx = xxx
    //   rf.yyy = yyy
    // }
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
    // Your data here (2A, 2B).

    Term int
    CandidateId int
    LastLogIndex int
    LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
    // Your data here (2A).

    Term int
    VoteGranted bool
}

func (rf *Raft) isUpToDate(lastLogTerm, lastLogIndex int) bool {
    if len(rf.log) == 0 {
        return true
    }

    lastLog := rf.log[len(rf.log) - 1]

    // DPrintf("i %v isUpToDate: lastLogTerm %v lastLogIndex %v this lastLog %v", rf.me, lastLogTerm, lastLogIndex, lastLog)
    if (lastLogTerm > lastLog.Term) {
        return true
    }

    if (lastLogTerm == lastLog.Term && lastLogIndex >= lastLog.Index) {
        return true
    }

    return false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    LPrintf("Request vote Lock - %v\n", rf.me);
    rf.mu.Lock()
    LPrintf("Request vote Locked - %v\n", rf.me);

    DPrintf("Received RequestVote for id: %v term: %v from CandidateId: %v Term: %v\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)

    // Your code here (2A, 2B).
    if ((args.Term == rf.currentTerm && (rf.votedFor == nil || *rf.votedFor == args.CandidateId)) ||
        (args.Term > rf.currentTerm)) &&
       (rf.isUpToDate(args.LastLogTerm, args.LastLogIndex)) {

        reply.VoteGranted = true
        rf.currentTerm = args.Term
        rf.votedFor = &args.CandidateId

        if rf.status == 1 {
            rf.status = 0
            rf.stopHeartbeatTimeout()
        }

        if rf.status == 0 {
            rf.resetElectionTimeout()
        }
    } else {
        reply.VoteGranted = false
    }

    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
    }

    reply.Term = rf.currentTerm

    LPrintf("Request vote Unlock - %v\n", rf.me);
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

// field names must start with capital letters!
type AppendEntriesArgs struct {
    Term         int
    LeaderId     int
    PrevLogIndex int
    PrevLogTerm  int
    Entries      []LogEntry
    LeaderCommit int
}

// field names must start with capital letters!
type AppendEntriesReply struct {
    Term    int
    Success bool
    ConflictTerm int
    FirstIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    LPrintf("Append entries Lock - %v\n", rf.me);
    rf.mu.Lock()
    LPrintf("Append entries Locked - %v\n", rf.me);

    // DPrintf("Received AppendEntries for id: %v term: %v log: %v from LeaderId: %v Term: %v PrevLogIndex: %v PrevLogTerm: %v  CommitIdx: %v, log entries: %v\n", rf.me, rf.currentTerm, rf.log, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)

    if args.Term >= rf.currentTerm {
        rf.currentTerm = args.Term
        
        if rf.status == 1 {
            rf.stopHeartbeatTimeout()
        }

        rf.status = 0

        // handle entries
        if args.PrevLogIndex == 0 || ((len(rf.log) > args.PrevLogIndex - 1) && (rf.log[args.PrevLogIndex - 1].Term == args.PrevLogTerm)) {
            entries := args.Entries

            for _, entry := range entries {
                if len(rf.log) >= entry.Index {
                    if rf.log[entry.Index - 1].Term != entry.Term {
                        rf.log = rf.log[:entry.Index - 1]
                        rf.log = append(rf.log, entry)
                    }
                } else {
                    rf.log = append(rf.log, entry)
                }
            }

            oldCommitIndex := rf.commitIndex

            if len(entries) == 0 {
                rf.commitIndex = args.LeaderCommit
            } else {
                rf.commitIndex = MinInt(args.LeaderCommit, entries[len(entries) - 1].Index)
            }

            for i := oldCommitIndex + 1; i <= rf.commitIndex; i++ {
                rf.applyCh <- ApplyMsg{
                    CommandValid: true,
                    Command: rf.log[i - 1].Command,
                    CommandIndex: i,
                }
            }

            rf.lastApplied = rf.commitIndex
            
            reply.Success = true
        } else {
            if (len(rf.log) > args.PrevLogIndex - 1) && (rf.log[args.PrevLogIndex - 1].Term != args.PrevLogTerm) {
                DPrintf("Conflict term for %v in %v, prevLogTerm: %v,  term: %v \n", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.log[args.PrevLogIndex - 1].Term)
                reply.ConflictTerm = rf.log[args.PrevLogIndex - 1].Term

                firstIndex := args.PrevLogIndex - 1

                for i := firstIndex; i >= 0; i-- {
                    if rf.log[i].Term != reply.ConflictTerm {
                        break
                    } else {
                        firstIndex = i
                    }
                }

                reply.FirstIndex = firstIndex + 1
            }

            reply.Success = false
        }

        rf.resetElectionTimeout()
    } else {
        reply.Success = false
    }

    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
    }

    reply.Term = rf.currentTerm
    // DPrintf("Current log for %v: %v, commitIndex: %v\n", rf.me, rf.log, rf.commitIndex)
    LPrintf("Apeend entries Unlock - %v\n", rf.me);
    rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    // Your code here (2A, 2B).
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

func (rf *Raft) ConvertToFollower() {
    DPrintf("Converted to follower to due term: %v\n", rf.me)
    rf.status = 0
    rf.stopHeartbeatTimeout()
    rf.resetElectionTimeout()
}

func f(now time.Time, rf *Raft) func() {
    return func() {
        rf.startNewElection(now)
    }
}

func (rf *Raft) resetElectionTimeout() {
    timeout := time.Duration(rand.Intn(200) + 300) * time.Millisecond

    if rf.electionTimeout != nil {
        rf.electionTimeout.Stop()
        // DPrintf("Stopped election timer for %v.\n", rf.me)
    }

    timenow := time.Now()
    // DPrintf("Create new election timer for %v at %v : timeout %v", rf.me, timenow.UnixNano(), timeout)
    F := f(timenow, rf)
    rf.electionTimeout = time.AfterFunc(
        timeout,
        F,
    )
}

func (rf *Raft) stopHeartbeatTimeout() {
    if rf.heartbeatTimeout != nil {
        rf.heartbeatTimeout.Stop()
    }
}

func (rf *Raft) sendHeartbeat(term int) {
    LPrintf("Send heartbeat Lock - %v\n", rf.me);
    rf.mu.Lock()
    LPrintf("Send heartbeat Locked - %v\n", rf.me);

    if rf.stepDownTerm != -1 {
        rf.currentTerm = rf.stepDownTerm
        rf.ConvertToFollower()
        rf.stepDownTerm = -1
        rf.mu.Unlock()
        return
    } 

    rf.IncCommitIdx()

    commitIndex := rf.commitIndex
    logCopy := make([]LogEntry, len(rf.log))
    copy(logCopy, rf.log)

    for i := range rf.peers {
        go func(i int) {
            if i == rf.me {
                return
            }
            
            rf.mu.Lock()

            var prevLogIndex, prevLogTerm int
            var entries []LogEntry

            lastLogIndex := len(logCopy)

            if lastLogIndex < rf.nextIndex[i] { // entries = []
                prevLogIndex = lastLogIndex
                if lastLogIndex == 0 {
                    prevLogTerm = 0
                } else {
                    prevLogTerm = logCopy[lastLogIndex - 1].Term
                }
                entries = []LogEntry{}
            } else {
                prevLogIndex, prevLogTerm = rf.GetLastLogInfo(logCopy[:rf.nextIndex[i] - 1])
                entries = logCopy[rf.nextIndex[i] - 1:]
            }
 
            // DPrintf("Sent append entries for %v: PrevLogIndex: %v PrevLogTerm %v Entries %v LeaderCommit %v, nextIndex %v matchIndex %v", i, prevLogIndex, prevLogTerm, entries, commitIndex, rf.nextIndex[i], rf.matchIndex[i])
            
            args := AppendEntriesArgs{
                Term: term,
                LeaderId: rf.me,
                PrevLogIndex: prevLogIndex,
                PrevLogTerm: prevLogTerm,
                Entries: entries,
                LeaderCommit: commitIndex,
            }

            reply := AppendEntriesReply{
                Success: false,
                ConflictTerm: -1,
                FirstIndex: -1,
            }
            rf.mu.Unlock()

            ok := rf.sendAppendEntries(i, &args, &reply)

            if ok {
                rf.mu.Lock()


                if reply.Success {

                    DPrintf("Commit matched for %v\n", i)

                    rf.nextIndex[i] = prevLogIndex + len(entries) + 1
                    rf.matchIndex[i] = prevLogIndex + len(entries)
                    
                } else {

                    if reply.Term > rf.currentTerm {
                        rf.stepDownTerm = reply.Term
                    } else {
                        DPrintf("Commit not matched for %v, current next index %v, ConflictTerm %v, FirstIndex %v\n", i, rf.nextIndex[i], reply.ConflictTerm, reply.FirstIndex)
                        
                        if reply.ConflictTerm != -1 && reply.FirstIndex != -1 {

                            lastIdx := -1
                            for idx := len(logCopy) - 1; idx >= 0; idx-- {
                                if rf.log[idx].Term == reply.ConflictTerm {
                                    lastIdx = idx + 1
                                    break
                                }
                            }

                            if lastIdx != -1 {
                                rf.nextIndex[i] = lastIdx
                            } else {
                                rf.nextIndex[i] = reply.FirstIndex
                            }

                            DPrintf("Optimization: new next index for %v: %v", i, rf.nextIndex[i])
                        } else {
                            rf.nextIndex[i] = MaxInt(1, rf.nextIndex[i] - 1)
                        }
                    }

                }

                rf.mu.Unlock()
            }
            
        }(i)
    }

    rf.stopHeartbeatTimeout()

    DPrintf("Create new heartbeat timer for %v, term: %v\n", rf.me, term)
    
    rf.heartbeatTimeout = time.AfterFunc(
        Heartbeat * time.Millisecond,
        func() {
            rf.sendHeartbeat(term)
        },
    )

    LPrintf("Send heartbeat Unlock - %v\n", rf.me);
    rf.mu.Unlock()
}


func (rf *Raft) startNewElection(now time.Time) {
    LPrintf("Start new election Lock - %v\n", rf.me);
    rf.mu.Lock()
    LPrintf("Start new election Locked - %v\n", rf.me);

    // DPrintf("Start election from %v, using timeout created at %v\n", rf.me, now.UnixNano())

    rf.status = 2
    rf.currentTerm += 1
    rf.votedFor = &rf.me
    
    rf.resetElectionTimeout()

    totalVotes := 1
    term := rf.currentTerm
    lastLogIndex, lastLogTerm := rf.GetLastLogInfo(rf.log)

    for i := range rf.peers {
        go func(i int) {
            if i == rf.me {
                return
            }

            args := RequestVoteArgs{
                Term: term,
                CandidateId: rf.me,
                LastLogIndex: lastLogIndex,
                LastLogTerm: lastLogTerm,
            }

            reply := RequestVoteReply{
                VoteGranted: false,
            }
            DPrintf("Request vote from %v to %v\n", rf.me, i)
            ok := rf.sendRequestVote(i, &args, &reply)

            if ok {
                rf.mu.Lock()

                if reply.Term > rf.currentTerm {
                    rf.currentTerm = reply.Term
                    rf.ConvertToFollower()
                }

                rf.mu.Unlock()
            }

                
            if ok && reply.VoteGranted {
                totalVotes++ 
            }

            rf.mu.Lock()

            if totalVotes > (len(rf.peers) / 2) && rf.status == 2 {
                LPrintf("Leader selected Lock - %v\n", rf.me);
                
                DPrintf("Leader selected: %v\n", rf.me)
                rf.status = 1

                lastLogIndex := len(rf.log)
                rf.nextIndex = make([]int, len(rf.peers))
                rf.matchIndex = make([]int, len(rf.peers))

                for idx := range rf.nextIndex {
                    rf.nextIndex[idx] = lastLogIndex + 1
                }

                for idx := range rf.matchIndex {
                    rf.matchIndex[idx] = 0
                }

                if rf.electionTimeout.Stop() {
                    DPrintf("Stopped timeout for leader %v\n", rf.me)
                }

                LPrintf("Leader selected Unlock - %v\n", rf.me);
                
                rf.mu.Unlock()
                rf.sendHeartbeat(term)
            } else {
                rf.mu.Unlock()
            }

        }(i)
    }

    LPrintf("Start new election Unlock - %v\n", rf.me);
    rf.mu.Unlock()
}

func (rf *Raft) GetLastLogInfo(prevLog []LogEntry) (int, int) {
    if len(prevLog) == 0 {
        return 0, 0
    }

    lastLog := prevLog[len(prevLog) - 1]

    return lastLog.Index, lastLog.Term
}


func (rf *Raft) IncCommitIdx() {
    // DPrintf("Leader log %v\n", rf.log)
    maxCommitIdx := rf.commitIndex
    for idx := rf.commitIndex + 1; idx <= len(rf.log); idx++ {
        if (rf.log[idx - 1].Term != rf.currentTerm) {
            continue
        }

        totalCommited := 1
        for i := range rf.peers {
            if i != rf.me && rf.matchIndex[i] >= idx {
                totalCommited++
            }
        } 

        if totalCommited > len(rf.peers) / 2 {
            maxCommitIdx = MaxInt(maxCommitIdx, idx)
        }
    }

    for idx := rf.commitIndex + 1; idx <= maxCommitIdx; idx++ {
        DPrintf("Leader Apply msg: command %v commandIndex %v\n", rf.log[idx - 1].Command, idx)
        rf.applyCh <- ApplyMsg{
            CommandValid: true,
            Command: rf.log[idx - 1].Command,
            CommandIndex: idx,
        }
    }

    rf.commitIndex = maxCommitIdx
    rf.lastApplied = rf.commitIndex

    DPrintf("Leader commitIdx: %v\n", rf.commitIndex)
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
    rf.mu.Lock()

    if rf.status != 1 {
        rf.mu.Unlock()
        return -1, -1, false
    }

    prevLogIndex, prevLogTerm := rf.GetLastLogInfo(rf.log)
    DPrintf("Command received for leader %v, currentTerm %v, commitIndex %v, prevLogIndex %v, prevLogTerm %v\n", rf.me, rf.currentTerm, rf.commitIndex, prevLogIndex, prevLogTerm)

    newEntry := LogEntry{
        Command: command,
        Term: rf.currentTerm,
        Index: len(rf.log) + 1,
    }
    rf.log = append(rf.log, newEntry)

    // Your code here (2B)
    rf.mu.Unlock()
    return len(rf.log), rf.currentTerm, rf.status == 1
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
    // Your code here, if desired.

    if rf.electionTimeout != nil {
        rf.electionTimeout.Stop()
    }

    if rf.heartbeatTimeout != nil {
        rf.heartbeatTimeout.Stop()
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
    rf.applyCh = applyCh

    rf.currentTerm = 0
    rf.votedFor = nil
    rf.status = 0

    rf.log = make([]LogEntry, 0)
    rf.commitIndex = 0
    rf.lastApplied = 0

    rf.stepDownTerm = -1
    

    // Your initialization code here (2A, 2B, 2C).

    go func() {
        LPrintf("First Lock - %v\n", rf.me);
        rf.mu.Lock()
        LPrintf("First Locked - %v\n", rf.me);
        rf.resetElectionTimeout()

        LPrintf("First Unlock - %v\n", rf.me);
        rf.mu.Unlock()
    }()

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())

    return rf
}