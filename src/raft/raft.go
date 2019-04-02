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
import "labgob"
import "bytes"
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
    votedFor    int
    status      int // 0 follower, 1 leader, 2 candidate

    electionTimeout *time.Timer
    heartbeatTimeout *time.Timer

    log         []LogEntry

    commitIndex int
    lastApplied int

    nextIndex   []int
    matchIndex  []int
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

    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
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
    // Example:

    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)
    var currentTerm int
    var votedFor int
    var log []LogEntry
    if d.Decode(&currentTerm) != nil ||
       d.Decode(&votedFor) != nil ||
       d.Decode(&log) != nil {
      DPrintf("readPersist error.\n")
    } else {
        rf.currentTerm = currentTerm
        rf.votedFor = votedFor
        rf.log = log
    }
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

func (rf *Raft) getLastLog(log []LogEntry) LogEntry {
    return log[len(log) - 1]
}

func (rf *Raft) isCandidateLogUpToDate(lastLogTerm, lastLogIndex int) bool {
    if len(rf.log) == 0 {
        return true
    }

    lastLog := rf.getLastLog(rf.log)

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

    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.votedFor = -1

        if rf.status != 0 {
            DPrintf("%v convert to follower inside request vote, previous status: %v\n", rf.me, rf.status)
            rf.convertToFollower()
        }
    }

    // Your code here (2A, 2B).
    if (args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId)) &&
       (rf.isCandidateLogUpToDate(args.LastLogTerm, args.LastLogIndex)) {

        reply.VoteGranted = true
        rf.votedFor = args.CandidateId

        if rf.status == 0 {
            rf.resetElectionTimeout()
        }
    } else {
        reply.VoteGranted = false
    }

    reply.Term = rf.currentTerm

    rf.persist()

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

func (rf *Raft) apply(messages [] ApplyMsg) {
    go func() {
        for _, msg := range messages {
            rf.applyCh <- msg
        }
    }()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    LPrintf("Append entries Lock - %v\n", rf.me);
    rf.mu.Lock()
    LPrintf("Append entries Locked - %v\n", rf.me);

    DPrintf("Received AppendEntries for id: %v term: %v from LeaderId: %v Term: %v PrevLogIndex: %v PrevLogTerm: %v  CommitIdx: %v\n", rf.me, rf.currentTerm, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)

    if args.Term >= rf.currentTerm {
        rf.currentTerm = args.Term
        
        if rf.status != 0 {
            DPrintf("%v convert to follower inside append entries, previous status: %v\n", rf.me, rf.status)
            rf.status = 0
            rf.stopHeartbeatTimeout()
        }      

        // handle entries
        if args.PrevLogIndex == 0 || ((len(rf.log) >= args.PrevLogIndex) && (rf.log[args.PrevLogIndex - 1].Term == args.PrevLogTerm)) {
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

            messages := make([]ApplyMsg, 0)
            for i := oldCommitIndex + 1; i <= rf.commitIndex; i++ {
                DPrintf("Follower %v Apply msg: command %v commandIndex %v\n", rf.me, rf.log[i - 1].Command, i)
                messages = append(messages, ApplyMsg{
                    CommandValid: true,
                    Command: rf.log[i - 1].Command,
                    CommandIndex: i,
                })
            }

            rf.apply(messages)

            rf.lastApplied = rf.commitIndex
            
            reply.Success = true
        } else {
            if (len(rf.log) >= args.PrevLogIndex) && (rf.log[args.PrevLogIndex - 1].Term != args.PrevLogTerm) {
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
            } else if len(rf.log) < args.PrevLogIndex {
                reply.FirstIndex = len(rf.log)
            }

            reply.Success = false
        }

        rf.resetElectionTimeout()
    } else {
        reply.Success = false
    }

    reply.Term = rf.currentTerm
    // DPrintf("Current log for %v: %v, commitIndex: %v\n", rf.me, rf.log, rf.commitIndex)
    rf.persist()

    LPrintf("Apeend entries Unlock - %v\n", rf.me);
    rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    // Your code here (2A, 2B).
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

func (rf *Raft) convertToFollower() {
    rf.status = 0
    rf.stopHeartbeatTimeout()
    rf.resetElectionTimeout()
}

func (rf *Raft) resetElectionTimeout() {
    timeout := time.Duration(rand.Intn(300) + 300) * time.Millisecond

    if rf.electionTimeout != nil {
        rf.electionTimeout.Stop()
    }

    rf.electionTimeout = time.AfterFunc(
        timeout,
        rf.startNewElection,
    )
}

func (rf *Raft) stopHeartbeatTimeout() {
    if rf.heartbeatTimeout != nil {
        rf.heartbeatTimeout.Stop()
    }
}

func (rf *Raft) sendHeartbeat(i int) {
    rf.mu.Lock()

    term := rf.currentTerm
    logCopy := make([]LogEntry, len(rf.log))
    copy(logCopy, rf.log)

    var prevLogIndex, prevLogTerm int
    var entries []LogEntry

    lastLogIndex := len(rf.log)

    if lastLogIndex < rf.nextIndex[i] { // entries = []
        prevLogIndex = lastLogIndex
        if lastLogIndex == 0 {
            prevLogTerm = 0
        } else {
            prevLogTerm = rf.log[lastLogIndex - 1].Term
        }
        entries = []LogEntry{}
    } else {
        prevLogIndex, prevLogTerm = rf.getLastLogInfo(rf.log[:rf.nextIndex[i] - 1])
        entries = rf.log[rf.nextIndex[i] - 1:]
    }

    args := AppendEntriesArgs{
        Term: term,
        LeaderId: rf.me,
        PrevLogIndex: prevLogIndex,
        PrevLogTerm: prevLogTerm,
        Entries: entries,
        LeaderCommit: rf.commitIndex,
    }

    reply := AppendEntriesReply{
        Success: false,
    }

    go func(i int) {
        ok := rf.sendAppendEntries(i, &args, &reply)
        rf.mu.Lock()

        if term != rf.currentTerm {
            rf.mu.Unlock()
            return
        }

        if ok {
            if reply.Success {
                
                rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
                rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)

                DPrintf("Commit matched for %v, len(args.Entries) %v, nextIndex %v, matchIndex %v\n", i, len(args.Entries), rf.nextIndex[i], rf.matchIndex[i])
            } else {
                if reply.Term > rf.currentTerm {
                    rf.currentTerm = reply.Term
                    rf.convertToFollower()
                    DPrintf("Leader %v steps down.\n", rf.me)
                    rf.mu.Unlock()
                    return
                }

                DPrintf("Commit not matched for %v, reply.term %v, args.term %v, current term %v, current next index %v, ConflictTerm %v, FirstIndex %v\n", i, reply.Term, args.Term, rf.currentTerm, rf.nextIndex[i], reply.ConflictTerm, reply.FirstIndex)
                
                if reply.ConflictTerm != 0 && reply.FirstIndex != 0 {

                    lastIdx := -1
                    for idx := len(logCopy) - 1; idx >= 0; idx-- {
                        if logCopy[idx].Term == reply.ConflictTerm {
                            lastIdx = idx + 1
                            break
                        }
                    }

                    if lastIdx != -1 {
                        rf.nextIndex[i] = lastIdx
                    } else {
                        rf.nextIndex[i] = reply.FirstIndex
                    }

                    DPrintf("Optimization 1: new next index for %v: %v", i, rf.nextIndex[i])
                } else if (reply.ConflictTerm == 0 && reply.FirstIndex != 0) {
                    rf.nextIndex[i] = reply.FirstIndex + 1

                    DPrintf("Optimization 2: new next index for %v: %v", i, rf.nextIndex[i])
                } else {
                    rf.nextIndex[i] = MaxInt(1, rf.nextIndex[i] - 1)
                }

                rf.mu.Unlock()
                DPrintf("Retry AppendEntries with new nextIndex from leader %v to %v.\n", rf.me, i)
                rf.sendHeartbeat(i)
                rf.mu.Lock()
            }
        } else {
            DPrintf("AppendEntries RPC failed from %v to %v.\n", rf.me, i)
        }

        rf.mu.Unlock()
    }(i)

    rf.persist()
    rf.mu.Unlock()
}


func (rf *Raft) sendHeartbeatToAll() {
    rf.mu.Lock()

    rf.incCommitIdx()

    for j := range rf.peers {
        if j != rf.me {
            go func(j int) {
                rf.sendHeartbeat(j)
            }(j)
        }
    }

    rf.stopHeartbeatTimeout()
    
    rf.heartbeatTimeout = time.AfterFunc(
        Heartbeat * time.Millisecond,
        func() {
            rf.sendHeartbeatToAll()
        },
    )

    rf.mu.Unlock()
}

func (rf *Raft) startNewElection() {
    rf.mu.Lock()

    rf.status = 2
    rf.currentTerm++
    rf.votedFor = rf.me
    
    rf.resetElectionTimeout()

    totalVotes := 1
    term := rf.currentTerm
    lastLogIndex, lastLogTerm := rf.getLastLogInfo(rf.log)

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
            ok := rf.sendRequestVote(i, &args, &reply)

            rf.mu.Lock()

            if term != rf.currentTerm {
                rf.mu.Unlock()
                return
            }

            if ok {

                if reply.Term > rf.currentTerm {
                    rf.currentTerm = reply.Term
                    DPrintf("%v convert to follower due to term , previous status: %v\n", rf.me, rf.status)
                    rf.convertToFollower()
                    rf.mu.Unlock()
                    return
                }

                if reply.VoteGranted {
                    totalVotes++ 
                }
            } else {
                DPrintf("Request vote RPC failed from %v to %v.\n", rf.me, i)
            }

            if totalVotes > (len(rf.peers) / 2) && rf.status == 2 && rf.currentTerm == term {
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

                rf.electionTimeout.Stop()
                
                rf.mu.Unlock()
                rf.sendHeartbeatToAll()
            } else {
                rf.mu.Unlock()
            }
        }(i)
    }

    rf.persist()
    rf.mu.Unlock()
}

func (rf *Raft) getLastLogInfo(log []LogEntry) (int, int) {
    if len(log) == 0 {
        return 0, 0
    }

    lastLog := rf.getLastLog(log)

    return lastLog.Index, lastLog.Term
}


func (rf *Raft) incCommitIdx() {
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

    messages := make([]ApplyMsg, 0)
    for idx := rf.commitIndex + 1; idx <= maxCommitIdx; idx++ {
        DPrintf("Leader Apply msg: command %v commandIndex %v\n", rf.log[idx - 1].Command, idx)
        messages = append(messages, ApplyMsg{
            CommandValid: true,
            Command: rf.log[idx - 1].Command,
            CommandIndex: idx,
        })
    }

    rf.apply(messages)

    rf.commitIndex = maxCommitIdx
    rf.lastApplied = rf.commitIndex

    DPrintf("Leader %v commitIdx: %v\n", rf.me, rf.commitIndex)
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

    DPrintf("Command %v received for leader %v, currentTerm %v, commitIndex %v\n", command , rf.me, rf.currentTerm, rf.commitIndex)

    newEntry := LogEntry{
        Command: command,
        Term: rf.currentTerm,
        Index: len(rf.log) + 1,
    }
    rf.log = append(rf.log, newEntry)

    rf.persist()
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
    rf.votedFor = -1
    rf.status = 0

    rf.log = make([]LogEntry, 0)
    rf.commitIndex = 0
    rf.lastApplied = 0

    // Your initialization code here (2A, 2B, 2C).

    go func() {
        rf.mu.Lock()
        rf.resetElectionTimeout()
        rf.mu.Unlock()
    }()

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())

    return rf
}