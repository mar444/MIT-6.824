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
// import "log"

const Heartbeat = 100 // ms

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
    State        []byte
    LastIndex    int
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

    log         Log

    commitIndex int
    lastApplied int

    nextIndex   []int
    matchIndex  []int

    snapshot    Snapshot
}

type Snapshot struct {
    LastIndex int
    LastTerm int
    State []byte
}

type LogEntry struct {
    Command interface{}
    Term    int
    Index   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
    return rf.currentTerm, rf.status == 1
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
    data := rf.getStateBytes()
    rf.persister.SaveRaftState(data)
}

func (rf *Raft) getStateBytes() []byte {
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.log)
    return w.Bytes()
}

func (rf *Raft) getSnapshotBytes() []byte {
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.snapshot)
    return w.Bytes()
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersistState(data []byte) {
    if data == nil || len(data) < 1 {
        return
    }

    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)
    var currentTerm int
    var votedFor int
    var log Log
    if d.Decode(&currentTerm) != nil ||
       d.Decode(&votedFor) != nil ||
       d.Decode(&log) != nil {
      DPrintf("readPersistState error.\n")
    } else {
        rf.currentTerm = currentTerm
        rf.votedFor = votedFor
        rf.log = log
    }
}

func (rf *Raft) readPersistSnapshot(data []byte) {
    if data == nil || len(data) < 1 {
        return
    }

    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)
    var snapshot Snapshot
    if d.Decode(&snapshot) != nil {
      DPrintf("readPersistSnapshot error.\n")
    } else {
        rf.snapshot = snapshot
    }
}

// RequestVote RPC request structure
type RequestVoteArgs struct {
    Term int
    CandidateId int
    LastLogIndex int
    LastLogTerm int
}

// RequestVote RPC reply structure
type RequestVoteReply struct {
    Term int
    VoteGranted bool
}

func (rf *Raft) isCandidateLogUpToDate(lastLogTerm, lastLogIndex int) bool {
    thisLastLogIndex, thisLastLogTerm := rf.log.GetLastLogInfo(rf.log.LastIndex, rf.snapshot)
    // DPrintf("candidate lastlogterm %v lastlogindex %v this %v last log index %v snapshot index %v\n", lastLogTerm, lastLogIndex, rf.log.LastIndex, rf.snapshot.LastIndex)

    if (lastLogTerm > thisLastLogTerm) {
        return true
    }

    if (lastLogTerm == thisLastLogTerm && lastLogIndex >= thisLastLogIndex) {
        return true
    }

    return false
}

// RequestVote RPC handler.
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

// AppendEntries request structure
type AppendEntriesArgs struct {
    Term         int
    LeaderId     int
    PrevLogIndex int
    PrevLogTerm  int
    Entries      []LogEntry
    LeaderCommit int
}

// AppendEntries reply structure
type AppendEntriesReply struct {
    Term    int
    Success bool
    ConflictTerm int
    FirstIndex int
}

// InstallSnapshot request structure
type InstallSnapshotArgs struct {
    Term              int
    LeaderId          int
    LastIncludedIndex int
    LastIncludedTerm  int
    Data              []byte  
}

// InstallSnapshot reply structure
type InstallSnapshotReply struct {
    Term    int
}

func (rf *Raft) ReceiveSnapshot(state []byte, index int) {
    // log.Printf("Received snapshot: %v %v %v\n", state, index, term)
    rf.mu.Lock()
    startIndex := rf.log.GetFirstIndex()
    endIndex := index
    
    DPrintf("%v remove range from %v to %v commitIndex %v", rf.me, startIndex, endIndex, rf.commitIndex)

    rf.snapshot = Snapshot{
        LastIndex: endIndex,
        LastTerm: rf.log.Get(endIndex).Term,
        State: state,
    }

    // DPrintf("Before remove %v len data: %v state size: %v snapshot size: %v", rf.me, len(rf.log.Data), len(rf.getStateBytes()), len(rf.getSnapshotBytes()))
    rf.log.RemoveRange(startIndex, endIndex)
    rf.persister.SaveStateAndSnapshot(rf.getStateBytes(), rf.getSnapshotBytes())
    // DPrintf("After remove %v len data: %v state size: %v snapshot size: %v", rf.me, len(rf.log.Data), len(rf.getStateBytes()), len(rf.getSnapshotBytes()))

    rf.mu.Unlock()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    LPrintf("Install snapshot Lock - %v\n", rf.me);
    rf.mu.Lock()
    LPrintf("Install snapshot Locked - %v\n", rf.me);

    if args.Term >= rf.currentTerm {
        if rf.snapshot.LastIndex < args.LastIncludedIndex {
            rf.snapshot = Snapshot{
                LastIndex: args.LastIncludedIndex,
                LastTerm: args.LastIncludedTerm,
                State: args.Data,
            }

            DPrintf("follower %v with first index %v, last index %v snapshot last index %v leader snapshot index %v\n", rf.me, rf.log.GetFirstIndex(), rf.log.LastIndex, rf.snapshot.LastIndex,  args.LastIncludedIndex)
            
            // prefix
            if rf.log.LastIndex > rf.snapshot.LastIndex {
                DPrintf("follower %v snapshot with prefix", rf.me)
                rf.log.RemoveRange(rf.log.GetFirstIndex(), rf.snapshot.LastIndex)
            } else {
                rf.log.RemoveRange(rf.log.GetFirstIndex(), rf.log.LastIndex)
                // DPrintf("%v receive install snapshot without prefix, send data to server", rf.me)                
            }

            go func() {
                rf.applyCh <- ApplyMsg{
                    CommandValid: false,
                    LastIndex: args.LastIncludedIndex,
                    State: args.Data,
                }
            }()

            rf.persister.SaveStateAndSnapshot(rf.getStateBytes(), rf.getSnapshotBytes())
        }
    } 

    reply.Term = rf.currentTerm
    rf.persist()

    LPrintf("Install snapshot Unlock - %v\n", rf.me);
    rf.mu.Unlock()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
    ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
    return ok
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
        if args.PrevLogIndex == 0 ||
           (rf.log.ContainsKey(args.PrevLogIndex) && (rf.log.Get(args.PrevLogIndex).Term == args.PrevLogTerm)) ||
           (rf.snapshot.LastIndex == args.PrevLogIndex && rf.snapshot.LastTerm == args.PrevLogTerm) {
            entries := args.Entries

            for _, entry := range entries {
                if rf.log.ContainsKey(entry.Index) {
                    if rf.log.Get(entry.Index).Term != entry.Term {
                        rf.log.RemoveRange(entry.Index, rf.log.LastIndex)
                        rf.log.Put(entry.Index, entry)
                    }
                } else {
                    rf.log.Put(entry.Index, entry)
                }
            }

            oldCommitIndex := rf.commitIndex

            if len(entries) == 0 {
                rf.commitIndex = args.LeaderCommit
            } else {
                rf.commitIndex = MinInt(args.LeaderCommit, entries[len(entries) - 1].Index)
            }

            DPrintf("follower %v oldCommitIndex %v newCommitIndex %v", rf.me, oldCommitIndex, rf.commitIndex)

            messages := make([]ApplyMsg, 0)
            for i := oldCommitIndex + 1; i <= rf.commitIndex; i++ {
                if rf.log.ContainsKey(i) {
                    DPrintf("Follower %v Apply msg: command %v commandIndex %v\n", rf.me, rf.log.Get(i).Command, i)
                    messages = append(messages, ApplyMsg{
                        CommandValid: true,
                        Command: rf.log.Get(i).Command,
                        CommandIndex: i,
                    })
                }
            }

            rf.apply(messages)
            rf.lastApplied = rf.commitIndex
            reply.Success = true
        } else {
            if (rf.log.ContainsKey(args.PrevLogIndex) && (rf.log.Get(args.PrevLogIndex).Term != args.PrevLogTerm)) {
                DPrintf("Conflict term for %v in %v, prevLogTerm: %v,  term: %v \n", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.log.Get(args.PrevLogIndex).Term)
                reply.ConflictTerm = rf.log.Get(args.PrevLogIndex).Term

                firstIndex := args.PrevLogIndex

                for i := firstIndex; i >= rf.log.GetFirstIndex(); i-- {
                    if rf.log.Get(i).Term != reply.ConflictTerm {
                        break
                    } else {
                        firstIndex = i
                    }
                }

                reply.FirstIndex = firstIndex
            } else if rf.log.LastIndex < args.PrevLogIndex {
                reply.FirstIndex = rf.log.LastIndex
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

func (rf *Raft) installSnapshotOnFollower(i, term int, snapshot Snapshot) {
    args := InstallSnapshotArgs{
        Term: term,
        LeaderId: rf.me,
        LastIncludedIndex: snapshot.LastIndex,
        LastIncludedTerm: snapshot.LastTerm,
        Data: snapshot.State,
    }

    reply := InstallSnapshotReply{}

    ok := rf.sendInstallSnapshot(i, &args, &reply)
    rf.mu.Lock()

    if term != rf.currentTerm {
        rf.mu.Unlock()
        return
    }

    if ok {
        if reply.Term > rf.currentTerm {
            rf.currentTerm = reply.Term
            rf.convertToFollower()
            DPrintf("Leader %v steps down.\n", rf.me)
            rf.mu.Unlock()
            return
        } else {
            DPrintf("sent snapshot to follower %v succeeded. next index: %v\n", i, rf.log.GetFirstIndex())
            rf.nextIndex[i] = rf.log.GetFirstIndex()
        }
    }
    rf.mu.Unlock()
}

func (rf *Raft) sendHeartbeat(i, term int) {
    rf.mu.Lock()

    var prevLogIndex, prevLogTerm int
    var entries []LogEntry

    lastLogIndex := rf.log.LastIndex

    // Leader entries [rf.log.GetFirstIndex(), rf.log.LastIndex]
    if rf.nextIndex[i] > lastLogIndex {
        prevLogIndex = lastLogIndex
        if lastLogIndex == 0 {
            prevLogTerm = 0
        } else {
            prevLogTerm = rf.log.Get(lastLogIndex).Term
        }
        entries = []LogEntry{}
    } else if rf.nextIndex[i] >= rf.log.GetFirstIndex() {
        if rf.nextIndex[i] == rf.log.GetFirstIndex() {
            // First entry after snapshot
            if rf.snapshot.State != nil {
                prevLogIndex = rf.snapshot.LastIndex
                prevLogTerm = rf.snapshot.LastTerm
            } else {
                prevLogIndex = 0
                prevLogTerm = 0
            }
        } else {
            prevLogIndex, prevLogTerm = rf.log.GetLastLogInfo(rf.nextIndex[i] - 1, rf.snapshot)
        }
        entries = rf.log.GetRange(rf.nextIndex[i], rf.log.LastIndex)
    } else {
        // install snapshot
        DPrintf("leader %v should install snapshot last index %v first index %v follower %v next index %v\n", rf.me, rf.snapshot.LastIndex, rf.log.GetFirstIndex(), i, rf.nextIndex[i])
        snapshot := rf.snapshot
        
        go func(i int) {
            rf.installSnapshotOnFollower(i, term, snapshot)
        }(i)
        
        rf.persist()
        rf.mu.Unlock()
        return;
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
                
                if reply.ConflictTerm != 0 && reply.FirstIndex != 0 { // conflict term at same index

                    lastIdx := 0
                    for idx := rf.log.LastIndex; idx >= rf.log.GetFirstIndex(); idx-- {
                        if rf.log.Get(idx).Term == reply.ConflictTerm {
                            lastIdx = idx
                            break
                        }
                    }

                    if lastIdx != 0 {
                        rf.nextIndex[i] = lastIdx
                    } else {
                        rf.nextIndex[i] = reply.FirstIndex
                    }

                    DPrintf("Optimization 1: new next index for %v: %v", i, rf.nextIndex[i])
                } else if (reply.ConflictTerm == 0 && reply.FirstIndex != 0) {
                    // follower's log does not contain leader's prevlog, so no conflict term
                    // first index is follower's last log index
                    rf.nextIndex[i] = reply.FirstIndex

                    DPrintf("Optimization 2: new next index for %v: %v", i, rf.nextIndex[i])
                } else {
                    // no optimization, next index - 1
                    rf.nextIndex[i] = MaxInt(1, rf.nextIndex[i] - 1)
                }

                rf.mu.Unlock()
                DPrintf("Retry AppendEntries with new nextIndex from leader %v to %v.\n", rf.me, i)
                rf.sendHeartbeat(i, rf.currentTerm)
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
    term := rf.currentTerm

    for j := range rf.peers {
        if j != rf.me {
            go func(j int) {
                rf.sendHeartbeat(j, term)
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
    lastLogIndex, lastLogTerm := rf.log.GetLastLogInfo(rf.log.LastIndex, rf.snapshot)

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

                lastLogIndex := rf.log.GetLastIndex(rf.snapshot)
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

func (rf *Raft) incCommitIdx() {
    maxCommitIdx := rf.commitIndex
    for idx := rf.commitIndex + 1; idx <= rf.log.LastIndex; idx++ {
        if (rf.log.Get(idx).Term != rf.currentTerm) {
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
        if rf.log.ContainsKey(idx) {
            DPrintf("Leader Apply msg: command %v commandIndex %v\n", rf.log.Get(idx).Command, idx)
            messages = append(messages, ApplyMsg{
                CommandValid: true,
                Command: rf.log.Get(idx).Command,
                CommandIndex: idx,
            })
        }
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

    index := rf.log.GetLastIndex(rf.snapshot) + 1

    newEntry := LogEntry{
        Command: command,
        Term: rf.currentTerm,
        Index: index,
    }

    rf.log.Put(index, newEntry)

    rf.persist()
    rf.mu.Unlock()
    return index, rf.currentTerm, rf.status == 1
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
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

    rf.log = Log{
        LastIndex: 0,
        Data: make(map[int]LogEntry),
    }

    rf.commitIndex = 0
    rf.lastApplied = 0

    rf.snapshot = Snapshot{
        LastIndex: -1,
        LastTerm: -1,
        State: nil,
    }

    // Your initialization code here (2A, 2B, 2C).

    go func() {
        rf.mu.Lock()
        rf.resetElectionTimeout()
        rf.mu.Unlock()
    }()

    // initialize from state persisted before a crash
    rf.readPersistState(persister.ReadRaftState())
    rf.readPersistSnapshot(persister.ReadSnapshot())

    return rf
}