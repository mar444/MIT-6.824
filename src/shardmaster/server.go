package shardmaster


import "raft"
import "labrpc"
import "sync"
import "labgob"
import "time"


type ShardMaster struct {
    mu      sync.Mutex
    me      int
    rf      *raft.Raft
    applyCh chan raft.ApplyMsg

    // Your data here.

    configs []Config // indexed by config num
    processing bool
    totalServers int
    shardMap map[int][]int

    reqMap map[int64][]int
    channelMap map[int][]chan AppliedResult
}


type Op struct {
    // Your data here.
    Type string
    Servers map[int][]string
    GIDs []int
    Shard int
    GID   int
    Num int
    Id    int64
    Seq   int
}

type AppliedResult struct {
    Id int64
    Seq int
    Err Err
    Config Config
}

func (sm *ShardMaster) handleRequest(op Op, reply *Reply) {
    for sm.processing {
        time.Sleep(100 * time.Millisecond)
    }

    sm.mu.Lock()
    sm.processing = true

    _, isLeader := sm.rf.GetState()

    if !isLeader {
        reply.WrongLeader = true
        sm.processing = false
        sm.mu.Unlock()
        return
    }

    for _, seq := range sm.reqMap[op.Id] {
        if op.Seq == seq {
            if op.Type == "Query" {
                reply.Config = sm.getConfig(op.Num)
            }
            sm.processing = false
            sm.mu.Unlock()
            return
        }
    }

    DPrintf("%v send op %v to raft\n", sm.me, op)
    index, _, ok := sm.rf.Start(op)

    if !ok {
        reply.WrongLeader = true
        sm.processing = false
        sm.mu.Unlock()
        return
    }

    ch := make(chan AppliedResult)
    sm.channelMap[index] = append(sm.channelMap[index], ch)

    sm.mu.Unlock()

    select {
        case m := <-ch:
            sm.mu.Lock()
            if m.Id == op.Id && m.Seq == op.Seq {
                if op.Type == "Query" {
                    reply.Config = sm.getConfig(op.Num)
                }
                reply.Err = m.Err
            } else {
                reply.Err = ErrWrongIndex
            }

            DPrintf("%v send %v from server to client. \n", sm.me, op)
            sm.processing = false
            sm.mu.Unlock()
            return
        case <-time.After(time.Second):
            reply.WrongLeader = true
            reply.Err = "timeout"

            sm.removeChannel(index, ch)
            sm.processing = false
            return
    }
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
    op := Op{
        Type: "Join",
        Servers: args.Servers,
        Id: args.Id,
        Seq: args.Seq,
    }

    joinReply := Reply{}
    sm.handleRequest(op, &joinReply)
    reply.Err = joinReply.Err
    reply.WrongLeader = joinReply.WrongLeader
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
    op := Op{
        Type: "Leave",
        GIDs: args.GIDs,
        Id: args.Id,
        Seq: args.Seq,
    }

    leaveReply := Reply{}
    sm.handleRequest(op, &leaveReply)
    reply.Err = leaveReply.Err
    reply.WrongLeader = leaveReply.WrongLeader
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
    op := Op{
        Type: "Move",
        Shard: args.Shard,
        GID: args.GID,
        Id: args.Id,
        Seq: args.Seq,
    }

    moveReply := Reply{}
    sm.handleRequest(op, &moveReply)
    reply.Err = moveReply.Err
    reply.WrongLeader = moveReply.WrongLeader
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
    op := Op{
        Type: "Query",
        Num: args.Num,
        Id: args.Id,
        Seq: args.Seq,
    }

    queryReply := Reply{}
    sm.handleRequest(op, &queryReply)
    reply.Err = queryReply.Err
    reply.WrongLeader = queryReply.WrongLeader
    reply.Config = queryReply.Config
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
    sm.rf.Kill()
    // Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
    return sm.rf
}

func (sm *ShardMaster) rebalance(groups map[int][]string, shards [NShards]int) (map[int][]string, [NShards]int) {
    groupIds := getGroupIds(groups)
    remainShards := NShards
    assignCountMap := make(map[int]int)  // gid -> shards count

    for i := 0; i < len(groupIds); i++ {
        groupId := groupIds[i]
        servers := groups[groupId]

        if i == len(groupIds) - 1 {
            assignCountMap[groupId] = remainShards
        } else {
            assignCount := MinInt((len(servers) * NShards) / sm.totalServers, remainShards)
            assignCountMap[groupId] = assignCount
            remainShards -= assignCount
        }
    }

    for gid, count := range assignCountMap {
        currentShards := sm.shardMap[gid]

        if count == len(currentShards) {
            continue
        }

        for count > len(currentShards) {
            finished := false
            for shardId, assignedGid := range shards {
                if assignedGid == 0 {
                    sm.shardMap[gid] = append(sm.shardMap[gid], shardId)
                    shards[shardId] = gid

                    if count == len(sm.shardMap[gid]) {
                        finished = true
                        break
                    }
                }
            }

            if !finished {
                for shardId, assignedGid := range shards {
                    if assignedGid != 0 && assignedGid != gid {
                        targetCount := assignCountMap[assignedGid]
                        if targetCount < len(sm.shardMap[assignedGid]) {
                            sm.shardMap[assignedGid] = removeGroup(sm.shardMap[assignedGid], shardId)
                            sm.shardMap[gid] = append(sm.shardMap[gid], shardId)
                            shards[shardId] = gid
                        }

                        if count == len(sm.shardMap[gid]) {
                            finished = true
                            break
                        }
                    }
                }
            }

            if finished {
                break
            }
        }
    }
    return groups, shards
}

func (sm *ShardMaster) getConfig(num int) Config {
    var index int

    if num >= 0 && num < len(sm.configs) {
        index = num
    } else {
        index = len(sm.configs) - 1
    }
    return sm.configs[index]
}

func (sm *ShardMaster) removeChannel(index int, ch chan AppliedResult) {
    sm.mu.Lock()
    for i, ich := range sm.channelMap[index]  {
        if ich == ch {
            sm.channelMap[index] = remove(sm.channelMap[index], i)
            break
        }
    }
    sm.mu.Unlock()
}

func (sm *ShardMaster) getLastConfig() Config {
    return sm.configs[len(sm.configs) - 1]
}

func (sm *ShardMaster) copyLastConfig() (map[int][]string, [NShards]int) {
    lastConfig := sm.getLastConfig()

    newGroups := make(map[int][]string)
    for k,v := range lastConfig.Groups {
        newGroups[k] = v
    }

    var newShards [NShards]int

    copy(newShards[:], lastConfig.Shards[:NShards])
    return newGroups, newShards
}


//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
    sm := new(ShardMaster)
    sm.me = me

    sm.configs = make([]Config, 1)
    sm.configs[0].Groups = map[int][]string{}

    labgob.Register(Op{})
    sm.applyCh = make(chan raft.ApplyMsg)
    sm.rf = raft.Make(servers, me, persister, sm.applyCh)

    // Your code here.
    sm.reqMap = make(map[int64][]int)
    sm.shardMap = make(map[int][]int)
    sm.channelMap = make(map[int][]chan AppliedResult)

    go func() {
       
        for m := range sm.applyCh {
            applied := false

            DPrintf("%v received %v from applyCh\n", me, m)
            sm.mu.Lock()

            command := (m.Command).(Op)

            for _, seq := range sm.reqMap[command.Id] {
                if command.Seq == seq {
                    applied = true
                    break
                }
            }

            if applied {
                DPrintf("%v already applied %v", sm.me, command)
                sm.mu.Unlock()
                continue
            }

            result := AppliedResult{
                Id: command.Id,
                Seq: command.Seq,
            }

            newGroups, newShards := sm.copyLastConfig()

            if command.Type == "Join" {
                for gid, servers := range command.Servers {
                    newGroups[gid] = servers
                    sm.totalServers += len(servers)
                }

                newGroups, newShards := sm.rebalance(newGroups, newShards)
                num := len(sm.configs)

                config := Config{
                    Num: num,
                    Groups: newGroups,
                    Shards: newShards,
                }
                
                sm.configs = append(sm.configs, config)
            } else if command.Type == "Leave" {
                for _, gid := range command.GIDs {
                    sm.totalServers -= len(newGroups[gid])
                    delete(newGroups, gid)

                    shards := sm.shardMap[gid]
                    for _, shardId := range shards {
                        newShards[shardId] = 0
                    }
                    delete(sm.shardMap, gid)
                }

                newGroups, newShards := sm.rebalance(newGroups, newShards)
                num := len(sm.configs)

                config := Config{
                    Num: num,
                    Groups: newGroups,
                    Shards: newShards,
                }
                
                sm.configs = append(sm.configs, config)

            } else if command.Type == "Move" {
                assignedGid := newShards[command.Shard]
                if assignedGid != command.GID {
                    newShards[command.Shard] = command.GID
                    sm.shardMap[assignedGid] = removeGroup(sm.shardMap[assignedGid], command.Shard)
                    sm.shardMap[command.GID] = append(sm.shardMap[command.GID], command.Shard)
                }

                num := len(sm.configs)
                config := Config{
                    Num: num,
                    Groups: newGroups,
                    Shards: newShards,
                }
                
                sm.configs = append(sm.configs, config)
            } else if command.Type == "Query" {
                result.Config = sm.getConfig(command.Num)
            }


            sm.reqMap[command.Id] = append(sm.reqMap[command.Id], command.Seq)

            sm.mu.Unlock()

            DPrintf("%v send %v from applier loop to server. \n", sm.me, command)
            
            for _, ch := range sm.channelMap[m.CommandIndex] {
                ch <- result
            }
        }
    }()

    return sm
}
