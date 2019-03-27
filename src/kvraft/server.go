package raftkv

import (
    "labgob"
    "labrpc"
    "raft"
    "sync"
    "time"
)

type Op struct {
    // Your definitions here.
    // Field names must start with capital letters,
    // otherwise RPC will break.
    Type string
    Key string
    Value string
    Id    int64
    Seq   int
}

type AppliedResult struct {
    Op Op
    Err Err
    Value string
    Index int
}


type KVServer struct {
    mu      sync.Mutex
    me      int
    rf      *raft.Raft
    applyCh chan raft.ApplyMsg

    maxraftstate int // snapshot if log grows this big

    // Your definitions here.

    data map[string]string
    
    channelMap map[int][]chan AppliedResult
    reqMap map[int64][]int
}

func (kv *KVServer) apply(command Op) {
    if command.Type != "Get" {
        kv.data[command.Key] = command.Value
    }
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
    // Your code here.
    LPrintf("%v get lock for %v\n", kv.me, args.Key)
    kv.mu.Lock()

    DPrintf("req id %v, req seq %v\n", args.Id, args.Seq)

    LPrintf("%v get locked for %v\n", kv.me, args.Key)

    op := Op{
        Type: "Get",
        Key: args.Key,
        Id: args.Id,
        Seq: args.Seq,
    }

    DPrintf("%v send Get command %v to raft\n", kv.me, op)
    index, _, ok := kv.rf.Start(op)

    DPrintf("%v receive result from raft %v \n", kv.me, ok)

    if !ok {
        reply.WrongLeader = true
        LPrintf("%v get unlock for due to wrong leader %v\n", kv.me, op)
        kv.mu.Unlock()
        return
    }

    ch := make(chan AppliedResult)
    kv.channelMap[index] = append(kv.channelMap[index], ch)
    LPrintf("%v get unlock for %v\n", kv.me, op)

    kv.mu.Unlock()

    timeout := time.NewTimer(time.Second)

    for {
        timeout.Reset(time.Second)
        select {
        case m := <- ch:
            DPrintf("%v received from channel inside Get Index: %v, Op: %v, Err: %v, Value: %v, %v, %v\n", kv.me , m.Index, m.Op, m.Err, m.Value, m.Err == "", index)

            if m.Op == op {
                reply.Err = m.Err
                reply.Value = m.Value
            } else {
                reply.Err = ErrWrongIndex
            }

            return
        case <-timeout.C:
            reply.Err = "timeout"
            kv.mu.Lock()

            for i, ich := range kv.channelMap[index]  {
                if ich == ch {
                    kv.channelMap[index] = remove(kv.channelMap[index], i)
                    break
                }
            }
            kv.mu.Unlock()
            return
        }
    }
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    // Your code here.
    LPrintf("%v put lock for %v %v\n", kv.me, args.Key, args.Value)
    kv.mu.Lock()

    DPrintf("req id %v, req seq %v\n", args.Id, args.Seq)

    for _, seq := range kv.reqMap[args.Id] {
        if args.Seq == seq {
            reply.Err = ErrDuplicate
            LPrintf("%v try to unlock for %v %v due to replication\n", kv.me, args.Id, args.Seq)
            kv.mu.Unlock()
            LPrintf("%v unlocked for %v %v due to replication\n", kv.me, args.Id, args.Seq)
            return
        }
    }

    LPrintf("%v put locked for %v %v\n", kv.me, args.Key, args.Value)

    op := Op{
        Type: args.Op,
        Key: args.Key,
        Value: args.Value,
        Id: args.Id,
        Seq: args.Seq,
    }

    DPrintf("%v send PutAppend command %v to raft\n", kv.me, op)
    index, _, ok := kv.rf.Start(op)

    DPrintf("%v receive result from raft %v \n", kv.me, ok)

    if !ok {
        reply.WrongLeader = true
        LPrintf("%v get unlock for %v due to wrong leader\n", kv.me, op)
        kv.mu.Unlock()
        return
    }


    ch := make(chan AppliedResult)
    kv.channelMap[index] = append(kv.channelMap[index], ch)

    LPrintf("%v put unlock for %v\n", kv.me, op)

    kv.mu.Unlock()

    timeout := time.NewTimer(time.Second)

    for {
        timeout.Reset(time.Second)
        select {
        case m := <- ch:
            DPrintf("%v received from channel inside PutAppend Index: %v, Op: %v, Err: %v, Value: %v, %v, %v\n", kv.me , m.Index, m.Op, m.Err, m.Value, m.Err == "", index)

            if m.Op == op {
                reply.Err = m.Err
            } else {
                reply.Err = ErrWrongIndex
            }

            return
        case <-timeout.C:
            reply.Err = "timeout"
            kv.mu.Lock()

            for i, ich := range kv.channelMap[index]  {
                if ich == ch {
                    kv.channelMap[index] = remove(kv.channelMap[index], i)
                    break
                }
            }
            kv.mu.Unlock()
            return
        }
    }
}


//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
    kv.rf.Kill()
    // Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
    // call labgob.Register on structures you want
    // Go's RPC library to marshall/unmarshall.
    labgob.Register(Op{})

    kv := new(KVServer)
    kv.me = me
    kv.maxraftstate = maxraftstate

    // You may need initialization code here.

    kv.applyCh = make(chan raft.ApplyMsg)
    kv.rf = raft.Make(servers, me, persister, kv.applyCh)

    // You may need initialization code here.
    kv.data = make(map[string]string)
    kv.channelMap = make(map[int][]chan AppliedResult)
    kv.reqMap = make(map[int64][]int)

    go func() {
        DPrintf("start receiving from applyCh in %v\n", me)
        for m := range kv.applyCh {
            DPrintf("%v received %v from applyCh\n", me, m)
            kv.mu.Lock()

            DPrintf("%v handle message %v\n", me, m)

            command := (m.Command).(Op)
            
            result := AppliedResult{
                Op: command,
                Index: m.CommandIndex,
            }

            if command.Type == "Put" {
                kv.data[command.Key] = command.Value
            } else if command.Type == "Append" {
                kv.data[command.Key] += command.Value
            } else if command.Type == "Get" {
                if val, ok := kv.data[command.Key]; ok {
                    result.Value = val;
                } else {
                    result.Err = ErrNoKey
                }
            }
            
            for _, ch := range kv.channelMap[m.CommandIndex] {
                ch <- result
            }
            DPrintf("%v send to all channel %v", kv.me, result)
            kv.reqMap[command.Id] = append(kv.reqMap[command.Id], command.Seq)
            DPrintf("%v %v added to map\n", command.Id, command.Seq)

            kv.mu.Unlock()
        }
    }()

    return kv
}