package raftkv

import (
    "labgob"
    "labrpc"
    "raft"
    "sync"
    "time"
    "bytes"
)

const Threshold = 100 // bytes

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
    processing bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
    for kv.processing {
        time.Sleep(100 * time.Millisecond)
    }

    LPrintf("%v get lock for %v\n", kv.me, args.Key)
    kv.mu.Lock()
    LPrintf("%v get locked for %v\n", kv.me, args.Key)
    kv.processing = true

    _, isLeader := kv.rf.GetState()

    if !isLeader {
        reply.WrongLeader = true
        LPrintf("%v get unlock for %v %v due to wrong leader\n", kv.me, args.Id, args.Seq)
        kv.processing = false
        kv.mu.Unlock()
        return
    }

    for _, seq := range kv.reqMap[args.Id] {
        if args.Seq == seq {
            if val, ok := kv.data[args.Key]; ok {
                reply.Value = val;
            } else {
                reply.Err = ErrNoKey
            }
            LPrintf("%v try to unlock for %v %v due to replication\n", kv.me, args.Id, args.Seq)
            kv.processing = false
            kv.mu.Unlock()
            LPrintf("%v unlocked for %v %v due to replication\n", kv.me, args.Id, args.Seq)
            return
        }
    }

    op := Op{
        Type: "Get",
        Key: args.Key,
        Id: args.Id,
        Seq: args.Seq,
    }

    DPrintf("%v send Get command %v %v %v to raft\n", kv.me, args.Id, args.Seq, op)
    index, _, ok := kv.rf.Start(op)

    if !ok {
        reply.WrongLeader = true
        LPrintf("%v get unlock for %v %v %v due to wrong leader\n", kv.me, args.Id, args.Seq, op)
        kv.processing = false
        kv.mu.Unlock()
        return
    }

    ch := make(chan AppliedResult)
    kv.channelMap[index] = append(kv.channelMap[index], ch)
    
    LPrintf("%v get unlock for %v\n", kv.me, op)
    kv.mu.Unlock()

    select {
        case m := <-ch:
            kv.mu.Lock()
            if m.Op == op {
                // DPrintf("%v received from channel inside Get Index: %v, Op: %v, Err: %v, Value: %v, %v, %v\n", kv.me , m.Index, m.Op, m.Err, m.Value, m.Err == "", index)
                reply.Err = m.Err
                reply.Value = m.Value
            } else {
                // DPrintf("%v received from channel inside Get WRONG INDEX Index: %v, Op: %v, Err: %v, Value: %v, %v, %v\n", kv.me , m.Index, m.Op, m.Err, m.Value, m.Err == "", index)
                reply.Err = ErrWrongIndex
            }

            DPrintf("%v send %v from server to client. \n", kv.me, op)
            kv.processing = false
            kv.mu.Unlock()
            return
        case <-time.After(time.Second):
            // DPrintf("%v did not achieve agreement on time for %v\n", kv.me, op)
            reply.WrongLeader = true
            reply.Err = "timeout"

            kv.removeChannel(index, ch)
            kv.processing = false
            // DPrintf("%v send %v timeout from server to client. \n", kv.me, op)
            return
    }
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    for kv.processing {
        time.Sleep(100 * time.Millisecond)
    }

    LPrintf("%v put lock for %v %v\n", kv.me, args.Key, args.Value)
    kv.mu.Lock()
    LPrintf("%v put locked for %v %v\n", kv.me, args.Key, args.Value)
    kv.processing = true

     _, isLeader := kv.rf.GetState()

    if !isLeader {
        reply.WrongLeader = true
        LPrintf("%v get unlock for %v %v due to wrong leader\n", kv.me, args.Id, args.Seq)
        kv.processing = false
        kv.mu.Unlock()
        return
    }

    for _, seq := range kv.reqMap[args.Id] {
        if args.Seq == seq {
            reply.Err = ErrDuplicate
            LPrintf("%v try to unlock for %v %v due to replication\n", kv.me, args.Id, args.Seq)
            kv.processing = false
            kv.mu.Unlock()
            LPrintf("%v unlocked for %v %v due to replication\n", kv.me, args.Id, args.Seq)
            return
        }
    }

    op := Op{
        Type: args.Op,
        Key: args.Key,
        Value: args.Value,
        Id: args.Id,
        Seq: args.Seq,
    }

    DPrintf("%v send PutAppend command %v to raft\n", kv.me, op)
    index, _, ok := kv.rf.Start(op)

    if !ok {
        reply.WrongLeader = true
        kv.processing = false
        LPrintf("%v get unlock for %v %v %v due to wrong leader\n", kv.me, args.Id, args.Seq, op)
        kv.mu.Unlock()
        return
    }

    ch := make(chan AppliedResult)
    kv.channelMap[index] = append(kv.channelMap[index], ch)

    LPrintf("%v put unlock for %v\n", kv.me, op)
    kv.mu.Unlock()

    select {
        case m := <-ch:
            kv.mu.Lock()
            if m.Op == op {
                // DPrintf("%v received from channel inside PutAppend Index: %v, Op: %v, Err: %v, Value: %v, %v, %v\n", kv.me , m.Index, m.Op, m.Err, m.Value, m.Err == "", index)
                reply.Err = m.Err
            } else {
                // DPrintf("%v received from channel inside PutAppend WRONG INDEX  Index: %v, Op: %v, Err: %v, Value: %v, %v, %v\n", kv.me , m.Index, m.Op, m.Err, m.Value, m.Err == "", index)
                reply.Err = ErrWrongIndex
            }

            DPrintf("%v send %v from server to client. \n", kv.me, op)
            kv.processing = false
            kv.mu.Unlock()
            return
        case <-time.After(time.Second):
            // DPrintf("%v did not achieve agreement on time for %v\n", kv.me, op)
            reply.WrongLeader = true
            reply.Err = "timeout"
            kv.processing = false
            kv.removeChannel(index, ch)
            // DPrintf("%v send %v from server to client. \n", kv.me, op)
            return
    }
}

func (kv *KVServer) removeChannel(index int, ch chan AppliedResult) {
    kv.mu.Lock()
    for i, ich := range kv.channelMap[index]  {
        if ich == ch {
            kv.channelMap[index] = remove(kv.channelMap[index], i)
            break
        }
    }
    kv.mu.Unlock()
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

func (kv *KVServer) getStateBytes() []byte {
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(kv.data)
    e.Encode(kv.reqMap)
    return w.Bytes()
}

func (kv *KVServer) readSnapshot(data []byte) {
    if data == nil || len(data) < 1 {
        return
    }

    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)
    var snapshot raft.Snapshot
    if d.Decode(&snapshot) != nil {
      DPrintf("readSnapshot error.\n")
    } else {
        kv.readState(snapshot.State)
    }
}

func (kv *KVServer) readState(data []byte) {
    if data == nil || len(data) < 1 {
        return
    }

    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)
    var kvdata map[string]string
    var reqMap map[int64][]int

    if d.Decode(&kvdata) != nil ||
       d.Decode(&reqMap) != nil {
      DPrintf("readState error.\n")
    } else {
        kv.data = kvdata
        kv.reqMap = reqMap
    }
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

    kv.data = make(map[string]string)
    kv.channelMap = make(map[int][]chan AppliedResult)
    kv.reqMap = make(map[int64][]int)

    go func() {
        for m := range kv.applyCh {
            applied := false

            DPrintf("%v received %v from applyCh\n", me, m)
            kv.mu.Lock()

            if !m.CommandValid {
                kv.readState(m.State)
                DPrintf("%v get snapshot from raft. \n", kv.me)
                kv.mu.Unlock()
                continue
            }

            command := (m.Command).(Op)

            for _, seq := range kv.reqMap[command.Id] {
                if command.Seq == seq {
                    applied = true
                    break
                }
            }

            if applied {
                DPrintf("%v already applied %v", kv.me, command)
                kv.mu.Unlock()
                continue
            }
            
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

            kv.reqMap[command.Id] = append(kv.reqMap[command.Id], command.Seq)

            if kv.maxraftstate != -1 && kv.maxraftstate - persister.RaftStateSize() < Threshold {
                state := kv.getStateBytes()
                DPrintf("%v send snapshot with last index %v", kv.me, m.CommandIndex)
                // for k, v := range kv.data {
                //     DPrintf("%v state in snapshot k: %v v: %v", kv.me, k, v)
                // }
                kv.rf.ReceiveSnapshot(state, m.CommandIndex)
            }

            kv.mu.Unlock()

            LPrintf("%v %v unlocked\n", kv.me, command)
            DPrintf("%v send %v from applier loop to server. \n", kv.me, command)
            
            for _, ch := range kv.channelMap[m.CommandIndex] {
                ch <- result
            }
        }
    }()

    kv.readSnapshot(persister.ReadSnapshot())

    return kv
}
