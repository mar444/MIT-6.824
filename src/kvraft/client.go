package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"


type Clerk struct {
    servers []*labrpc.ClientEnd
    // You will have to modify this struct.
    mu sync.Mutex
    id int64
    seq int
    leaderIndex int
}

func nrand() int64 {
    max := big.NewInt(int64(1) << 62)
    bigx, _ := rand.Int(rand.Reader, max)
    x := bigx.Int64()
    return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
    ck := new(Clerk)
    ck.servers = servers
    // You'll have to add code here.

    ck.id = nrand()
    ck.seq = 0
    ck.leaderIndex = 0

    return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

    // You will have to modify this function.
    ck.mu.Lock()
    ck.seq++
    seq := ck.seq
    idx := ck.leaderIndex
    ck.mu.Unlock()

    i := idx

    for {
        args := GetArgs{
            Key: key,
            Id: ck.id,
            Seq: seq,
        }

        reply := GetReply{}
        DPrintf("%v %v send GET RPC for %v key: %v\n", ck.id, seq, i, key)
        ok := ck.servers[i].Call("KVServer.Get", &args, &reply)

        DPrintf("%v %v get result back from server %v", ck.id, seq, ok)

        if ok {
            if !reply.WrongLeader {
                ck.mu.Lock()
                ck.leaderIndex = i
                ck.mu.Unlock()

                if reply.Err == "" {
                    DPrintf("%v %v get Get succeeded result from server %v %v.\n", ck.id, seq, key, reply.Value)
                    return reply.Value
                } else if reply.Err == ErrNoKey {
                    DPrintf("%v %v get no key error result from server %v.\n", ck.id, seq, key)
                    return ""
                } else {
                    DPrintf("%v %v get Get error result from server %v.\n", ck.id, seq, reply.Err)
                }
            } else {
                DPrintf("%v %v wrong leader\n", ck.id, seq)
            }
            
        } else {
            DPrintf("%v %v RPC failed to %v\n", ck.id, seq, i)
        }
        i = (i + 1) % len(ck.servers)
    }
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
    // You will have to modify this function.

    ck.mu.Lock()
    ck.seq++
    seq := ck.seq
    idx := ck.leaderIndex
    ck.mu.Unlock()

    i := idx

    for {
        args := PutAppendArgs{
            Key: key,
            Value: value,
            Op: op,
            Id: ck.id,
            Seq: seq,
        }

        reply := PutAppendReply{}

        DPrintf("%v %v send PUT RPC for key: %v val: %v op: %v\n", ck.id, seq, key, value, op)
        ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)

        if ok {
            if !reply.WrongLeader {
                ck.mu.Lock()
                ck.leaderIndex = i
                ck.mu.Unlock()

                if reply.Err == "" {
                    DPrintf("%v %v get succeeded result from server.\n", ck.id, seq)
                    return
                } else if reply.Err == ErrDuplicate {
                    DPrintf("%v %v get duplicate error result %v from server.\n", ck.id, seq, reply.Err)
                    return
                } else {
                    DPrintf("%v %v get error result %v from server.\n", ck.id, seq, reply.Err)
                }
            } else {
                DPrintf("%v %v wrong leader\n", ck.id, seq)
            }
            
        } else {
            DPrintf("%v %v RPC failed to %v\n", ck.id, seq, i)
        }
        i = (i + 1) % len(ck.servers)
    }
}

func (ck *Clerk) Put(key string, value string) {
    ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
    ck.PutAppend(key, value, "Append")
}