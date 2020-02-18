package raftkv

import (
	"labrpc"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
	cid int64
	seq int32
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
	ck.lastLeader = 0
	ck.cid = nrand()
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
//	DPrintf("**********client:Get key:%v**********\n", key)
	if key == "" {
		return ""
	} else {
		n := len(ck.servers)
		i := 0
		args := GetArgs{key, ck.cid, ck.seq}
		for {
			reply := GetReply{}
			server := (ck.lastLeader + i) % n
			ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
			if ok && reply.WrongLeader == false{
				ck.lastLeader = server
				if reply.Err == OK {
				//	DPrintf("Get%v success\n", key)
					return reply.Value
				}
			} else {
				i++
			}
			time.Sleep(100*time.Millisecond)
		}
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
//	DPrintf("**********client:%v key:%v, value:%v**********\n", op, key, value)
	seq := atomic.AddInt32(&ck.seq, 1)
	n := len(ck.servers)
	i := 0
	args := PutAppendArgs{key, value, op, ck.cid, seq}
	for {
		reply := PutAppendReply{}
		server := (ck.lastLeader+i) % n
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.WrongLeader == false {
			ck.lastLeader = server
			if reply.Err == OK {
			//	DPrintf("PutAppend%v success\n", key)
				return
			}
		} else {
			i++
		}
		time.Sleep(100*time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
