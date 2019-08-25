package raftkv

import (
	"labrpc"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
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
	DPrintf("**********client:Get key:%v**********\n", key)
	if key == "" {
		return ""
	} else {
		args := GetArgs{key}
		n := len(ck.servers)
		i := 0
		for {
			reply := GetReply{}
			server := (ck.lastLeader + i) % n
			ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
			if ok && reply.WrongLeader == false{
				ck.lastLeader = server
				DPrintf("Get success\n")
				return reply.Value
			} else {
				i++
			}
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
	DPrintf("**********client:%v key:%v, value:%v**********\n", op, key, value)
	args := PutAppendArgs{key, value, op}
	n := len(ck.servers)
	i := 0
	for {
		reply := PutAppendReply{}
		server := (ck.lastLeader+i) % n
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.WrongLeader == false {
			ck.lastLeader = server
			DPrintf("PutAppend success\n")
			return
		} else {
			i++
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
