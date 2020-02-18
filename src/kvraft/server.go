package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key string
	Value string
	Type string

	Cid int64
	Seq int32
}

func (a *Op) equals(b Op) bool {
	return a.Seq == b.Seq && a.Cid == b.Cid && a.Value == b.Value && a.Key == b.Key && a.Type == b.Type
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	keyValue map[string] string
	getCh map[int]chan Op
	cid_seq map[int64] int32
	persister *raft.Persister

	stat int
}

func (kv *KVServer) getAgreeCh(index int) chan Op{
	kv.mu.Lock()
	ch, ok := kv.getCh[index]
	if !ok{
		ch = make(chan Op, 1)
		kv.getCh[index] = ch
	}
	kv.mu.Unlock()
	return ch
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.WrongLeader = false
	seq, ok := kv.cid_seq[args.Cid]
	if ok && seq > args.Seq {
		kv.mu.Lock()
		reply.Value = kv.keyValue[args.Key]
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	cmd := Op{args.Key, "", "Get", args.Cid, args.Seq}
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		return
	} else {
		ch := kv.getAgreeCh(index)
	//	defer delete(kv.getCh, index)
		op := Op{}
		DPrintf("select ")
		select {
		case op = <-ch:
			reply.Err = OK
			DPrintf("%v agree\n", kv.me)
			close(ch)
		case <-time.After(1000*time.Millisecond):
			reply.Err = ErrTimeOut
			reply.WrongLeader = true
			DPrintf("timeout\n")
			return
		}
		DPrintf("success\n")
		if !cmd.equals(op) {
			reply.Err = ErrWrongOp
			reply.WrongLeader = true
			return
		}
		kv.mu.Lock()
		reply.Value = kv.keyValue[args.Key]
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	cmd := Op{args.Key, args.Value, args.Op, args.Cid, args.Seq}
	index, _, isLeader := kv.rf.Start(cmd)
	reply.WrongLeader = false
	if !isLeader {
		reply.WrongLeader = true
		return
	} else {
		ch := kv.getAgreeCh(index)
	//	defer delete(kv.getCh, index)
	//	DPrintf("%v is leader\n", kv.me)
		op := Op{}
		DPrintf("select ")
		select {
		case op = <- ch:
			reply.Err = OK
			DPrintf("%v agree\n", kv.me)
			close(ch)
		case <-time.After(1000*time.Millisecond):
			reply.Err = ErrTimeOut
			reply.WrongLeader = true
			DPrintf("timeout\n")
			return
		}
		DPrintf("success\n")
		if !cmd.equals(op) {
			reply.Err = ErrWrongOp
			reply.WrongLeader = true
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
	kv.mu.Lock()
	kv.stat = Dead
	kv.mu.Unlock()
	DPrintf("%v is dead\n", kv.me)
	// Your code here, if desired.
}
func (kv *KVServer) checkSnapShot(index int) {
	if kv.maxraftstate == -1 {
		return
	}
	if kv.persister.RaftStateSize() < kv.maxraftstate*9/10 {
		return
	}
//	DPrintf("%v's log too large\n", kv.me)
	go kv.rf.TakeSnapShot(index, kv.cid_seq, kv.keyValue)
}
func (kv *KVServer) waitSubmitLoop() {
	for {
		kv.mu.Lock()
		stat := kv.stat
		kv.mu.Unlock()
		if stat == Dead {
			return
		}
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				op := msg.Command.(Op)
				//	DPrintf("%v %v key:%v, value:%v\n", kv.me, op.Type, op.Key, op.Value)
				kv.mu.Lock()
				maxSeq, ok := kv.cid_seq[op.Cid]
				if !ok || op.Seq > maxSeq {
					switch op.Type {
					case "Put":
						kv.keyValue[op.Key] = op.Value
					case "Append":
						kv.keyValue[op.Key] += op.Value
					}
					kv.cid_seq[op.Cid] = op.Seq
				}
				kv.mu.Unlock()
				kv.checkSnapShot(msg.CommandIndex)
				kv.getAgreeCh(msg.CommandIndex) <- op
			} else {
				kv.readSnapShot(msg.Data)
			}
		}
	}
}
func (kv *KVServer) readSnapShot(data []byte)  {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var cid_seq map[int64] int32
	var keyValue map[string] string

	err := d.Decode(&cid_seq)
	if err != nil {
		log.Fatal("decode cid_seq error:", err)
	} else {
		kv.cid_seq = cid_seq
	}
	err = d.Decode(&keyValue)
	if err != nil {
		log.Fatal("decode keyValue error:", err)
	} else {
		kv.keyValue = keyValue
	}
//	DPrintf("%v read snapshot success\n", kv.me)
}
func (kv *KVServer) checkStatLoop()  {
	for {
		DPrintf("%v's stat:%v\n", kv.me, kv.stat)
		time.Sleep(100*time.Millisecond)
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

	// You may need initialization code here.
	kv.getCh = make(map[int]chan Op)
	kv.keyValue = make(map[string] string)
	kv.cid_seq = make(map[int64] int32)
	kv.stat = Alive
	kv.persister = persister
	kv.readSnapShot(persister.ReadSnapshot())

	go kv.waitSubmitLoop()

	return kv
}
