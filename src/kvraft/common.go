package raftkv

const (
	Alive = iota
	Dead

	OK       = "OK"
	ErrNoKey = "ErrNoKey"
	ErrTimeOut = "ErrTimeOut"
	ErrWrongOp = "ErrWrongOp"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cid int64
	Seq int32
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Cid int64
	Seq int32
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
