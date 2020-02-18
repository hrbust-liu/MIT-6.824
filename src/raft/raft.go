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

import (
	"bytes"
	"fmt"
	"labgob"
	"log"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"


const (
	Follower = iota
	Candidate
	Leader
	Dead
)
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
	Data []byte
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
	timer* time.Timer
	stat int

	applyCh chan ApplyMsg
	appendCh chan struct{}
	voteCh chan struct{}
	commitCh chan struct{}
	voteCount int

	currentTerm int		//latest term server has seen (initialized to 0 on first boot, increases monotonically)
	voteFor int			//candidateId that received vote in current term (or null if none)
	log[] Entry			//log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	commitIndex int		//index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int		//index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	nextIndex[] int		//for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex[] int	//for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}
type Entry struct {
	Term int
	Index int
	Command interface{}
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.stat == Leader)
	if isleader {
	//	fmt.Printf("raft%v is leader\n", rf.me)
	} /*else {
		fmt.Printf("raft%v is not leader\n", rf.me)
	}*/
	/*fmt.Printf("raft%v's term:%v\n", rf.me, rf.currentTerm)*/
	return term, isleader
}
func min(x int, y int)  int{
	if x < y {
		return x
	} else {
		return y
	}
}
func max(x int, y int)  int{
	if x > y {
		return x
	} else {
		return y
	}
}
func getRandomTimeOut() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	timeout := time.Millisecond*time.Duration(r.Int63n(500)+250)
	return timeout
}
func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		log.Fatal("encode term error:", err)
	}
	err = e.Encode(rf.voteFor)
	if err != nil {
		log.Fatal("encode voteFor error:", err)
	}
	err = e.Encode(rf.log)
	if err != nil {
		log.Fatal("encode log error:", err)
	}
	data := w.Bytes()
	return data
}
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	 data := rf.getPersistData()
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
	 var term int
	 var voteFor int
	 var entries []Entry
	 err := d.Decode(&term)
	 if err != nil {
	 	log.Fatal("decode term error:", err)
	 } else {
	 	rf.currentTerm = term
	 }
	err = d.Decode(&voteFor)
	if err != nil {
		log.Fatal("decode voteFor error:", err)
	} else {
		rf.voteFor = voteFor
	}
	err = d.Decode(&entries)
	if err != nil {
		log.Fatal("decode log error:", err)
	} else {
		rf.log = entries
	}
}
//delete entries after index
func (rf *Raft) TakeSnapShot(index int, cid_seq map[int64]int32, kv map[string]string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	FirstIndex := rf.log[0].Index
	if index < FirstIndex {
		fmt.Printf("index:%v, FirstIndex:%v\n", index, FirstIndex)
		return
	}
//	fmt.Printf("before trimming:%v\n", len(rf.log))
	rf.log = rf.log[index-FirstIndex:]
//	fmt.Printf("after trimming:%v\n", len(rf.log))
	//snapshot
	w2 := new(bytes.Buffer)
	e2 := labgob.NewEncoder(w2)
	err := e2.Encode(cid_seq)
	if err != nil {
		log.Fatal("encode KVServer's data error:", err)
	}
	err = e2.Encode(kv)
	if err != nil {
		log.Fatal("encode KVServer's data error:", err)
	}
	snapshotsData := w2.Bytes()
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshotsData)
}
type InstallSnapshotArgs struct {
	Term int	//leader’s term
	LeaderId int	//so follower can redirect clients
	LeaderCommit int
	LastIncludedIndex int	//the snapshot replaces all entries up through and including this index
	LastIncludedTerm int	//term of lastIncludedIndex
	Data[] byte	//raw bytes of the snapshot chunk, starting at offset
	Entries	[]Entry
}
type InstallSnapshotReply struct {
	Term int
	Success bool
	PrevIndex int
}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply)  {
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.mu.Lock()
		//	fmt.Printf("%v become follower, argsTerm:%v, currentTerm:%v\n", rf.me, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.stat = Follower
		rf.voteFor = -1
		rf.mu.Unlock()
		rf.persist()
	}
	reply.Term = rf.currentTerm
	FirstIndex := rf.log[0].Index
	index := args.LastIncludedIndex - FirstIndex
	if index < 0 {
		reply.PrevIndex = FirstIndex
		return
	}
	rf.log = args.Entries
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LeaderCommit
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), args.Data)
	msg := ApplyMsg{}
	msg.Data = args.Data
	msg.CommandValid = false
	rf.applyCh <- msg
	reply.Success = true
	go func() {
		rf.appendCh <- struct{}{}
	}()
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply)  bool{
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term int			//leader’s term
	LeaderId int		//so follower can redirect clients
	PrevLogIndex int	//index of log entry immediately preceding new ones
	PrevLogTerm int		//term of prevLogIndex entry
	Entries	[]Entry		//log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int	//leader’s commitIndex
}
type AppendEntriesReply struct {
	Term int		//currentTerm, for leader to update itself
	Success bool	//true if follower contained entry matching prevLogIndex and prevLogTerm
	PrevIndex int
	LastLogIndex int
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	//	fmt.Printf("receiver:%v, currentTerm:%v\n", rf.me, rf.currentTerm)
	} else if args.Term > rf.currentTerm && rf.stat != Dead{
		rf.mu.Lock()
	//	fmt.Printf("%v become follower, argsTerm:%v, currentTerm:%v\n", rf.me, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.stat = Follower
		rf.voteFor = -1
		rf.mu.Unlock()
		rf.persist()
	}
	if args.Term >= rf.currentTerm{
		FirstIndex := rf.log[0].Index
		PrevIndex := args.PrevLogIndex - FirstIndex
		if PrevIndex < 0 {
			reply.PrevIndex = FirstIndex
			return
		}
		if args.PrevLogIndex != -1 && args.PrevLogTerm != -1 {
			if len(rf.log)-1 < PrevIndex ||  rf.log[PrevIndex].Term != args.PrevLogTerm {
				reply.Success = false
				if len(rf.log)-1 >= PrevIndex {
					tmp := PrevIndex
					for tmp > 0 && rf.log[tmp].Term == rf.log[PrevIndex].Term {
						tmp--
					}
					reply.PrevIndex = rf.log[tmp].Index
			//		rf.log = append(rf.log[:args.PrevLogIndex], rf.log[args.PrevLogIndex+1:]...)
				} else {
					reply.PrevIndex = rf.log[len(rf.log)-1].Index
				}
			} else if rf.stat == Follower && rf.commitIndex <= args.LeaderCommit{
				rf.mu.Lock()
				fmt.Printf("appendentries %v lock\n", rf.me)
				reply.Success = true
				j := 0

				if rf.log[len(rf.log)-1].Index > PrevIndex {	//If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it.
					for i := PrevIndex+1; i <= rf.log[len(rf.log)-1].Index && j < len(args.Entries); i++{
						if rf.log[i].Index == args.Entries[j].Index && rf.log[i].Term != args.Entries[j].Term {
							rf.log = rf.log[:i]
							break
						}
						j++
					}
				}

				rf.log = append(rf.log, args.Entries[j:]...)

				rf.persist()
				if args.LeaderCommit > rf.commitIndex {
			//		fmt.Printf("%v's leaderCommit:%v, lastLogIndex:%v, commitIndex:%v\n", rf.me, args.LeaderCommit, rf.log[len(rf.log)-1].Index, rf.commitIndex)
					if (len(args.Entries) > 0) {
						rf.commitIndex = min(args.LeaderCommit, args.Entries[len(args.Entries)-1].Index)
					} else {
						rf.commitIndex = args.LeaderCommit
					}
					rf.commitCh <- struct{}{}
				}
				rf.mu.Unlock()
				fmt.Printf("appendentries %v unlock\n", rf.me)
			}
		}
	}
	reply.Term = rf.currentTerm
	reply.LastLogIndex = rf.log[len(rf.log)-1].Index

	if reply.Success {
		go func() {
			rf.appendCh <- struct{}{}
		}()
	}
	/*if rf.log[args.prevLogIndex].term != args.prevLogTerm {
		reply.success = false
	}*/
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int			//candidate’s term
	CandidateId int		//candidate requesting vote
	LastLogIndex int	//index of candidate’s last log entry
	LastLogTerm int		//term of candidate’s last log entry
}
//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int			//currentTerm, for candidate to update itself
	VoteGranted bool	//true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//fmt.Printf("raft%v Lock\n", rf.me)
//	defer func() {fmt.Printf("raft%v Unlock\n", rf.me); rf.mu.Unlock()}()
	reply.VoteGranted = false
//	fmt.Printf("args.term:%v, currentTerm:%v, received server:%v, stat:%v, voteFor:%v\n", args.Term, rf.currentTerm, rf.me, rf.stat, rf.voteFor)
	if args.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.currentTerm = args.Term
	//	rf.updateStatTo(Follower)
		rf.stat = Follower
		rf.voteFor = -1
		rf.mu.Unlock()
	//	fmt.Printf("%v become follower, updater: RequestVote: args.Term > rf.currentTerm, argsTerm:%v, currentTerm:%v\n", rf.me, args.Term, rf.currentTerm)
		rf.persist()
	}
	if args.Term >= rf.currentTerm && (rf.voteFor == -1 || rf.voteFor == args.CandidateId){
		lastLogIndex := len(rf.log)-1
		lastLogTerm := rf.log[len(rf.log)-1].Term
	//	fmt.Printf("%v's lastLogIndex:%v, argsIndex:%v, lastLogTerm:%v, argsTerm:%v\n", rf.me, rf.log[len(rf.log)-1].Index, args.LastLogIndex, rf.log[len(rf.log)-1].Term, args.LastLogTerm)
		if (lastLogTerm < args.LastLogTerm) || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
			reply.VoteGranted = true
			rf.mu.Lock()
			rf.voteFor = args.CandidateId
			rf.mu.Unlock()
			rf.persist()
		//	fmt.Printf("argsTerm:%v, %v's currentTerm:%v, argsIndex:%v, lastLogIndex:%v\n", args.Term, rf.me, rf.currentTerm, args.LastLogIndex, rf.log[len(rf.log)-1].Index)
		}
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
	//	fmt.Printf("raft%v don't vote for raft%v, currentTerm:%v, argsTerm:%v\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		reply.VoteGranted = false
	} else {
		go func() {
			rf.voteCh <- struct{}{}
		}()
	}
}
//
//update raft stat
//
func (rf *Raft) updateStatTo(stat int)  {
	if rf.stat == stat || rf.stat == Dead{
		return
	}
	if stat == Follower {
	//	fmt.Printf("raft%v become follower, before:%v\n", rf.me, rf.stat)
		rf.stat = Follower
		rf.voteFor = -1
	}
	if stat == Candidate {
		rf.mu.Lock()
		rf.stat = Candidate
		rf.mu.Unlock()
	//	fmt.Printf("raft%v become candidate, currentTerm:%v\n", rf.me, rf.currentTerm)
		rf.startElection()
	}
	if stat == Leader {
		rf.stat = Leader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i, _ := range rf.peers {
			if i != rf.me {
				rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
				rf.matchIndex[i] = 0
				//	fmt.Printf("%v's nextIndex:%v\n", i, rf.nextIndex[i])
			}
		}
	//	fmt.Printf("raft%v become leader\n", rf.me)
	//	rf.broadcastAppendEntriesRPC()
	}
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
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = (rf.stat == Leader)
	rf.mu.Unlock()
	if isLeader {
		rf.mu.Lock()
		index = rf.log[len(rf.log)-1].Index+1
		entry := Entry{term, index, command}
		rf.log = append(rf.log, entry)
		rf.mu.Unlock()
		rf.persist()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	rf.stat = Dead
	rf.mu.Unlock()
}
func (rf *Raft) startElection() {
//	fmt.Printf("raft%v is starting election\n", rf.me)
	rf.mu.Lock()
	stat := rf.stat
	rf.mu.Unlock()
	if stat == Dead{
		return
	}
	rf.mu.Lock()
	rf.voteFor = rf.me
	rf.currentTerm += 1
	rf.mu.Unlock()

	rf.voteCount = 1
	rf.timer.Reset(getRandomTimeOut())
	rf.persist()

	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	if len(rf.log) > 0 {
		args.LastLogIndex = rf.log[len(rf.log)-1].Index
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
	}

	for i, _ := range rf.peers {
		if i != rf.me {
			go func(server int) {
			//	fmt.Printf("raft%v is sending RequestVote RPC to raft%v\n", rf.me, server)
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, &args, &reply)
				if rf.stat == Candidate && ok && reply.Term == rf.currentTerm{
					if reply.VoteGranted {
						rf.mu.Lock()
						rf.voteCount += 1
						rf.mu.Unlock()
					//	fmt.Printf("%v vote for %v, vote count:%v\n", server, rf.me, rf.voteCount)
					} else if reply.Term > rf.currentTerm{
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.stat = Follower
						rf.voteFor = -1
						rf.mu.Unlock()
						rf.persist()
					}
				} else {
				//	fmt.Printf("%v no reply\n", server)
				}
			}(i)
		}
	}
}
func (rf *Raft) broadcastAppendEntriesRPC()  {
	for i, _ := range rf.peers {
		FirstIndex := rf.log[0].Index
		if i != rf.me {
			go func(server int) {
			//	fmt.Printf("FirstIndex:%v, %v's nextIndex: %v\n", FirstIndex, server, rf.nextIndex[server])
				if rf.nextIndex[server] > FirstIndex {
					args := AppendEntriesArgs{}
					reply := AppendEntriesReply{}
					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					args.LeaderCommit = rf.commitIndex
					args.PrevLogIndex = -1
					args.PrevLogTerm = -1
					index := min(rf.matchIndex[server], rf.log[len(rf.log)-1].Index)
					//	fmt.Printf("%v's last log:%v, nextIndex: %v\n", server, rf.commitIndex, rf.nextIndex[server])
					if len(rf.log) > 0 && len(rf.log) > index{
					//	fmt.Printf("%v index:%v, FirstIndex:%v, leader:%v\n", server, index, FirstIndex, rf.me)
						args.PrevLogIndex = index
						args.PrevLogTerm = rf.log[index].Term
						if index < rf.log[len(rf.log)-1].Index {
							args.Entries = rf.log[index+1:]
						} else {
							args.Entries = make([]Entry, 0)
						}
						//		fmt.Printf("%v is sending Entry with logs to %v\n", rf.me, server)
					}
					ok := rf.sendAppendEntries(server, &args, &reply)
					ok = ok && reply.Term == rf.currentTerm
					if reply.Term > rf.currentTerm {
						//		fmt.Printf("%v become follower, %v's term:%v, %v's currentTerm:%v\n", rf.me, server, reply.Term, rf.me, rf.currentTerm)
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.stat = Follower
						rf.voteFor = -1
						rf.mu.Unlock()
						rf.persist()
					}
					if ok && rf.stat == Leader{
						if len(rf.log) > 0 && len(args.Entries) > 0{
							rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
							rf.matchIndex[server] = rf.nextIndex[server] - 1
						}
						if rf.commitIndex < rf.matchIndex[server] {
							//	fmt.Printf("%v's matchIndex:%v, %v's commitIndex:%v\n", server, rf.matchIndex[server], rf.me, rf.commitIndex)
							rf.updateCommitIndex()
							if rf.commitIndex <= reply.LastLogIndex {
								rf.commitCh <- struct{}{}
							}
						}
					} else if rf.stat == Leader {
						if rf.nextIndex[server] > reply.PrevIndex+1 {
							rf.nextIndex[server] = reply.PrevIndex + 1
						} else {
							rf.nextIndex[server] = max(rf.nextIndex[server]-1, 1)
						}
					}
				} else {
				//	fmt.Printf("%v sending installsnapshotRPC to %v\n", rf.me, server)
					args := InstallSnapshotArgs{rf.currentTerm, rf.me, rf.commitIndex, rf.log[0].Index, rf.log[0].Term, rf.persister.ReadSnapshot(), rf.log}
					reply := InstallSnapshotReply{}
					ok := rf.sendInstallSnapshot(server, &args, &reply)
					if reply.Term > rf.currentTerm {
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.stat = Follower
						rf.voteFor = -1
						rf.mu.Unlock()
						rf.persist()
					}
					if ok && rf.stat == Leader {
						if reply.Success {
							rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
							rf.matchIndex[server] = rf.nextIndex[server] - 1
						//	fmt.Printf("%v's nextIndex:%v, %v's commitIndex:%v\n", server, rf.nextIndex[server], rf.me, rf.commitIndex)
							if rf.commitIndex < rf.matchIndex[server] {
								rf.updateCommitIndex()
								rf.commitCh <- struct{}{}
							}
						} else {
							rf.nextIndex[server] = reply.PrevIndex + 1
						}
					}
				}
			}(i)
		}
	}
}
func (rf *Raft) doCommitLoop()  {
	for {
		rf.mu.Lock()
		stat := rf.stat
		rf.mu.Unlock()
		if stat == Dead {
			return
		}
		select {
		case <-rf.commitCh:
			for {
				if rf.commitIndex > rf.lastApplied {
					FirstIndex := rf.log[0].Index
					if rf.lastApplied+1 >= FirstIndex {
						rf.mu.Lock()
						rf.lastApplied += 1
						index := min(rf.lastApplied-FirstIndex, len(rf.log))
						rf.mu.Unlock()
					//	fmt.Printf("%v is applying command%v to state machine, logTerm:%v, currentTerm:%v\n", rf.me, rf.log[index].Command, rf.log[index].Term, rf.currentTerm)
						msg := ApplyMsg{}
						msg.Command = rf.log[index].Command
						msg.CommandIndex = rf.lastApplied
						msg.CommandValid = true
						rf.applyCh <- msg
					//	fmt.Printf("index:%v, logLen:%v\n", index, len(rf.log))
					}
				} else {
				//	fmt.Printf("commitIndex:%v, lastApplied:%v\n", rf.commitIndex, rf.lastApplied)
					break
				}
			}
		}
	}
}
func (rf *Raft) updateCommitIndex()  {
	N := rf.commitIndex
	FirstIndex := rf.log[0].Index
	for i := max(rf.commitIndex+1, FirstIndex+1); i <= rf.log[len(rf.log)-1].Index; i++ {
		cnt := 1
		for j := range rf.peers {
			if j != rf.me {
				if rf.matchIndex[j] >= i && rf.log[i-FirstIndex].Term == rf.currentTerm{
					cnt++
				}
			}
		}
		if cnt > len(rf.peers)/2 {
			N = i
		}
	}
	if N > rf.commitIndex && rf.stat == Leader {
		rf.mu.Lock()
		rf.commitIndex = N
		rf.mu.Unlock()
	//	fmt.Printf("update %v's commitIndex to %v\n", rf.me, N)
	}
}
func (rf *Raft) mainLoop()  {
	for {
		switch rf.stat {
		case Follower:
			select {
			case <-rf.appendCh:
				rf.timer.Reset(getRandomTimeOut())
			case <-rf.voteCh:
				rf.timer.Reset(getRandomTimeOut())
			case <-rf.timer.C:
				rf.updateStatTo(Candidate)
			}
		case Candidate:
			select {
			case <-rf.appendCh:
				rf.timer.Reset(getRandomTimeOut())
		//		fmt.Printf("%v become follower, updater: <-rf.appendCh, currentTerm:%v\n", rf.me, rf.currentTerm)
				rf.mu.Lock()
				rf.stat = Follower
				rf.voteFor = -1
				rf.mu.Unlock()
				rf.persist()
			case <-rf.timer.C:
			//	fmt.Printf("%v timeout and startElection, currentTerm:%v\n", rf.me, rf.currentTerm)
				rf.startElection()
			default:
				if rf.voteCount > len(rf.peers)/2 {
				//	fmt.Printf("voteCount:%v, majority:%v\n", rf.voteCount, len(rf.peers)/2)
					rf.mu.Lock()
					rf.updateStatTo(Leader)
					rf.mu.Unlock()
				}
			}
		case Leader:
			rf.broadcastAppendEntriesRPC()
			time.Sleep(50*time.Millisecond)
		case Dead:
			return
		}
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
	// Your initialization code here (2A, 2B, 2C).
	rf.stat = Follower
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{0, 0, 0})
	rf.voteCh = make(chan struct{})
	rf.appendCh = make(chan struct{})
	rf.commitCh = make(chan struct{})
	rf.applyCh = applyCh

	rf.timer = time.NewTimer(getRandomTimeOut())
	go rf.mainLoop()
	go rf.doCommitLoop()
	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()


	return rf
}
