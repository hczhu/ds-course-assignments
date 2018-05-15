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
import "fmt"

import "bytes"
import "labgob"
import "time"
import "log"
import "math/rand"
// import "math"


const (
  HeartbeatMil = 200
  ElectionTimeoutMil = 800
  ElectionTimeoutDvMil = 300
  PreFollower = 0
  Follower = 1
  Candidate = 2
  Leader = 3
)

func assert(cond bool, format string, v ...interface {}) {
  if !cond {
    log.Fatalf(format, v...)
  }
}

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
}

type LogEntry struct {
  term int
	cmd interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
  // Ready-only members
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	mx sync.RWMutex
  // For all servers
  currentTerm int
  votedFor int
  log []LogEntry
  leader int

  commitIndex int
  lastApplied int

  // For the leader
  nextIndex []int
  matchIndex []int

  af *AsyncFSA
  live bool
}

func (rf *Raft) Log(format string, v ...interface {}) {
  fmt.Println(
    fmt.Sprintf("Peer #%d @%07d:", rf.me, int64(time.Since(gStartTime) / time.Millisecond)),
    fmt.Sprintf(format, v...))
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
  rf.mx.RLock()
  defer rf.mx.RUnlock()
  term = rf.currentTerm
  isleader = rf.af.GetState() == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
  e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

  var currentTerm, votedFor int
  var log []LogEntry
  if d.Decode(&currentTerm) != nil ||
      d.Decode(&votedFor) != nil ||
      d.Decode(&log) != nil {
   fmt.Println("Failed to restore the persisted data.")
  } else {
    rf.currentTerm = currentTerm
    rf.votedFor = votedFor
    rf.log = rf.log
  }
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
  Term int
  CandidateId int
  LastLogIndex int
  LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
  Term int
  VoteGranted bool
}

func encodeReply(reply RequestVoteReply) Bytes {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
  e.Encode(reply)
	return w.Bytes()
}

func decodeReply(data Bytes) (reply RequestVoteReply) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
  d.Decode(&reply)
	return
}

func (rf *Raft) lastTermIndex() (int, int) {
  rf.mx.RLock()
  defer rf.mx.RUnlock()
  return rf.log[len(rf.log) - 1].term, len(rf.log) - 1
}

func (rf *Raft) voteFor(peer int) {
  rf.mx.Lock()
  rf.votedFor = peer
  rf.mx.Unlock()
  rf.Log("Voted for %d", peer)
}
func (rf *Raft) updateTerm(term int) {
  updated := false
  rf.mx.Lock()
  if (rf.currentTerm < term) {
    rf.currentTerm = term
    updated = true
  }
  rf.mx.Unlock()
  if updated {
    rf.Log("Updated term to %d", term)
  }
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
  reply.Term = rf.curTerm()
  reply.VoteGranted = false
  if args.Term < rf.currentTerm ||
      (rf.votedFor >=0 && rf.votedFor != args.CandidateId) {
    return
  }
  lastTerm, lastIndex := rf.lastTermIndex()
  if lastTerm > args.LastLogTerm ||
      (lastTerm == args.LastLogTerm && args.LastLogIndex > lastIndex) {
    return
  }
  rf.voteFor(args.CandidateId)
  reply.VoteGranted = true
  if rf.curTerm() < args.Term {
    rf.updateTerm(args.Term)
    rf.af.Transit(Follower)
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

type AppendEntriesReq struct {

}

type AppendEntriesReply struct {

}

func (rf *Raft) AppendEntries(args *AppendEntriesReq, reply *AppendEntriesReply) {

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesReq, reply *AppendEntriesReply) bool {
  return false
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


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
  rf.af.Stop()
}

func (rf *Raft) checkRole(expect int) {
  role := rf.af.GetState()
  assert(role == expect, "Peer %v should be the leader", rf.me)
}

func (rf *Raft) curTerm() int {
  rf.mx.RLock()
  defer rf.mx.RUnlock()
  return rf.currentTerm
}

func (rf *Raft) getElectionTimeout() time.Duration {
  return time.Duration(ElectionTimeoutMil + rand.Intn(ElectionTimeoutDvMil)) * time.Millisecond
}

func (rf *Raft)onPreFollower(af *AsyncFSA) int {
  return Follower
}

func (rf *Raft)onFollower(af *AsyncFSA) int {
  timeout, _, nextSt := rf.af.MultiWait(nil, rf.getElectionTimeout())
  if timeout {
    return Candidate
  }
  return nextSt
}

func (rf *Raft)onCandidate(af *AsyncFSA) int {
  rf.Log("Becoming a candidate")
  timeoutCh := time.After(rf.getElectionTimeout())
  rf.mx.Lock()
  rf.currentTerm++
  rf.votedFor = rf.me
  rf.mx.Unlock()

  rf.mx.RLock()
  args := RequestVoteArgs{
    rf.currentTerm,
    rf.me,
    len(rf.log) - 1,
    rf.log[len(rf.log) - 1].term,
  }
  rf.mx.RUnlock()
  ch := make(Gchan, len(rf.peers))
  for p, _ := range rf.peers {
    if p == rf.me {
      continue
    }
    peer := p
    go func() {
      reply := RequestVoteReply{}
      ok := rf.sendRequestVote(peer, &args, &reply)
      if !ok {
        reply.Term = -1
        reply.VoteGranted = false
      }
      ch <- encodeReply(reply)
    }()
  }

  votes := 1
  for 2 * votes <= len(rf.peers) {
    timeout, it, nextSt := rf.af.MultiWaitCh(ch, timeoutCh)
    if timeout {
      return Follower
    }
    if nextSt >= 0 {
      return nextSt
    }
    if it != nil {
      reply := decodeReply(it)
      if reply.Term > rf.curTerm() {
        rf.updateTerm(reply.Term)
        return Follower
      } else if reply.VoteGranted {
        votes++
      }
    }
  }
  rf.Log("Got majority votes")
  return Leader
}

func (rf *Raft)onLeader(af *AsyncFSA) int {
  rf.Log("Becoming the leader")
  _, _, nextSt := rf.af.MultiWait(nil, time.Second)
  if nextSt > 0 {
    return nextSt
  }
  return Leader
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
var gStartTime time.Time = time.Now()

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

  rf.currentTerm = 1
  rf.votedFor = -1
  rf.log = make([]LogEntry, 1)

  rf.leader = -1

  rf.commitIndex = 0
  rf.lastApplied = 0

  rf.af = MakeAsyncFSA(PreFollower)
  rf.af.AddCallback(
    PreFollower, rf.onPreFollower).AddCallback(
    Follower, rf.onFollower).AddCallback(
    Candidate, rf.onCandidate).AddCallback(
    Leader, rf.onLeader).SetLogger(rf.Log).Start()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
  rand.Seed(int64(time.Now().Nanosecond()))
	return rf
}
