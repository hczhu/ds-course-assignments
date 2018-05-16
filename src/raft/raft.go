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

// import "sync"
import "labrpc"
import "fmt"

import "bytes"
import "labgob"
import "time"
// import "log"
import "math/rand"
// import "math"


const (
  HeartbeatMil = 200
  ElectionTimeoutMil = 800
  ElectionTimeoutDvMil = 300
  Follower = 0
  Candidate = 1
  PreLeader = 2
  Leader = 3
)

var gStartTime time.Time = time.Now()

func assert(cond bool, format string, v ...interface {}) {
  if !cond {
    panic(fmt.Sprintf(format, v...))
  }
}

func (rf *Raft) Log(format string, v ...interface {}) {
  fmt.Println(
    fmt.Sprintf("Peer #%d @%07d:", rf.me, int64(time.Since(gStartTime) / time.Millisecond)),
    fmt.Sprintf(format, v...))
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
  rf.Lock()
  defer rf.Unlock()

	return rf.currentTerm, rf.af.GetState() == Leader
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

func encodeReply(reply RequestReply) Bytes {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
  e.Encode(reply)
	return w.Bytes()
}

func decodeReply(data Bytes) (reply RequestReply) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
  d.Decode(&reply)
	return
}

// Returns true if 'currentTerm' got updated to peerTerm
// No lock should be held by the caller
func (rf *Raft) updateTerm(peerTerm int) bool {
  rf.Lock()
  defer rf.Unlock()
  if rf.currentTerm < peerTerm {
    rf.currentTerm = peerTerm
    rf.votedFor = -1
    rf.af.Transit(Follower)
    return true
  }
  return false
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestReply) {
  termUpdated := rf.updateTerm(args.Term)
  rf.Lock()
  defer rf.Unlock()
  defer func() {
    if !termUpdated && reply.Success {
      // Reset election timer
      rf.af.Transit(Follower)
    }
  }()

  reply.Term = rf.currentTerm
  reply.Success = false
  if args.Term < rf.currentTerm ||
      (rf.votedFor >=0 && rf.votedFor != args.CandidateId) {
    return
  }
  lastTerm, lastIndex := rf.log[len(rf.log) - 1].Term, len(rf.log) - 1
  if lastTerm > args.LastLogTerm ||
      (lastTerm == args.LastLogTerm && lastIndex > args.LastLogIndex) {
    return
  }
  reply.Success = true
  // Can the following steps be done asynchronously (after returning the RPC)?
  rf.votedFor = args.CandidateId
  rf.Log("Voted for %d during term %d", rf.votedFor, rf.currentTerm)
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Receiver's handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *RequestReply) {
  termUpdated := rf.updateTerm(args.Term)

  rf.Lock()
  defer rf.Unlock()

  reply.Term = rf.currentTerm
  reply.Success = false
  if args.Term < rf.currentTerm {
    // out-of-date leader
    return
  }
  rf.leader = args.LeaderId
  defer func() {
    if !termUpdated {
      // Reset election timer
      rf.af.Transit(Follower)
    }
  }()

  if len(args.Entries) == 0 {
    return
  }

  // Fix me!!!
  if args.PrevLogIndex >= len(rf.log) ||
     args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
    return
  }
  matchedLen := 0
  for matchedLen < len(args.Entries) &&
      args.PrevLogIndex + matchedLen + 1 < len(rf.log) &&
      rf.log[args.PrevLogIndex + matchedLen + 1].Term == args.Entries[matchedLen].Term {
    matchedLen++
  }
  reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *RequestReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

func getElectionTimeout() time.Duration {
  return time.Duration(ElectionTimeoutMil + rand.Intn(ElectionTimeoutDvMil)) * time.Millisecond
}

func getHearbeatTimeout() time.Duration {
  return time.Duration(HeartbeatMil) * time.Millisecond
}

func (rf *Raft)onFollower(af *AsyncFSA) int {
  timeout, _, nextSt := rf.af.MultiWait(nil, getElectionTimeout())
  if timeout {
    return Candidate
  }
  return nextSt
}

func (rf *Raft) onCandidate(af *AsyncFSA) int {
  timeoutCh := time.After(getElectionTimeout())

  rf.Lock()
  rf.currentTerm++
  rf.votedFor = rf.me
  args := RequestVoteArgs{
    Term: rf.currentTerm,
    CandidateId: rf.me,
    LastLogIndex: len(rf.log) - 1,
    LastLogTerm: rf.log[len(rf.log) - 1].Term,
  }
  rf.Unlock()

  ch := make(Gchan, len(rf.peers))
  for p, _ := range rf.peers {
    if p == rf.me {
      continue
    }
    peer := p
    go func() {
      reply := RequestReply{}
      ok := rf.sendRequestVote(peer, &args, &reply)
      if !ok {
        reply.Term = -1
        reply.Success = false
      }
      ch <- encodeReply(reply)
    }()
  }
  // rf.Log("Fanning out RequestVote %+v", args)
  votes := 1
  for 2 * votes <= len(rf.peers) {
    timeout, it, nextSt := rf.af.MultiWaitCh(ch, timeoutCh)
    if timeout {
      // rf.Log("RequestVote timeouts")
      return Candidate
    }
    if nextSt >= 0 {
      // rf.Log("RequestVote got interrupted by state %d", nextSt)
      return nextSt
    }
    if it != nil {
      reply := decodeReply(it)
      // rf.Log("Got RequestVote reply: %+v", reply)
      if rf.updateTerm(reply.Term) {
        return Follower
      }
      if reply.Success {
        votes++
      }
    }
  }
  rf.Log("Got majority votes")
  return PreLeader
}

func (rf *Raft) fanOutAppendEntries(entries []LogEntry) Gchan {
  rf.Lock()
  argsTemp := AppendEntriesArgs{
    Term: rf.currentTerm,
    LeaderId: rf.me,
    PrevLogIndex: 0,
    PrevLogTerm: 0,
    Entries: entries,
    LeaderCommit: rf.commitIndex,
  }
  rf.Unlock()
  gchan := make(Gchan, len(rf.peers))
  for p, _ := range rf.peers {
    if p == rf.me {
      continue
    }
    peer := p
    args := argsTemp
    args.PrevLogIndex = rf.nextIndex[p] - 1
    args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
    go func() {
      reply := RequestReply{}
      ok := rf.sendAppendEntries(peer, &args, &reply)
      if !ok {
        reply.Term = -1
        reply.Success = false
      }
      gchan <- encodeReply(reply)
    }()
  }
  return gchan
}

func (rf *Raft) sendHeartbeats(af *AsyncFSA) int {
  toCh := time.After(HeartbeatMil * time.Millisecond)
  replyChan := rf.fanOutAppendEntries([]LogEntry{})
  for {
    timeout, replyBytes, nextSt := af.MultiWaitCh(replyChan, toCh)
    if timeout {
      return Leader
    }
    if nextSt >= 0 {
      return nextSt
    }
    assert(replyBytes != nil, "Should have got a reply")
    reply := decodeReply(replyBytes)
    if rf.updateTerm(reply.Term) {
      return Follower
    }
  }
}

func (rf *Raft)onPreLeader(af *AsyncFSA) int {
  rf.Log("Became leader at term %d", rf.currentTerm)
  rf.Lock()
  rf.nextIndex = make([]int, len(rf.peers))
  rf.matchIndex = make([]int, len(rf.peers))
  for i, _ := range rf.nextIndex {
    rf.nextIndex[i] = len(rf.log)
    rf.matchIndex[i] = 0
  }
  rf.Unlock()
  return rf.sendHeartbeats(af)
}

func (rf *Raft)onLeader(af *AsyncFSA) int {
  timeout, _, nextSt := rf.af.MultiWait(nil, getHearbeatTimeout())
  if nextSt >= 0 {
    return nextSt
  }
  assert(timeout, "Should timeout!")
  return rf.sendHeartbeats(af)
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

  rf.currentTerm = 1
  rf.votedFor = -1
  // The first entry is a sentinel
  rf.log = make([]LogEntry, 1)

  rf.leader = -1

  rf.commitIndex = 0
  rf.lastApplied = 0

  rf.af = MakeAsyncFSA(Follower)
  rf.af.AddCallback(Follower, rf.onFollower).AddCallback(
    Candidate, rf.onCandidate).AddCallback(
    PreLeader, rf.onPreLeader).AddCallback(
    Leader, rf.onLeader).SetLogger(rf.Log).Start()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
  rand.Seed(int64(time.Now().Nanosecond()))
	return rf
}
