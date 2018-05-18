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

type Gchan chan []byte
type Bytes []byte

var gStartTime time.Time = time.Now()

var gPrintLog bool = true

func min(a, b int) int {
  if a < b {
    return a
  }
  return b
}

func assert(cond bool, format string, v ...interface {}) {
  if !cond {
    panic(fmt.Sprintf(format, v...))
  }
}

func (rf *Raft) Log(format string, v ...interface {}) {
  if !gPrintLog {
    return
  }
  fmt.Println(
    fmt.Sprintf("Peer #%d @%07d:", rf.me, int64(time.Since(gStartTime) / time.Millisecond)),
    fmt.Sprintf(format, v...))
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (term int, isLeader bool) {
  rf.RLock()
  defer rf.RUnlock()
	return rf.cdata.currentTerm, rf.cdata.role == Leader
}

func (rf *Raft) getRole() int {
  rf.RLock()
  defer rf.RUnlock()
	return rf.cdata.role
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
  rf.RLock()
  e.Encode(rf.cdata.currentTerm)
	e.Encode(rf.cdata.votedFor)
  e.Encode(rf.cdata.log)
  rf.RUnlock()
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
    rf.Lock()
    cdata.currentTerm = currentTerm
    cdata.votedFor = votedFor
    cdata.log = log
    rf.Unlock()
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

func (rf *Raft) updateTerm(peerTerm int, notify bool) bool {
  updated := false
  rf.Lock()
  if cdata.currentTerm < peerTerm {
    cdata.role = Follower
    cdata.currentTerm = peerTerm
    cdata.votedFor = -1
    updated = true
  }
  rf.Unlock()
  if updated && notify {
    rf.notifyQ<-true
  }
  return updated
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestReply) {
  termUpdated := rf.updateTerm(args.Term, true)

  defer func() {
    if !termUpdated && reply.Success {
      // Reset election timer
      rf.notifyQ<-true
    }
  }()

  rf.Lock()
  defer rf.Unlock()
  cdata := &rf.cdata
  reply.Term = cdata.currentTerm
  reply.Peer = rf.me
  reply.Success = false
  if args.Term < cdata.currentTerm ||
      (cdata.votedFor >=0 && cdata.votedFor != args.CandidateId) {
    return
  }
  // This server must be a follower
  lastTerm, lastIndex := cdata.log[len(cdata.log) - 1].Term, len(cdata.log) - 1
  if lastTerm > args.LastLogTerm ||
      (lastTerm == args.LastLogTerm && lastIndex > args.LastLogIndex) {
    return
  }
  reply.Success = true
  // Can the following steps be done asynchronously (after returning the RPC)?
  cdata.votedFor = args.CandidateId
  rf.Log("Voted for %d during term %d", cdata.votedFor, cdata.currentTerm)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Receiver's handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *RequestReply) {
  termUpdated := rf.updateTerm(args.Term, true)

  defer func() {
    if rf.commitIndex > rf.lastApplied {
      rf.applierWakeup<-true
    }
  }()

  reply.Peer = rf.me
  reply.Success = false
  shouldResetElectionTimer := false
  rf.Lock()
  defer func() {
    rf.Unlock()
    if shouldResetElectionTimer {
      // Reset election timer
      rf.notifyQ<-true
    }
  }()
  cdata := &rf.cdata
  reply.Term = cdata.currentTerm
  if args.Term < cdata.currentTerm {
    // out-of-date leader
    return
  }
  // This server can't be a leader from here
  shouldResetElectionTimer = !termUpdated
  // A heartbeat must go through all the following steps as well
  rf.leader = args.LeaderId
  if args.PrevLogIndex >= len(cdata.log) ||
     args.PrevLogTerm != cdata.log[args.PrevLogIndex].Term {
    return
  }
  matchedLen := 0
  for matchedLen < len(args.Entries) &&
      args.PrevLogIndex + matchedLen + 1 < len(cdata.log) &&
      cdata.log[args.PrevLogIndex + matchedLen + 1].Term == args.Entries[matchedLen].Term {
    matchedLen++
  }
  cdata.log = cdata.log[:args.PrevLogIndex + matchedLen + 1]
  for ;matchedLen < len(args.Entries); matchedLen++ {
    cdata.log = append(cdata.log, args.Entries[matchedLen])
  }
  reply.Success = true
  if args.LeaderCommit > rf.commitIndex {
    rf.commitIndex = min(args.LeaderCommit, len(cdata.log) - 1)
  }
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *RequestReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = -1
	term = -1
  isLeader = false
  rf.Lock()
  defer rf.Unlock()
  cdata := &rf.cdata
  if cdata.role != Leader {
    return
  }
  isLeader = true
  index = len(cdata.log)
  entry := LogEntry{
    Term: cdata.currentTerm,
    Cmd: command,
  }
  cdata.log = append(cdata.log, entry)
  rf.Log("Appended entry %+v at %d.", entry, index)
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
  rf.Log("Killing the server")
  rf.live = false
  rf.applierWakeup<-false
  rf.applierWakeup<-false
  rf.notifyQ<-false
}

func getElectionTimeout() time.Duration {
  return time.Duration(ElectionTimeoutMil + rand.Intn(ElectionTimeoutDvMil)) * time.Millisecond
}

func getHearbeatTimeout() time.Duration {
  return time.Duration(HeartbeatMil) * time.Millisecond
}

type WaitResult struct {
  timeout bool
  interrupted bool
  RequestReply reply
}

func (rf *Raft)MultiWait(gchan Gchan, timeout time.Duration) WaitResult {
  if timeout <= 0 {
    timeout = time.Duration(100000) * time.Hour
  }
  return rf.MultiWaitCh(gchan, time.After(timeout))
}

func (rf *Raft) MultiWaitCh(gchan Gchan, toCh <-chan time.Time) (result WaitResult) {
  if gchan == nil {
    gchan = make(Gchan)
  }
  select {
    case <-toCh:
      result.timeout = true
    case <-rf.notifyQ:
      result.interrupted = true
    case replyBytes := <-gchan:
      result.reply = decodeReply(replyBytes)
  }
  return
}

func (rf *Raft)onFollower() {
  rf.Log("Became a follower")
  for rf.live {
    result := rf.MultiWait(nil, getElectionTimeout())
      case result.timeout:
        rf.Lock()
        cdata := &rf.cdata
        cdata.currentTerm++
        cdata.role = Candidate
        cdata.votedFor = rf.me
        rf.Unlock()
        return
    }
  }
}

func (rf *Raft) onCandidate() int {
  rf.Log("Became a candidate")
  for rf.live {
    timeoutCh := time.After(getElectionTimeout())
    var args RequestVoteArgs
    prepareCandidacy := func() {
      rf.Lock()
      defer rf.Unlock()
      cdata := &rf.cdata

      if cdata.role != Candidate {
        return false
      }
      cdata.currentTerm++
      cdata.votedFor = rf.me
      args = RequestVoteArgs{
        Term: cdata.currentTerm,
        CandidateId: rf.me,
        LastLogIndex: len(cdata.log) - 1,
        LastLogTerm: cdata.log[len(cdata.log) - 1].Term,
      }
      return true
    }
    if !prepareCandidacy() {
      return
    }
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
      result := rf.MultiWaitCh(ch, timeoutCh)
      switch {
        case result.timeout:
          // Restart the eleciton
          break
        case result.interrupted:
          // Restart the eleciton
          break
        default:
          reply := result.reply
          // rf.Log("Got RequestVote reply: %+v", reply)
          if rf.updateTerm(reply.Term, false) {
            return
          }
          if reply.Success {
            votes++
          }
        }
      }
    }
    becameLeader := func() bool {
      if 2 * votes <= len(rf.peers) {
        return false
      }
      rf.RLock()
      defer rf.RUnlock()
      return rf.cdata.role == Candidate
    }
    if becameLeader() {
      return
    }
  }
}

func (rf *Raft)onLeader() {
  rf.Log("Became leader at term %d", rf.cdata.currentTerm)
  func() {
    rf.nextIndex = make([]int, len(rf.peers))
    rf.matchIndex = make([]int, len(rf.peers))
    rf.Lock()
    logLen := len(rf.cdata.log)
    rf.Unlock()
    for i, _ := range rf.nextIndex {
      rf.nextIndex[i] = logLen
      rf.matchIndex[i] = 0
    }
  }()
  for rf.live {
    if _, isLeader := rf.GetState(); !isLeader {
      return
    }
    rf.replicateLogs()
  }
}

func (rf *Raft) replicateLogs() int {
  toCh := time.After(getHearbeatTimeout())
  replyChan := make(Gchan, len(rf.peers))

  numRPCs := 0
  numReplies := 0
  defer func() {
    rf.Log("Sent %d RPCs and got %d replies during one replicateLogs call at term %d.",
      numRPCs, numReplies, rf.cdata.currentTerm)
  }()

  stillLeader := true
  sendOne := func (peer int) {
    // rf.Log("Peer %d nextIndex %d", peer, rf.nextIndex[peer])
    args := AppendEntriesArgs{
      LeaderId: rf.me,
      // Stale value also works
      LeaderCommit: rf.commitIndex,
      PrevLogIndex: rf.nextIndex[peer] - 1,
    }
    prepareArgs := func() bool {
      rf.Lock()
      defer rf.Unlock()
      cdata := &rf.cdata
      if cdata.role != Leader {
        return false
      }
      args.Term = cdata.currentTerm
      args.PrevLogTerm = cdata.log[rf.nextIndex[peer] - 1].Term
      args.Entries = cdata.log[rf.nextIndex[peer]:]
      return true
    })

    if !prepareArgs() {
      stillLeader = false
    }
    reply := RequestReply{}
    ok := rf.sendAppendEntries(peer, &args, &reply)
    if ok {
      reply.Peer = peer
      reply.AppendedNewEntries = len(args.Entries)
      // rf.Log("AppendEntries request %+v got reply %+v", args, reply)
      replyChan <- encodeReply(reply)
    }
    numRPCs++
  }

  updateMatchIndex := func(peer, appendedNewEntries int) {
    rf.Lock()
    defer rf.Unlock()
    cdata := &rf.cdata
    if cdata.role != Leader {
      stillLeader = false
      return
    }
    rf.nextIndex[peer] += appendedNewEntries
    rf.matchIndex[peer] = rf.nextIndex[peer] - 1
    // rf.Log("matchIndex %+v", rf.matchIndex)
    // rf.Log("nextIndex %+v", rf.nextIndex)
    N := rf.matchIndex[peer]
    if N > rf.commitIndex && cdata.log[N].Term == cdata.currentTerm {
      numGoodPeers := 1
      for p, match := range rf.matchIndex {
        if p == rf.me {
          continue
        }
        if match >= N {
          numGoodPeers++
          if 2 * numGoodPeers > len(rf.peers) {
            rf.commitIndex = N
            if rf.commitIndex > rf.lastApplied {
              rf.applierWakeup<-true
            }
            break
          }
        }
      }
    }
  }

  for p, _ := range rf.peers {
    if !stillLeader {
      return
    }
    if p == rf.me {
      continue
    }
    peer := p
    go func() {
      if !sendOne(peer) {
        stillLeader = false
      }
    }()
  }

  for stillLeader && rf.live {
    result := af.MultiWaitCh(replyChan, toCh)
    switch {
      case result.timeout:
        // Start over
        return
      case restart.interrupted:
        // Start over
        return
      default:
        numReplies++
        reply := result.reply
        if rf.updateTerm(reply.Term, false) {
          return
        }
        if reply.Success {
          if reply.AppendedNewEntries > 0 {
            updateMatchIndex(reply.Peer, reply.AppendedNewEntries)
          }
        } else {
          rf.nextIndex[reply.Peer]--
          go sendOne(reply.Peer)
        }
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
  rf.applyCh = applyCh

  rf.leader = -1
  rf.commitIndex = 0
  rf.lastApplied = 0

  rf.applierWakeup = make(WakeupChan, 10)
  rf.appliedLogIndex = make(chan int, 10)
  rf.live = true

  cdata := CoreData {
    currentTerm: 1,
    votedFor: -1,
    // The first entry is a sentinel.
    log: make([]LogEntry, 1),
    role: Follower,
  }

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
  rand.Seed(int64(time.Now().Nanosecond()))
  // rand.Seed(int64(1234567))
  go func() {
    for rf.live {
      <-rf.applierWakeup
      for rf.lastApplied < rf.commitIndex {
        msg := ApplyMsg{
          CommandValid: true,
          CommandIndex: rf.lastApplied + 1,
        }
        term := 0
        rf.af.WithRlock(func(st int, cdata *CoreData) {
          msg.Command = cdata.log[rf.lastApplied + 1].Cmd
          term = cdata.log[rf.lastApplied + 1].Term
        })
        rf.Log("Applied %+v at term %d", msg, term)
        select {
          case rf.applyCh<-msg:
            // No other threads touch 'lastApplied'
            rf.lastApplied++
          case <-rf.applierWakeup:
        }
      }
    }
    rf.Log("Applier is exiting.")
  }()

  go func() {
    for rf.live {
      switch rf.getRole() {
        case Follower:
          onFollower()
        case Candidate:
          onCandidate()
        case Leader:
          onLeader()
        default:
          break
      }
    }
  }
	return rf
}
