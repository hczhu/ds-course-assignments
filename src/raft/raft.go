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
  term, isLeader = -1, false
  rf.af.WithRlock(func(st int, cdata *CoreData) {
	  term, isLeader = cdata.currentTerm, st == Leader || st == PreLeader
  })
  return
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

  rf.af.WithRlock(func(st int, cdata *CoreData) {
    e.Encode(cdata.currentTerm)
	  e.Encode(cdata.votedFor)
    e.Encode(cdata.log)
  })
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
    rf.af.WithLock(func(st int, cdata *CoreData) {
      cdata.currentTerm = currentTerm
      cdata.votedFor = votedFor
      cdata.log = log
    })
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
func (rf *Raft) updateTerm(peerTerm int) bool {
  return rf.updateTermAndTrans(peerTerm, true)
}
func (rf *Raft) updateTermAndTrans(peerTerm int, transit bool) bool {
  updated := false
  rf.af.WithRlock(func(st int, cdata *CoreData) {
    if cdata.currentTerm < peerTerm {
      cdata.currentTerm = peerTerm
      cdata.votedFor = -1
      updated = true
      if transit {
        rf.af.Transit(Follower)
      }
    }
  })
  return updated
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestReply) {
  termUpdated := rf.updateTerm(args.Term)

  defer func() {
    if !termUpdated && reply.Success {
      // Reset election timer
      rf.af.Transit(Follower)
    }
  }()

  rf.af.WithLock(func(st int, cdata *CoreData) {
    reply.Term = cdata.currentTerm
    reply.Peer = rf.me
    reply.Success = false
    if args.Term < cdata.currentTerm ||
        (cdata.votedFor >=0 && cdata.votedFor != args.CandidateId) {
      return
    }
    lastTerm, lastIndex := cdata.log[len(cdata.log) - 1].Term, len(cdata.log) - 1
    if lastTerm > args.LastLogTerm ||
        (lastTerm == args.LastLogTerm && lastIndex > args.LastLogIndex) {
      return
    }
    reply.Success = true
    // Can the following steps be done asynchronously (after returning the RPC)?
    cdata.votedFor = args.CandidateId
    rf.Log("Voted for %d during term %d", cdata.votedFor, cdata.currentTerm)
  })
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

  defer func() {
    if rf.commitIndex > rf.lastApplied {
      rf.applierWakeup<-true
    }
  }()

  curTermSnapshot := 0
  rf.af.WithRlock(func(st int, cdata *CoreData) {
    curTermSnapshot = cdata.currentTerm
  })

  reply.Term = curTermSnapshot
  reply.Peer = rf.me
  reply.Success = false
  if args.Term < curTermSnapshot {
    // out-of-date leader
    return
  }
  // A heartbeat must go through all the following steps as well
  rf.leader = args.LeaderId
  defer func() {
    if !termUpdated {
      // Reset election timer
      rf.af.Transit(Follower)
    }
  }()

  rf.af.WithLock(func(st int, cdata *CoreData) {
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
  })
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
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = -1
	term = -1
  isLeader = false
  rf.af.WithLock(func(st int, cdata *CoreData) {
    if st != Leader && st != PreLeader {
      return
    }
    isLeader = true
    term = cdata.currentTerm
    index = len(cdata.log)
    entry := LogEntry{
      Term: term,
      Cmd: command,
    }
    cdata.log = append(cdata.log, entry)
    rf.Log("Appended entry %+v at %d.", entry, index)
  })

  if isLeader {
    toCh := time.After(2 * getHearbeatTimeout())
    for {
      select {
        case <-toCh:
          rf.Log("Agreement timeouts")
          return
        case logIndex := <-rf.appliedLogIndex:
          if logIndex >= index {
            return
          }
      }
    }
  }
	return
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
  rf.Log("Killing the server")
  rf.applierWakeup<-false
  rf.Log("Stopping the AsyncFSA")
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
  var args RequestVoteArgs
  af.WithLock(func(st int, cdata *CoreData) {
    cdata.currentTerm++
    cdata.votedFor = rf.me
    args = RequestVoteArgs{
      Term: cdata.currentTerm,
      CandidateId: rf.me,
      LastLogIndex: len(cdata.log) - 1,
      LastLogTerm: cdata.log[len(cdata.log) - 1].Term,
    }
  })

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
      if rf.updateTermAndTrans(reply.Term, false) {
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

func (rf *Raft)onPreLeader(af *AsyncFSA) int {
  rf.af.WithRlock(func(st int, cdata *CoreData) {
    rf.Log("Became leader at term %d", cdata.currentTerm)
  })
  rf.nextIndex = make([]int, len(rf.peers))
  rf.matchIndex = make([]int, len(rf.peers))
  logLen := 0

  rf.af.WithRlock(func(st int, cdata *CoreData) {
    logLen = len(cdata.log)
  })
  for i, _ := range rf.nextIndex {
    rf.nextIndex[i] = logLen
    rf.matchIndex[i] = 0
  }
  return Leader
}

func (rf *Raft)onLeader(af *AsyncFSA) int {
  return rf.replicateLogs(af)
}

func (rf *Raft) replicateLogs(af *AsyncFSA) int {
  toCh := time.After(getHearbeatTimeout())
  replyChan := make(Gchan, len(rf.peers))

  numRPCs := 0
  numReplies := 0
  defer func() {
    af.WithRlock(func(st int, cdata *CoreData) {
      rf.Log("Sent %d RPCs and got %d replies during one replicateLogs call at term %d.", numRPCs, numReplies, cdata.currentTerm)
    })
  }()

  sendOne := func (peer int) {
    numRPCs++
    // rf.Log("Peer %d nextIndex %d", peer, rf.nextIndex[peer])
    args := AppendEntriesArgs{
      LeaderId: rf.me,
      // Stale value also works
      LeaderCommit: rf.commitIndex,
      PrevLogIndex: rf.nextIndex[peer] - 1,
    }
    af.WithRlock(func(st int, cdata *CoreData) {
      args.Term = cdata.currentTerm
      args.PrevLogTerm = cdata.log[rf.nextIndex[peer] - 1].Term
      args.Entries = cdata.log[rf.nextIndex[peer]:]
    })

    reply := RequestReply{}
    ok := rf.sendAppendEntries(peer, &args, &reply)
    if ok {
      reply.Peer = peer
      reply.AppendedNewEntries = len(args.Entries)
      // rf.Log("AppendEntries request %+v got reply %+v", args, reply)
      replyChan <- encodeReply(reply)
    }
  }

  updateMatchIndex := func(peer, appendedNewEntries int) {
    rf.nextIndex[peer] += appendedNewEntries
    rf.matchIndex[peer] = rf.nextIndex[peer] - 1
    // rf.Log("matchIndex %+v", rf.matchIndex)
    // rf.Log("nextIndex %+v", rf.nextIndex)
    N := rf.matchIndex[peer]
    inCurrentTern := false
    rf.af.WithRlock(func(st int, cdata *CoreData) {
      assert(N < len(cdata.log), "%d out of log bound %d", N, len(cdata.log))
      inCurrentTern = cdata.log[N].Term == cdata.currentTerm
    })
    if N > rf.commitIndex && inCurrentTern {
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
    if p == rf.me {
      continue
    }
    peer := p
    go sendOne(peer)
  }

  for {
    timeout, replyBytes, nextSt := af.MultiWaitCh(replyChan, toCh)
    if timeout {
      break
    }
    if nextSt >= 0 {
      // Interrupted
      return nextSt
    }
    assert(replyBytes != nil, "Should have got a reply")
    reply := decodeReply(replyBytes)
    if rf.updateTermAndTrans(reply.Term, false) {
      return Follower
    }
    if reply.Success {
      if reply.AppendedNewEntries > 0 {
        updateMatchIndex(reply.Peer, reply.AppendedNewEntries)
      }
      numReplies++
    } else {
      rf.nextIndex[reply.Peer]--
      assert(rf.nextIndex[reply.Peer] > 0, "next index shouldn't reach 0")
      go sendOne(reply.Peer)
    }
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
  rf.appliedLogIndex = make(chan int, 100)

  cdata := CoreData {
    currentTerm: 1,
    votedFor: -1,
    // The first entry is a sentinel.
    log: make([]LogEntry, 1),
  }

  rf.af = MakeAsyncFSA(Follower, cdata)
  rf.af.AddCallback(Follower, rf.onFollower).AddCallback(
    Candidate, rf.onCandidate).AddCallback(
    PreLeader, rf.onPreLeader).AddCallback(
    Leader, rf.onLeader).SetLogger(rf.Log).Start()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
  rand.Seed(int64(time.Now().Nanosecond()))
  // rand.Seed(int64(1234567))

  go func() {
    for {
      if live := <-rf.applierWakeup; !live {
        rf.Log("Applier is exiting.")
        break
      }
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
        rf.applyCh<-msg
        // No other threads touch 'lastApplied'
        rf.lastApplied++
        rf.appliedLogIndex<-rf.lastApplied
      }
    }
  }()
	return rf
}
