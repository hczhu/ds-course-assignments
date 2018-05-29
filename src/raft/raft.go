package raft

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
  HeartbeatMil = 50
  ElectionTimeoutMil = 400
  ElectionTimeoutDvMil = 300
  Follower = 0
  Candidate = 1
  Leader = 2
)

type Gchan chan []byte
type Bytes []byte

var gStartTime time.Time = time.Now()

var gPrintLog bool = false
var gPersist bool = true

func min(a, b int) int {
  if a < b {
    return a
  }
  return b
}

func (rf *Raft) assert(cond bool, format string, v ...interface {}) {
  if !cond {
    panic(fmt.Sprintf(format, v...) + fmt.Sprintf(" Rack struct: %+v", *rf))
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
	return rf.cdata.CurrentTerm, rf.cdata.Role == Leader
}

func (rf *Raft) getRole() int {
  rf.RLock()
  defer rf.RUnlock()
	return rf.cdata.Role
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
  if !gPersist {
    return
  }
  switch rf.cdata.Role {
    case Follower:
    case Candidate:
      fallthrough
    case Leader:
      rf.assert(rf.cdata.VotedFor == rf.me, "A non-follower should vote for itself")
  }
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
  e.Encode(rf.cdata)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte, snapshot Bytes) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

  cdata := CoreData{}
  err := d.Decode(&cdata)
  rf.assert(err == nil, "Failed to restore the persisted data with error %+v.", err)
  rf.cdata = cdata
  rf.snapshot = snapshot
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
  cdata := &rf.cdata
  if cdata.CurrentTerm < peerTerm {
    cdata.Role = Follower
    cdata.CurrentTerm = peerTerm
    cdata.VotedFor = -1
    updated = true
    rf.persist()
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
  reply.Term = cdata.CurrentTerm
  reply.Peer = rf.me
  reply.Success = false
  if args.Term < cdata.CurrentTerm ||
      (cdata.VotedFor >=0 && cdata.VotedFor != args.CandidateId) {
    return
  }
  // This server must be a follower
  rf.assert(cdata.Role == Follower, "Should be a follower")
  lastTerm, lastIndex := cdata.LastLogTerm(), cdata.LastLogIndex()
  if lastTerm > args.LastLogTerm ||
      (lastTerm == args.LastLogTerm && lastIndex > args.LastLogIndex) {
    return
  }
  reply.Success = true
  // Can the following steps be done asynchronously (after returning the RPC)?
  cdata.VotedFor = args.CandidateId
  rf.persist()
  rf.Log("Voted for %d during term %d", cdata.VotedFor, cdata.CurrentTerm)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Receiver's handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *RequestReply) {
  // rf.Log("Got AppendEntriesArgs %+v", *args)
  if args == nil {
    rf.Log("Nil input args for AppendEntriesArgs")
    return
  }
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
  reply.Term = cdata.CurrentTerm
  if args.Term < cdata.CurrentTerm {
    // out-of-date leader
    return
  }
  // This server can't be a leader from here
  shouldResetElectionTimer = !termUpdated
  // A heartbeat must go through all the following steps as well
  rf.leader = args.LeaderId
  if args.PrevLogIndex > cdata.LastLogIndex() {
    reply.ConflictingTerm = -1
    reply.FirstLogIndex = cdata.LastLogIndex() + 1
    return
  }
  if args.PrevLogTerm != cdata.LogEntry(args.PrevLogIndex).Term {
    reply.ConflictingTerm = cdata.LogEntry(args.PrevLogIndex).Term;
    for reply.FirstLogIndex = args.PrevLogIndex;
        reply.FirstLogIndex > 0 &&
        cdata.LogEntry(reply.FirstLogIndex - 1).Term == reply.ConflictingTerm;
        reply.FirstLogIndex-- {}
    return
  }
  reply.Success = true
  matchedLen := 0
  for matchedLen < len(args.Entries) &&
      args.PrevLogIndex + matchedLen + 1 <= cdata.LastLogIndex() &&
      cdata.LogEntry(args.PrevLogIndex + matchedLen + 1).Term == args.Entries[matchedLen].Term {
    matchedLen++
  }
  // Don't truncate if there is not conflict. 'cdata.Log' may be longer than
  // the leader's log represented by this request because this request may be
  // delayed by the network and have stale data.
  modified := false
  if matchedLen < len(args.Entries) && args.PrevLogIndex + matchedLen + 1 <= cdata.LastLogIndex() {
    cdata.Log = cdata.Log[:args.PrevLogIndex + matchedLen + 1 - cdata.LastCompactedIndex]
    modified = true
  }
  for ;matchedLen < len(args.Entries); matchedLen++ {
    cdata.Log = append(cdata.Log, args.Entries[matchedLen])
    modified = true
  }
  if modified {
    rf.persist()
  }
  newCommitIndex := min(args.LeaderCommit, cdata.LastLogIndex())
  if newCommitIndex > rf.commitIndex {
    rf.commitIndex = newCommitIndex
  }
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *RequestReply) {
  termUpdated := rf.updateTerm(args.Term, true)

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
  reply.Term = cdata.CurrentTerm
  if args.Term < cdata.CurrentTerm {
    // out-of-date leader
    return
  }
  // This server can't be a leader from here
  shouldResetElectionTimer = !termUpdated
  if args.LastLogIndex <= rf.commitIndex {
    // old request
    return
  }
  rf.leader = args.LeaderId
  reply.Success = true

  cdata.Log = make([]LogEntry, 1)
  cdata.Log[0] = LogEntry{
    Term: args.LastLogTerm,
  }
  cdata.LastCompactedIndex  = args.LastLogIndex
  rf.commitIndex = cdata.LastCompactedIndex
  rf.lastApplied = cdata.LastCompactedIndex
  rf.snapshot = args.Snapshot
  rf.persist()
  rf.applyCh<-ApplyMsg{
    InstallSnapshot: true,
    Command: rf.snapshot,
  }
  rf.Log("Installed snapshot from leader %d with last applied index %d at term %d",
    args.LeaderId, args.LastLogIndex, args.Term)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *RequestReply) bool {
  // rf.Log("Sent AppendEntriesArgs %+v to peer %d", *args, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int,
  args *InstallSnapshotArgs, reply *RequestReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = -1
	term = -1
  isLeader = false
  rf.Lock()
  defer rf.Unlock()
  cdata := &rf.cdata
  if cdata.Role != Leader {
    return
  }
  defer func() {
    // rf.notifyQ<-true
  }()
  isLeader = true
  term = cdata.CurrentTerm
  entry := LogEntry{
    Term: term,
    Cmd: command,
  }
  cdata.Log = append(cdata.Log, entry)
  index = cdata.LastLogIndex()
  rf.persist()
  rf.Log("Appended entry %+v at %d.", entry, index)
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
  rf.live = false
  rf.applierWakeup<-false
  rf.notifyQ<-false
  rf.wg.Wait()
}

func (rf *Raft) GetLeader() int {
  return rf.leader
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
  reply RequestReply
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
  // Can only be a follower. Don't need to double check
  result := rf.MultiWait(nil, getElectionTimeout())
  switch {
    case result.timeout:
      rf.Lock()
      rf.cdata.Role = Candidate
      // cdata.CurrentTerm++
      rf.cdata.VotedFor = rf.me
      // Don't need to persist
      rf.Unlock()
    case result.interrupted:
  }
}

func (rf *Raft) onCandidate() {
  // rf.Log("Became a candidate at term %d", rf.cdata.CurrentTerm)
  timeoutCh := time.After(getElectionTimeout())
  var args RequestVoteArgs
  prepareCandidacy := func() bool {
    rf.Lock()
    defer rf.Unlock()
    cdata := &rf.cdata

    if cdata.Role != Candidate {
      return false
    }
    cdata.CurrentTerm++
    cdata.VotedFor = rf.me
    // Don't need to persist
    // rf.persist()
    // rf.Log("%+v", cdata)
    args = RequestVoteArgs{
      Term: cdata.CurrentTerm,
      CandidateId: rf.me,
      LastLogIndex: cdata.LastLogIndex(),
      LastLogTerm: cdata.LastLogTerm(),
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
      if ok {
        ch <- encodeReply(reply)
      }
    }()
  }
  rf.Log("Fanning out RequestVote %+v", args)
  votes := 1
  for 2 * votes <= len(rf.peers) {
    result := rf.MultiWaitCh(ch, timeoutCh)
    switch {
      case result.timeout:
        return
      case result.interrupted:
        return
      default:
        reply := result.reply
        rf.Log("Got RequestVote reply: %+v", reply)
        if rf.updateTerm(reply.Term, false) {
          return
        }
        if reply.Success {
          votes++
        }
    }
  }
  if 2 * votes <= len(rf.peers) {
    return
  }
  rf.RLock()
  defer rf.RUnlock()
  if rf.cdata.Role == Candidate {
    rf.cdata.Role = Leader
    rf.persist()
  }
}

func (rf *Raft) onLeader() {
  func() {
    rf.nextIndex = make([]int, len(rf.peers))
    rf.matchIndex = make([]int, len(rf.peers))
    rf.RLock()
    rf.leader = rf.me
    logLen := len(rf.cdata.Log)
    rf.RUnlock()
    for i, _ := range rf.nextIndex {
      rf.nextIndex[i] = logLen
      rf.matchIndex[i] = 0
    }
  }()
  for rf.live && rf.getRole() == Leader {
    rf.replicateLogs()
  }
}

func (rf *Raft) replicateLogs() {
  // var wg sync.WaitGroup
  // defer wg.Wait()
  replyChan := make(Gchan, len(rf.peers))

  numRPCs := 0
  numReplies := 0
  defer func() {
    rf.Log("Sent %d RPCs and got %d replies during one replicateLogs call at term %d.",
      numRPCs, numReplies, rf.cdata.CurrentTerm)
  }()
  var sendRecursively func(peer int)
  var sendInstallSnapshot func(peer int)
  sendRecursively = func (peer int) {
    var args AppendEntriesArgs
    prepareArgs := func() bool {
      rf.RLock()
      defer rf.RUnlock()
      cdata := &rf.cdata
      if cdata.Role != Leader || !rf.live {
        return false
      }

      rf.Log("Sending to Peer %d nextIndex %d LastCompactedIndex %d with role %d term %d",
        peer, rf.nextIndex[peer], cdata.LastCompactedIndex, cdata.Role, cdata.CurrentTerm)
      if rf.nextIndex[peer] <= cdata.LastCompactedIndex {
        sendInstallSnapshot(peer)
        return false
      }
      args = AppendEntriesArgs{
        Term: cdata.CurrentTerm,
        LeaderId: rf.me,
        // Stale value also works
        LeaderCommit: rf.commitIndex,
        PrevLogIndex: rf.nextIndex[peer] - 1,
        PrevLogTerm: cdata.LogEntry(rf.nextIndex[peer] - 1).Term,
        Entries: cdata.Log[rf.nextIndex[peer] - cdata.LastCompactedIndex :],
      }
      return true
    }
    if !prepareArgs() {
      return
    }
    // wg.Add(1)
		go func() {
      time.Sleep(getHearbeatTimeout())
      sendRecursively(peer)
      // wg.Done()
    }()

    reply := RequestReply{}
    ok := rf.sendAppendEntries(peer, &args, &reply)
    if ok {
      reply.Peer = peer
      reply.NextIndex = args.PrevLogIndex + 1
      reply.AppendedNewEntries = len(args.Entries)
      rf.Log("AppendEntries request %+v got reply %+v", args, reply)
      replyChan <- encodeReply(reply)
    }
  }

  sendInstallSnapshot = func(peer int) {
    var args InstallSnapshotArgs
    nextIndex := rf.nextIndex[peer]
    prepareArgs := func() bool {
      rf.RLock()
      defer rf.RUnlock()
      cdata := &rf.cdata
      if cdata.Role != Leader || !rf.live {
        return false
      }
      if rf.nextIndex[peer] > cdata.LastCompactedIndex {
        // Stale request
        return false
      }
      args = InstallSnapshotArgs{
        Term: cdata.CurrentTerm,
        LeaderId: rf.me,
        Snapshot: rf.persister.ReadSnapshot(),
        LastLogIndex: cdata.LastCompactedIndex,
        LastLogTerm: cdata.LogEntry(cdata.LastCompactedIndex).Term,
      }
      defer rf.Log("Prepared Peer %d install snapshot with LastLogIndex = %d LastLogTerm = %d",
        peer, args.LastLogIndex, args.LastLogTerm)
      return true
    }
    if !prepareArgs() {
      return
    }
    // wg.Add(1)
		go func() {
      time.Sleep(getHearbeatTimeout())
      sendRecursively(peer)
      // wg.Done()
    }()

    reply := RequestReply{}
    ok := rf.sendInstallSnapshot(peer, &args, &reply)
    if ok {
      reply.Peer = peer
      reply.NextIndex = nextIndex
      reply.AppendedNewEntries = args.LastLogIndex - nextIndex + 1
      rf.Log("InstallSnapshot request %+v got reply %+v", args, reply)
      replyChan <- encodeReply(reply)
    }
  }

  updateMatchIndex := func(reply RequestReply) bool {
    peer := reply.Peer
    newMatchIndex := reply.NextIndex + reply.AppendedNewEntries - 1
    if newMatchIndex <= rf.matchIndex[peer] {
      rf.Log("Got a stale reply: %+v", reply)
      return true
    }
    rf.matchIndex[peer] = newMatchIndex
    rf.nextIndex[peer] = newMatchIndex + 1
    N := rf.matchIndex[peer]
    if N <= rf.commitIndex {
      return true
    }
    isCurrentTerm := false
    rf.RLock()
    cdata := &rf.cdata
    if cdata.Role != Leader || !rf.live {
      return false
    }
    isCurrentTerm = cdata.LogEntry(N).Term == cdata.CurrentTerm
    rf.RUnlock()

    if isCurrentTerm {
      numGoodPeers := 1
      for p, match := range rf.matchIndex {
        if p == rf.me {
          continue
        }
        if match >= N {
          numGoodPeers++
          if 2 * numGoodPeers > len(rf.peers) {
            if cdata.Role != Leader || !rf.live {
              return false
            }
            rf.commitIndex = N
            // rf.persist()
            if rf.commitIndex > rf.lastApplied {
              rf.applierWakeup<-true
            }
            break
          }
        }
      }
    }
    return true
  }

  skipConflictingEntries := func(reply RequestReply) bool {
    rf.Lock()
    peer := reply.Peer
    next := rf.nextIndex[peer]
    defer func() {
      if next != rf.nextIndex[peer] {
        rf.Log("Updated peer %d next index from %d to %d",
          peer, rf.nextIndex[peer], next)
        rf.nextIndex[peer] = next
      }
      rf.Unlock()
    }()

    cdata := &rf.cdata
    if cdata.Role != Leader {
      rf.Log("Not the leader any more.")
      return false
    }
    if next != reply.NextIndex {
      // Stale reply
      rf.Log("Got a stale reply: %+v", reply)
      return true
    }
    conflictingTerm, firstIndex := reply.ConflictingTerm, reply.FirstLogIndex

    if conflictingTerm < 0 {
      // The follower's log is shorter than the previou probe
      next = firstIndex
      return true
    }

    for next--; next > firstIndex; next-- {
      if cdata.LogEntry(next - 1).Term == conflictingTerm {
        return true
      }
    }
    for next > 0 && cdata.LogEntry(next - 1).Term == conflictingTerm {
      next--
    }
    return true
  }

  for p, _ := range rf.peers {
    if rf.getRole() != Leader {
      return
    }
    if p == rf.me {
      continue
    }
    peer := p
    numRPCs++
    // wg.Add(1)
    go func() {
      sendRecursively(peer)
      // wg.Done()
    }()
  }

  for rf.getRole() == Leader {
    result := rf.MultiWait(replyChan, 0)
    switch {
      case result.timeout:
        rf.assert(false, "Timeout is impossible")
        // Start over
        return
      case result.interrupted:
        // Start over
        return
      default:
        numReplies++
        reply := result.reply
        if rf.updateTerm(reply.Term, false) {
          return
        }
        if reply.Success {
          if reply.AppendedNewEntries > 0 &&
              !updateMatchIndex(reply) {
            return
          }
        } else {
          numRPCs++
          rf.Log("Reacting to reply %+v", reply)
          if !skipConflictingEntries(reply) {
            return
          }
        }
    }
  }
}

func (rf *Raft) SetMaxStateSize(size int) {
  rf.maxStateSize = size
}

func (rf *Raft) RaftStateSize() int {
  rf.Lock()
  defer rf.Unlock()
  return rf.persister.RaftStateSize()
}

func (rf *Raft) CompactLogs(snapshot Bytes, lastAppliedIndex int) {
  rf.Lock()
  defer rf.Unlock()
  cdata := &rf.cdata
  rf.assert(lastAppliedIndex >= cdata.LastCompactedIndex,
    "Snapshot last applied index %d < already compacted last index %d",
    lastAppliedIndex, cdata.LastCompactedIndex)
  cdata.Log = cdata.Log[lastAppliedIndex - cdata.LastCompactedIndex:]
  rf.snapshot = snapshot
  cdata.LastCompactedIndex = lastAppliedIndex
  rf.persist()
  rf.Log("Compacted logs with last compacted index: %d",
    cdata.LastCompactedIndex)
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

  rf.applierWakeup = make(WakeupChan, 1000)
  rf.appliedLogIndex = make(chan int, 1000)
  rf.notifyQ = make(chan bool, 10)
  rf.live = true

  rf.cdata = CoreData {
    CurrentTerm: 1,
    VotedFor: -1,
    // THe first entry is a sentinel.
    Log: make([]LogEntry, 1),
    Role: Follower,
    LastCompactedIndex: 0,
  }

  rf.callbackMap = map[int]func() {
    Follower: rf.onFollower,
    Candidate: rf.onCandidate,
    Leader: rf.onLeader,
  }

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())
  rf.Log("Made raft %+v", rf.cdata)
  rand.Seed(int64(time.Now().Nanosecond()))
  // rand.Seed(int64(1234567))
  rf.wg.Add(1)
  go func() {
    defer rf.Log("Applier is exiting.")
    defer rf.wg.Done()
    for {
      live := <-rf.applierWakeup
      if !live {
        return
      }
      for rf.lastApplied < rf.commitIndex {
        rf.Lock()
        msg := ApplyMsg{
          CommandValid: true,
          CommandIndex: rf.lastApplied + 1,
        }
        term := 0
        rf.assert(rf.lastApplied <= rf.commitIndex,
          "lastApplied should <= rf.commitIndex")
        if rf.lastApplied == rf.commitIndex {
          break
        }
        rf.assert(rf.lastApplied + 1 <= rf.cdata.LastLogIndex(),
          "last applied too large %d > %d.", rf.lastApplied, rf.cdata.LastLogIndex())
        msg.Command = rf.cdata.LogEntry(rf.lastApplied + 1).Cmd
        term = rf.cdata.LogEntry(rf.lastApplied + 1).Term
        rf.lastApplied++
        rf.Unlock()
        rf.Log("Applied %+v at term %d", msg, term)
        sent := false
        for !sent {
          select {
            case rf.applyCh<-msg:
              sent = true
            case live := <-rf.applierWakeup:
              if !live {
                return
              }
          }
        }
      }
    }
  }()

  rf.wg.Add(1)
  go func() {
    defer rf.Log("Exiting main loop")
    defer rf.wg.Done()
    role := -1
    for rf.live {
      newRole := rf.getRole()
      term, _ := rf.GetState()
      rf.Log("Became role %d at term %d", newRole, term)
      role = newRole
      callback, ok := rf.callbackMap[role]
      if !ok {
        break
      }
      callback()
    }
  }()
	return rf
}
