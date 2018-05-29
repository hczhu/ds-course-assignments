package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
  "time"
  "bytes"
  // "fmt"
  // "sync/atomic"
)

const (
  Debug = 0
  CommitTimeout = time.Duration(500) * time.Millisecond
  Success = 0
  ErrWrongLeader = 1
  ErrCommitTimeout = 2
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Snapshot struct {
  LastAppliedIndex int
  KvMap map[string]string
  ClientLastSeq map[int64]uint64
}

type KVServer struct {
	sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

  lastAppliedIndex int

  kvMap map[string]string
  clientLastSeq map[int64]uint64

  wg sync.WaitGroup

  callerCh chan bool
}

func (kv *KVServer) commitOne(cmd Cmd) int {
  kv.rf.Log("Trying to commit %+v", cmd)
  logIndex, term, isLeader := kv.rf.Start(cmd)
  if !isLeader {
    return ErrWrongLeader
  }
  kv.RLock()
  lastSeq := kv.clientLastSeq[cmd.ClientId]
  kv.RUnlock()
  if cmd.Seq <= lastSeq {
    kv.rf.Log("Duplicate cmd: %+v", cmd)
    return Success
  }
  ret := ErrCommitTimeout
  defer func() {
    kv.rf.Log("Committing log item: %+v at index %d at term %d status: %d.\n",
      cmd, logIndex, term, ret)
  }()
  to := time.After(CommitTimeout)
  for {
    timeout := false
    select {
      case <-to:
        kv.rf.Log("committing timeouted")
        timeout = true
      case <-kv.callerCh:
        kv.rf.Log("Waken up")
    }
    rfTerm, leader := kv.rf.GetState()
    if term != rfTerm || !leader {
      return ret
    }
    if kv.lastAppliedIndex >= logIndex {
      ret = Success
      return ret
    }
    if timeout {
      break
    }
  }
  return ret
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
  cmd := Cmd{
    CmdType: OpGet,
    Seq: args.Seq,
    ClientId: args.Client,
  }
  ret := kv.commitOne(cmd)
  if ret == ErrWrongLeader {
    reply.WrongLeader = true
    reply.Leader = kv.rf.GetLeader()
    return
  } else if ret == ErrCommitTimeout {
    reply.Err = "Commit timeout"
    return
  }
  kv.RLock()
  v, ok := kv.kvMap[args.Key]
  kv.RUnlock()
  if ok {
    reply.Value = v
  } else {
    reply.Err = ErrNoKey
  }
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
  cmd := Cmd{
    CmdType: OpPut,
    Key: args.Key,
    Value: args.Value,
    Seq: args.Seq,
    ClientId: args.Client,
  }
  if args.Op == "Append" {
    cmd.CmdType = OpAppend
  }
  ret := kv.commitOne(cmd)
  if ret == ErrWrongLeader {
    reply.Leader = kv.rf.GetLeader()
    reply.WrongLeader = true
  } else if ret == ErrCommitTimeout {
    reply.Err = "Commit timeout"
  }
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
  kv.rf.Log("Killing raft")
	kv.rf.Kill()
  kv.rf.Log("Killed raft")
  kv.applyCh <- raft.ApplyMsg {
    Command: Cmd {
      Quit: true,
    },
  }
  kv.wg.Wait()
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
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(raft.AppendEntriesArgs{})
	labgob.Register(Cmd{})
	labgob.Register(raft.LogEntry{})
	labgob.Register(raft.ApplyMsg{})

	kv := new(KVServer)
	kv.me = me
  maxraftstate = 1
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
  kv.rf.SetMaxStateSize(maxraftstate)

  kv.lastAppliedIndex = 0
  kv.kvMap = make(map[string]string)
  kv.clientLastSeq = make(map[int64]uint64)
  kv.callerCh = make(chan bool, 1000)
  // kv.cond = sync.NewCond(&sync.Mutex{})

  kv.wg.Add(1)
  go func() {
    dupCmds := 0
    defer kv.rf.Log("Exiting KV applier with %d duplicate cmds\n.", dupCmds)
    defer kv.wg.Done()
    snapshot := func() []byte{
      ss := Snapshot{
        LastAppliedIndex: kv.lastAppliedIndex,
        KvMap: kv.kvMap,
        ClientLastSeq: kv.clientLastSeq,
      }
      w := new(bytes.Buffer)
      e := labgob.NewEncoder(w)
      e.Encode(ss)
	    return w.Bytes()
    }
    installSnapshot := func(data []byte) {
	    r := bytes.NewBuffer(data)
	    d := labgob.NewDecoder(r)
      ss := Snapshot{}
      d.Decode(&ss)
      kv.lastAppliedIndex = ss.LastAppliedIndex
      kv.kvMap = ss.KvMap
      kv.clientLastSeq = ss.ClientLastSeq
      kv.rf.Log("Installed snapshot with last applied index: %d",
        ss.LastAppliedIndex)
    }

    // kv.maxraftstate = 1
    run := func() bool {
      msg := <-kv.applyCh
      defer func() {
        if _, isLeader := kv.rf.GetState(); isLeader {
          kv.rf.Log("Waking up the caller.")
          kv.callerCh<-true
        }
      }()
      if msg.InstallSnapshot {
        kv.Lock()
        defer kv.Unlock()
        installSnapshot(msg.Command.(raft.Bytes))
        return true
      }
      cmd := msg.Command.(Cmd)
      if cmd.Quit {
        return false
      }
      kv.Lock()
      if cmd.Seq <= kv.clientLastSeq[cmd.ClientId] {
        dupCmds++
      } else {
        switch cmd.CmdType {
          case OpPut:
            kv.kvMap[cmd.Key] = cmd.Value
          case OpAppend:
            kv.kvMap[cmd.Key] += cmd.Value
          case OpGet:
        }
        kv.clientLastSeq[cmd.ClientId] = cmd.Seq
      }
      kv.lastAppliedIndex = msg.CommandIndex
      kv.rf.Log("Applied %+v\n", msg)
      var ss []byte
      if kv.maxraftstate > 0 && kv.rf.RaftStateSize() > kv.maxraftstate {
        ss = snapshot()
      }
      kv.Unlock()
      if ss != nil {
        kv.rf.Log("Compacting raft logs...")
        kv.rf.CompactLogs(ss, kv.lastAppliedIndex)
      }
      return true
    }
    for run() { }
  }()
	return kv
}
