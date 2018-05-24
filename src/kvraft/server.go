package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
  "time"
  "fmt"
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
}

func (kv *KVServer) commitOne(cmd Cmd) int {
  logIndex, term, isLeader := kv.rf.Start(cmd)
  if !isLeader {
    return ErrWrongLeader
  }
  ret := ErrCommitTimeout
  defer func() {
    fmt.Printf("Committing log item: %+v at index %d at term %d status: %d.\n",
      cmd, logIndex, term, ret)
  }()
  <-time.After(CommitTimeout)
  rfTerm, leader := kv.rf.GetState()
  if term == rfTerm && leader && kv.lastAppliedIndex >= logIndex {
    ret = Success
  }
  return ret
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
  cmd := Cmd{
    CmdType: OpGet,
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
  v, ok := kv.kvMap[args.Key]
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
	kv.rf.Kill()
  fmt.Println("Killed raft")
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
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

  kv.lastAppliedIndex = 0
  kv.kvMap = make(map[string]string)
  kv.clientLastSeq = make(map[int64]uint64)

  kv.wg.Add(1)
  go func() {
    dupCmds := 0
    defer fmt.Printf("Exiting KV applier with %d duplicate cmds\n.", dupCmds)
    defer kv.wg.Done()
    for {
      msg := <-kv.applyCh
      cmd := msg.Command.(Cmd)
      if cmd.Quit {
        return
      }
      kv.lastAppliedIndex = msg.CommandIndex
      if cmd.Seq <= kv.clientLastSeq[cmd.ClientId] {
        dupCmds++
        continue
      }
      kv.clientLastSeq[cmd.ClientId] = cmd.Seq
      switch cmd.CmdType {
        case OpPut:
          kv.kvMap[cmd.Key] = cmd.Value
        case OpAppend:
          kv.kvMap[cmd.Key] += cmd.Value
      }
      fmt.Printf("Applied %+v\n", msg)
    }
  }()
	return kv
}
