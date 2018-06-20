package shardmaster


import "raft"
import "labrpc"
import "sync"
import "labgob"
import "kvraft"
// import "fmt"
// import "bytes"
import "strconv"
import "time"


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	// applyCh chan raft.ApplyMsg

	// Your data here.
  kvserver *raftkv.KVServer

	configs []Config // indexed by config num
}

func (sm *ShardMaster) getConfig(idx int) Config {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  return sm.configs[idx]
}

func (sm *ShardMaster) configsSize() int {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  return len(sm.configs)
}

func (sm *ShardMaster)rebalance(config *Config) {
  sm.rf.Log("Rebalancing config: %+v", *config)
  nGroups := len(config.Groups)
  if nGroups == 0 {
    for sh, _ := range config.Shards {
      config.Shards[sh] = 0
    }
    return
  }
  base := len(config.Shards) / nGroups
  extra := len(config.Shards) - nGroups * base
  mpCnt := make(map[int]int)
  for k, _ := range config.Groups {
    mpCnt[k] = 0
  }
  pendingShards := make([]int, 0)
  for sh, gid := range config.Shards {
    if _, ok := mpCnt[gid]; ok && (mpCnt[gid] < base || (mpCnt[gid] == base && extra > 0)) {
      mpCnt[gid]++
      if mpCnt[gid] > base {
        extra--
      }
    } else {
      pendingShards = append(pendingShards, sh)
    }
  }
  sm.rf.Log("Pending shards %+v", pendingShards)
  for _, sh := range pendingShards {
    for gid, cnt := range mpCnt {
      if cnt < base || (cnt == base && extra > 0) {
        mpCnt[gid]++
        if mpCnt[gid] > base {
          extra--
        }
        config.Shards[sh] = gid
        break
      }
    }
  }
}

func (sm *ShardMaster)serversJoin(config Config, args JoinArgs) Config {
  newConfig := Config{
    Num: config.Num + 1,
    Shards: config.Shards,
    Groups: make(map[int][]string),
  }
  for k, v := range config.Groups {
    newConfig.Groups[k] = v
  }
  for k, v := range args.Servers {
    newConfig.Groups[k] = v
  }
  sm.rebalance(&newConfig)
  sm.rf.Log("New config from server joining %+v", newConfig)
  return newConfig
}

func (sm *ShardMaster)serversLeave(config Config, args LeaveArgs) Config {
  newConfig := Config{
    Num: config.Num + 1,
    Shards: config.Shards,
    Groups: make(map[int][]string),
  }
  for k, v := range config.Groups {
    found := false
    for _, gid := range args.GIDs {
      if gid == k {
        found = true
      }
    }
    if !found {
      newConfig.Groups[k] = v
    }
  }
  sm.rebalance(&newConfig)
  sm.rf.Log("New config from server leaving %+v", newConfig)
  return newConfig
}

func (sm *ShardMaster)serversMove(config Config, args MoveArgs) Config {
  newConfig := Config{
    Num: config.Num + 1,
    Shards: config.Shards,
    Groups: config.Groups,
  }
  newConfig.Shards[args.Shard] = args.GID
  return newConfig
}

func (sm *ShardMaster) appendCb(value string, ap string)string {
  config := sm.getConfig(0)
  if idx, err := strconv.Atoi(value); err == nil {
    config = sm.getConfig(idx)
  }
  args := decode(ap).Args
  switch args.(type) {
    case JoinArgs:
      config = sm.serversJoin(config, args.(JoinArgs))
    case LeaveArgs:
      config = sm.serversLeave(config, args.(LeaveArgs))
    case MoveArgs:
      config = sm.serversMove(config, args.(MoveArgs))
    default:
      sm.rf.Log("Unknown args %+v", args)
      return value
  }

  sm.mu.Lock()
  defer sm.mu.Unlock()
  sm.configs = append(sm.configs, config)
  return strconv.Itoa(len(sm.configs) - 1)
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
  sm.rf.Assert(false, "Not implemented")
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
  sm.rf.Assert(false, "Not implemented")
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
  sm.rf.Assert(false, "Not implemented")
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
  if _, isLeader := sm.rf.GetState(); !isLeader {
    reply.WrongLeader = true
    return
  }
  configsSize := sm.configsSize()
  if args.Num >= configsSize || args.Num < 0{
    args.Num = configsSize - 1
  }
  reply.Config = sm.getConfig(args.Num)
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
  sm.kvserver.Kill()
	// sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	// sm.applyCh = make(chan raft.ApplyMsg)

	// Your code here.
  sm.kvserver = raftkv.StartKVServer(
    servers,
    me,
    persister,
    -1,
  )
	sm.rf = sm.kvserver.GetRaft()
  sm.kvserver.SetAppendCb(
    func(value string, ap string) string {
      return sm.appendCb(value, ap)
    },
  )
  // Wait for leader election
  time.Sleep(200 * time.Millisecond)
	return sm
}
