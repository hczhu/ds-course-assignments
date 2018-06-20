package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync/atomic"
import "time"
// import "fmt"


type Clerk struct {
	servers []*labrpc.ClientEnd
  leader int
  client int64
  seq uint64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
  ck.client = nrand()
  ck.leader = 0
	return ck
}

func (ck *Clerk) tryAllServers(f func(serverId int)bool) {
  for {
    visited := make([]bool, len(ck.servers))
    for nextServer := 0; nextServer < len(ck.servers); {
      server := ck.leader
      if server < 0 || visited[server] {
        server = nextServer
        nextServer++
      }
      if !visited[server] {
        visited[server] = true
        if f(server) {
          ck.leader = server
          return
        }
      }
    }
    time.Sleep(100 * time.Millisecond)
    // fmt.Println("Tried all servers. No luck.")
  }
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
  // fmt.Println("Getting key:", key)
  args := GetArgs{
    Key: key,
    Client: ck.client,
    Seq: atomic.AddUint64(&ck.seq, 1),
  }
  var reply GetReply
  ck.tryAllServers(func(server int) bool {
    reply = GetReply{}
    ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
    if ok {
      // fmt.Printf("RPC Get got reply %+v\n", reply)
      if reply.WrongLeader {
        ck.leader = reply.Leader
        return false
      }
      if reply.Err == "" || reply.Err == ErrNoKey {
        return true
      }
    } else {
      // fmt.Printf("RPC timeout: %+v\n", args)
    }
    return false
  })
  return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
  args := PutAppendArgs{
    Key: key,
    Value: value,
    Op: op,
    Client: ck.client,
    Seq: atomic.AddUint64(&ck.seq, 1),
  }
  ck.tryAllServers(func(server int) bool {
    reply := PutAppendReply{}
    ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
    if ok {
      // fmt.Printf("PutAppend got reply %+v\n", reply)
      if reply.WrongLeader {
        ck.leader = reply.Leader
        return false
      } else if reply.Err == "" {
        return true
      }
    }
    return false
  })
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
