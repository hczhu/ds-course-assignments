package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "kvraft"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
  kvClerk *raftkv.Clerk
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
	// Your code here.
  ck.kvClerk = raftkv.MakeClerk(servers)
	return ck
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
  ck.kvClerk.Get(CONFIG_KEY)

	args := &QueryArgs{}
	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	args.Servers = servers
	// Your code here.
  ck.kvClerk.PutAppend(
    CONFIG_KEY,
    encode(
      Op{
        Args: args,
      },
    ),
    "Append",
  )
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids

  ck.kvClerk.PutAppend(
    CONFIG_KEY,
    encode(
      Op{
        Args: args,
      },
    ),
    "Append",
  )
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

  ck.kvClerk.PutAppend(
    CONFIG_KEY,
    encode(
      Op{
        Args: args,
      },
    ),
    "Append",
  )
}
