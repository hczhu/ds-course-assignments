package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
  OpPut = 0
  OpAppend = 1
  OpGet = 2
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
  Client int64
  Seq uint64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
  Leader      int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
  Leader      int
	Err         Err
	Value       string
}

type Cmd struct {
  CmdType int
  Key string
  Value string
  Seq uint64
  ClientId int64
  Quit bool
}
