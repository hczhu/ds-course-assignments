package raft
// package main

import "time"
import "sync"
import "fmt"
import "log"

type Callback func(af *AsyncFSA)int
type Gchan chan []byte
type Bytes []byte
type Logger func(string, ...interface {})

const (
  StopState = 123456789
)

type AsyncFSA struct {
  transMap map[int]Callback
  msgQ chan int

  wg sync.WaitGroup

	mx sync.RWMutex
  st int

  logger Logger
}

func MakeAsyncFSA(initStat int) *AsyncFSA {
  af := &AsyncFSA{}
  af.transMap = make(map[int]Callback)
  af.msgQ = make(chan int, 100)
  af.st = initStat
  af.logger = func(format string, v ...interface{}) {
    fmt.Println(fmt.Sprintf(format, v...))
  }
  return af
}

func (af *AsyncFSA)SetLogger(logger Logger) *AsyncFSA{
  af.logger = logger
  return af
}

func (af *AsyncFSA)AddCallback(st int, callback Callback) *AsyncFSA{
  af.transMap[st] = callback
  return af
}

func (af *AsyncFSA)GetState() int {
  af.mx.RLock()
  defer af.mx.RUnlock()
  return af.st
}

func (af *AsyncFSA)setState(st int) {
  af.mx.Lock()
  af.st = st
  af.mx.Unlock()
}

func (af *AsyncFSA)Start() {
  af.wg.Add(1)
  go func() {
    // This is the only thread which modifies the internal state
    // of AsyncFSA.
    for {
      st := af.GetState()
      callback, ok := af.transMap[st]
      if !ok {
        af.logger("Exiting AsyncFSA.")
        break
      }
      nextSt := callback(af)
      af.setState(nextSt)
      if nextSt != st {
        af.logger("AsyncFSA transited to state: %d", nextSt)
      }
    }
    af.wg.Done()
  }()
}

func (af *AsyncFSA)Stop() {
  af.msgQ<- StopState
  af.Wait()
}

func (af *AsyncFSA)Wait() {
  af.wg.Wait()
}

func (af *AsyncFSA)Transit(st int) {
  af.msgQ<-st
}

// Return -1, if timeout
func (af *AsyncFSA) MultiWait(gchan Gchan, timeout time.Duration) (
  bool, Bytes, int) {
  if timeout <= 0 {
    timeout = time.Duration(100000) * time.Hour
  }
  return af.MultiWaitCh(gchan, time.After(timeout))
}

func (af *AsyncFSA) MultiWaitCh(gchan Gchan, toCh <-chan time.Time) (
  bool, Bytes, int) {
  if gchan == nil {
    gchan = make(Gchan)
  }
  select {
    case <-toCh:
      return true, nil, -1
    case nextState := <-af.msgQ:
      return false, nil, nextState
    case gv := <-gchan:
      return false, gv, -1
  }
  log.Fatal("Shouldn't reach here")
  return false, nil, -1
}

/*
func main() {
  callback := func(af *AsyncFSA) int {
    st := af.GetState()
    switch(st) {
      case 0:
        return 1
      case 1:
        timeout, _, nextSt := af.MultiWait(nil, 3 * time.Second)
        if timeout {
          return 2
        }
        return nextSt
      case 2:
        af.MultiWait(nil, 1 * time.Second)
        return -1
    }
    return -1
  }
  af := MakeAsyncFSA(0)
  af.AddCallback(0, callback).AddCallback(1, callback).AddCallback(2, callback).Start()
  for r := 0; r < 5; r++ {
    time.Sleep(2 * time.Second)
    af.Transit(0)
  }
  af.Wait()
}
*/
