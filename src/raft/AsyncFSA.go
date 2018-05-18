package raft
// package main

import "time"
import "sync"
import "fmt"
import "log"

type Callback func(af *AsyncFSA)int
type Logger func(string, ...interface {})
type CdataCob func(cdata *CoreData)

const (
  StopState = 123456789
)

type AsyncFSA struct {
  transMap map[int]Callback
  msgQ chan int

  wg sync.WaitGroup
  logger Logger

	sync.RWMutex
  cdata CoreData
}

func MakeAsyncFSA(cdata CoreData) *AsyncFSA {
  af := &AsyncFSA{}
  af.transMap = make(map[int]Callback)
  af.msgQ = make(chan int, 100)
  af.cdata = cdata
  af.logger = func(format string, v ...interface{}) {
    fmt.Println(fmt.Sprintf(format, v...))
  }
  return af
}

func (af *AsyncFSA)WithRlock(callback CdataCob) {
  af.RLock()
  defer af.RUnlock()
  callback(af.st, &af.cdata)
}

func (af *AsyncFSA)WithLock(callback CdataCob) {
  af.Lock()
  defer af.Unlock()
  callback(af.st, &af.cdata)
}

func (af *AsyncFSA)SetLogger(logger Logger) *AsyncFSA{
  af.logger = logger
  return af
}

func (af *AsyncFSA)AddCallback(st int, callback Callback) *AsyncFSA{
  af.transMap[st] = callback
  return af
}

func (af *AsyncFSA)Start() {
  af.wg.Add(1)
  go func() {
    // This is the only thread which modifies the internal state
    // of AsyncFSA.
    role := StopState
    af.WithRlock(func(cdata *CoreData) {
      role = cdata.role
    })
    for {
      callback, ok := af.transMap[role]
      if !ok {
        af.logger("Exiting AsyncFSA.")
        break
      }
      oldRole := role
      callback(af)
      af.WithRlock(func(cdata *CoreData) {
        role = cdata.role
      })
      if oldRole != role {
        af.logger("AsyncFSA transited to state: %d", role)
      }
    }
    af.wg.Done()
  }()
}

func (af *AsyncFSA)Stop() {
  af.WithLock(func(cdata *CoreData) {
    cdata.role = Stop
  })
  af.msgQ<-StopState
  af.Wait()
}

func (af *AsyncFSA)Wait() {
  af.wg.Wait()
}

func (af *AsyncFSA)Transit(st int) {
  af.msgQ<-st
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
