package mapreduce

import (
  "container/heap"
  "encoding/json"
  "os"
  "log"
)

	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	hp := &Heap{}
  var decoders []*json.Decoder
  for mapTask := 0; mapTask < nMap; mapTask++ {
    name := reduceName(jobName, mapTask, reduceTask)
    input, err := os.Open(name)
    if err != nil {
      log.Fatal("Failed to open the input file ", name, " due to: ", err)
    }
    defer input.Close()
    decoder := json.NewDecoder(input);
    decoders = append(decoders, decoder)
    var kv KeyValue
    err = decoder.Decode(&kv)
    if err != nil {
      debug("Failed to decode a KV from file:%s\n ", name)
      continue
    }
    hp.KVs = append(hp.KVs, kv)
    hp.Hp = append(hp.Hp, mapTask)
  }
	heap.Init(hp)
	f, err := os.Create(outFile)
  if err != nil {
	 log.Fatal("Failed to create file ", outFile, "  due to: ", err)
  }
  defer f.Close()
  encoder := json.NewEncoder(f)
  for hp.Len() > 0 {
    r := heap.Pop(hp).(int)
    err := encoder.Encode(&hp.KVs[r])
    if err != nil {
      log.Fatal("Failed to encode a KV ", hp.KVs[r], " to file: ", outFile)
    }
    err = decoders[r].Decode(&hp.KVs[r])
    if err == nil {
      heap.Push(hp, r)
    }
  }
}

// An IntHeap is a min-heap of ints.
type Heap struct {
  Hp []int
  KVs []KeyValue
}

func (h Heap) Len() int {
   return len(h.Hp)
}

func (h Heap) Less(i, j int) bool {
  a := h.Hp[i]
  b := h.Hp[j]
  return h.KVs[a].Key  < h.KVs[b].Key
}

func (h Heap) Swap(i, j int)      {
  h.Hp[i], h.Hp[j] = h.Hp[j], h.Hp[i]
}

func (h *Heap) Push(x interface{}) {
	h.Hp = append(h.Hp, x.(int))
}

func (h *Heap) Pop() interface{} {
	old := h.Hp
	n := len(old)
	x := old[n-1]
	h.Hp = old[0 : n-1]
	return x
}
