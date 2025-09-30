package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type Workers struct {
	Wid int
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Send RPC to register worker and get Wid from
// coordinator
func (wk *Workers) RegisterWorker() error {
	args := RegisterArgs{}
	reply := RegisterReply{}
	ok := call("Coordinator.RegisterRPC", &args, &reply)
	if ok {
		wk.Wid = reply.Wid
	} else {
		return fmt.Errorf("RegisterWorker failed")
	}
	return nil
}

// Send RPC to request a task from coordinator
func (wk *Workers) RequestTask() *TaskRequestReply {
	args := TaskRequestArgs{}
	args.Wid = wk.Wid

	reply := &TaskRequestReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		return reply
	} else {
		return nil
	}
}

func TaskComplete(wid int, fileNames []string) error {
	args := TaskCompleteArgs{}
	args.Wid = wid
	args.InterFileNames = fileNames
	reply := TaskCompleteReply{}

	ok := call("Coordinator.TaskComplete", &args, &reply)
	if ok {
		return nil
	} else {
		return fmt.Errorf("Task complete RPC failed")
	}
}

// Execute the Map Task
func (wk *Workers) MapTask(fileName string, nReduce int, mapId int,
	mapf func(string, string) []KeyValue) ([]string, error) {
	// From mrsequential.go
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("Open file %v failed", fileName)
		return nil, err
	}
	content, err := os.ReadFile(fileName)
	if err != nil {
		log.Fatalf("Read file %v failed:", fileName)
		return nil, err
	}
	file.Close()
	kvs := mapf(fileName, string(content))

	encoders := make([]*json.Encoder, nReduce)
	files := make([]*os.File, nReduce)
	names := make([]string, nReduce)

	for i := range nReduce {
		intermediateName := fmt.Sprintf("mr-%d-%d", mapId, i)

		file, err := os.Create(intermediateName)
		if err != nil {
			log.Fatalf("Create file %v failed", intermediateName)
			return nil, err
		}
		defer file.Close()

		files[i] = file
		encoders[i] = json.NewEncoder(file)
		names[i] = intermediateName
	}

	for _, kv := range kvs {
		reduceTask := ihash(kv.Key) % nReduce
		if err := encoders[reduceTask].Encode(&kv); err != nil {
			log.Fatalf("Encode kv %v failed", kv)
			return nil, err
		}
	}

	for _, f := range files {
		f.Close()
	}

	return names, nil
}

// Excute the Reduce Task
func (wk *Workers) ReduceTask(reduceId int, intermediateFiles []string,
	reducef func(string, []string) string) (string, error) {
	kvs := make(map[string][]string)

	for _, fileName := range intermediateFiles {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("Open file %v failed", fileName)
			return "", err
		}

		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				if err == io.EOF {
					break
				}
				file.Close()
				log.Fatalf("Decode kv %v failed", kv)
				return "", err
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
		file.Close()
	}

	keys := make([]string, 0, len(kvs))
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	intermediateName := fmt.Sprintf("mr-out-%d_%d", reduceId, wk.Wid)
	outputFile, err := os.Create(intermediateName)
	if err != nil {
		log.Fatalf("Create output file for reduce task %d failed", reduceId)
		return "", err
	}
	defer outputFile.Close()

	for _, k := range keys {
		output := reducef(k, kvs[k])
		fmt.Fprintf(outputFile, "%v %v\n", k, output)
	}

	return intermediateName, nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	wk := new(Workers)

	if err := wk.RegisterWorker(); err != nil {
		return
	}
	go continuousPing(wk.Wid)

	for {
		reply := wk.RequestTask()
		if reply == nil {
			break
		}
		switch reply.Type {
		case "map":
			names, err := wk.MapTask(reply.FileName, reply.NReduce, reply.MapId, mapf)
			if err != nil {
				return
			}
			err = TaskComplete(wk.Wid, names)
			if err != nil {
				return
			}
		case "reduce":
			name, err := wk.ReduceTask(reply.ReduceId, reply.IntermediateFiles, reducef)
			if err != nil {
				return
			}
			err = TaskComplete(wk.Wid, []string{name})
			if err != nil {
				return
			}
		case "wait":
			time.Sleep(time.Second)
		case "exit":
			return
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func continuousPing(workerID int) {
	for {
		pingCoordinator(workerID)
		time.Sleep(1 * time.Second)
	}
}

func pingCoordinator(workerID int) {
	args := HeartbeatArgs{}
	args.Wid = workerID

	reply := HeartbeatReply{}

	ok := call("Coordinator.PingCoordinator", &args, &reply)
	if !ok {
		fmt.Printf("ping failed\n")
	}
}
