package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
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

// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))

// JobComplete RPC handler
func (wk *Workers) JobComplete(args *ExampleArgs, reply *ExampleReply) error {
	return nil
}

// start a thread that listens for RPCs from coordinator.go
func (wk *Workers) server() {
	rpc.Register(wk)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Send RPC to register worker and get Wid from
// coordinator
func (wk *Workers) RegisterWorker() {
	args := RegisterArgs{}
	reply := RegisterReply{}
	ok := call("Coordinator.RegisterRPC", &args, &reply)
	if ok {
		wk.Wid = reply.Wid
	} else {
		fmt.Printf("Register Worker failed\n")
	}
}

// Send RPC to request a task from coordinator
func (wk *Workers) RequestTask() *TaskRequestReply {
	args := TaskRequestArgs{}
	args.Wid = wk.Wid

	reply := &TaskRequestReply{}

	ok := call("Coordinator.TaskRPC", &args, &reply)
	if ok {
		return reply
	} else {
		fmt.Println("Request task failed")
		return nil
	}
}

// Execute the Map Task
func (wk *Workers) MapTask(fileName string, nReduce int,
	mapf func(string, string) []KeyValue) error {
	// From mrsequential.go
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("Open file %v failed", fileName)
		return err
	}
	content, err := os.ReadFile(fileName)
	if err != nil {
		log.Fatalf("Read file %v failed:", fileName)
		return err
	}
	file.Close()
	kva := mapf(fileName, string(content))

	encoders := make([]*json.Encoder, nReduce)
	files := make([]*os.File, nReduce)

	for i := range nReduce {
		intermediateName := fmt.Sprintf("mr-%s-%d", fileName, i)

		file, err := os.Create(intermediateName)
		if err != nil {
			log.Fatalf("Create file %v failed", intermediateName)
			return err
		}
		defer file.Close()

		files[i] = file
		encoders[i] = json.NewEncoder(file)
	}

	for _, kv := range kva {
		reduceTask := ihash(kv.Key) % nReduce
		if err := encoders[reduceTask].Encode(&kv); err != nil {
			log.Fatalf("Encode kv %v failed", kv)
			return err
		}
	}

	for _, f := range files {
		f.Close()
	}

	return nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	wk := new(Workers)
	wk.server()

	wk.RegisterWorker()

	for {
		reply := wk.RequestTask()
		if reply == nil {
			break
		}
		switch reply.Type {
		case "map":
			fmt.Printf("Worker: %d | Map Task Input File: %s", wk.Wid, reply.FileName)
			err := wk.MapTask(reply.FileName, reply.NReduce, mapf)
			if err != nil {
				// what does worker do when failing the task?
			}
		case "reduce":
		case "wait":
			fmt.Printf("Worker %d no incoming task, waiting \n", wk.Wid)
			time.Sleep(time.Second)
		case "exit":
			fmt.Printf("Worker: %d shutting down \n", wk.Wid)
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

func pingCoordinator(workerID int64) {
	args := HeartbeatArgs{}
	args.Wid = int(workerID)

	reply := HeartbeatReply{}

	ok := call("Coordinator.pingCoordinator", &args, &reply)
	if !ok {
		fmt.Printf("ping failed\n")
	}
}
