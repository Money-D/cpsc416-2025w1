package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu            sync.Mutex
	workerCounter int64
	workers       map[int64]*WorkerStatus
	mapTasks      map[string]*Task
	reduceTasks   map[int]*Task
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}

func (c *Coordinator) heartbeatMonitor() {
	for {
		c.checkExpiredTasks()
		time.Sleep(1 * time.Second)
	}
}

func (c *Coordinator) checkExpiredTasks() {
	curTIme := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, workerStatus := range c.workers {
		if workerStatus.CurrentTask != nil {
			workerMissing := curTIme.Sub(workerStatus.LastSeen) > 10*time.Second
			isTaskExpired := curTIme.Sub(workerStatus.CurrentTask.StartTime) > 10*time.Second

			if workerMissing || isTaskExpired {
				expiredTask := workerStatus.CurrentTask
				expiredTask.StartTime = time.Time{}
				expiredTask.Completed = false

				workerStatus.CurrentTask = nil
			}

		}
	}
}

func (c *Coordinator) pingCoordinator(args *HeartbeatArgs, reply *HeartbeatReply) {
	c.mu.Lock()
	defer c.mu.Unlock()
	workerStatus, exists := c.workers[int64(args.Wid)]
	if exists {
		workerStatus.LastSeen = time.Now()
	}
	return
}
