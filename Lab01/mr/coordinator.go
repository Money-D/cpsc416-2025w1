package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Task struct {
	Type              string   // "map" or "reduce"
	FileName          string   // only for map tasks
	NReduce           int      // only for map tasks
	IntermediateFiles []string // only for reduce tasks
	ReduceId          int      // only for reduce tasks
	StartTime         time.Time
	InProgress        bool
	Done              bool
	Wid               int // worker id
}

type WorkerStatus struct {
	Alive       bool
	CurrentTask *Task
	LastSeen    time.Time
}
type Coordinator struct {
	mu           sync.Mutex
	nextWorkerId int
	workers      map[int]*WorkerStatus
	mapTasks     map[string]*Task
	reduceTasks  map[int]*Task
	nReduce      int
	nextMapId    int
}

// TODO Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RegisterRPC(args *RegisterArgs, reply *RegisterReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// assign worker id
	newWorkerId := c.nextWorkerId
	c.nextWorkerId++

	status := &WorkerStatus{
		Alive:       true,
		CurrentTask: nil,
		LastSeen:    time.Now(),
	}
	c.workers[newWorkerId] = status
	reply.Wid = newWorkerId

	return nil
}

func (c *Coordinator) RequestTask(args *TaskRequestArgs, reply *TaskRequestReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// TODO what if you receive task comple from crashed worker?
	worker, exists := c.workers[args.Wid]
	if !exists {
		return fmt.Errorf("Worker %v not found", args.Wid)
	}

	// Check if there are any available map tasks
	for _, task := range c.mapTasks {
		if !task.Done && !task.InProgress {
			reply.Type = "map"
			reply.FileName = task.FileName
			reply.NReduce = c.nReduce
			reply.MapId = c.nextMapId
			c.nextMapId++

			task.InProgress = true
			task.StartTime = time.Now()
			task.Wid = args.Wid

			worker.CurrentTask = task
			return nil
		}
	}

	if !c.mapPhaseDone() {
		reply.Type = "wait"
		return nil
	}

	// Check for reduce tasks
	for _, task := range c.reduceTasks {
		if !task.Done && !task.InProgress {
			reply.Type = "reduce"
			reply.ReduceId = task.ReduceId
			reply.IntermediateFiles = task.IntermediateFiles

			task.InProgress = true
			task.StartTime = time.Now()
			task.Wid = args.Wid

			worker.CurrentTask = task
			return nil
		}
	}

	if !c.reducePhaseDone() {
		reply.Type = "wait"
		return nil
	}

	reply.Type = "exit"
	return nil
}

func (c *Coordinator) TaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	// TODO handle slow or crashed worker
	c.mu.Lock()
	defer c.mu.Unlock()

	workerId := args.Wid
	worker, exists := c.workers[workerId]
	if !exists {
		return fmt.Errorf("Worker %v not found", workerId)
	}

	task := worker.CurrentTask
	if task == nil {
		for _, fname := range args.InterFileNames {
			if err := os.Remove(fname); err != nil {
				fmt.Printf("Failed to Delete %s: %v\n", fname, err)
			}
		}
		return nil
	}
	fmt.Printf("task complete\n")

	if task.Type == "map" {
		for _, fname := range args.InterFileNames {
			parts := strings.Split(filepath.Base(fname), "-")
			reduceId, err := strconv.Atoi(parts[len(parts)-1])
			if err != nil || reduceId < 0 || reduceId >= c.nReduce {
				return fmt.Errorf("bad intermediate filename %q: %v", fname, err)
			}
			c.reduceTasks[reduceId].IntermediateFiles = append(c.reduceTasks[reduceId].IntermediateFiles, fname)
		}
	} else {
		for _, fname := range args.InterFileNames {
			parts := strings.Split(filepath.Base(fname), "_")
			finalOutputFname := parts[0]
			os.Rename(fname, finalOutputFname)
		}
	}

	task.Done = true
	task.InProgress = false
	worker.CurrentTask = nil
	worker.LastSeen = time.Now()

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
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.reducePhaseDone()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		workers:      make(map[int]*WorkerStatus),
		mapTasks:     make(map[string]*Task),
		reduceTasks:  make(map[int]*Task),
		nextWorkerId: 1,
		nReduce:      nReduce,
		nextMapId:    0,
	}

	c.initMapTasks(files)
	c.initReduceTasks(files, nReduce)
	c.server()

	// go c.mapTasksMonitor()
	// go c.reduceTasksMonitor()
	go c.heartbeatMonitor()
	return &c
}

func (c *Coordinator) heartbeatMonitor() {
	for {
		c.checkExpiredTasks()
		time.Sleep(1 * time.Second)
	}
}

func (c *Coordinator) checkExpiredTasks() {
	curTime := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, workerStatus := range c.workers {
		if workerStatus.CurrentTask != nil {
			workerMissing := curTime.Sub(workerStatus.LastSeen) > 10*time.Second
			isTaskExpired := curTime.Sub(workerStatus.CurrentTask.StartTime) > 10*time.Second

			if workerMissing || isTaskExpired {
				fmt.Printf("expiring task\n")
				expiredTask := workerStatus.CurrentTask
				expiredTask.StartTime = time.Time{}
				expiredTask.InProgress = false
				expiredTask.Done = false

				workerStatus.CurrentTask = nil
			}
		}
	}
}

func (c *Coordinator) initMapTasks(files []string) {
	for _, filename := range files {
		c.mapTasks[filename] = &Task{
			Type:     "map",
			FileName: filename,
			Done:     false,
		}
	}
}

func (c *Coordinator) PingCoordinator(args *HeartbeatArgs, reply *HeartbeatReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	workerStatus, exists := c.workers[args.Wid]
	if exists {
		workerStatus.LastSeen = time.Now()
	}
	return nil
}

func (c *Coordinator) initReduceTasks(files []string, nReduce int) {
	for i := range nReduce {
		interFiles := []string{}
		c.reduceTasks[i] = &Task{
			Type:              "reduce",
			ReduceId:          i,
			Done:              false,
			IntermediateFiles: interFiles,
		}
	}
}

func (c *Coordinator) mapTasksMonitor() {
	for {
		c.mu.Lock()
		for _, task := range c.mapTasks {

			if task.InProgress && !task.Done {
				currTime := time.Now()
				if currTime.Sub(task.StartTime) > 10*time.Second {
					task.InProgress = false
					task.StartTime = time.Time{}
					worker, exists := c.workers[task.Wid]
					if exists {
						worker.CurrentTask = nil
					}
				}
			}
		}
		c.mu.Unlock()
		time.Sleep(time.Second * 1)

	}
}

func (c *Coordinator) reduceTasksMonitor() {
	for {
		c.mu.Lock()
		for _, task := range c.reduceTasks {
			if task.InProgress && !task.Done {
				currTime := time.Now()
				if currTime.Sub(task.StartTime) > 10*time.Second {
					task.InProgress = false
					task.StartTime = time.Time{}
					worker, exists := c.workers[task.Wid]
					if exists {
						worker.CurrentTask = nil
					}
				}
			}
		}
		c.mu.Unlock()
		time.Sleep(time.Second * 1)

	}
}

func (c *Coordinator) mapPhaseDone() bool {
	for _, task := range c.mapTasks {
		if !task.Done {
			return false
		}
	}
	return true
}

func (c *Coordinator) reducePhaseDone() bool {
	for _, task := range c.reduceTasks {
		if !task.Done {
			return false
		}
	}
	return true
}
