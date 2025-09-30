package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type RegisterArgs struct{}

type RegisterReply struct {
	Wid int
}

type TaskRequestArgs struct {
	Wid int
}
type TaskRequestReply struct {
	Type              string   // "map", "reduce", "wait", "exit"
	FileName          string   // only for map tasks
	NReduce           int      // only for map tasks
	ReduceId          int      // only for reduce tasks
	IntermediateFiles []string // only for reduce tasks
	MapId             int      // only for map tasks
}

type TaskCompleteArgs struct {
	Wid            int
	InterFileNames []string
}
type TaskCompleteReply struct {
}

type HeartbeatArgs struct {
	Wid int
}
type HeartbeatReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
func coordinatorSock() string {
	s := "/var/tmp/416-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
