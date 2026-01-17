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

type TaskType string

const (
	TaskTypeMap    = "MapTask"
	TaskTypeReduce = "ReduceTask"
	TaskTypeNone   = "None"
)

type RequestTaskArgs struct{}

type DoMapTask struct {
	InputFile string
	Buckets   int64
}

type DoReduceTask struct {
	Bucket int64
	Files  []string
}

type RequestTaskReply struct {
	TaskType   TaskType
	TaskId     string
	MapTask    DoMapTask
	ReduceTask DoReduceTask
}

type DoneMapTask struct {
	InputFile         string
	IntermediateFiles []string
}

type DoneReduceTask struct {
	Bucket int64
}

type CompleteTaskArgs struct {
	TaskType   TaskType
	TaskId     string
	MapTask    DoneMapTask
	ReduceTask DoneReduceTask
}

type CompleteTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
