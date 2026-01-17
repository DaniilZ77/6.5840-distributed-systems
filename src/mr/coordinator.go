package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

type taskStatus string

const (
	statusIdle    taskStatus = "idle"
	statusRunning taskStatus = "running"
	statusDone    taskStatus = "done"

	deadThreshold   = 10 * time.Second
	checkTaskPeriod = 2 * time.Second
)

type taskState struct {
	id        string
	status    taskStatus
	startedAt time.Time
}

func newTaskState() *taskState {
	return &taskState{
		status: statusIdle,
	}
}

type Coordinator struct {
	mu                *sync.Mutex
	mapTasks          map[string]*taskState
	reduceTasks       map[int]*taskState
	intermediateFiles map[int][]string
	buckets           int64
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

	for _, task := range c.mapTasks {
		if task.status != statusDone {
			return false
		}
	}
	for _, task := range c.reduceTasks {
		if task.status != statusDone {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := make(map[string]*taskState)
	for _, file := range files {
		mapTasks[file] = newTaskState()
	}
	reduceTasks := make(map[int]*taskState)
	for bucket := range nReduce {
		reduceTasks[bucket] = newTaskState()
	}

	c := Coordinator{
		mu:                &sync.Mutex{},
		mapTasks:          mapTasks,
		reduceTasks:       reduceTasks,
		intermediateFiles: make(map[int][]string),
		buckets:           int64(nReduce),
	}

	go c.checkTasks()

	c.server()
	return &c
}

func (c *Coordinator) checkTasks() {
	ticker := time.NewTicker(checkTaskPeriod)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()
		for _, task := range c.mapTasks {
			if time.Since(task.startedAt) > deadThreshold && task.status == statusRunning {
				task.reset()
			}
		}
		for _, task := range c.reduceTasks {
			if time.Since(task.startedAt) > deadThreshold && task.status == statusRunning {
				task.reset()
			}
		}
		c.mu.Unlock()
	}
}

func (ts *taskState) start() {
	ts.id = uuid.NewString()
	ts.status = statusRunning
	ts.startedAt = time.Now()
}

func (ts *taskState) reset() {
	ts.id = ""
	ts.status = statusIdle
	ts.startedAt = time.Time{}
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	mapDone := true
	for name, task := range c.mapTasks {
		if task.status == statusIdle {
			task.start()

			reply.TaskId = task.id
			reply.TaskType = TaskTypeMap
			reply.MapTask = DoMapTask{
				InputFile: name,
				Buckets:   c.buckets,
			}

			return nil
		} else if task.status != statusDone {
			mapDone = false
		}
	}
	if !mapDone {
		reply.TaskType = TaskTypeNone
		return nil
	}

	for bucket, task := range c.reduceTasks {
		if task.status == statusIdle {
			task.start()

			files := make([]string, len(c.intermediateFiles[bucket]))
			copy(files, c.intermediateFiles[bucket])

			reply.TaskId = task.id
			reply.TaskType = TaskTypeReduce
			reply.ReduceTask = DoReduceTask{
				Bucket: int64(bucket),
				Files:  files,
			}

			return nil
		}
	}

	reply.TaskType = TaskTypeNone
	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case TaskTypeMap:
		mt, ok := c.mapTasks[args.MapTask.InputFile]
		if !ok || mt.id != args.TaskId {
			return nil
		}
		mt.status = statusDone

		for bucket, file := range args.MapTask.IntermediateFiles {
			c.intermediateFiles[bucket] = append(c.intermediateFiles[bucket], file)
		}
	case TaskTypeReduce:
		rt, ok := c.reduceTasks[int(args.ReduceTask.Bucket)]
		if !ok || rt.id != args.TaskId {
			return nil
		}
		rt.status = statusDone
	}

	return nil
}
