package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

const (
	requestTaskPeriod = 500 * time.Millisecond
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		task, ok := requestTask()
		if !ok {
			log.Println("failed to request task. coordinator is down")
			return
		}

		switch task.TaskType {
		case TaskTypeMap:
			if err := handleMap(task.TaskId, task.MapTask, mapf); err != nil {
				log.Printf("handle map operation %v: %v\n", task.MapTask.InputFile, err.Error())
			}
		case TaskTypeReduce:
			if err := handleReduce(task.TaskId, task.ReduceTask, reducef); err != nil {
				log.Printf("handle reduce operation %v: %v\n", task.ReduceTask.Bucket, err.Error())
			}
		}

		time.Sleep(requestTaskPeriod)
	}
}

func requestTask() (RequestTaskReply, bool) {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	return reply, call("Coordinator.RequestTask", &args, &reply)
}

func completeTask(args *CompleteTaskArgs) bool {
	reply := CompleteTaskReply{}
	return call("Coordinator.CompleteTask", args, &reply)
}

func handleMap(taskId string, doTask DoMapTask, mapf func(string, string) []KeyValue) error {
	content, err := os.ReadFile(doTask.InputFile)
	if err != nil {
		return fmt.Errorf("read file %v: %w", doTask.InputFile, err)
	}

	kvs := mapf(doTask.InputFile, string(content))

	var files []*os.File
	var intermediate []string
	for bucket := range doTask.Buckets {
		name := fmt.Sprintf("mr-%s-%d", taskId, bucket)
		file, err := os.Create(name)
		if err != nil {
			return fmt.Errorf("create file %v: %w", intermediate, err)
		}
		defer file.Close()

		files = append(files, file)
		intermediate = append(intermediate, name)
	}

	for _, kv := range kvs {
		bucket := ihash(kv.Key) % int(doTask.Buckets)
		if err = json.NewEncoder(files[bucket]).Encode(kv); err != nil {
			return fmt.Errorf("encode %v: %w", kv.Key, err)
		}
	}

	for _, file := range files {
		if err := file.Sync(); err != nil {
			return fmt.Errorf("sync file %v: %w", file.Name(), err)
		}
	}

	doneTask := &CompleteTaskArgs{
		TaskType: TaskTypeMap,
		TaskId:   taskId,
		MapTask: DoneMapTask{
			InputFile:         doTask.InputFile,
			IntermediateFiles: intermediate,
		},
	}
	if !completeTask(doneTask) {
		return fmt.Errorf("failed to complete map task for file %v", doTask.InputFile)
	}

	return nil
}

func handleReduce(taskId string, doTask DoReduceTask, reducef func(string, []string) string) error {
	var intermediate []KeyValue

	for _, name := range doTask.Files {
		file, err := os.Open(name)
		if err != nil {
			return fmt.Errorf("open file %v: %w", name, err)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	tmpFile, err := os.CreateTemp("", "mr-out-")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		_, err := fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			os.Remove(tmpFile.Name())
			return fmt.Errorf("write to file %v: %w", tmpFile.Name(), err)
		}

		i = j
	}
	tmpFile.Close()

	oname := fmt.Sprintf("mr-out-%v", doTask.Bucket)
	if err := os.Rename(tmpFile.Name(), oname); err != nil {
		os.Remove(tmpFile.Name())
		return fmt.Errorf("rename %v to %v: %w", tmpFile.Name(), oname, err)
	}

	task := &CompleteTaskArgs{
		TaskType: TaskTypeReduce,
		TaskId:   taskId,
		ReduceTask: DoneReduceTask{
			Bucket: doTask.Bucket,
		},
	}
	if !completeTask(task) {
		return fmt.Errorf("failed to complete reduce task for bucket %v", doTask.Bucket)
	}

	return nil
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
