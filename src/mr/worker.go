package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io"
import "os"
import "encoding/json"
import "time"

const timeout = time.Second * 10

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for i, task := range c.mapTasks {
		if task.status == Idle {
			task.status = InProgress
			c.mapTasks[i] = task
			reply.mapIndex = i
			reply.reduceIndex = -1
			reply.taskType = "map"
			break
		}
	}

	if c.allMapTasksCompleted() {
		for i, task := range c.reduceTasks {
			if task.status == Idle {
				task.status = InProgress
				c.reduceTasks[i] = task
				reply.mapIndex = -1
				reply.reduceIndex = i
				reply.taskType = "reduce"
				break
			}
		}
	}

    reply.taskType = "done"
}

func HandleMapTask(mapf func(string, string) []KeyValue, filename string, mapIndex int, nReduce int) {
	file, err := os.Open(filename)
	defer file.Close()

	if err != nil {
		log.Fatalf("Cannot open file %s: %v", filename, err)
	}

	content, err := io.ReadAll(file)

	if err != nil {
		log.Fatalf("Cannot read file %s: %v", filename, err)
	}

	kva := mapf(filename, string(content))
    intermediate := make([][]KeyValue, nReduce)

	for _, kv := range kva {
		reduceIndex := ihash(kv.Key) % nReduce
		intermediate[reduceIndex] = append(intermediate[reduceIndex], kv)
	}

	for reduceIndex, bucket := range intermediate {
		intermediateFilename := fmt.Sprintf("mr-%d-%d", mapIndex, reduceIndex)
		file, err := os.Create(intermediateFilename)

        if err != nil {
			log.Fatalf("Cannot create intermediate file %s: %v", intermediateFilename, err)
		}

		enc := json.NewEncoder(file)
		
		for _, kv := range bucket {
			err := enc.Encode(&kv)

			if err != nil {
				log.Fatalf("Cannot write to intermediate file %s: %v", intermediateFilename, err)
			}
		}

		file.Close()
	}
}

func HandleReduceTask(reducef func(string, []string) string, reduceIndex int, nReduce int) {
	
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
	    ok := call("Coordinator.GetTask", &args, &reply)

		if ok {
			switch (reply.taskType) {
				case "map":
					HandleMapTask(mapf, reply.filename, reply.mapIndex, reply.nReduce)
				case "reduce":
					HandleReduceTask(reducef, reply.reduceIndex, reply.nReduce)
				case "wait":
					time.Sleep(timeout)
					continue
				case "done":
					return
			}
		}

		completeTaskArgs := CompleteTaskArgs {
			taskType: reply.taskType,
			mapIndex: reply.mapIndex,
			reduceIndex: reply.reduceIndex,
		}
		completeTaskReply := CompleteTaskReply{}
		ok = call("Coordinator.completeTask", &completeTaskArgs, &completeTaskReply)

		if !ok {
			log.Println("Failed to report task completion to coordinator")
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
