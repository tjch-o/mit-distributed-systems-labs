package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

const  (
	Idle TaskStatus iota 
	InProgress
	Completed
)

type Task struct {
	taskType string,
	status TaskStatus
}

type Coordinator struct {
	// Your definitions here.
	mutex sync.mutex
	files string[]
	mapTasks Task[]
	reduceTasks Task[]
	nMap int
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	for _, task := range c.mapTasks {
		if task.status != Complete {
			return false
		}
	}

	for _, task := range c.reduceTasks {
		if task.status != Complete {
			return false
		}
	}

	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files: files,
		mapTasks: make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
		nMap: len(files),
		nReduce: nReduce
	}

	// Your code here.
	for i := len(files) {
		c.mapTasks[i] = {
			status: Idle,
			taskType: "map"
		}
	}

	for j := nReduce {
		c.reduceTasks[j] = {
			status: Idle,
			taskType: "reduce"
		}
	}

	c.server()
	return &c
}
