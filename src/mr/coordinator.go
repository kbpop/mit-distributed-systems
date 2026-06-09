package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

// worker = processing
// start the time out check for processing
// connects to server and says its done

// 9.9999998 s
// 10.00000000001 s

// we may need locks later


type WorkerType int
const (
    MapJob WorkerType = iota 
	ReducerJob
)

type Job struct {
	// file
	file string
	// bool - isDone
	isDone bool
	// type of job
	workerType WorkerType
}

type WorkerState int
const (
    StatusDone WorkerState = iota 
    StatusProcessing
    StatusUnbegun
)

type WorkerMeta struct {
	// job - Job struct
	job *Job
	// workerState - 0, 1, 2 done, processing, unstarted
	workerState WorkerState
	// lock - go data structure
	mu sync.Mutex
}

type Coordinator struct {
	// queue of mapJobs
	mapJobQueue chan Job
	// queue of reducerJobs
	reduceJobQueue chan Job
	// hashmap of Workers
	workerMap map[int]WorkerMeta
	// int - nreduce
	nreduce int
	// string - sockname
	sockname string
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
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return len(c.mapJobQueue) == 0 && len(c.reduceJobQueue) == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.


	c.server(sockname)
	return &c
}
