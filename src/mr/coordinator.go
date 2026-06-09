package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type WorkerType int // setting the types of jobs enum
const (
    MapJob WorkerType = iota // 0
	ReducerJob // 1
)

type Job struct {
	fileName string
	isDone bool
	// type of job : MapJob or ReduceJob
	workerType WorkerType

	jobId int // this would be the identifier for the workerMap
}

type WorkerState int // setting the worker states
const (
    StatusDone WorkerState = iota // 0
    StatusProcessing // 1
    StatusUnbegun // 2
)

type WorkerMeta struct {
	job *Job
	// workerState - 0, 1, 2 done, processing, unbegun --> StatusDone, StatusProcessing, StatusUnbegun
	workerState WorkerState
	// lock for each worker
	lock sync.Mutex
	// init worker start time so then we can later check worker.currTime or smth
	StartTime time.Time
}

type Coordinator struct {

	nReduce int // number of reduce tasks
	
	mapJobQueue chan *Job 
	reduceJobQueue chan *Job

	workerMap map[int]*WorkerMeta // this the main thing to "coordinate" *** need to think on the key for the map
															/* the key should be a "job id"
															   based off of Job.jobId  */

	sockName string

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
	ret := false

	// Your code here.


	return ret
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
