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
	// create a timer data 
	prevTime time.Time
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

// function for creating a map job
func CreateMapJob(file string) Job {
	return Job{
		file: file,
		isDone: false,
		workerType: MapJob,
	}
}

// function for creating a reduce job
func CreateReduceJob(file string) Job {
	return Job{
		file: file,
		isDone: false,
		workerType: ReducerJob,
	}
}

// function for adding function to appropriate queue
// 		-> use if statement on the job to assign to appropriate queue
func (c *Coordinator) RouteJob(job Job) {
	// route to reducer Job queue OR map job queue
	if(job.workerType == ReducerJob){
		c.reduceJobQueue <- job
	} else if (job.workerType == MapJob ){
		c.mapJobQueue <- job
	}
}

// createBackgroundTimeout
func (c *Coordinator) CreateBackgroundTimeout(){
	// create timer and set sleep
	// call clean up workerMap timeout and put back on the queue
}

// function that is called when worker requests for job
func (c *Coordinator) AssignJobToWorker(){

}

// function to 
// 1) Put current Job back on queue, 
// 2) Reset worker state + prevTime
func (c *Coordinator) ResetWorker(){

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapJobQueue: make(chan Job, nReduce), // create buffer of length nReduce
		reduceJobQueue: make(chan Job, nReduce), // create buffer of length nReduce
		workerMap: make(map[int]WorkerMeta), 
		nreduce: nReduce,
		sockname: sockname,
	}

	// use the files variable to create the mapJobs? 

	c.server(sockname)
	return &c
}
