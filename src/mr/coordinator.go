package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

	/* 
		while Coordinater is not done
			-- check if worker available
			if worker idle/available:
				assign(Worker(work)) -- where work is either a map or reduce task (ordering doesnt matter bc we are reading from file system)
					-- and where Worker is an arbitrary worker that was assigned work
				while Worker.isAlive() -- sub function that monitors worker
			
	*/


type Coordinator struct {
	// Your definitions here.

	// Coordinater is a manager it needs to keep track of the state of each worker and each task for the whole MapReduce job
	
	// 	if one worker per book, keep metadata about each book, e.g. which worker is processing it, whether it's done
	// 	if one worker per map task " "
	// 	if one worker per reduce task " "
	
	//  Coordinater is always on, but workers can be doing a job, idle, where idle could mean its done or idle is waiting
							// --- where doneness can be calculated by # of done workers  
	// A coordinater wants to optimize tasks, so a idle worker needs to be given a task 
	// at the same time if a worker is dead/not doing the current task the coordinater needs to reassign task // may be a edge case

	// workers don't come to coordinater, coordinater assings work // important to keep this in mind 
	// this may change how RPC is designed 

	/* 
		while Coordinater is not done
			-- init list of all workers available (this may be dynamically increased as a worker is spawned)
			for worker in workers:
				worker.assign()
			
	*/

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
