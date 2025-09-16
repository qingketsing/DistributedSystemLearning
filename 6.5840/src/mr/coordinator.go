package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mapTasks        []Task
	reduceTasks     []Task
	nReduce         int
	nMap            int
	mu              sync.Mutex
	nMapFinished    int
	nReduceFinished int
}

type Task struct {
	ID        string
	Type      string // "Map" or "Reduce"
	Filename  string
	Status    string // "Idle", "InProgress", "Completed"
	StartTime time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RPCHandlers(args *Args, reply *Reply) error {
	// assign a task to the worker

	switch args.WorkStatus {
	case "Wait":
		err := c.AssignTask(args, reply)
		if err != nil {
			log.Printf("AssignTask error: %v", err) // 改为Printf，不要Fatal
			return err
		}
		return nil
	case "Completed":
		c.mu.Lock()
		// set the task status to completed
		if args.WorkType == "Map" {
			found := false
			for i := range c.mapTasks {
				if c.mapTasks[i].ID == args.TaskID && c.mapTasks[i].Status == "InProgress" {
					c.mapTasks[i].Status = "Completed"
					found = true
					break
				}
			}
			if found {
				c.nMapFinished++
			}
		} else {
			found := false
			for i := range c.reduceTasks {
				if c.reduceTasks[i].ID == args.TaskID && c.reduceTasks[i].Status == "InProgress" {
					c.reduceTasks[i].Status = "Completed"
					found = true
					break
				}
			}
			if found {
				c.nReduceFinished++
			}
		}
		c.mu.Unlock()
	default: // args.WorkStatus == "InProcess"
		// do nothing
	}
	return nil
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

	ret := false
	if c.nMapFinished == c.nMap && c.nReduceFinished == c.nReduce {
		ret = true
	}
	// stop the coordinator if all map and reduce tasks are finished
	if ret {
		// stop the coordinator
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// initial the map task
	c.nMap = len(files)
	c.nReduce = nReduce
	c.mapTasks = make([]Task, c.nMap)
	c.reduceTasks = make([]Task, c.nReduce)
	for i := 0; i < c.nMap; i++ {
		c.mapTasks[i] = Task{
			ID:       fmt.Sprintf("%d", i),
			Type:     "Map",
			Filename: files[i],
			Status:   "Idle",
		}
	}
	// initial the reduce task
	for i := 0; i < c.nReduce; i++ {
		intermediateFiles := ""
		for j := 0; j < c.nMap; j++ {
			intermediateFiles += fmt.Sprintf("mr-%d-%d ", j, i)
		}
		c.reduceTasks[i] = Task{
			ID:       fmt.Sprintf("%d", i),
			Type:     "Reduce",
			Filename: intermediateFiles,
			Status:   "Idle",
		}
	}

	c.server()
	return &c
}

func (c *Coordinator) AssignTask(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check for timed out tasks and reset them
	c.checkTimeoutTasks()

	// First, try to assign a map task if any are available
	for i := range c.mapTasks {
		if c.mapTasks[i].Status == "Idle" {
			c.mapTasks[i].Status = "InProgress"
			c.mapTasks[i].StartTime = time.Now()

			reply.TaskID = c.mapTasks[i].ID
			reply.TaskType = "Map"
			reply.NReduce = c.nReduce
			reply.Filename = c.mapTasks[i].Filename

			return nil
		}
	}

	// If all map tasks are finished, try to assign reduce tasks
	if c.nMapFinished == c.nMap {
		for i := range c.reduceTasks {
			if c.reduceTasks[i].Status == "Idle" {
				c.reduceTasks[i].Status = "InProgress"
				c.reduceTasks[i].StartTime = time.Now()

				reply.TaskID = c.reduceTasks[i].ID
				reply.TaskType = "Reduce"
				reply.NReduce = c.nReduce
				reply.Filename = c.reduceTasks[i].Filename

				return nil
			}
		}
	}

	// No tasks available right now, tell worker to wait
	reply.TaskType = "Wait"
	return nil
}

// Check for timed out tasks and reset them to Idle
func (c *Coordinator) checkTimeoutTasks() {
	timeout := 10 * time.Second
	now := time.Now()

	// Check map tasks for timeout
	for i := range c.mapTasks {
		if c.mapTasks[i].Status == "InProgress" &&
			!c.mapTasks[i].StartTime.IsZero() &&
			now.Sub(c.mapTasks[i].StartTime) > timeout {
			c.mapTasks[i].Status = "Idle"
			c.mapTasks[i].StartTime = time.Time{} // Reset start time
		}
	}

	// Check reduce tasks for timeout
	for i := range c.reduceTasks {
		if c.reduceTasks[i].Status == "InProgress" &&
			!c.reduceTasks[i].StartTime.IsZero() &&
			now.Sub(c.reduceTasks[i].StartTime) > timeout {
			c.reduceTasks[i].Status = "Idle"
			c.reduceTasks[i].StartTime = time.Time{} // Reset start time
		}
	}
}
