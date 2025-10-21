package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	nMap         int
	nReduce      int
	Task         string
	mu           sync.Mutex
	mapStatus    []int // 0 is not online ,1 is dealing ,2 is finished
	mapFiles     []string
	reduceStatus []int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RPCHandler(arg *Args, reply *Reply) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if arg.Status == 0 {
		if c.Task == "Map" {
			// TODO : arrenge ID to reply
			for i := 0; i < c.nMap; i++ {
				if c.mapStatus[i] == 0 {
					c.mapStatus[i] = 1
					reply.FileName = c.mapFiles[i]
					reply.Id = i
					reply.NReduce = c.nReduce
					reply.Task = "Map"
					reply.NMap = c.nMap
					return
				}
			}
			allDone := true
			for i := 0; i < c.nMap; i++ {
				if c.mapStatus[i] != 2 {
					allDone = false
					break
				}
			}
			if allDone {
				c.Task = "Reduce"
			} else {
				reply.Task = "Wait"
			}
			return
		}
		if c.Task == "Reduce" {
			reply.Task = "Reduce"
			// TODO : arrenge ID to reply
			for i := 0; i < c.nReduce; i++ {
				if c.reduceStatus[i] == 0 {
					c.reduceStatus[i] = 1
					reply.Id = i
					reply.NReduce = c.nReduce
					reply.Task = "Reduce"
					reply.NMap = c.nMap
					// reply.Filename = ?
					return
				}
			}
			allDone := true
			for i := 0; i < c.nReduce; i++ {
				if c.reduceStatus[i] != 2 {
					allDone = false
					break
				}
			}
			if allDone {
				c.Task = "Exit"

			} else {
				reply.Task = "Wait"
			}
			return
		}
	}
	// other status , such as 1,2,3
	if arg.Status == 1 {
		if arg.Task == "Map" {
			c.mapStatus[arg.Id] = 1
		}
		if arg.Task == "Reduce" {
			c.reduceStatus[arg.Id] = 1
		}
		return
	}

	if arg.Status == 2 {
		if arg.Task == "Map" {
			c.mapStatus[arg.Id] = 2
		}
		if arg.Task == "Reduce" {
			c.reduceStatus[arg.Id] = 2
		}
		return
	}

	if arg.Status == 3 {
		if arg.Task == "Map" {
			c.mapStatus[arg.Id] = 0
		}
		if arg.Task == "Reduce" {
			c.reduceStatus[arg.Id] = 0
		}
		return
	}
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
	ret := true
	for i := 0; i < c.nMap; i++ {
		if c.mapStatus[i] != 2 {
			ret = false
		}
	}
	for i := 0; i < c.nReduce; i++ {
		if c.reduceStatus[i] != 2 {
			ret = false
		}
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.nMap = len(files)
	c.nReduce = nReduce
	c.Task = "Map"
	c.mapFiles = make([]string, c.nMap)
	copy(c.mapFiles, files)
	c.mapStatus = make([]int, c.nMap)
	c.reduceStatus = make([]int, c.nReduce)

	c.server()
	return &c
}
