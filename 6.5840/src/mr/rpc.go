package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.
type Args struct {
	WorkerID   string // a unique ID for each worker
	WorkType   string // "Map" or "Reduce"
	WorkStatus string // "InProcess", "Completed", "Wait", "Finish"
	TaskID     string
}

type Reply struct {
	TaskID   string
	TaskType string
	NReduce  int
	Filename string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
