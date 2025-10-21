package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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
	arg := Args{}
	reply := Reply{}

	ok := call("Coordinator.RPCHandler", &arg, &reply)
	// get reply
	for !ok {
		time.Sleep(100 * time.Millisecond)
		ok = call("Coordinator.RPCHandler", &arg, &reply)
	}

	for {
		if reply.Task == "Map" {
			arg.Id = reply.Id
			arg.Status = 1
			arg.Task = "Map"
			content, err := ioutil.ReadFile(reply.FileName)
			if err != nil {
				arg.Status = 3
			}
			kva := mapf(reply.FileName, string(content))
			nReduce := reply.NReduce
			tmpFiles := make([]*os.File, nReduce)
			encs := make([]*json.Encoder, nReduce)
			tmpNames := make([]string, nReduce)

			for i := 0; i < nReduce; i++ {
				tmp, err := ioutil.TempFile("", "mr-tmp")
				if err != nil {
					fmt.Print(err)
				}
				tmpFiles[i] = tmp
				tmpNames[i] = tmp.Name()
				encs[i] = json.NewEncoder(tmp)
			}

			for _, kv := range kva {
				tempid := ihash(kv.Key) % nReduce
				err := encs[tempid].Encode(&kv)
				if err != nil {
					fmt.Println(err)
				}
			}

			for i := 0; i < nReduce; i++ {
				err := tmpFiles[i].Close()
				if err != nil {
					fmt.Println(err)
				}
				tmpname := tmpNames[i]
				storeName := fmt.Sprintf("mr-%d-%d", arg.Id, i)
				os.Rename(tmpname, storeName)
			}
			arg.Status = 2
			call("Coordinator.RPCHandler", &arg, &reply)

			time.Sleep(50 * time.Millisecond)
			call("Coordinator.RPCHandler", &arg, &reply)
			continue
		}

		intermediate := make(map[string][]string)
		if reply.Task == "Reduce" {
			arg.Id = reply.Id
			arg.Task = reply.Task
			arg.Status = 1
			nMap := reply.NMap
			for i := 0; i < nMap; i++ {
				fn := fmt.Sprintf("mr-%d-%d", i, reply.Id)
				f, err := os.Open(fn)
				if err != nil {
					continue
				}
				dec := json.NewDecoder(f)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
				}
				f.Close()
			}
			var keys []string
			for k := range intermediate {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			outputFile, err := ioutil.TempFile("", fmt.Sprintf("mr-out-%d", arg.Id))
			if err != nil {
				arg.Status = 3
				call("Coordinator.RPCHandler", &arg, &reply)
				continue
			}

			for _, key := range keys {
				v := reducef(key, intermediate[key])
				if _, err := fmt.Fprintf(outputFile, "%v %v\n", key, v); err != nil {
					log.Printf("worker: write out tmp failed: %v", err)
					outputFile.Close()
					_ = os.Remove(outputFile.Name())
					arg.Status = 3
					call("Coordinator.RPCHandler", &arg, &reply)
					continue
				}
			}

			outputFile.Close()
			finalName := fmt.Sprintf("mr-out-%d", arg.Id)
			if err := os.Rename(outputFile.Name(), finalName); err != nil {
				log.Printf("worker: rename out tmp failed: %v", err)
				_ = os.Remove(outputFile.Name())
				arg.Status = 3
				call("Coordinator.RPCHandler", &arg, &reply)
				continue
			}

			arg.Status = 2
			call("Coordinator.RPCHandler", &arg, &reply)
			continue
		}

		if reply.Task == "Wait" {
			time.Sleep(100 * time.Millisecond)
			call("Coordinator.RPCHandler", &arg, &reply)
			continue
		}

		if reply.Task == "Exit" {
			return
		}

	}

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
