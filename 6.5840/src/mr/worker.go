package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
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

func SendHeartBeat(args *Args, stop chan bool) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			statusArgs := *args
			statusArgs.WorkStatus = "InProgress" // 明确设置为正在进行
			var statusReply Reply
			call("Coordinator.RPCHandlers", &statusArgs, &statusReply)
		case <-stop:
			return
		}
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerID := fmt.Sprintf("worker-%d", os.Getpid()) // 生成唯一的worker ID

	for {
		args := Args{
			WorkStatus: "Wait",
			WorkerID:   workerID,
		}
		reply := Reply{}

		ok := call("Coordinator.RPCHandlers", &args, &reply)
		if ok {
			switch reply.TaskType {
			case "Map":

				// 修复：先设置任务信息，再启动心跳
				args.WorkType = "Map"
				args.TaskID = reply.TaskID
				args.WorkerID = workerID
				args.WorkStatus = "InProgress"

				stopStatus := make(chan bool)
				go SendHeartBeat(&args, stopStatus)

				// 创建10秒超时机制
				taskDone := make(chan bool)

				// 在goroutine中执行map任务
				go func() {
					defer func() {
						if r := recover(); r != nil {
							// 如果任务panic，发送false到taskDone
							taskDone <- false
						}
					}()

					// 执行map任务
					// read the input file
					content, err := os.ReadFile(reply.Filename)
					if err != nil {
						log.Printf("cannot read %v: %v", reply.Filename, err)
						taskDone <- false
						return
					}

					// 创建中间文件
					for i := 0; i < reply.NReduce; i++ {
						intermediateFilename := fmt.Sprintf("mr-%s-%d", reply.TaskID, i)
						file, err := os.Create(intermediateFilename)
						if err != nil {
							log.Printf("cannot create intermediate file %s: %v", intermediateFilename, err)
							taskDone <- false
							return
						}
						file.Close()
					}

					var interContent []KeyValue = mapf(reply.Filename, string(content))

					// 打开所有中间文件
					intermediateFiles := make([]*os.File, reply.NReduce)
					for i := 0; i < reply.NReduce; i++ {
						intermediateFilename := fmt.Sprintf("mr-%s-%d", reply.TaskID, i)
						file, err := os.OpenFile(intermediateFilename, os.O_APPEND|os.O_WRONLY, 0600)
						if err != nil {
							log.Printf("cannot open intermediate file %s: %v", intermediateFilename, err)
							taskDone <- false
							return
						}
						intermediateFiles[i] = file
					}

					// 将键值对写入对应的中间文件
					for _, kv := range interContent {
						reduceTaskNumber := ihash(kv.Key) % reply.NReduce
						enc := json.NewEncoder(intermediateFiles[reduceTaskNumber])
						err := enc.Encode(&kv)
						if err != nil {
							log.Printf("cannot encode to intermediate file: %v", err)
							// 关闭所有文件
							for i := 0; i < reply.NReduce; i++ {
								intermediateFiles[i].Close()
							}
							taskDone <- false
							return
						}
					}

					// 关闭所有文件
					for i := 0; i < reply.NReduce; i++ {
						intermediateFiles[i].Close()
					}

					taskDone <- true
				}()

				// 等待任务完成或超时
				select {
				case success := <-taskDone:
					stopStatus <- true
					if success {
						// 任务正常完成
						args.WorkStatus = "Completed"
						var dummyReply Reply
						call("Coordinator.RPCHandlers", &args, &dummyReply)
					}
					// 如果success为false，不通知完成，让coordinator超时重新分配
				case <-time.After(10 * time.Second):
					// 任务超时，停止心跳并继续等待新任务
					stopStatus <- true
				}

			case "Reduce":

				// 设置任务信息并启动心跳
				args.WorkType = "Reduce"
				args.TaskID = reply.TaskID
				args.WorkerID = workerID
				args.WorkStatus = "InProgress"

				stopStatus := make(chan bool)
				go SendHeartBeat(&args, stopStatus)

				// 创建10秒超时机制
				taskDone := make(chan bool)

				// 在goroutine中执行reduce任务
				go func() {
					defer func() {
						if r := recover(); r != nil {
							// 如果任务panic，发送false到taskDone
							taskDone <- false
						}
					}()

					// 读取所有相关的中间文件
					intermediate := []KeyValue{}
					// 从coordinator的filename字段解析文件名
					filenames := strings.Fields(reply.Filename) // reply.Filename包含空格分隔的文件名

					for _, filename := range filenames {
						filename = strings.TrimSpace(filename)
						if filename == "" {
							continue
						}

						file, err := os.Open(filename)
						if err != nil {
							log.Printf("cannot open intermediate file %s: %v", filename, err)
							taskDone <- false
							return
						}

						dec := json.NewDecoder(file)
						for {
							var kv KeyValue
							if err := dec.Decode(&kv); err != nil {
								break
							}
							intermediate = append(intermediate, kv)
						}
						file.Close()
					}

					// 按照key排序
					sort.Slice(intermediate, func(i, j int) bool {
						return intermediate[i].Key < intermediate[j].Key
					})

					// 创建输出文件
					oname := fmt.Sprintf("mr-out-%s", reply.TaskID)
					ofile, err := os.Create(oname)
					if err != nil {
						log.Printf("cannot create output file %s: %v", oname, err)
						taskDone <- false
						return
					}

					// 处理每个key的所有values
					i := 0
					for i < len(intermediate) {
						j := i + 1
						// 找到所有相同key的values
						for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
							j++
						}

						// 收集当前key的所有values
						values := []string{}
						for k := i; k < j; k++ {
							values = append(values, intermediate[k].Value)
						}

						// 调用reduce函数
						output := reducef(intermediate[i].Key, values)

						// 写入结果到输出文件
						fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

						i = j
					}

					ofile.Close()
					taskDone <- true
				}()

				// 等待任务完成或超时
				select {
				case success := <-taskDone:
					stopStatus <- true
					if success {
						// 任务正常完成
						args.WorkStatus = "Completed"
						var dummyReply Reply
						call("Coordinator.RPCHandlers", &args, &dummyReply)
					}
					// 如果success为false，不通知完成，让coordinator超时重新分配
				case <-time.After(10 * time.Second):
					// 任务超时，停止心跳并继续等待新任务
					stopStatus <- true
				}

			case "Wait":
				time.Sleep(1 * time.Second) // 等待一段时间再请求

			case "Exit":
				return
			}
		} else {
			time.Sleep(1 * time.Second) // 连接失败时等待
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
