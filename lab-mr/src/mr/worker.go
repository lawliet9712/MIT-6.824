package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func tmpMapOutFile(workerId int, mapId int, reduceId int) string {
	return fmt.Sprintf("tmp-worker-%d-%d-%d", workerId, mapId, reduceId)
}

func finalMapOutFile(mapId int, reduceId int) string {
	return fmt.Sprintf("mr-%d-%d", mapId, reduceId)
}

func tmpReduceOutFile(workerId int, reduceId int) string {
	return fmt.Sprintf("tmp-worker-%d-out-%d", workerId, reduceId)
}

func finalReduceOutFile(reduceId int) string {
	return fmt.Sprintf("mr-out-%d", reduceId)
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func NotifiyTaskDone(taskId int, taskType CoordinatorPhase) {
	args := NotifyArgs{}
	reply := NotifyReplyArgs{}
	args.TaskID = taskId
	args.TaskType = taskType
	args.WorkerID = os.Getpid()
	ok := call("Coordinator.RequestTaskDone", &args, &reply)
	if !ok {
		fmt.Printf("Call Coordinator.RequestTaskDone failed ...")
		return
	}

	if reply.Confirm {
		fmt.Printf("Task %d Success, Continue Next Task ...", taskId)
	}
}

func DoMapTask(Task ReplyArgs, mapf func(string, string) []KeyValue) bool {
	fmt.Printf("starting do map task ...\n")
	file, err := os.Open(Task.FileName)
	if err != nil {
		fmt.Printf("Open File Failed %s\n", Task.FileName)
		return false
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("ReadAll file Failed %s\n", Task.FileName)
		return false
	}

	file.Close()
	fmt.Printf("starting map %s \n", Task.FileName)
	kva := mapf(Task.FileName, string(content))
	hashedKva := make(map[int][]KeyValue)
	for _, kv := range kva {
		hashed := ihash(kv.Key) % Task.ReduceNum
		hashedKva[hashed] = append(hashedKva[hashed], kv)
	}

	for i := 0; i < Task.ReduceNum; i++ {
		outFile, _ := os.Create(tmpMapOutFile(os.Getpid(), Task.TaskID, i))
		for _, kv := range hashedKva[i] {
			fmt.Fprintf(outFile, "%v\t%v\n", kv.Key, kv.Value)
		}
		outFile.Close()
	}
	NotifiyTaskDone(Task.TaskID, Task.TaskType)
	return true
}

func DoReduceTask(Task ReplyArgs, reducef func(string, []string) string) bool {
	/*
	 1. 先获取所有 tmp-{mapid}-{reduceid} 中 reduce id 相同的 task
	*/
	fmt.Printf("starting do reduce task ...\n")
	var lines []string
	for i := 0; i < Task.MapNum; i++ {
		filename := finalMapOutFile(i, Task.TaskID)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		/*
			2. 将所有文件的内容读取出来，合并到一个数组中
		*/
		lines = append(lines, strings.Split(string(content), "\n")...)
	}
	/*
		3. 过滤数据，将每行字符串转成 KeyValue, 归并到数组
	*/
	var kva []KeyValue
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		split := strings.Split(line, "\t")
		kva = append(kva, KeyValue{
			Key:   split[0],
			Value: split[1],
		})
	}

	/*
		4. 模仿 mrsequential.go 的 reduce 操作，将结果写入到文件，并 commit
	*/
	sort.Sort(ByKey(kva))
	outFile, _ := os.Create(tmpReduceOutFile(os.Getpid(), Task.TaskID))
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	outFile.Close()
	NotifiyTaskDone(Task.TaskID, Task.TaskType)
	return true
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := RequestArgs{}
		reply := ReplyArgs{}
		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			fmt.Printf("call request task failed ...\n")
			return
		}

		fmt.Printf("call finish ... file name %v\n", reply)
		switch reply.TaskType {
		case PHASE_MAP:
			DoMapTask(reply, mapf)
		case PHASE_REDUCE:
			DoReduceTask(reply, reducef)
		case PHASE_WAITTING: // 当前 coordinator 任务已经分配完了，worker 等待一会再试
			time.Sleep(5 * time.Second)
		case PHASE_FINISH:
			fmt.Printf("coordinator all task finish ... close worker")
			return
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
