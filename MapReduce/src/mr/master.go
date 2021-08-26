package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
//import "fmt"
//import "strconv"
import "sync"
import "regexp"

type Master struct {
	files []string
	maps []string          //all files needed to be put into map tasks
	reduces []int          // all reduce tasks
	left_map int           //number of left map tasks
	left_reduce int        // number of left reduce tasks
	nReduce int           //number of total reduce tasks
	nMap int               //number of files   
	map_map map[string]bool   //map that records each map task's status(if it has been finished)
	reduce_map map[int]bool   //map that records each reduce task's status
	lock sync.Mutex  //the lock
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	m.lock.Lock()
	if args.Status==1{
		if m.left_map!=0 &&len(m.maps)!=0{  //there are still map tasks
			reply.Task=1
			num:=len(m.maps)-1  //num-th map task will be worked
			reply.Filename=m.maps[num]
			m.maps=m.maps[0:num] //delete the map task in the list
			reply.Reduce=-1   //not a reduce job
			reply.NReduce=m.nReduce
			//fmt.Println("Worked has requested a map task"+reply.Filename)
			go m.waitTask(reply.Task,reply.Filename,reply.Reduce)
		}else if len(m.maps)==0&&m.left_map!=0{ //all map tasks have been assigned
			reply.Task=3 //wait for all map tasks
			//fmt.Println("Some map tasks haven't been done, worker have to wait")
		}else if m.left_reduce!=0&&len(m.reduces)!=0{		//all map tasks have been done, assign a reduce task
			reply.Task=2
			reply.Files=m.files
			reply.NReduce=m.nReduce
			num:=len(m.reduces)-1  //num-th reduce task will be worked
			reply.Reduce=m.reduces[num]	
			m.reduces=m.reduces[0:num]
			//fmt.Println("Worked has requested a reduce task"+strconv.Itoa(reply.Reduce))
			go m.waitTask(reply.Task,reply.Filename,reply.Reduce)
		}else if len(m.reduces)==0&&m.left_reduce!=0{
			reply.Task=3 //wait for all map tasks
			//fmt.Println("Some reduce tasks haven't been done, worker have to wait")
		}else{
			reply.Task=-1 //all tasks have been done
			//fmt.Println("all tasks have been done")
		}
	}else{   //report that some task has been finished
		if args.Task==1{
			//fmt.Println("Worked has finished this map task")
			if m.map_map[args.Filename]==true{

			}else{
				m.map_map[args.Filename]=true
				m.left_map--
			}
		}else{
			//fmt.Println("Worked has finished this reduce task")
			if m.reduce_map[args.Reduce]==true{

			}else{
				m.reduce_map[args.Reduce]=true
				m.left_reduce--
			}
		}
	}
	m.lock.Unlock()
	return nil
}

func(m* Master) waitTask(task int,filename string,reduce_num int){
	time.Sleep(10*time.Second)
	m.lock.Lock()
	if task==1{
		if m.map_map[filename]==true{

		}else{
			//fmt.Println("The map task: "+filename+" is not finished(worker crashed)")
			m.maps=append(m.maps,filename)
		}
	} else{
		if m.reduce_map[reduce_num]==true{

		}else{
			//fmt.Println("The reduce task: "+strconv.Itoa(reduce_num)+ " is not finished(worker crashed)")
			m.reduces=append(m.reduces,reduce_num)
		}
	}
	m.lock.Unlock()
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if m.left_map==0&&m.left_reduce==0{
		//time.Sleep(5*time.Second)
		dd, err := os.Open(".")
		if err != nil {
			panic(err)
		}
		names, err := dd.Readdirnames(1000000)
		if err != nil {
			panic(err)
		}
		reg, err := regexp.Compile("mr-pg-.*txt.*")
		for _, name := range names {
			if reg.MatchString(name){
				os.Remove(name)
			}
		}
		dd.Close()
		ret= true
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	//os.Remove("mr-*txt*")
	// Your code here.
	m.files=make([]string,len(files))
	m.maps=make([]string,len(files))
	copy(m.files,files)
	copy(m.maps,files)
	for i:=0;i<nReduce;i++{
		m.reduces=append(m.reduces,i)
	}
	m.map_map=make(map[string]bool)
	m.reduce_map=make(map[int]bool)
	for _,v :=range files{
		m.map_map[v]=false
	}
	for _,v:=range m.reduces{
		m.reduce_map[v]=false
	}
	m.left_map=len(files)
	m.nMap=len(files)
	m.left_reduce=nReduce
	m.nReduce=nReduce
	m.server()
	return &m
}
