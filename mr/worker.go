package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "sort"
import "strconv"
import "encoding/json"
import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for true{
		args:=ExampleArgs{}
		reply:=ExampleReply{}
		args.Status=1
		call("Master.Example", &args, &reply)
		if reply.Task==-1{   //all tasks have been finished
			//fmt.Println("All tasks have been finished, the worker exits")
			return
		}
		if reply.Task==3{  //wait for map tasks
			//fmt.Println("Some map tasks haven't been done, the worker have to wait")
		} 
		if reply.Task==1{   //assigned a map task
			//fmt.Println("Worked has got a map task file: "+reply.Filename)
			intermediate := []KeyValue{}
			file, err := os.Open(reply.Filename)
			//fmt.Println("map file: "+reply.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", reply.Filename)
			}
			content, err := ioutil.ReadAll(file)
			//fmt.Println(content)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}
			file.Close()
			kva := mapf(reply.Filename, string(content))
			intermediate = append(intermediate, kva...)
			sort.Sort(ByKey(intermediate))
			//fmt.Println("map intermediate: ",intermediate)
			var ofiles []*os.File
			for m:=0;m<reply.NReduce;m++{
				oname := "mr-"+reply.Filename[3:]+"-"+strconv.Itoa(m)
				ofile,_:=os.Create(oname)
				ofiles=append(ofiles,ofile)
			}
			i := 0
			for i < len(intermediate) {
				j := i + 1
				num:=ihash(intermediate[i].Key)%reply.NReduce
				ofile:=ofiles[num]
				//fmt.Println("oname: "+oname)
				enc:=json.NewEncoder(ofile)
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				//values := []string{}
				for k := i; k < j; k++ {
					//values = append(values, intermediate[k].Value)
					enc.Encode(&intermediate[k])
				}
				//output := reducef(intermediate[i].Key, values)
				// this is the correct format for each line of Reduce output.
				//fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			for m:=0;m<reply.NReduce;m++{
				ofiles[m].Close()
			}
			args.Status=2
			args.Task=1
			args.Filename=reply.Filename
			call("Master.Example", &args, &reply)
			//fmt.Println("Worked has finished this map task")
		}
		if reply.Task==2{  //assigned a reduce task
			//fmt.Println("Worked has got a reduce task: number "+strconv.Itoa(reply.Reduce))
			intermediate:=[]KeyValue{}
			for _,filename:=range reply.Files{
					ofile:="mr-"+filename[3:]+"-"+strconv.Itoa(reply.Reduce)
					file, err := os.Open(ofile)
					//fmt.Println("reduce file: "+ofile)
					if err != nil {
						continue
						//log.Fatalf("cannot open %v", reply.Filename)
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
			//time.Sleep(5*time.Second)  //for reduce parallel
			sort.Sort(ByKey(intermediate))
			oname:="mr-out-"+strconv.Itoa(reply.Reduce)
			ofile, _ := os.Create(oname)
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)

				}
				output := reducef(intermediate[i].Key, values)
				//fmt.Println("reduce output: "+output)
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		
				i = j
			}
			ofile.Close()
			args.Status=2
			args.Task=2
			args.Reduce=reply.Reduce
			call("Master.Example", &args, &reply)
			//fmt.Println("Worked has finished this reduce task")
		}
		time.Sleep(1*time.Second)
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}



//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

func isExist(path string)(bool){

    _, err := os.Stat(path)

    if err != nil{

        if os.IsExist(err){

            return true

        }

        if os.IsNotExist(err){

            return false

        }

        fmt.Println(err)

        return false

    }

    return true

}
