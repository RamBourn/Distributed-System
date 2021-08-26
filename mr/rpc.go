package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	Status int    //the status of task: 1:request a task 2: finish the task  
	Task int  //type of task
	Filename string  //the filename of map task
	Reduce int //the number of reduce task
}

type ExampleReply struct {
	Task int     //type of task:-1:exit  1:map task  2:reduce task  3:wait
	Filename string   // filename of map task
	Reduce int   //the number of reduce task
	Files []string //files of all reduce task
	NReduce int
	NMap int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
