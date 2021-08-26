package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"sync/atomic"

	"6.824/labrpc"
	//"math/rand"
	"time"
	"fmt"
)

import "bytes"
import "6.824/labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).

	//persistent state on all servers
	currentTerm int
	voteFor  int
	logs  []LogEntry

	//volatile state  on all servers
	commitIndex int
	lastApplied int
	vote  int  // number of the votes this server get in a election
	votes []bool   //which one has voted for this term
	appends []bool  //which one has append an entry
	append int //number of servers that have append the log entry successfully
	leaderId int  //the index of leader
	currentSend int
	//volatile state  on leaders
	nextIndex  []int
	matchIndex []int
	snapshot []byte
	lastIncludedIndex int
	lastIncludedTerm int
	//state about time
	heartbeat  int   //intervals of heartsbeats
	election int   //election timeout for starting an election
	electionTimeout int //the time within which an election must be finished
	electionflag bool
	initflag bool
	// channel
	toFollower chan int   //this server needs to become a follower
	toLeader chan int  //this server needs to become a leader
	applyCh chan ApplyMsg //this channel is userd to apply a command
	commandCh chan int  //incoming command to be processed
	snapshotCh chan SnapShotInfo //need to snapshot
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.


}

// data structure for log entry
type LogEntry struct {
	Index int
	Term int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term=rf.currentTerm
	if rf.leaderId==rf.me{
		isleader=true
	}else{
		isleader=false
	}
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.commitIndex)
	e.Encode(rf.nextIndex)
	e.Encode(rf.matchIndex)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.lastIncludedIndex)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data,rf.snapshot)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var commitIndex int
	var nextIndex []int
	var matchIndex []int
	var logs []LogEntry
	var lastIncludedTerm int
	var lastIncludedIndex int
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&commitIndex) != nil ||
	   d.Decode(&nextIndex) !=nil ||
	   d.Decode(&matchIndex) !=nil||
	   d.Decode(&logs) !=nil ||
	   d.Decode(&lastIncludedTerm) !=nil ||
	   d.Decode(&lastIncludedIndex) !=nil {
		fmt.Println("Decode Error")
	} else {
	  rf.currentTerm = currentTerm
	  rf.commitIndex = commitIndex
	  rf.nextIndex=nextIndex
	  rf.matchIndex=matchIndex
	  rf.logs=logs
	  rf.lastIncludedTerm=lastIncludedTerm
	  rf.lastIncludedIndex=lastIncludedIndex
	}
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	snapShotInfo:=SnapShotInfo{}
	snapShotInfo.Index=index
	snapShotInfo.SnapShot=snapshot
	rf.snapshotCh<-snapShotInfo

}

func (rf *Raft) MakeSnapshot(){
	for rf.killed()==false{
		select{
		case snapShotInfo:=<-rf.snapshotCh:
			rf.mu.Lock()
			rf.snapshot=snapShotInfo.SnapShot
			var tmp []LogEntry
			for i:=len(rf.logs)-1;i>=1;i--{
				if rf.logs[i].Index==snapShotInfo.Index{
					rf.lastIncludedTerm=rf.logs[i].Term
					rf.lastIncludedIndex=rf.logs[i].Index
					tmp=rf.logs[i+1:]
					break
				}
			}
			//fmt.Println("server: ",rf.me," start making a snapshot, lastIncludedTerm: ",rf.lastIncludedTerm," lastIncludedIndex: ",rf.lastIncludedIndex)
			rf.logs=make([]LogEntry,0)
			dummy:=LogEntry{}
			dummy.Term=rf.lastIncludedTerm
			dummy.Index=rf.lastIncludedIndex
			dummy.Command=nil
			rf.logs=append(rf.logs,dummy)
			rf.logs=append(rf.logs,tmp...)
			rf.mu.Unlock()
		}
	}
}

type SnapShotArgs struct{
	Term int
	LastIncludedTerm int
	LastIncludedIndex int
	SnapShot []byte
	CurrentSend int
} 

type SnapShotReply struct{
	Term int
	Success bool
	CurrentSend int
}

type SnapShotInfo struct{
	Index int
	SnapShot []byte
}
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int 
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//AppendEntries RPC arguments structure
type AppendEntriesArgs struct{
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries  []LogEntry
	LeaderCommit int
	CurrentSend int
	HeartBeat bool
}

//AppendEntries RPC reply structure
type AppendEntriesReply struct{
	Term int
	Success bool
	CurrentSend int
}
//Snapshot RPC handler
func (rf *Raft) SnapShotHandler(args *SnapShotArgs, reply *SnapShotReply) {
	rf.mu.Lock()
	if args.Term>=rf.currentTerm{
		rf.leaderId=-1
		rf.currentTerm=args.Term
		rf.voteFor=-1
		rf.vote=0
		rf.append=0
		reply.CurrentSend=args.CurrentSend
		//var flag bool
		if rf.logs[len(rf.logs)-1].Term>args.LastIncludedTerm||(rf.logs[len(rf.logs)-1].Term==args.LastIncludedTerm&&rf.logs[len(rf.logs)-1].Index>args.LastIncludedIndex){
			//flag=false
		}else{
			//fmt.Println("server: ",rf.me," start installing a snapshot, lastIncludedTerm: ",args.LastIncludedTerm," lastIncludedIndex: ",args.LastIncludedIndex)
			rf.snapshot=args.SnapShot
			rf.logs=make([]LogEntry,0)
			dummy:=LogEntry{}
			dummy.Command=nil
			dummy.Term=args.LastIncludedTerm
			dummy.Index=args.LastIncludedIndex
			rf.logs=append(rf.logs,dummy)
			rf.lastIncludedTerm=args.LastIncludedTerm
			rf.lastIncludedIndex=args.LastIncludedIndex
			rf.lastApplied=args.LastIncludedIndex
			rf.commitIndex=args.LastIncludedIndex
			applyMsg:=ApplyMsg{}
			applyMsg.SnapshotValid=true
			applyMsg.SnapshotTerm=args.LastIncludedTerm
			applyMsg.SnapshotIndex=args.LastIncludedIndex
			applyMsg.Snapshot=args.SnapShot
			rf.applyCh<-applyMsg
			//flag=true
		}
		/*
		applyMsgs:=make([]ApplyMsg,0)
		applyMsgs=append(applyMsgs,applyMsg)
		go rf.apply(applyMsgs)
		*/
		reply.Term=rf.currentTerm
		reply.Success=true
	}else{
		reply.Term=rf.currentTerm
		reply.Success=true
	}
	rf.persist()
	rf.mu.Unlock()
}

//SendSnapShot Rpc
func (rf *Raft) sendSnapShot(server int, args *SnapShotArgs, reply *SnapShotReply) bool {
	rf.mu.Lock()
	if rf.leaderId!=rf.me{
		rf.leaderId=-1
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.SnapShotHandler", args, reply)
	rf.mu.Lock()
	if ok{
		if reply.Term>rf.currentTerm{
			rf.leaderId=-1
			rf.currentTerm=reply.Term
			rf.voteFor=-1
			rf.vote=0
			rf.append=0
		}else{
			if reply.Term==rf.currentTerm{
				if reply.CurrentSend==rf.logs[len(rf.logs)-1].Index{
					if reply.Success&&rf.nextIndex[server]<rf.lastIncludedIndex+1{
						rf.nextIndex[server]=rf.logs[len(rf.logs)-1].Index+1
					}
				}
			}
		}
	}
	rf.persist()
	rf.mu.Unlock()
	return ok
}


// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	//fmt.Println("server: ",rf.me,"len: ",len(args.Entries))
	if args.HeartBeat{   //heartheat
		if args.Term>=rf.currentTerm{
			if len(args.Entries)==0{
				rf.leaderId=-1
				/*
				rf.mu.Unlock()
				rf.toFollower<-1
				rf.mu.Lock()
				*/
				rf.currentTerm=args.Term
				rf.voteFor=-1
				rf.vote=0
				rf.append=0
				reply.Term=rf.currentTerm
				reply.CurrentSend=args.CurrentSend
				if rf.logs[len(rf.logs)-1].Term==args.PrevLogTerm&&rf.logs[len(rf.logs)-1].Index==args.PrevLogIndex{
					
					if(args.LeaderCommit>rf.logs[len(rf.logs)-1].Index){
						rf.commitIndex=rf.logs[len(rf.logs)-1].Index
					}else{
						rf.commitIndex=args.LeaderCommit
					}
					//rf.persist()
					rf.commit()
					reply.Success=true
				}else{
					//fmt.Println("lastterm:",rf.logs[len(rf.logs)-1].Term," prevterm: ",args.PrevLogTerm," lastIndex: ",rf.logs[len(rf.logs)-1].Index," previndex: ",args.PrevLogIndex)
					reply.Success=false
				}
				//if args.LeaderCommit>rf.commitIndex&&rf.logs[len(rf.logs)-1].Term==args.PrevLogTerm&&rf.logs[len(rf.logs)-1].Index==args.PrevLogIndex{

				//}
				//rf.mu.Unlock()
			}else{
				rf.leaderId=-1
				/*
				rf.mu.Unlock()
				rf.toFollower<-1
				rf.mu.Lock()
				*/
				rf.currentTerm=args.Term
				rf.voteFor=-1
				rf.vote=0
				rf.append=0
				reply.Term=rf.currentTerm
				reply.CurrentSend=args.CurrentSend
				/*
				reply.CurrentSend=args.CurrentSend
				find:=false
				for i:=0;i<len(args.Entries);i++{
					if args.Entries[i].Index>rf.commitIndex&&args.Entries[i].Term<rf.logs[rf.commitIndex].Term{
						find=true
					}
				}
				if rf.commitIndex>args.LeaderCommit||find{
					reply.Success=false
				}else{
					reply.Success=true
					rf.logs=rf.logs[:args.PrevLogIndex+1]
					for i:=0;i<len(args.Entries);i++{
						rf.logs=append(rf.logs,args.Entries[i])
					}
					if args.LeaderCommit>rf.logs[len(rf.logs)-1].Index{
						rf.commitIndex=rf.logs[len(rf.logs)-1].Index
					}else{
						rf.commitIndex=args.LeaderCommit
					}
					
					//rf.persist()
					rf.commit()
				}
				*/
				if args.Entries[len(args.Entries)-1].Term<=rf.logs[len(rf.logs)-1].Term&&args.Entries[len(args.Entries)-1].Index<=rf.logs[len(rf.logs)-1].Index{
					reply.Success=true
					if args.LeaderCommit>rf.logs[len(rf.logs)-1].Index{
						rf.commitIndex=rf.logs[len(rf.logs)-1].Index
					}else{
						rf.commitIndex=args.LeaderCommit
					}
					
					//rf.persist()
					rf.commit()
					//fmt.Println("args: ",args.Entries[len(args.Entries)-1].Term," ",args.Entries[len(args.Entries)-1].Index," mine: ",rf.logs[len(rf.logs)-1].Term," ",rf.logs[len(rf.logs)-1].Index)
				}else{
					find:=-1
				for i:=len(rf.logs)-1;i>=0;i--{
					if rf.logs[i].Index==args.PrevLogIndex&&rf.logs[i].Term==args.PrevLogTerm{
						find=i
						break
					}
				}
				if find==-1{
					reply.Success=false
				}else{
					/*
					//replace method
					for i:=0;i<len(args.Entries);i++{
						if (find+1+i)<=len(rf.logs)-1{
							rf.logs[find+1+i].Term=args.Entries[i].Term
							rf.logs[find+1+i].Index=args.Entries[i].Index
							rf.logs[find+1+i].Command=args.Entries[i].Command
						}else{
							rf.logs=append(rf.logs,args.Entries[i])
						}
					}
					if args.LeaderCommit>rf.logs[find+len(args.Entries)].Index{
						rf.commitIndex=rf.logs[find+len(args.Entries)].Index
					}else{
						rf.commitIndex=args.LeaderCommit
					}
					*/
					
					rf.logs=rf.logs[:find+1]
					for i:=0;i<len(args.Entries);i++{
						rf.logs=append(rf.logs,args.Entries[i])
					}
					if args.LeaderCommit>rf.logs[len(rf.logs)-1].Index{
						rf.commitIndex=rf.logs[len(rf.logs)-1].Index
					}else{
						rf.commitIndex=args.LeaderCommit
					}
					//rf.persist()
					rf.commit()
					reply.Success=true
				}
			}
				
				//rf.mu.Unlock()
		}
		}else{
			reply.Term=rf.currentTerm
			reply.Success=true
			//rf.mu.Unlock()
			
		}
	}else{   //append entries
		//fmt.Println(args.PrevLogTerm,args.PrevLogIndex)
		if args.Term>=rf.currentTerm{
			if len(args.Entries)==0{
				rf.leaderId=-1
				/*
				rf.mu.Unlock()
				rf.toFollower<-1
				rf.mu.Lock()
				*/
				rf.currentTerm=args.Term
				rf.voteFor=-1
				rf.vote=0
				rf.append=0
				reply.Term=rf.currentTerm
				reply.CurrentSend=args.CurrentSend
				
				if rf.logs[len(rf.logs)-1].Term==args.PrevLogTerm&&rf.logs[len(rf.logs)-1].Index==args.PrevLogIndex{
					if(args.LeaderCommit>rf.logs[len(rf.logs)-1].Index){
						rf.commitIndex=rf.logs[len(rf.logs)-1].Index
					}else{
						rf.commitIndex=args.LeaderCommit
					}
					//rf.persist()
					rf.commit()
					reply.Success=true
				}else{
					reply.Success=false
				}
				//rf.persist()
				//rf.commit()
				
				//if args.LeaderCommit>rf.commitIndex&&rf.logs[len(rf.logs)-1].Term==args.PrevLogTerm&&rf.logs[len(rf.logs)-1].Index==args.PrevLogIndex{
				//if(args.LeaderCommit>rf.logs[len(rf.logs)-1].Index){
				//	rf.commitIndex=rf.logs[len(rf.logs)-1].Index
				//}else{
				//	rf.commitIndex=args.LeaderCommit
				//}
				//rf.persist()
				//rf.commit()
				//}
				//rf.mu.Unlock()
			}else{
				rf.leaderId=-1
				/*
				rf.mu.Unlock()
				rf.toFollower<-1
				rf.mu.Lock()
				*/
				rf.currentTerm=args.Term
				rf.voteFor=-1
				rf.vote=0
				rf.append=0
				reply.Term=rf.currentTerm
				reply.CurrentSend=args.CurrentSend
				/*
				find:=false
				for i:=0;i<len(args.Entries);i++{
					if args.Entries[i].Index>rf.commitIndex&&args.Entries[i].Term<rf.logs[rf.commitIndex].Term{
						find=true
					}
				}
				if rf.commitIndex>args.LeaderCommit||find{
					reply.Success=false
				}else{
					reply.Success=true
					rf.logs=rf.logs[:args.PrevLogIndex+1]
					for i:=0;i<len(args.Entries);i++{
						rf.logs=append(rf.logs,args.Entries[i])
					}
					if args.LeaderCommit>rf.logs[len(rf.logs)-1].Index{
						rf.commitIndex=rf.logs[len(rf.logs)-1].Index
					}else{
						rf.commitIndex=args.LeaderCommit
					}
					
					//rf.persist()
					rf.commit()
				}
				reply.CurrentSend=rf.logs[len(rf.logs)-1].Index
				*/
				
				if args.Entries[len(args.Entries)-1].Term<=rf.logs[len(rf.logs)-1].Term&&args.Entries[len(args.Entries)-1].Index<=rf.logs[len(rf.logs)-1].Index{
					reply.Success=true
					if args.LeaderCommit>rf.logs[len(rf.logs)-1].Index{
						rf.commitIndex=rf.logs[len(rf.logs)-1].Index
					}else{
						rf.commitIndex=args.LeaderCommit
					}
					
					//rf.persist()
					rf.commit()
				}else{
					find:=-1
					for i:=len(rf.logs)-1;i>=0;i--{
					if rf.logs[i].Index==args.PrevLogIndex&&rf.logs[i].Term==args.PrevLogTerm{
						find=i
						break
					}
				}
					if find==-1{
						reply.Success=false
					}else{
						/*
						//replace method
						for i:=0;i<len(args.Entries);i++{
							if (find+1+i)<=len(rf.logs)-1{
								rf.logs[find+1+i].Term=args.Entries[i].Term
								rf.logs[find+1+i].Index=args.Entries[i].Index
								rf.logs[find+1+i].Command=args.Entries[i].Command
							}else{
								rf.logs=append(rf.logs,args.Entries[i])
							}
						}
						if args.LeaderCommit>rf.logs[find+len(args.Entries)].Index{
							rf.commitIndex=rf.logs[find+len(args.Entries)].Index
						}else{
							rf.commitIndex=args.LeaderCommit
						}
						*/
						
						rf.logs=rf.logs[:find+1]
						for i:=0;i<len(args.Entries);i++{
							rf.logs=append(rf.logs,args.Entries[i])
						}
						if args.LeaderCommit>rf.logs[len(rf.logs)-1].Index{
							rf.commitIndex=rf.logs[len(rf.logs)-1].Index
						}else{
							rf.commitIndex=args.LeaderCommit
						}
						
						//rf.persist()
						rf.commit()
						reply.Success=true
					}
				}
				//rf.mu.Unlock()
			}
		}else{
				reply.Term=rf.currentTerm
				//reply.CurrentSend=args.CurrentSend
				//fmt.Println(rf.me," term is bigger,reply term: ",reply.Term)
				//rf.mu.Unlock()
				reply.Success=true
		}
	}
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	
	rf.mu.Lock()
	if rf.leaderId!=rf.me{
		rf.mu.Unlock()
		return false
	}
	//fmt.Println("server:",rf.me,"send append","term: ",rf.currentTerm,"leaderId: ",rf.leaderId)
	rf.mu.Unlock()
	//fmt.Println("i: ",server,"len: ",len(args.Entries))
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	if ok{
		if args.HeartBeat{   //heartbeat
			//rf.mu.Lock()
			if reply.Term>rf.currentTerm{
				rf.leaderId=-1
				/*
				rf.mu.Unlock()
				rf.toFollower<-1
				rf.mu.Lock()
				*/
				rf.currentTerm=reply.Term
				//rf.persist()
				rf.voteFor=-1
				rf.vote=0
				rf.append=0
				//rf.mu.Unlock()
			}else{
				if reply.Term==rf.currentTerm&&!reply.Success&&reply.CurrentSend==rf.logs[len(rf.logs)-1].Index{
					rf.nextIndex[server]--
					//fmt.Println("next--: ",server)
				}else{
					if reply.Term==rf.currentTerm&&reply.Success&&reply.CurrentSend==rf.logs[len(rf.logs)-1].Index{
						rf.nextIndex[server]=rf.logs[len(rf.logs)-1].Index+1
					}
				}
				//rf.mu.Unlock()
			}
		}else{    //append entries
			if reply.Success{
				//rf.mu.Lock()
				if reply.Term>rf.currentTerm{
					rf.leaderId=-1
					/*
					rf.mu.Unlock()
					rf.toFollower<-1
					rf.mu.Lock()
					*/
					rf.currentTerm=reply.Term
					//rf.persist()
					rf.voteFor=-1
					rf.vote=0
					rf.append=0
					//rf.mu.Unlock()
				}else {
						if reply.Term==rf.currentTerm&&reply.CurrentSend==rf.currentSend{
							if rf.appends[server]==false{
								rf.appends[server]=true
								rf.append++
								rf.nextIndex[server]=rf.currentSend+1
								rf.matchIndex[server]=args.LeaderCommit
								//rf.persist()
								//rf.mu.Unlock()
							}else{
								//rf.mu.Unlock()
							}
						}else{
							//rf.mu.Unlock()
						}
					}
			}else{
				if reply.Term==rf.currentTerm&&reply.CurrentSend==rf.currentSend{
					rf.nextIndex[server]--
				}
				//rf.mu.Lock()
				//rf.persist()
				//rf.mu.Unlock()
			}
		}
	}
	rf.persist()
	rf.mu.Unlock()
	return ok
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if args.Term>rf.currentTerm{
		if rf.leaderId==rf.me{
			rf.leaderId=-1
			rf.vote=0
			rf.append=0
			rf.currentTerm=args.Term
			rf.voteFor=-1
		}else{
			//rf.leaderId=-1
			rf.currentTerm=args.Term
			rf.voteFor=-1
			rf.vote=0
			rf.append=0
		}
		
		/*
		rf.mu.Unlock()
		rf.toFollower<-1
		rf.mu.Lock()
		*/
		//rf.persist()
	}
	/*
	lastTerm:=0
	lastIndex:=0
	for i:=0;i<len(rf.logs);i++{
		if rf.logs[i].Term>=lastTerm{
			lastTerm=rf.logs[i].Term
			lastIndex=rf.logs[i].Index
		}
	}
	*/
	if args.Term==rf.currentTerm{
		/*
		if rf.electionflag{
			
			if args.LastLogTerm>lastTerm||(args.LastLogTerm==lastTerm&&args.LastLogIndex>=lastIndex){
				rf.leaderId=-1
				
				rf.mu.Unlock()
				rf.toFollower<-1
				rf.mu.Lock()
				
				//fmt.Println("server:",rf.me," grant,args.LastLogTerm",args.LastLogTerm,".me.lastTerm:",rf.logs[len(rf.logs)-1].Term," args.LastLogIndex:",args.LastLogIndex," me.lastINdex:",rf.logs[len(rf.logs)-1].Index)
				reply.Term=rf.currentTerm
				reply.VoteGranted=true
				rf.voteFor=args.CandidateId
				rf.vote=0
				rf.append=0
				rf.electionflag=false
				//rf.mu.Unlock()
			}else{
				reply.Term=rf.currentTerm
				reply.VoteGranted=false
				//rf.mu.Unlock()
			}
		}else{
			*/
			if rf.voteFor==-1 || rf.voteFor==args.CandidateId{
				if args.LastLogTerm>rf.logs[len(rf.logs)-1].Term||(args.LastLogTerm==rf.logs[len(rf.logs)-1].Term&&args.LastLogIndex>=rf.logs[len(rf.logs)-1].Index){
					rf.leaderId=-1
					/*
					rf.mu.Unlock()
					rf.toFollower<-1
					rf.mu.Lock()
					*/
					//fmt.Println("server:",rf.me," grant,args.LastLogTerm",args.LastLogTerm,".me.lastTerm:",rf.logs[len(rf.logs)-1].Term," args.LastLogIndex:",args.LastLogIndex," me.lastINdex:",rf.logs[len(rf.logs)-1].Index)
					reply.Term=rf.currentTerm
					reply.VoteGranted=true
					rf.voteFor=args.CandidateId
					rf.vote=0
					rf.append=0
					//rf.mu.Unlock()
				}else{
					reply.Term=rf.currentTerm
					reply.VoteGranted=false
					//rf.mu.Unlock()
				}
			}else{
				reply.Term=rf.currentTerm
				reply.VoteGranted=false
				//rf.mu.Unlock()
			}
		}else{
		reply.Term=rf.currentTerm
		reply.VoteGranted=false
		}
		//rf.mu.Unlock()
	rf.persist()
	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//


func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	
	rf.mu.Lock()
	if rf.leaderId!=-2{
		rf.mu.Unlock()
		return false
	}
	//fmt.Println("server:",rf.me,"send requestvote ","term: ",rf.currentTerm,"leaderId: ",rf.leaderId)
	rf.mu.Unlock()
	
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	if ok {
		//rf.mu.Lock()
		if reply.Term>rf.currentTerm{
			rf.leaderId=-1
			/*
			rf.mu.Unlock()
			rf.toFollower<-1
			rf.mu.Lock()
			*/
			rf.currentTerm=reply.Term
			//rf.persist()
			rf.voteFor=-1
			rf.vote=0
			rf.append=0
			
			//rf.mu.Unlock()
		}else{
			if reply.Term==rf.currentTerm{
				if reply.VoteGranted{
					//fmt.Println("server",server,"grant")
					if rf.votes[server]==false{
						rf.votes[server]=true 
						rf.vote++
						//rf.mu.Unlock()
					}else{
						//rf.mu.Unlock()
					}
				}else{
					//rf.mu.Unlock()
				}
			}else{
				//rf.mu.Unlock()
			}
		}
	}
	rf.persist()
	rf.mu.Unlock()
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	if rf.leaderId!=rf.me{
		isLeader=false
		
		rf.mu.Unlock()
		time.Sleep(10*time.Millisecond)	
		return index, term, isLeader
		
	}else{
		isLeader=true
		
		for i:=0;i<len(rf.logs);i++{
			if command==rf.logs[i].Command{
				index=rf.logs[i].Index
				term=rf.logs[i].Term	
				rf.mu.Unlock()
				return index, term, isLeader
			}
		}

		logEntry:=LogEntry{}
		logEntry.Term=rf.currentTerm
		logEntry.Index=rf.logs[len(rf.logs)-1].Index+1
		logEntry.Command=command
		rf.logs=append(rf.logs,logEntry)
		//rf.persist()
		
		index=logEntry.Index
		//fmt.Println("server:",rf.me," start() command:",command," index:",index)
		term=rf.currentTerm
		//fmt.Println("leader:",rf.me," term:",rf.currentTerm," received a command:",command," Index:",index," start appending entry")

		rf.commandCh<-index
		
	}
	rf.mu.Unlock()
	/*
	for iter:=0;iter<3;iter++{
		time.Sleep(10*time.Millisecond)	
		rf.mu.Lock()
		if rf.leaderId!=rf.me{
			isLeader=false
			rf.mu.Unlock()
			return index, term, isLeader
		}else{
			if rf.lastApplied>=index{
				count:=1
				for i:=0;i<len(rf.matchIndex);i++{
					if rf.matchIndex[i]>=index{
						count++
					}
				}
				if count>=len(rf.peers)/2+1{
					rf.mu.Unlock()
					return index, term, isLeader
				}else{
					rf.mu.Unlock()
				}
			}else{
				rf.mu.Unlock()
			}
		}
	}
	*/
	time.Sleep(10*time.Millisecond)
	//isLeader=false	
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(time.Duration(rf.election)*time.Millisecond)
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

//follower/candidate loop to receive entry and election
func (rf *Raft) follower(){
	
	rf.mu.Lock()
	rf.leaderId=-2
	rf.electionflag=false
	rf.mu.Unlock()
	
	var timer *time.Timer
	var check *time.Timer
	timer=time.NewTimer(time.Duration(rf.election)*time.Millisecond)
	check=time.NewTimer(time.Duration(100)*time.Millisecond)
	//r := rand.New(rand.NewSource(time.Now().UnixNano()))
	//var timeout *time.Timer
	for rf.killed()==false{
		select{
		case <-timer.C:
			//t:=time.NewTimer(time.Duration(rf.electionTimeout)*time.Millisecond)
			/*
			rf.mu.Lock()
			rf.leaderId=-2
			rf.mu.Unlock()
			time.Sleep(time.Duration(10)*time.Millisecond)
			*/
			rf.mu.Lock()
			if rf.leaderId==-1{  
				//rf.toFollower=make(chan int,100)
				rf.leaderId=-1
				rf.mu.Unlock()
				go rf.follower()
				return 
			}
			
			//rf.mu.Lock()
			//rf.leaderId=-2
			rf.electionflag=true
			rf.currentTerm++
			//rf.persist()
			rf.voteFor=rf.me
			rf.vote=1
			rf.votes=make([]bool,len(rf.peers))
			rf.votes[rf.me]=true
			//fmt.Println("election timeout, server:",rf.me," start election for the term:",rf.currentTerm," lastIncludedTerm: ",rf.lastIncludedTerm," lastIncludedIndex: ",rf.lastIncludedIndex)
			//rf.electionTimeout=100+r.Intn(500)
			//if rf.currentTerm/2==0{
			//	rf.electionTimeout=300+50*rf.me
			//}else{
			//	rf.electionTimeout=300+50*(4-rf.me)
			//}
			rf.mu.Unlock()
			var timeout *time.Timer
			timeout=time.NewTimer(time.Duration(rf.electionTimeout)*time.Millisecond)
			for rf.killed()==false{
				select{
				case <-timeout.C:
					//time.Sleep(time.Duration(10)*time.Millisecond)
					
					rf.mu.Lock()
					if rf.leaderId==-1{  
						//rf.toFollower=make(chan int,100)
						rf.leaderId=-1
						rf.mu.Unlock()
						go rf.follower()
						return 
					}
					
					//rf.mu.Lock()
					rf.currentTerm++
					//rf.persist()
					//fmt.Println(rf.me," election has passed 1 s, start another election for the term ",rf.currentTerm,"last term:",rf.logs[len(rf.logs)-1].Term,"last index:",rf.logs[len(rf.logs)-1].Index," lastIncludedTerm: ",rf.lastIncludedTerm," lastIncludedIndex: ",rf.lastIncludedIndex)
					rf.voteFor=rf.me
					rf.vote=1
					rf.votes=make([]bool,len(rf.peers))
					rf.votes[rf.me]=true
					//rf.leaderId=-2
					//rf.electionTimeout=100+r.Intn(500)
					//if rf.currentTerm/2==0{
					//	rf.electionTimeout=300+200*rf.me
					//}else{
					//	rf.electionTimeout=300+200*(4-rf.me)
					//}
					rf.mu.Unlock()
					/*
					time.Sleep(time.Duration(10)*time.Millisecond)
					rf.mu.Lock()
					if rf.leaderId==-1{  
						//rf.toFollower=make(chan int,100)
						rf.mu.Unlock()
						go rf.follower()
						return 
					}
					rf.mu.Unlock()
					*/
					timeout=time.NewTimer(time.Duration(rf.electionTimeout)*time.Millisecond)
				case <-check.C:
					//fmt.Println(rf.me," turn to a follower")
					//rf.mu.Lock()
					//rf.toFollower=make(chan int,100)
					//rf.mu.Unlock()
					//go rf.follower()
					//return
					rf.mu.Lock()
					if rf.leaderId==-1{
						//fmt.Println(rf.me," turn to a follower")
						rf.mu.Unlock()
						go rf.follower()
						return
					}
					rf.mu.Unlock()
					check=time.NewTimer(time.Duration(100)*time.Millisecond)
				default:	
					
					//rf.mu.Unlock()
					//time.Sleep(time.Duration(10)*time.Millisecond)
					rf.mu.Lock()
					if rf.leaderId==-1{  
						//rf.toFollower=make(chan int,100)
						rf.leaderId=-1
						rf.mu.Unlock()
						go rf.follower()
						return 
					}
					
					for i:=0;i<len(rf.peers);i++{
						if i!=rf.me{
							args:=&RequestVoteArgs{}
							reply:=&RequestVoteReply{}
							args.Term=rf.currentTerm
							args.CandidateId=rf.me
							args.LastLogIndex=rf.logs[len(rf.logs)-1].Index
							args.LastLogTerm=rf.logs[len(rf.logs)-1].Term
							go rf.sendRequestVote(i,args,reply)
						}
					}
					rf.mu.Unlock()
					time.Sleep(50*time.Millisecond)
					rf.mu.Lock()
					
					if rf.leaderId==-1{
						//rf.toFollower=make(chan int,100)
						rf.leaderId=-1
						rf.mu.Unlock()
						go rf.follower()
						return 
					}
					
					if rf.vote>= len(rf.peers)/2+1{
						//rf.commandCh=make(chan interface{},100)
						
						//fmt.Println(rf.me," has successfully been elected as a leader in the term ",rf.currentTerm," ",rf.votes,"last index:",rf.logs[len(rf.logs)-1].Index,"last term:",rf.logs[len(rf.logs)-1].Term,"commit index:",rf.commitIndex," lastIncludedTerm: ",rf.lastIncludedTerm," lastIncludedIndex: ",rf.lastIncludedIndex)
						for i:=0;i<len(rf.nextIndex);i++{
							if rf.nextIndex[i]-rf.lastIncludedIndex>len(rf.logs){
								rf.nextIndex[i]=len(rf.logs)+rf.lastIncludedIndex
							}
						}
						rf.leaderId=rf.me
						//rf.toFollower=make(chan int,100)
						rf.mu.Unlock()
						go rf.leader()
						return
					}else{
						rf.mu.Unlock()
					}
				}
			}
		case <-check.C:
			//fmt.Println(rf.me," turn to a follower")
			//rf.mu.Lock()
			//rf.toFollower=make(chan int,100)
			//rf.mu.Unlock()
			//return
			rf.mu.Lock()
			if rf.leaderId==-1{
				//fmt.Println(rf.me," turn to a follower")
				rf.mu.Unlock()
				go rf.follower()
				return
			}
			rf.mu.Unlock()
			check=time.NewTimer(time.Duration(100)*time.Millisecond)
		}
	}
}
//leader loop to send heartbeat
func (rf *Raft) leader(){
	//time.Sleep(10*time.Millisecond)
	//fmt.Println("leader:",rf.me," become a leader, start sending heartbeat")
	if rf.killed(){
		return
	}
	rf.mu.Lock()
	if rf.leaderId!=rf.me{
		rf.commandCh=make(chan int,1000)
		//rf.toFollower=make(chan int,100)
		rf.leaderId=-1
		rf.mu.Unlock()
		go rf.follower()
		return
	}
	for i:=0;i<len(rf.peers);i++{
		if i!=rf.me{
			//send appendentries
			if rf.nextIndex[i]>rf.lastIncludedIndex{
				args:=&AppendEntriesArgs{}
				reply:=&AppendEntriesReply{}
				var entries []LogEntry
				//fmt.Println("heartbeat 1: nextindex:",rf.nextIndex[i],"logs:",len(rf.logs))	
				entries=make([]LogEntry,len(rf.logs)-(rf.nextIndex[i]-rf.lastIncludedIndex))
				copy(entries,rf.logs[(rf.nextIndex[i]-rf.lastIncludedIndex):])
				args.PrevLogTerm=rf.logs[(rf.nextIndex[i]-rf.lastIncludedIndex)-1].Term
				args.PrevLogIndex=rf.logs[(rf.nextIndex[i]-rf.lastIncludedIndex)-1].Index
				/*
				entries=make([]LogEntry,rf.commitIndex-rf.matchIndex[i])
				copy(entries,rf.logs[rf.matchIndex[i]+1:rf.commitIndex+1])
				args.PrevLogTerm=rf.logs[rf.matchIndex[i]].Term
				args.PrevLogIndex=rf.logs[rf.matchIndex[i]].Index
				*/
				args.Term=rf.currentTerm
				args.LeaderId=rf.me
				args.Entries=entries
				args.LeaderCommit=rf.commitIndex
				args.HeartBeat=true
				args.CurrentSend=rf.logs[len(rf.logs)-1].Index
				go rf.sendAppendEntries(i,args,reply)
			}else{   
				//fmt.Println("send snapshot")
				args:=&SnapShotArgs{}
				reply:=&SnapShotReply{}
				args.Term=rf.currentTerm
				args.CurrentSend=rf.logs[len(rf.logs)-1].Index
				args.SnapShot=rf.snapshot
				args.LastIncludedTerm=rf.lastIncludedTerm
				args.LastIncludedIndex=rf.lastIncludedIndex
				go rf.sendSnapShot(i,args,reply)
			}
		}
	}
	if rf.commitIndex<rf.logs[len(rf.logs)-1].Index{
		rf.commandCh<-rf.logs[len(rf.logs)-1].Index
	}
	rf.mu.Unlock()
	var timer *time.Timer
	for rf.killed()==false{
		timer=time.NewTimer(time.Duration(rf.heartbeat)*time.Millisecond)
		select{
		case <-timer.C:
			//fmt.Println("leader "+strconv.Itoa(rf.me)+" heartbeat timeout, start sending heartbeat")
			rf.mu.Lock()
			if rf.leaderId!=rf.me{
				rf.commandCh=make(chan int,1000)
				//rf.toFollower=make(chan int,100)
				rf.leaderId=-1
				rf.mu.Unlock()
				go rf.follower()
				return
			}else{
				for i:=0;i<len(rf.peers);i++{
					if i!=rf.me{
						//send appendentries
						if rf.nextIndex[i]>rf.lastIncludedIndex{
							args:=&AppendEntriesArgs{}
							reply:=&AppendEntriesReply{}
							var entries []LogEntry
							//fmt.Println("heartbeat 1: nextindex:",rf.nextIndex[i],"logs:",len(rf.logs))	
							entries=make([]LogEntry,len(rf.logs)-(rf.nextIndex[i]-rf.lastIncludedIndex))
							copy(entries,rf.logs[(rf.nextIndex[i]-rf.lastIncludedIndex):])
							args.PrevLogTerm=rf.logs[(rf.nextIndex[i]-rf.lastIncludedIndex)-1].Term
							args.PrevLogIndex=rf.logs[(rf.nextIndex[i]-rf.lastIncludedIndex)-1].Index
							/*
							entries=make([]LogEntry,rf.commitIndex-rf.matchIndex[i])
							copy(entries,rf.logs[rf.matchIndex[i]+1:rf.commitIndex+1])
							args.PrevLogTerm=rf.logs[rf.matchIndex[i]].Term
							args.PrevLogIndex=rf.logs[rf.matchIndex[i]].Index
							*/
							args.Term=rf.currentTerm
							args.LeaderId=rf.me
							args.Entries=entries
							args.LeaderCommit=rf.commitIndex
							args.HeartBeat=true
							args.CurrentSend=rf.logs[len(rf.logs)-1].Index
							go rf.sendAppendEntries(i,args,reply)
						}else{   
							args:=&SnapShotArgs{}
							reply:=&SnapShotReply{}
							args.Term=rf.currentTerm
							args.CurrentSend=rf.logs[len(rf.logs)-1].Index
							args.SnapShot=rf.snapshot
							args.LastIncludedTerm=rf.lastIncludedTerm
							args.LastIncludedIndex=rf.lastIncludedIndex
							go rf.sendSnapShot(i,args,reply)
						}
					}
				}
			}
			rf.mu.Unlock()
			//time.Sleep(10*time.Millisecond)
		case <-rf.toFollower:
			//fmt.Println("leader ",rf.me," is going to become a follower")
			rf.mu.Lock()
			rf.commandCh=make(chan int,1000)
			//rf.toFollower=make(chan int,100)
			rf.leaderId=-1
			rf.mu.Unlock()
			go rf.follower()
			return
		case command:=<-rf.commandCh:
			//time.Sleep(10*time.Millisecond)
			rf.mu.Lock()
			rf.currentSend=rf.logs[len(rf.logs)-1].Index
			rf.appends=make([]bool,len(rf.peers))
			rf.appends[rf.me]=true
			rf.append=1
			rf.mu.Unlock()
			for rf.killed()==false{
				select{
				case <-rf.toFollower:
					//fmt.Println("leader ",rf.me," is going to become a follower")
					rf.mu.Lock()
					rf.commandCh=make(chan int,1000)
					//rf.toFollower=make(chan int,100)
					rf.leaderId=-1
					rf.mu.Unlock()
					go rf.follower()
					return
				default:
					rf.mu.Lock()
					if command<=rf.commitIndex{
						//rf.leaderId=rf.me
						rf.mu.Unlock()
						go rf.leader()
						return
					}
					if rf.leaderId!=rf.me{
						rf.commandCh=make(chan int,1000)
						//rf.toFollower=make(chan int,100)
						rf.leaderId=-1
						rf.mu.Unlock()
						go rf.follower()
						return
					}
					for i:=0;i<len(rf.peers);i++{
						if i!=rf.me{
							if rf.nextIndex[i]>rf.lastIncludedIndex{
								args:=&AppendEntriesArgs{}
								reply:=&AppendEntriesReply{}
								var entries []LogEntry
								//fmt.Println("append: nextindex:",rf.nextIndex[i],"logs:",len(rf.logs),"currentsend:",rf.currentSend)
								entries=make([]LogEntry,rf.currentSend+1-rf.nextIndex[i])
								copy(entries,rf.logs[(rf.nextIndex[i]-rf.lastIncludedIndex):(rf.currentSend-rf.lastIncludedIndex+1)])
								args.PrevLogTerm=rf.logs[(rf.nextIndex[i]-rf.lastIncludedIndex)-1].Term
								args.PrevLogIndex=rf.logs[(rf.nextIndex[i]-rf.lastIncludedIndex)-1].Index
								/*
								entries=make([]LogEntry,rf.currentSend-rf.matchIndex[i])
								copy(entries,rf.logs[rf.matchIndex[i]+1:rf.currentSend+1])
								args.PrevLogTerm=rf.logs[rf.matchIndex[i]].Term
								args.PrevLogIndex=rf.logs[rf.matchIndex[i]].Index
								*/
								args.Term=rf.currentTerm
								args.LeaderId=rf.me
								args.Entries=entries
								args.LeaderCommit=rf.commitIndex
								args.CurrentSend=rf.currentSend
								args.HeartBeat=false
								go rf.sendAppendEntries(i,args,reply)
							}else{
								args:=&SnapShotArgs{}
								reply:=&SnapShotReply{}
								args.Term=rf.currentTerm
								args.CurrentSend=rf.logs[len(rf.logs)-1].Index
								args.SnapShot=rf.snapshot
								args.LastIncludedTerm=rf.lastIncludedTerm
								args.LastIncludedIndex=rf.lastIncludedIndex
								go rf.sendSnapShot(i,args,reply)
							}
						}
					}
					rf.mu.Unlock()
					time.Sleep(50*time.Millisecond)
					rf.mu.Lock()
					if rf.append>=len(rf.peers)/2+1{
						//fmt.Println("most servers have appended this entry to its log,leader:",rf.me," commit,term:",rf.currentTerm,rf.appends," commit index:",rf.currentSend," lastIncludedTerm: ",rf.lastIncludedTerm," lastIncludedIndex: ",rf.lastIncludedIndex)
						if rf.leaderId!=rf.me{
							rf.commandCh=make(chan int,1000)
							//rf.toFollower=make(chan int,100)
							rf.leaderId=-1
							rf.mu.Unlock()
							go rf.follower()
							return
						}
						rf.commitIndex=rf.currentSend
						rf.commit()
						rf.persist()
						//rf.leaderId=rf.me
						rf.mu.Unlock()
						go rf.leader()
						return 
					}
					rf.mu.Unlock()
				}
			}
		}
	}
}
// for a server to apply a command
func (rf *Raft) applier(applyCh chan ApplyMsg){
	var applyMsg ApplyMsg
	for rf.killed()==false{
		applyMsg=<-rf.applyCh
		applyCh<-applyMsg
	}
}
//apply a list of logs to applier
func (rf *Raft) apply(msgs []ApplyMsg){
	for i:=0;i<len(msgs);i++{
		rf.applyCh<-msgs[i]
	}
}
// for a server to commit
func (rf* Raft) commit(){
	//rf.mu.Lock()
	//tmp:=make([]ApplyMsg,0)
	//applymsgs:=make([]ApplyMsg,rf.commitIndex-rf.lastApplied)
	for i:=rf.lastApplied-rf.lastIncludedIndex+1;i<=rf.commitIndex-rf.lastIncludedIndex;i++{
		applyMsg:=ApplyMsg{}
		applyMsg.Command=rf.logs[i].Command
		applyMsg.CommandIndex=rf.logs[i].Index
		if applyMsg.Command==nil{
			applyMsg.CommandValid=false
		}else{
			applyMsg.CommandValid=true
		}
		/*
		fmt.Println("server:",rf.me,"logs:")
		for i:=0;i<len(rf.logs)-1;i++{
			fmt.Println("Index:",rf.logs[i].Index,"command:",rf.logs[i].Command)
		}
		*/
		//fmt.Println("server:",rf.me," commit command:",applyMsg.Command," index:",applyMsg.CommandIndex,"term:",rf.logs[i].Term," lastIncludedTerm: ",rf.lastIncludedTerm," lastIncludedIndex: ",rf.lastIncludedIndex)
		//rf.mu.Unlock()
		rf.applyCh<-applyMsg
		//tmp=append(tmp,applyMsg)
		//rf.mu.Lock()
		rf.lastApplied++
	}
	//rf.persist()
	//rf.mu.Unlock()
	//fmt.Println("server "+strconv.Itoa(rf.me)+" has commited")
	//copy(applymsgs,tmp)
	//go rf.apply(applymsgs)
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	//read persistent data from before persister
	if rf.persister.RaftStateSize()<=0{   //there is no persistent data to read
		rf.currentTerm=0
		rf.commitIndex=0
		rf.nextIndex=make([]int,len(peers))
		for i:=0;i<len(peers);i++{
			rf.nextIndex[i]=1
		}
		rf.matchIndex=make([]int,len(peers))
		for i:=0;i<len(peers);i++{
			rf.matchIndex[i]=0
		}
			//dummy entry
		dummy:= LogEntry{}
		dummy.Index=0
		dummy.Term=0
		dummy.Command=nil
		rf.logs=append(rf.logs,dummy)
		rf.lastIncludedTerm=0
		rf.lastIncludedIndex=0
	}else{   //there is persistent data to read
		// initialize from state persisted before a crash
		rf.readPersist(persister.ReadRaftState())
	}
	if rf.persister.SnapshotSize()<=0{
		rf.snapshot=nil
	}else{
		rf.snapshot=rf.persister.ReadSnapshot()
	}
	//persistent state on all servers

	rf.voteFor=-1
	//log  []*LogEntry
	
	//volatile state  on all servers
	//rf.commitIndex=-1
	rf.lastApplied=-1
	rf.vote=0  // number the votes this server get in a election
	
	//volatile state  on leaders

	//matchIndex []int
	//r := rand.New(rand.NewSource(time.Now().UnixNano()))
	//state about time
	rf.heartbeat=50   //intervals of heartsbeats:120 milliseconds
	//rf.election=250+r.Intn(100)+r.Intn(100)+r.Intn(100)+r.Intn(100)   //election timeout for starting an election:rand int between 350 and 500 milliseconds
	rf.election=100*me+400
	rf.electionTimeout=200*me+300//the time within which an election must be finished: 5 seconds
	//channel
	rf.toFollower=make(chan int)
	rf.toLeader=make(chan int)
	//rf.applyCh=make(chan ApplyMsg,100)
	rf.commandCh=make(chan int,1000)
	rf.snapshotCh=make(chan SnapShotInfo)
	rf.leaderId=-1

	//fmt.Println("server "+strconv.Itoa(rf.me)+" election time out: "+strconv.Itoa(rf.election))
	// start ticker goroutine to start elections
	//go rf.ticker()
	//go rf.applier(applyCh)
	rf.applyCh=applyCh
	rf.initflag=true
	//fmt.Println("server:",rf.me,"lastcommit:",rf.commitIndex," lastIncludedTerm: ",rf.lastIncludedTerm," lastIncludedIndex: ",rf.lastIncludedIndex)
	go rf.init()
	go rf.MakeSnapshot()
	return rf
}

func (rf* Raft) init(){
	time.Sleep(10*time.Millisecond)
	rf.mu.Lock()
	if rf.snapshot!=nil{
		applyMsg:=ApplyMsg{}
		applyMsg.SnapshotValid=true
		applyMsg.SnapshotTerm=rf.lastIncludedTerm
		applyMsg.SnapshotIndex=rf.lastIncludedIndex
		applyMsg.Snapshot=rf.snapshot
		rf.lastApplied=rf.lastIncludedIndex
		rf.applyCh<-applyMsg
	}
	rf.commit()
	rf.mu.Unlock()
	go rf.follower()
}
