package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {

	mapChan := make(chan int, mr.nMap)
	reduceChan := make(chan int, mr.nReduce)

	var workerDoJob = func(worker string, args *DoJobArgs) bool {
		var reply DoJobReply
		ok := call(worker, "Worker.DoJob", args, &reply)
		return ok
	}

	for i := 0; i < mr.nMap; i++ {
		go func(index int) {
			for {
				worker := <-mr.registerChannel
				args := &DoJobArgs{mr.file, Map, index, mr.nReduce}
				ok := workerDoJob(worker, args)
				if ok {
					mapChan <- index
					mr.registerChannel <- worker
					return
				}
			}
		}(i)
	}

	for i := 0; i < mr.nMap; i++ {
		<-mapChan
	}

	for i := 0; i < mr.nReduce; i++ {
		go func(index int) {
			worker := <-mr.registerChannel
			args := &DoJobArgs{mr.file, Reduce, index, mr.nMap}
			ok := workerDoJob(worker, args)
			if ok {
				reduceChan <- index
				mr.registerChannel <- worker
				return
			}
		}(i)
	}

	for i := 0; i < mr.nReduce; i++ {
		<-reduceChan
	}

	return mr.KillWorkers()
}
