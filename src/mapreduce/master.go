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
	jobs := make(chan *DoJobArgs)
	done := make(chan bool)

	workerDoJob := func(worker string, args *DoJobArgs) {
		var reply DoJobReply
		ok := call(worker, "Worker.DoJob", args, &reply)
		if ok {
			mr.registerChannel <- worker
		} else {
			jobs <- args
		}
	}

	go func() {
		for {
			job, more := <-jobs
			if more {
				worker := <-mr.registerChannel
				fmt.Println("received job", job)
				go workerDoJob(worker, job)
			} else {
				fmt.Println("received all jobs")
				done <- true
				return
			}
		}
	}()

	for i := 0; i < mr.nMap; i++ {
		args := &DoJobArgs{mr.file, Map, i, mr.nReduce}
		jobs <- args
	}
	//close(jobs)

	for i := 0; i < mr.nReduce; i++ {
		args := &DoJobArgs{mr.file, Reduce, i, mr.nMap}
		jobs <- args
	}
	close(jobs)

	<-done

	return mr.KillWorkers()
}
