package mapreduce

import "container/list"
import (
	"time"
	"sync"
)




func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	mr.handoutMapJob()
	mr.handoutReduceJob()
	return mr.workerMgr.KillWorkers()
}

func (mr *MapReduce) handoutMapJob() {
	var wg sync.WaitGroup
	for i := 0; i < mr.nMap; i += 1 {
		wg.Add(1)
		go func(id int) {
			worker := mr.selectWorkerUntil()
			arg := &DoJobArgs{
				File: mr.file,
				Operation: Map,
				JobNumber: id,
				NumOtherPhase: mr.nReduce,
			}
			var reply DoJobReply
			success := call(worker, "Worker.DoJob", arg, &reply)
			if !success {

			} else {
				mr.workerMgr.MarkIdle(worker)
				wg.Done()
			}
		}(i)
	}
	wg.Wait()
}

func (mr *MapReduce) handoutReduceJob() {
	var wg sync.WaitGroup
	for i := 0; i < mr.nReduce; i += 1 {
		wg.Add(1)
		go func(id int) {
			worker := mr.selectWorkerUntil()
			arg := &DoJobArgs{
				File: mr.file,
				Operation: Reduce,
				JobNumber: id,
				NumOtherPhase: mr.nMap,
			}
			var reply DoJobReply
			success := call(worker, "Worker.DoJob", arg, &reply)
			if !success {

			} else {
				mr.workerMgr.MarkIdle(worker)
				wg.Done()
			}
		}(i)
	}
	wg.Wait()

}

func (mr *MapReduce) selectWorkerUntil() string {
	ticker := time.NewTicker(10 * time.Millisecond)
	for {
		select {
		case <- ticker.C:
			addr := mr.workerMgr.SelectAndMarkBusy()
			if addr != "" {
				return addr
			}
		}
	}
}
