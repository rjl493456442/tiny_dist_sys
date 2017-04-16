package mapreduce

import (
	"sync"
	"log"
	"container/list"
	"fmt"
)

const (
	WORKER_STATUS_IDLE = iota
	WORKER_STATUS_BUSY
)

type WorkerInfo struct {
	address string
	status  int
}
type WorkerManager struct {
	workers map[string]*WorkerInfo
	lock    sync.RWMutex
}

func NewWorkerManager() *WorkerManager {
	workers := make(map[string]*WorkerInfo)
	return &WorkerManager{
		workers: workers,
	}
}

func (mgr *WorkerManager) Register(address string) {
	mgr.lock.Lock()
	defer mgr.lock.Unlock()
	if _, exist := mgr.workers[address]; exist == true {
		log.Printf("worker %s has already register\n", address)
		return
	}
	mgr.workers[address] = &WorkerInfo{
		address: address,
		status:  WORKER_STATUS_IDLE,
	}
}

func (mgr *WorkerManager) UnRegister(address string) {
	mgr.lock.Lock()
	defer mgr.lock.Unlock()
	if _, exist := mgr.workers[address]; exist == false {
		log.Printf("worker %s has already unregister\n", address)
		return
	}
	delete(mgr.workers, address)
}

func (mgr *WorkerManager) Select() string {
	mgr.lock.RLock()
	defer mgr.lock.RUnlock()
	// TODO use a more efficiency method
	for _, worker := range mgr.workers {
		if worker.status == WORKER_STATUS_IDLE {
			return worker.address
		}
	}
	return ""
}

func (mgr *WorkerManager) SelectAndMarkBusy() string {
	mgr.lock.Lock()
	defer mgr.lock.Unlock()
	// TODO use a more efficiency method
	for _, worker := range mgr.workers {
		if worker.status == WORKER_STATUS_IDLE {
			worker.status = WORKER_STATUS_BUSY
			return worker.address
		}
	}
	return ""
}

func (mgr *WorkerManager) MarkBusy(address string) {
	mgr.lock.Lock()
	defer mgr.lock.Unlock()
	if info, exist := mgr.workers[address]; exist != false {
		info.status = WORKER_STATUS_BUSY
	} else {
		log.Printf("worker %s doesn't exist\n", address)
		return
	}
}
func (mgr *WorkerManager) MarkIdle(address string) {
	mgr.lock.Lock()
	defer mgr.lock.Unlock()
	if info, exist := mgr.workers[address]; exist != false {
		info.status = WORKER_STATUS_IDLE
	} else {
		log.Printf("worker %s doesn't exist\n", address)
		return
	}
}

func (mgr *WorkerManager) HandleRegister(registerChannel chan string, exitChannel chan bool) {
	for {
		select {
		case <- exitChannel:
			break
		case addr := <- registerChannel:
			mgr.Register(addr)
		}
	}
}
// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mgr *WorkerManager) KillWorkers() *list.List {
	l := list.New()
	for addr := range mgr.workers {
		DPrintf("DoWork: shutdown %s\n", addr)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(addr, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", addr)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}
