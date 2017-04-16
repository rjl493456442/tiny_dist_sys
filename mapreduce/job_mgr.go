package mapreduce

import "sync"

const (
	JOB_PENDING = iota
	JOB_EXEC
	JOB_DONE
)

const (
	JOB_TYPE_MAP = iota
	JOB_TYPE_REDUCE
)

type JobInfo struct {
	Id            int
	FileName      string
	RemoteAddress string
	OutputName    string
	Status        int
}

type JobManager struct {
	mapLock         sync.RWMutex
	MapJob          map[int]*JobInfo
	reduceLock      sync.RWMutex
	ReduceJob       map[int]*JobInfo
}

func NewJobManager() *JobManager {
	mapJob := make(map[int]*JobInfo)
	reduceJob := make(map[int]*JobInfo)
	return &JobManager{
		MapJob:     mapJob,
		ReduceJob:  reduceJob,
	}
}

func (mgr *JobManager) AddJob(t int, jobId int, job *JobInfo) {
	switch t {
	case JOB_TYPE_MAP:
		mgr.addMapJob(jobId, job)
	case JOB_TYPE_REDUCE:
		mgr.addReduceJob(jobId, job)
	default:
		return
	}
}

/*
	Map
 */
func (mgr *JobManager) addMapJob(jobId int, job *JobInfo) {
	mgr.mapLock.Lock()
	defer mgr.mapLock.Unlock()
	mgr.MapJob[jobId] = job
}

func (mgr *JobManager) SelectPendingMapJob() int {
	mgr.mapLock.RLock()
	defer mgr.mapLock.RUnlock()
	for id, job := range mgr.MapJob {
		if job.Status == JOB_PENDING {
			return id
		}
	}
	return -1
}

func (mgr *JobManager) GetMapJob(jobId int) *JobInfo{
	mgr.mapLock.RLock()
	defer mgr.mapLock.RUnlock()
	return mgr.MapJob[jobId]
}

func (mgr *JobManager) MarkMapJobExec(jodId int) {
	mgr.mapLock.Lock()
	defer mgr.mapLock.Unlock()
	job, exist := mgr.MapJob[jodId]
	if exist == true {
		job.Status = JOB_EXEC
	}
}

func (mgr *JobManager) MarkMapJobDone(jodId int) {
	mgr.mapLock.Lock()
	defer mgr.mapLock.Unlock()
	job, exist := mgr.MapJob[jodId]
	if exist == true {
		job.Status = JOB_DONE
	}
}

func (mgr *JobManager) DelMapJob(jobId int) {
	mgr.mapLock.Lock()
	defer mgr.mapLock.Unlock()
	delete(mgr.MapJob, jobId)
}


/*
	Reduce
 */
func (mgr *JobManager) addReduceJob(jobId int, job *JobInfo) {
	mgr.reduceLock.Lock()
	defer mgr.reduceLock.Unlock()
	mgr.ReduceJob[jobId] = job
}

func (mgr *JobManager) SelectPendingReduceJob() int {
	mgr.reduceLock.RLock()
	defer mgr.reduceLock.RUnlock()
	for id, job := range mgr.ReduceJob {
		if job.Status == JOB_PENDING {
			return id
		}
	}
	return -1
}

func (mgr *JobManager) GetReduceJob(jobId int) *JobInfo{
	mgr.reduceLock.RLock()
	defer mgr.reduceLock.RUnlock()
	return mgr.ReduceJob[jobId]
}

func (mgr *JobManager) MarkReduceJobExec(jodId int) {
	mgr.reduceLock.Lock()
	defer mgr.reduceLock.Unlock()
	job, exist := mgr.ReduceJob[jodId]
	if exist == true {
		job.Status = JOB_EXEC
	}
}

func (mgr *JobManager) MarkReduceJobDone(jodId int) {
	mgr.reduceLock.Lock()
	defer mgr.reduceLock.Unlock()
	job, exist := mgr.ReduceJob[jodId]
	if exist == true {
		job.Status = JOB_DONE
	}
}
func (mgr *JobManager) DelReduceJob(jobId int) {
	mgr.reduceLock.Lock()
	defer mgr.reduceLock.Unlock()
	delete(mgr.ReduceJob, jobId)
}
