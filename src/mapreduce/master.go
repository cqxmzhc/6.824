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

func (mr *MapReduce) getWorker() {
    for {
        workerAddr := <- mr.registerChannel
        workerInfo := &WorkerInfo{workerAddr}

        mr.Workers[workerAddr] = workerInfo
        mr.availableWorkers <- workerInfo
    }
}

func (mr *MapReduce) threadCall(availableWorker *WorkerInfo,
                                jobType JobType,
                                jobNumber int,
                                numOtherPhase int,
                                jobDone chan int) {

    args := &DoJobArgs{mr.file,
                       jobType,
                       jobNumber,
                       numOtherPhase}
    reply := DoJobReply{}

    ok := call(availableWorker.address, "Worker.DoJob", args, &reply)
    if ok == false {
        fmt.Printf("DoWork: RPC %s dojob %s error; jobNumber:%d\n",
                  availableWorker.address,
                  jobType,
                  jobNumber)
    }

    jobDone <- 1
    mr.availableWorkers <- availableWorker
}

func (mr *MapReduce) RunMaster() *list.List {
    nRemainMap := mr.nMap
    nRemainReduce := mr.nReduce

    jobDone := make(chan int, 100)

    go mr.getWorker()

    var availableWorker *WorkerInfo
    for nRemainMap > 0 {
        availableWorker = <- mr.availableWorkers

        go mr.threadCall(availableWorker, Map, mr.nMap - nRemainMap,
                         mr.nReduce,
                         jobDone)

        nRemainMap--
    }

    //the main thread have to wait for all workers to finish before it can do Reduce jobs
    isMapDone := 0
    for {
        <- jobDone
        if isMapDone += 1; isMapDone >= mr.nMap {
            break
        }
    }

    for nRemainReduce > 0 {
        availableWorker := <- mr.availableWorkers

        go mr.threadCall(availableWorker, Reduce,
                      mr.nReduce - nRemainReduce,
                      mr.nMap,
                      jobDone)

        nRemainReduce--
    }

    isReduceDone := 0
    for {
        <- jobDone
        if isReduceDone += 1; isReduceDone >= mr.nReduce{
            break
        }
    }

    return mr.KillWorkers()
}
