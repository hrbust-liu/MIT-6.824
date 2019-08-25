package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	wg := sync.WaitGroup{}
	wg.Add(ntasks)
	args := new(DoTaskArgs)
	args.JobName = jobName
	args.Phase = phase
	args.NumOtherPhase = n_other
	for i := 0; i < ntasks; i++ {
		wk := <-registerChan
		go func(i int){
			for {
				fmt.Printf("task %d start\n", i)
				args.TaskNumber = i
				if (phase == mapPhase) {
					args.File = mapFiles[args.TaskNumber]
				}
				fmt.Printf("before given task %d\n", args.TaskNumber)
				ok := call(wk, "Worker.DoTask", args, nil)
				if ok {
					wg.Done()
					fmt.Printf("wg.Done%d\n", args.TaskNumber)
					registerChan <- wk
					fmt.Printf("register %s\n", wk)
					return
				} else {
					fmt.Printf("worker %s fail\n", wk)
					wk = <-registerChan
				}
			}
		}(i)
	}
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
