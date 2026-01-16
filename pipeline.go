package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/danhab99/idk/chans"
	"github.com/danhab99/idk/workers"
)

type Pipeline struct {
	db           *Database
	enabledSteps []Step
}

func NewPipeline(d *Database, steps []Step) Pipeline {
	return Pipeline{d, steps}
}

var pipelineLogger = log.New(os.Stderr, "[PIPELINE] ", log.Ldate|log.Ltime|log.Lmsgprefix)

func (p *Pipeline) Execute(startStepName string, maxParallel int) int64 {
	db := p.db

	pipelineLogger.Println("Starting pipeline execution")
	allSteps := <-chans.Accumulate(db.ListSteps())
	pipelineLogger.Printf("Loaded %d steps from database", len(allSteps))

	isStepAllowed := func(s Step) bool {
		return slices.ContainsFunc(p.enabledSteps, func(enabledStep Step) bool {
			return enabledStep.ID == s.ID
		})
	}

	numberOfExecutions := int64(0)

	pipelineLogger.Println("Starting seed phase")
	seedChan := p.Seed(startStepName)
	for t := range seedChan {
		pipelineLogger.Printf("Processing seed task %d", t.ID)
		p.ExecuteTask(t)
		numberOfExecutions++
	}
	pipelineLogger.Println("Seed phase completed")

	pipelineLogger.Println("Starting step execution phase")
	for _, step := range allSteps {
		if isStepAllowed(step) {
			pipelineLogger.Printf("Executing step: %s (ID: %d)", step.Name, step.ID)
			numberOfExecutions += p.ExecuteStep(step, maxParallel)
			pipelineLogger.Printf("Completed step: %s", step.Name)
		}
	}
	pipelineLogger.Println("All steps completed")

	return numberOfExecutions
}

func (p Pipeline) ExecuteTask(t Task) {
	db := p.db

	step, err := db.GetStep(*t.StepID)
	if err != nil {
		panic(err)
	}

	runLogger.Printf("Processing task %d for step '%s'", t.ID, step.Name)

	t.Processed = true

	inputFile, err := os.CreateTemp("/tmp", "input-*")
	if err != nil {
		panic(err)
	}
	defer os.Remove(inputFile.Name())

	if t.ObjectHash != "" {
		objectPath := db.GetObjectPath(t.ObjectHash)
		data, err := os.Open(objectPath)
		if err != nil {
			panic(err)
		}
		n, err := io.Copy(inputFile, data)
		if err != nil {
			panic(err)
		}
		runLogger.Printf("  Input: %d bytes from %s", n, t.ObjectHash[:16]+"...")

	} else {
		runLogger.Println("  Input: (empty - start step)")
	}
	inputFile.Close()

	outputDir, err := os.MkdirTemp("/tmp", "output-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(outputDir)

	runLogger.Printf("  Executing script for step '%s'", step.Name)
	cmd := exec.Command("sh", "-c", step.Script)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("INPUT_FILE=%s", inputFile.Name()),
		fmt.Sprintf("OUTPUT_DIR=%s", outputDir),
	)

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		panic(err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		panic(err)
	}

	if err := cmd.Start(); err != nil {
		runLogger.Printf("  Error starting script: %v", err)
		panic(err)
	}

	scriptLogger := log.New(os.Stderr, fmt.Sprintf("[SCRIPT:%s] ", step.Name), log.Ldate|log.Ltime|log.Lmsgprefix)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			scriptLogger.Println(scanner.Text())
		}
	}()

	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			// scriptLogger.Printf("[stderr] %s", scanner.Text())
		}
	}()

	wg.Wait()

	if err := cmd.Wait(); err != nil {
		runLogger.Printf("  Error executing script: %v", err)
	}

	// runtime.Breakpoint()
	err = db.UpdateStepStatus(t.ID, true)
	if err != nil {
		panic(err)
	}

	entries, err := os.ReadDir(outputDir)
	if err != nil {
		panic(err)
	}

	runLogger.Printf("  Found %d output entries to process", len(entries))

	entriesChan := make(chan os.DirEntry)

	go func() {
		defer close(entriesChan)
		defer runLogger.Printf("  Closed entriesChan for task %d", t.ID)
		for _, entry := range entries {
			entriesChan <- entry
		}
	}()

	// Buffer size should accommodate all potential outputs to prevent blocking
	outputTasks := make(chan Task, len(entries))

	err = db.UpdateTaskStatus(t.ID, true, nil)
	if err != nil {
		panic(err)
	}

	runLogger.Printf("  Starting workers to process entries for task %d", t.ID)
	workers.Parallel0(entriesChan, runtime.NumCPU(), func(entry os.DirEntry) {
		if entry.IsDir() {
			return
		}

		filename := entry.Name()
		stepName := extractStepName(filename)
		filePath := fmt.Sprintf("%s/%s", outputDir, filename)

		var isCompleted bool

		nextStep, err := db.GetStepByName(stepName)
		if err != nil {
			panic(err)
		}
		if nextStep != nil {
			isCompleted, err = db.IsTaskCompletedInNextStep(nextStep.ID, t.ID)
			if err != nil {
				panic(err)
			}

			if isCompleted {
				fmt.Printf("This step is already completed %d\n", t.ID)
				return
			}
		}

		runLogger.Printf("	Output: %s -> step '%s'", filename, stepName)

		hash, err := hashFileSHA256(filePath)
		if err != nil {
			panic(err)
		}

		// Only set InputTaskID if current task has a valid DB ID
		var inputTaskID *int64
		if t.ID > 0 {
			inputTaskID = &t.ID
		}

		pTask := Task{
			ObjectHash:  hash,
			InputTaskID: inputTaskID,
			Processed:   isCompleted,
		}

		if nextStep != nil {
			pTask.StepID = &nextStep.ID
		}
		t, err := db.CreateAndGetTask(pTask)
		if err != nil {
			panic(err)
		}

		outputTasks <- *t

		objectPath := db.GetObjectPath(hash)
		_, err = copyFileWithSHA256(filePath, objectPath)
		if err != nil {
			panic(err)
		}

		if !isCompleted {
			_, err = db.CreateTask(Task{
				ObjectHash:  hash,
				StepID:      t.StepID,
				InputTaskID: inputTaskID,
			})
			if err != nil {
				panic(err)
			}
		}
	})
	runLogger.Printf("  Workers finished for task %d", t.ID)

}

// func (p Pipeline) IterateUnprocessed() chan Task {
// 	db := p.db

// 	var tasksChans []chan Task

// 	for step := range db.ListSteps() {
// 		if !slices.Contains(p.enabledSteps, step) {
// 			continue
// 		}

// 		c := db.GetUnprocessedTasks(step.ID)
// 		tasksChans = append(tasksChans, c)
// 	}

// 	return chans.Merge(tasksChans...)
// }

func (p Pipeline) Seed(startStepName string) chan Task {
	db := p.db

	var startStep *Step
	var err error

	if startStepName != "" {
		startStep, err = db.GetStepByName(startStepName)
	} else {
		startStep, err = db.GetStartingStep()
	}
	if err != nil {
		panic(err)
	}
	if startStep == nil {
		panic("start step cannot be nil")
	}

	unprocessedCount, err := db.CountUnprocessedTasksForStep(startStep.ID)
	if err != nil {
		panic(err)
	}
	totalCount, err := db.CountTasksForStep(startStep.ID)
	if err != nil {
		panic(err)
	}

	pipelineLogger.Printf("Seed: step=%s, unprocessed=%d, total=%d", startStep.Name, unprocessedCount, totalCount)

	out := make(chan Task, 1)

	if unprocessedCount < totalCount {
		pipelineLogger.Println("Seed: returning unprocessed tasks channel")
		return db.GetUnprocessedTasks(startStep.ID)
	} else if totalCount == 0 {
		pipelineLogger.Println("Seed: creating new start task")
		prestartTask := Task{
			StepID: &startStep.ID,
		}

		startTaskId, err := db.CreateTask(prestartTask)
		if err != nil {
			panic(err)
		}
		startTask, err := db.GetTask(startTaskId)

		out <- *startTask
		close(out)
		pipelineLogger.Println("Seed: start task sent and channel closed")
	} else {
		pipelineLogger.Println("Seed: no tasks to process, closing empty channel")
		close(out)
	}

	return out
}

func (p Pipeline) ExecuteStep(s Step, maxParallel int) int64 {
	pipelineLogger.Printf("ExecuteStep: starting step '%s' (ID: %d)", s.Name, s.ID)
	unprocessedTasks := p.db.GetUnprocessedTasks(s.ID)

	parallel := maxParallel
	if s.Parallel != nil {
		parallel = *s.Parallel
	}

	pipelineLogger.Printf("ExecuteStep: using %d parallel workers for step '%s'", parallel, s.Name)

	var count atomic.Int64
	var wg sync.WaitGroup
	wg.Add(parallel)

	for i := range parallel {
		go func(workerID int) {
			defer wg.Done()
			pipelineLogger.Printf("ExecuteStep: worker %d started for step '%s'", workerID, s.Name)
			taskCount := 0
			for task := range unprocessedTasks {
				taskCount++
				pipelineLogger.Printf("ExecuteStep: worker %d processing task %d (step '%s')", workerID, task.ID, s.Name)
				p.ExecuteTask(task)

				count.Add(1)

				err := p.db.UpdateTaskStatus(task.ID, true, nil)
				if err != nil {
					panic(err)
				}
			}
			pipelineLogger.Printf("ExecuteStep: worker %d finished, processed %d tasks for step '%s'", workerID, taskCount, s.Name)
		}(i)
	}

	pipelineLogger.Printf("ExecuteStep: waiting for workers to complete for step '%s'", s.Name)
	wg.Wait()
	pipelineLogger.Printf("ExecuteStep: all workers completed for step '%s', total tasks: %d", s.Name, count.Load())

	return count.Load()
}
