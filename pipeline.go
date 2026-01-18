package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"runtime"
	// "slices"
	"sync"

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
	var numberOfExecutions int64

	steps := <-chans.Accumulate(db.ListSteps())
	stepsIndex := make(map[int64]Step)
	for _, s := range steps {
		// if s.IsStart {
		// 	continue
		// }
		// if slices.ContainsFunc(p.enabledSteps, func(e Step) bool {
		// 	return s.Name == e.Name
		// }) {
		// 	continue
		// }
		stepsIndex[s.ID] = s
	}

	// runtime.Breakpoint()
	p.Seed()

	for _, step := range stepsIndex {
		numberOfExecutions += p.ExecuteStep(step)
	}

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

	entriesChan := make(chan os.DirEntry)

	go func() {
		defer close(entriesChan)
		for _, entry := range entries {
			entriesChan <- entry
		}
	}()

	err = db.UpdateTaskStatus(t.ID, true, nil)
	if err != nil {
		panic(err)
	}

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
		_, err = db.CreateAndGetTask(pTask)
		if err != nil {
			panic(err)
		}

		objectPath := db.GetObjectPath(hash)
		_, err = copyFileWithSHA256(filePath, objectPath)
		if err != nil {
			panic(err)
		}
	})

}

// func (p Pipeline) IterateUnprocessed() chan Task {
// 	db := p.db

// 	var tasksChans []chan Task

// 	for step := range db.ListSteps() {
// 		if !slices.Contains(p.enabledSteps, step) {
// 			continue
// 		}
// 		if step.IsStart {
// 			continue
// 		}

// 		c := db.GetUnprocessedTasks(step.ID)
// 		tasksChans = append(tasksChans, c)
// 	}

// 	return chans.Merge(tasksChans...)
// }

func (p Pipeline) Seed() {
	db := p.db

	startStep, err := db.GetStartingStep()
	if err != nil {
		panic(err)
	}
	if startStep == nil {
		panic("start step cannot be nil")
	}

	processedTaskCount := 0

	for task := range db.GetTasksForStep(startStep.ID) {
		if task.Processed {
			processedTaskCount++
		}
	}

	if processedTaskCount == 0 {
		prestartTask := Task{
			StepID: &startStep.ID,
		}

		startTaskId, err := db.CreateTask(prestartTask)
		if err != nil {
			panic(err)
		}

		startTask, err := db.GetTask(startTaskId)
		p.ExecuteTask(*startTask)
	}

	err = db.UpdateStepStatus(startStep.ID, true)
	if err != nil {
		panic(err)
	}
}

func (p Pipeline) ExecuteStep(s Step) int64 {
	db := p.db

	pipelineLogger.Printf("Executing step id=%d name=%s\n", s.ID, s.Name)

	numberOfExecutions := int64(0)
	numberOfUnprocessedTasks := int64(0)

	for task := range db.GetUnprocessedTasks(s.ID) {
		numberOfUnprocessedTasks++
		if task.Processed {
			continue
		}

		p.ExecuteTask(task)
		numberOfExecutions++
	}

	pipelineLogger.Printf("Step %s is complete count=%d/%d\n", s.Name, numberOfExecutions, numberOfUnprocessedTasks)
	err := db.UpdateStepStatus(s.ID, true)
	if err != nil {
		panic(err)
	}

	return numberOfExecutions
}
