package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"runtime"
	"sync"

	"github.com/danhab99/idk/chans"
	"github.com/danhab99/idk/workers"
)

type Pipeline struct {
	db *Database
}

func NewPipeline(d *Database) Pipeline {
	return Pipeline{d}
}

var pipelineLogger = log.New(os.Stderr, "[PIPELINE] ", log.Ldate|log.Ltime|log.Lmsgprefix)

func (p *Pipeline) Execute(startStepName string, maxParallel int) int64 {
	db := p.db
	var numberOfExecutions int64

	steps := <-chans.Accumulate(db.ListSteps())
	stepsIndex := make(map[int64]Step)
	for _, s := range steps {
		stepsIndex[s.ID] = s
	}

	unprocessedTasks := p.IterateUnprocessed()
	seedTasks := p.Seed()

	inputTasks := chans.Merge(unprocessedTasks, seedTasks)

	type ScheduledTask struct {
		task Task
		done chan any
	}

	scheduledTasks := make(chan ScheduledTask)

	go func() {
		defer close(scheduledTasks)
		defer pipelineLogger.Println("Scheduling goroutine quitting")

		resourceMap := make(map[int64]int)

		for uTask := range inputTasks {
			resourceMap[uTask.ID]++
			id := *uTask.StepID

			if stepsIndex[id].Parallel == nil {
				doneChan := make(chan any)
				go func() {
					<-doneChan
					resourceMap[*uTask.StepID]--
				}()

				pipelineLogger.Printf("Acquired lock for %d, %d\n", &uTask.StepID, uTask.ID)

				scheduledTasks <- ScheduledTask{
					task: uTask,
					done: doneChan,
				}
			} else if resourceMap[id] <= *stepsIndex[id].Parallel {
				doneChan := make(chan any)
				go func() {
					<-doneChan
					resourceMap[*uTask.StepID]--
				}()

				pipelineLogger.Printf("Acquired lock for %d, %d\n", &uTask.StepID, uTask.ID)

				scheduledTasks <- ScheduledTask{
					task: uTask,
					done: doneChan,
				}
			}
		}
	}()

	updateChan := make(chan Task)

	go func() {
		defer close(updateChan)

		workers.Parallel0(scheduledTasks, maxParallel, func(inTask ScheduledTask) {
			task := inTask.task

			pipelineLogger.Printf("Executing taskID=%d\n", inTask.task.ID)
			nextTasks := p.ExecuteTask(task)
			numberOfExecutions++

			for nextTask := range nextTasks {
				updateChan <- nextTask
			}
		})
	}()

	for task := range updateChan {
		pipelineLogger.Printf("Task %d is complete\n", task.ID)
		db.UpdateTaskStatus(task.ID, true, nil)
	}

	return numberOfExecutions
}

func (p Pipeline) ExecuteTask(t Task) chan Task {
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

	// Buffer size should accommodate all potential outputs to prevent blocking
	outputTasks := make(chan Task, len(entries))

	var workerWg sync.WaitGroup
	workerWg.Add(1)

	defer workerWg.Wait()

	go func() {
		defer close(outputTasks)
		defer workerWg.Done()

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

			t := Task{
				ObjectHash:  hash,
				InputTaskID: inputTaskID,
				Processed:   isCompleted,
			}
			if nextStep != nil {
				t.StepID = &nextStep.ID
			}

			outputTasks <- t

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
	}()

	return outputTasks
}

func (p Pipeline) IterateUnprocessed() chan Task {
	db := p.db

	var tasksChans []chan Task

	for step := range db.ListSteps() {
		c := db.GetUnprocessedTasks(step.ID)
		tasksChans = append(tasksChans, c)
	}

	return chans.Merge(tasksChans...)
}

func (p Pipeline) Seed() chan Task {
	db := p.db

	startStep, err := db.GetStartingStep()
	if err != nil {
		panic(err)
	}

	t := Task{
		StepID: &startStep.ID,
	}

	return p.ExecuteTask(t)
}
