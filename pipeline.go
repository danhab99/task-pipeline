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

func (p *Pipeline) Execute(startStepName string, maxParallel int) int {
	db := p.db
	numberOfExecutions := 0

	steps := db.ListSteps()

	msgBus := make(map[string]chan Task)

	var wg sync.WaitGroup

	resourcePool := make(chan struct{}, 20) // capacity = max concurrent work

	stepsCount, err := db.CountStepsWithoutParallel()
	if err != nil {
		panic(err)
	}

	for step := range steps {
		msgBus[step.Name] = make(chan Task, 1000)

		parallel := 0
		if step.Parallel == nil {
			parallel = maxParallel / int(stepsCount)
		} else {
			parallel = *step.Parallel
		}

		var parallelWg sync.WaitGroup
		go func() {
			parallelWg.Wait()
			close(msgBus[step.Name])
		}()

		for range parallel {
			wg.Add(1)
			parallelWg.Add(1)
			go func() {
				defer wg.Done()
				defer parallelWg.Done()

				tasksChan := db.GetUnprocessedTasks(step.ID)

				for task := range chans.Merge(msgBus[step.Name], tasksChan) {
					resourcePool <- struct{}{} // acquire

					nextTasks := p.ExecuteTask(task)
					numberOfExecutions++

					<-resourcePool // release immediately after task completes

					for nextTask := range nextTasks {
						nextStepInfo, err := db.GetStep(*nextTask.StepID)
						if err != nil {
							panic(err)
						}
						msgBus[nextStepInfo.Name] <- nextTask
					}
				}
			}()
		}
	}

	var startStep *Step

	if startStepName == "" {
		startStep, err = db.GetStartingStep()
	} else {
		startStep, err = db.GetStepByName(startStepName)
	}

	if err != nil {
		panic(err)
	}

	msgBus[startStep.Name] <- Task{
		StepID: &startStep.ID,
	}

	wg.Wait()
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
			scriptLogger.Printf("[stderr] %s", scanner.Text())
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

			nextStep, err := db.GetStepByName(stepName)
			if err != nil {
				panic(err)
			}

			isCompleted, err := db.IsTaskCompletedInNextStep(nextStep.ID, t.ID)
			if err != nil {
				panic(err)
			}

			if isCompleted {
				fmt.Printf("This step is already completed %d\n", t.ID)
				return
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

			outputTasks <- Task{
				ObjectHash:  hash,
				StepID:      &nextStep.ID,
				InputTaskID: inputTaskID,
				Processed:   isCompleted,
			}

			objectPath := db.GetObjectPath(hash)
			_, err = copyFileWithSHA256(filePath, objectPath)
			if err != nil {
				panic(err)
			}

			_, err = db.CreateTask(hash, &nextStep.ID, inputTaskID)
			if err != nil {
				panic(err)
			}
		})
	}()

	return outputTasks
}
