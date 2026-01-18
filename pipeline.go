package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"

	"sync"

	"github.com/danhab99/idk/chans"
	"github.com/danhab99/idk/workers"
	"github.com/fatih/color"
)

type Pipeline struct {
	db           *Database
	enabledSteps []Step
}

func NewPipeline(d *Database, steps []Step) Pipeline {
	return Pipeline{d, steps}
}

var pipelineLogger = NewColorLogger("[PIPELINE] ", color.New(color.FgCyan, color.Bold))

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
		// if s.Processed {
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

	color.New(color.FgMagenta).Fprintf(os.Stderr, "  → ")
	pipelineLogger.Printf("Task %d | Step: %s", t.ID, color.New(color.FgMagenta, color.Bold).Sprint(step.Name))

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
		pipelineLogger.Verbosef("    Input: %d bytes from %s", n, t.ObjectHash[:16]+"...")

	} else {
		pipelineLogger.Verbosef("    Input: (empty - start step)")
	}
	inputFile.Close()

	outputDir, err := os.MkdirTemp("/tmp", "output-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(outputDir)

	pipelineLogger.Verbosef("    Executing: %s", step.Script)
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
		pipelineLogger.Errorf("    Error starting script: %v", err)
		panic(err)
	}

	scriptLogger := NewColorLogger(fmt.Sprintf("[SCRIPT:%s] ", step.Name), color.New(color.FgYellow))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			scriptLogger.Verboseln(scanner.Text())
		}
	}()

	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			scriptLogger.Verbosef("[stderr] %s", scanner.Text())
		}
	}()

	wg.Wait()

	if err := cmd.Wait(); err != nil {
		pipelineLogger.Errorf("    Error executing script: %v", err)
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
				pipelineLogger.Verbosef("    Task %d already completed in next step", t.ID)
				return
			}
		}

		pipelineLogger.Verbosef("    Output: %s -> %s", filename, color.MagentaString(stepName))

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

	color.New(color.FgCyan, color.Bold).Fprintf(os.Stderr, "\n▶ ")
	pipelineLogger.Printf("Step: %s", color.New(color.FgMagenta, color.Bold).Sprint(s.Name))

	numberOfExecutions := int64(0)
	numberOfUnprocessedTasks := int64(0)

	// Count unprocessed tasks first
	for task := range db.GetUnprocessedTasks(s.ID) {
		if !task.Processed {
			numberOfUnprocessedTasks++
		}
	}

	if numberOfUnprocessedTasks == 0 {
		pipelineLogger.Verbosef("  No unprocessed tasks for step '%s'", s.Name)
		return 0
	}

	// Create progress bar
	bar := NewProgressBar(numberOfUnprocessedTasks, fmt.Sprintf("  Processing %s", s.Name))

	for task := range db.GetUnprocessedTasks(s.ID) {
		if task.Processed {
			continue
		}

		p.ExecuteTask(task)
		numberOfExecutions++
		bar.Add(1)
	}

	bar.Finish()

	pipelineLogger.Successf("  Step '%s' complete: %d/%d tasks", s.Name, numberOfExecutions, numberOfUnprocessedTasks)
	err := db.UpdateStepStatus(s.ID, true)
	if err != nil {
		panic(err)
	}

	return numberOfExecutions
}
