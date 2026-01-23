package main

import (
	"fmt"
	"log"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/danhab99/idk/chans"
	"github.com/danhab99/idk/workers"
	"github.com/fatih/color"
)

const LOG_FLAGS = log.Lmicroseconds | log.Lshortfile

var pipelineLogger = log.New(os.Stderr, "[PIPELINE]", LOG_FLAGS)

type Pipeline struct {
	db           *Database
	enabledSteps []Step
	stepInputs   map[int64][]string // step_id -> list of input resource names
}

func NewPipeline(d *Database, steps []Step, stepInputs map[int64][]string) (*Pipeline, error) {
	p := &Pipeline{
		db:           d,
		enabledSteps: steps,
		stepInputs:   stepInputs,
	}

	return p, nil
}



func (p *Pipeline) GetStepsByInputName(resourceName string) []int64 {
	pipelineLogger.Printf("Looking up steps for resource name: '%s'\n", resourceName)
	pipelineLogger.Printf("Available stepInputs map: %+v\n", p.stepInputs)
	var stepIDs []int64
	for stepID, inputs := range p.stepInputs {
		for _, inputName := range inputs {
			if inputName == resourceName {
				pipelineLogger.Printf("  Found match: step %d consumes '%s'\n", stepID, resourceName)
				stepIDs = append(stepIDs, stepID)
				break
			}
		}
	}
	pipelineLogger.Printf("  Found %d steps that consume '%s'\n", len(stepIDs), resourceName)
	return stepIDs
}

func (p *Pipeline) Execute(startStepName string, maxParallel int) int64 {
	db := p.db
	var numberOfExecutions int64

	steps := <-chans.Accumulate(db.ListSteps())
	stepsIndex := make(map[int64]Step)
	for _, s := range steps {
		if len(p.enabledSteps) > 0 {
			if slices.ContainsFunc(p.enabledSteps, func(step Step) bool {
				return s.Name == step.Name
			}) {
				stepsIndex[s.ID] = s
			}
		} else {
			stepsIndex[s.ID] = s
		}
	}

	for _, step := range stepsIndex {
		numberOfExecutions += p.ExecuteStep(step, maxParallel)
	}

	return numberOfExecutions
}

func (p Pipeline) ExecuteTask(t Task, fuseWatcher *FuseWatcher) {
	db := p.db

	step, err := db.GetStep(t.StepID)
	if err != nil {
		panic(err)
	}

	t.Processed = true

	// Execute the script with FUSE mount path
	executor := NewScriptExecutor(db, &p, fuseWatcher.mountPath)
	execErr := executor.Execute(t, *step)

	// Update task status
	var errorMsg *string
	if execErr != nil {
		msg := execErr.Error()
		errorMsg = &msg
	}
	err = db.UpdateTaskStatus(t.ID, true, errorMsg)
	if err != nil {
		panic(err)
	}
}

func (p Pipeline) ExecuteStep(s Step, maxParallel int) int64 {
	db := p.db

	// Force save WAL before executing step
	if err := db.ForceSaveWAL(); err != nil {
		pipelineLogger.Printf("Failed to checkpoint WAL before step '%s': %v\n", s.Name, err)
		panic(err)
	}

	color.New(color.FgCyan, color.Bold).Fprintf(os.Stderr, "\n▶ ")
	pipelineLogger.Printf("Step: %s\n", color.New(color.FgMagenta, color.Bold).Sprint(s.Name))

	// Create FUSE server for this step
	fuseOutputs := make(chan FileData, 10)
	fuseWatcher, err := NewTempDirFuseWatcher(fuseOutputs)
	if err != nil {
		pipelineLogger.Printf("Failed to create FUSE watcher: %v\n", err)
		panic(err)
	}
	fuseWatcher.Start()
	pipelineLogger.Printf("FUSE server started at: %s\n", fuseWatcher.mountPath)

	// Start goroutine to process FUSE outputs for this step
	var outputsWg sync.WaitGroup
	outputsWg.Add(1)
	go func() {
		defer outputsWg.Done()
		for fileData := range fuseOutputs {
			// Extract step name from filename
			stepName := extractStepName(fileData.Name)

			// Get the next step
			nextStep, err := p.db.GetStepByName(stepName)
			if err != nil {
				pipelineLogger.Printf("Failed to get next step %s: %v", stepName, err)
				continue
			}
			if nextStep == nil {
				pipelineLogger.Printf("No step found for name: %s", stepName)
				continue
			}

			// Create resource from file content
			resourceID, hash, err := p.db.CreateResourceFromReader(fileData.Name, fileData.Reader, nextStep.ID)
			if err != nil {
				pipelineLogger.Printf("Failed to create resource: %v", err)
				continue
			}

			pipelineLogger.Printf("Output: %s -> %s (hash: %s)\n", fileData.Name, color.MagentaString(stepName), hash[:16]+"...")

			// Create task for the next step
			newTask := Task{
				StepID:          nextStep.ID,
				InputResourceID: &resourceID,
				Processed:       false,
			}

			_, err = p.db.CreateTask(newTask)
			if err != nil {
				pipelineLogger.Printf("Failed to create task: %v\n", err)
				continue
			}

			pipelineLogger.Printf("Created task for %s in step %s\n", hash[:16]+"...", stepName)
		}
	}()

	// Defer cleanup
	defer func() {
		pipelineLogger.Printf("Stopping FUSE server for step '%s'...\n", s.Name)
		if err := fuseWatcher.Stop(); err != nil {
			pipelineLogger.Printf("Error stopping FUSE: %v\n", err)
		}
		close(fuseOutputs)
		outputsWg.Wait() // Wait for output processing to complete
		pipelineLogger.Printf("FUSE server stopped for step '%s'\n", s.Name)
	}()

	numberOfExecutions := int64(0)
	numberOfUnprocessedTasks := int64(0)

	// Get unprocessed tasks channel once and collect them
	unprocessedTasksList := []Task{}
	for task := range db.GetUnprocessedTasks(s.ID) {
		if !task.Processed {
			numberOfUnprocessedTasks++
			unprocessedTasksList = append(unprocessedTasksList, task)
		}
	}
	pipelineLogger.Printf("  Collected %d unprocessed tasks for step '%s'\n", numberOfUnprocessedTasks, s.Name)

	if numberOfUnprocessedTasks == 0 {
		pipelineLogger.Printf("  No unprocessed tasks for step '%s'\n", s.Name)
		return 0
	}

	// Count total tasks and already-processed tasks for this step
	totalTasks, processedTasks, err := db.GetTaskCountsForStep(s.ID)
	if err != nil {
		pipelineLogger.Printf("Failed to get task counts: %v\n", err)
		panic(err)
	}

	pipelineLogger.Printf("  %s: Starting (%d/%d already completed)\n", s.Name, processedTasks, totalTasks)

	par := s.Parallel
	if par == nil {
		par = &maxParallel
	}

	// Simple progress tracking without progress bar
	var completedCount int64
	var mu sync.Mutex
	lastPrint := time.Now()

	// Create a channel from the collected tasks for workers
	unprocessedTasks := make(chan Task)
	go func() {
		defer close(unprocessedTasks)
		for _, task := range unprocessedTasksList {
			unprocessedTasks <- task
		}
	}()

	workers.Parallel0(unprocessedTasks, *par, func(task Task) {
		if task.Processed {
			return
		}

		p.ExecuteTask(task, fuseWatcher)
		numberOfExecutions++

		mu.Lock()
		defer mu.Unlock()
		completedCount++
		elapsed := time.Since(lastPrint)
		// Print progress update every 2 seconds or every 100 tasks
		if elapsed > 2*time.Second || completedCount%100 == 0 {
			pipelineLogger.Printf("  %s: %d/%d tasks completed\n", s.Name, completedCount, numberOfUnprocessedTasks)
			lastPrint = time.Now()
		}
	})

	// Print final status
	pipelineLogger.Printf("  %s: %d/%d tasks completed\n", s.Name, completedCount, numberOfUnprocessedTasks)

	pipelineLogger.Printf("  Step '%s' complete: %d/%d tasks", s.Name, numberOfExecutions, numberOfUnprocessedTasks)
	err = db.UpdateStepStatus(s.ID, true)
	if err != nil {
		panic(err)
	}

	fmt.Println("returning")

	return numberOfExecutions
}
