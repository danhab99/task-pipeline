package main

import (
	"log"
	"os"
	"runtime"
	"sync/atomic"

	"github.com/danhab99/idk/workers"
)

var pipelineLogger = log.New(os.Stderr, "[PIPELINE] ", log.Ldate|log.Ltime|log.Lmsgprefix)

type Pipeline struct {
	db          *Database
	fuseWatcher *FuseWatcher
	outputChan  chan FileData
}

func NewPipeline(db *Database) (*Pipeline, error) {
	outDir, err := os.MkdirTemp("/tmp", "output-*")
	if err != nil {
		return nil, err
	}

	outputChan := db.MakeResourceConsumer()

	fuseWatcher, err := NewFuseWatcher(outDir, outputChan)
	if err != nil {
		return nil, err
	}

	return &Pipeline{
		db:          db,
		fuseWatcher: fuseWatcher,
		outputChan:  outputChan,
	}, nil
}

func (p *Pipeline) ExecuteStep(step Step, maxParallel int) int64 {
	db := p.db

	// Schedule new tasks for this step
	tasksCreated, err := db.ScheduleTasksForStep(step.ID)
	if err != nil {
		pipelineLogger.Printf("Error scheduling tasks for step %s: %v", step.Name, err)
		return 0
	}

	if tasksCreated > 0 {
		pipelineLogger.Printf("Step %s: scheduled %d new tasks", step.Name, tasksCreated)
	}

	// Execute unprocessed tasks
	executor := NewScriptExecutor(db, p)
	taskChan := db.GetUnprocessedTasks(step.ID)

	var executionCount atomic.Int64
	pr := step.Parallel
	if pr == nil {
		x := runtime.NumCPU()
		pr = &x
	}
	workers.Parallel0(taskChan, *pr, func(task Task) {
		pipelineLogger.Printf("Executing task %d for step %s", task.ID, step.Name)

		execErr := executor.Execute(task, step, p.outputChan)

		var errorMsg *string
		if execErr != nil {
			msg := execErr.Error()
			errorMsg = &msg
			pipelineLogger.Printf("Task %d failed: %v", task.ID, execErr)
		}

		err = db.UpdateTaskStatus(task.ID, true, errorMsg)
		if err != nil {
			pipelineLogger.Printf("Error updating task %d: %v", task.ID, err)
		}

		executionCount.Add(1)
	})

	return executionCount.Load()
}

func (p *Pipeline) GetFusePath() string {
	return p.fuseWatcher.mountPath
}
