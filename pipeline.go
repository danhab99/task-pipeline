package main

import (
	"log"
	"os"
)

var pipelineLogger = log.New(os.Stderr, "[PIPELINE] ", log.Ldate|log.Ltime|log.Lmsgprefix)

type Pipeline struct {
	db          *Database
	fuseWatcher *FuseWatcher
}

func NewPipeline(db *Database) (*Pipeline, error) {
	outDir, err := os.MkdirTemp("/tmp", "output-*")
	if err != nil {
		return nil, err
	}

	outputChan := make(chan FileData)

	// Single FUSE server collects all resources
	go func() {
		for data := range outputChan {
			_, hash, err := db.CreateResourceFromReader(data.Name, data.Reader)
			if err != nil {
				pipelineLogger.Printf("Error creating resource %s: %v", data.Name, err)
				continue
			}
			pipelineLogger.Printf("Created resource %s (hash: %s)", data.Name, hash[:16]+"...")
		}
	}()

	fuseWatcher, err := NewFuseWatcher(outDir, outputChan)
	if err != nil {
		return nil, err
	}

	return &Pipeline{
		db:          db,
		fuseWatcher: fuseWatcher,
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

	var executionCount int64
	for task := range taskChan {
		pipelineLogger.Printf("Executing task %d for step %s", task.ID, step.Name)

		execErr := executor.Execute(task, step)

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

		executionCount++
	}

	return executionCount
}

func (p *Pipeline) GetFusePath() string {
	return p.fuseWatcher.mountPath
}
