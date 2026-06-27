package pipeline

import (
	"errors"
	"runtime"
	"sync/atomic"

	"grit/db"
	"grit/exec"
	"grit/log"
	"grit/types"

	"github.com/danhab99/idk/workers"
)

var pipelineLogger = log.NewLogger("PIPELINE")

type Pipeline struct {
	database *db.Database
	executor *exec.ScriptExecutor
}

func NewPipeline(executor *exec.ScriptExecutor, database *db.Database) (*Pipeline, error) {
	return &Pipeline{database, executor}, nil
}

func (p *Pipeline) ExecuteStep(step types.Step, maxParallel int) int64 {
	database := p.database

	if step.Input == "" {
		pipelineLogger.Printf("Executing seed step %s\n", step.Name)

		var startTask types.Task

		startStepCount := database.CountTasksForStep(step)

		if startStepCount > 0 {
			startTask = <-database.GetTasksForStep(step)
		} else {
			startTask = database.CreateTask(step, nil)
		}

		if !startTask.Processed {
			err := p.executor.Execute(startTask, step)
			if err != nil && !errors.Is(err, exec.ErrTimeout) {
				pipelineLogger.Printf("Seed task %s failed: %v\n", startTask.ID, err)

				// // Mark the seed task as processed
				// if err != nil {
				// 	pipelineLogger.Printf("Error updating seed task %s: %v\n", startTask.ID, err)
				// }
				return 1
			}
		}

		return 0
	}

	// Schedule new tasks for this step
	tasksCreated := database.ScheduleTasksForStep(step)

	if tasksCreated > 0 {
		pipelineLogger.Printf("Step %s: scheduled %d new tasks\n", step.Name, tasksCreated)
	}

	taskChan := database.GetUnprocessedTasks(step)

	var executionCount atomic.Int64
	pr := step.Parallel
	if pr == nil {
		x := runtime.NumCPU()
		pr = &x
	}

	workers.Parallel0(taskChan, *pr, func(task types.Task) {
		pipelineLogger.Verbosef("Executing task %s for step %s\n", task.ID, step.Name)

		execErr := p.executor.Execute(task, step)

		if execErr != nil && !errors.Is(execErr, exec.ErrTimeout) {
			pipelineLogger.Printf("Task %s failed: %v\n", task.ID, execErr)
		}

		executionCount.Add(1)
	})

	return executionCount.Load()
}
