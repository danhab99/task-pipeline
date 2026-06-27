package pipeline

import (
	"errors"
	"runtime"
	"sync"
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

		startStepCount, err := database.CountTasksForStep(step.ID)
		if err != nil {
			panic(err)
		}

		if startStepCount > 0 {
			startTask = <-database.GetTasksForStep(step.ID)
		} else {
			t, err := database.CreateAndGetTask(types.Task{
				StepID: step.ID,
			})
			if err != nil {
				panic(err)
			}
			startTask = *t
		}

		if !startTask.Processed {
			err = p.executor.Execute(startTask, step)
			var errorMsg *string
			if err != nil && !errors.Is(err, exec.ErrTimeout) {
				msg := err.Error()
				errorMsg = &msg
				pipelineLogger.Printf("Seed task %s failed: %v\n", startTask.ID, err)
			}

			// Mark the seed task as processed
			err = database.UpdateTaskStatus(startTask.ID, true, errorMsg)
			if err != nil {
				pipelineLogger.Printf("Error updating seed task %s: %v\n", startTask.ID, err)
			}
			return 1
		}

		return 0
	}

	// Schedule new tasks for this step
	tasksCreated, err := database.ScheduleTasksForStep(step.ID)
	if err != nil {
		pipelineLogger.Printf("Error scheduling tasks for step %s: %v\n", step.Name, err)
		return 0
	}

	if tasksCreated > 0 {
		pipelineLogger.Printf("Step %s: scheduled %d new tasks\n", step.Name, tasksCreated)
	}

	taskChan := database.GetUnprocessedTasks(step.ID)

	var executionCount atomic.Int64
	pr := step.Parallel
	if pr == nil {
		x := runtime.NumCPU()
		pr = &x
	}

	// Streaming flusher: writes status updates in bounded batches as tasks
	// complete, so the in-memory buffer never grows larger than 2×flushBatch
	// regardless of how many tasks the step contains.
	const flushBatch = 500
	updateCh := make(chan db.TaskStatusUpdate, flushBatch*2)

	var flusherDone sync.WaitGroup
	flusherDone.Add(1)
	var flushLastErr error
	go func() {
		defer flusherDone.Done()
		buf := make([]db.TaskStatusUpdate, 0, flushBatch)
		flush := func() {
			if len(buf) == 0 {
				return
			}
			if err := database.BatchUpdateTaskStatus(buf); err != nil {
				flushLastErr = err
			}
			buf = buf[:0]
		}
		for u := range updateCh {
			buf = append(buf, u)
			if len(buf) >= flushBatch {
				flush()
			}
		}
		flush()
	}()

	workers.Parallel0(taskChan, *pr, func(task types.Task) {
		pipelineLogger.Verbosef("Executing task %s for step %s\n", task.ID, step.Name)

		execErr := p.executor.Execute(task, step)

		var errorMsg *string
		if execErr != nil && !errors.Is(execErr, exec.ErrTimeout) {
			msg := execErr.Error()
			errorMsg = &msg
			pipelineLogger.Printf("Task %s failed: %v\n", task.ID, execErr)
		}

		updateCh <- db.TaskStatusUpdate{ID: task.ID, Processed: true, Error: errorMsg}
		executionCount.Add(1)
	})

	close(updateCh)
	flusherDone.Wait()

	if flushLastErr != nil {
		pipelineLogger.Printf("Error flushing task statuses for step %s: %v\n", step.Name, flushLastErr)
	}

	return executionCount.Load()
}
