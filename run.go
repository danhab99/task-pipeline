package main

import (
	"slices"
	"time"

	"github.com/fatih/color"
)

var runLogger = NewColorLogger("[RUN] ", color.New(color.FgBlue, color.Bold))

func run(manifest Manifest, database Database, parallel int, startStepName string, enabledSteps []string) {
	startTime := time.Now()

	// Register all steps from manifest
	var steps []Step
	for _, manifestStep := range manifest.Steps {
		step := Step{
			Name:     manifestStep.Name,
			Script:   manifestStep.Script,
			IsStart:  manifestStep.Start,
			Parallel: manifestStep.Parallel,
			Inputs:   manifestStep.Inputs,
		}

		id, err := database.CreateStep(step)
		if err != nil {
			panic(err)
		}
		step.ID = id

		// Filter to enabled steps if specified
		if len(enabledSteps) > 0 {
			if slices.Contains(enabledSteps, step.Name) {
				steps = append(steps, step)
			}
		} else {
			steps = append(steps, step)
		}
	}

	runLogger.Printf("Registered %d steps", len(manifest.Steps))

	// Create pipeline with single FUSE server
	pipeline, err := NewPipeline(&database)
	if err != nil {
		panic(err)
	}
	defer pipeline.fuseWatcher.Stop()

	pipeline.fuseWatcher.Start()
	runLogger.Printf("FUSE server started at: %s", pipeline.GetFusePath())

	// Check if we need to seed
	resourceCount, err := database.CountResources()
	if err != nil {
		panic(err)
	}

	if resourceCount == 0 {
		runLogger.Printf("No resources found, running seed step")
		startStep, err := database.GetStartingStep()
		if err != nil {
			panic(err)
		}
		if startStep == nil {
			panic("no start step found in manifest")
		}

		// Create and execute seed task
		seedTask := Task{
			StepID:          startStep.ID,
			InputResourceID: nil,
			Processed:       false,
		}
		seedTaskID, err := database.CreateTask(seedTask)
		if err != nil {
			panic(err)
		}
		seedTask.ID = seedTaskID

		executor := NewScriptExecutor(&database, pipeline)
		execErr := executor.Execute(seedTask, *startStep)

		var errorMsg *string
		if execErr != nil {
			msg := execErr.Error()
			errorMsg = &msg
			runLogger.Errorf("Seed task failed: %v", execErr)
		}

		err = database.UpdateTaskStatus(seedTask.ID, true, errorMsg)
		if err != nil {
			panic(err)
		}

		if execErr == nil {
			runLogger.Successf("Seed task completed")
		}

		if len(steps) >= 1 {
			database.ScheduleTasksForStep(steps[1].ID)
		}
	}

	// Execute all steps
	var totalExecutions int64
	for _, step := range steps {
		executions := pipeline.ExecuteStep(step, parallel)
		totalExecutions += executions
		
		if executions > 0 {
			runLogger.Printf("Step %s: executed %d tasks", step.Name, executions)
		}
	}

	duration := time.Since(startTime)
	runLogger.Successf("Pipeline complete: %d tasks executed in %s", totalExecutions, duration.Round(time.Millisecond))
}
