package main

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/fatih/color"
)

var runLogger = NewColorLogger("[RUN] ", color.New(color.FgBlue, color.Bold))

func extractStepName(filename string) string {
	base := filename
	if idx := strings.LastIndex(filename, "."); idx != -1 {
		base = filename[:idx]
	}

	if idx := strings.Index(base, "_"); idx != -1 {
		return base[:idx]
	}

	return base
}

func run(manifest Manifest, database Database, parallel int, startStepName string, enabledSteps []string) {
	startTime := time.Now()

	es := make([]Step, 0, len(manifest.Steps))
	stepInputs := make(map[int64][]string)

	for _, step := range manifest.Steps {
		s := Step{
			Name:     step.Name,
			Script:   step.Script,
			IsStart:  step.Start,
			Parallel: step.Parallel,
		}
		runLogger.Verbosef("Registered step %#v\n", s)

		id, err := database.CreateStep(s)
		if err != nil {
			panic(err)
		}
		s.ID = id

		// Store step inputs if any
		if len(step.Inputs) > 0 {
			stepInputs[id] = step.Inputs
		}

		if len(enabledSteps) > 0 {
			if slices.ContainsFunc(enabledSteps, func(stepName string) bool {
				return stepName == step.Name
			}) {
				es = append(es, s)
				database.MarkStepTasksUnprocessed(s.ID)
			}
		} else {
			es = append(es, s)
		}
	}

	runLogger.Printf("Registered %d steps", len(manifest.Steps))

	_, err := database.CreateStep(Step{
		Name:     "done",
		Script:   "true",
		IsStart:  false,
		Parallel: nil,
	})
	if err != nil {
		panic(err)
	}

	runLogger.Printf("Stubbed done task")

	pipeline, err := NewPipeline(&database, es, stepInputs)
	if err != nil {
		panic(err)
	}

	// Check if we need to seed the pipeline
	startStep, err := database.GetStartingStep()
	if err != nil {
		panic(err)
	}
	if startStep == nil {
		panic("start step cannot be nil")
	}

	// Check if any resources exist for the start step
	hasResources := false
	for range database.GetResourcesProducedByStep(startStep.ID) {
		hasResources = true
		break
	}

	if !hasResources {
		// No resources exist, we need to seed the pipeline
		runLogger.Printf("No seed resources found, creating and executing seed task")
		
		// Create FUSE server for seed task
		fuseOutputs := make(chan FileData, 10)
		fuseWatcher, err := NewTempDirFuseWatcher(fuseOutputs)
		if err != nil {
			panic(err)
		}
		fuseWatcher.Start()
		runLogger.Printf("FUSE server started for seed at: %s", fuseWatcher.mountPath)

		// Start goroutine to process FUSE outputs for seed task
		go func() {
			for fileData := range fuseOutputs {
				// Extract step name from filename
				stepName := extractStepName(fileData.Name)

				// Get the next step
				nextStep, err := database.GetStepByName(stepName)
				if err != nil {
					runLogger.Printf("Failed to get next step %s: %v", stepName, err)
					continue
				}
				if nextStep == nil {
					runLogger.Printf("No step found for name: %s", stepName)
					continue
				}

				// Create resource from file content
				resourceID, hash, err := database.CreateResourceFromReader(fileData.Name, fileData.Reader, nextStep.ID)
				if err != nil {
					runLogger.Printf("Failed to create resource: %v", err)
					continue
				}

				runLogger.Printf("Seed output: %s -> %s (hash: %s)", fileData.Name, stepName, hash[:16]+"...")

				// Create task for the next step
				newTask := Task{
					StepID:          nextStep.ID,
					InputResourceID: &resourceID,
					Processed:       false,
				}

				_, err = database.CreateTask(newTask)
				if err != nil {
					runLogger.Printf("Failed to create task: %v", err)
					continue
				}

				runLogger.Printf("Created seed task for %s in step %s", hash[:16]+"...", stepName)
			}
		}()
		
		// Create seed task
		seedTask := Task{
			StepID:    startStep.ID,
			Processed: false,
		}
		seedTaskID, err := database.CreateTask(seedTask)
		if err != nil {
			panic(err)
		}

		// Get the created task and execute it
		task, err := database.GetTask(seedTaskID)
		if err != nil {
			panic(err)
		}

		// Execute the seed task to create seed resources
		step, err := database.GetStep(task.StepID)
		if err != nil {
			panic(err)
		}

		executor := NewScriptExecutor(&database, pipeline, fuseWatcher.mountPath)
		execErr := executor.Execute(*task, *step)

		// Update task status
		var errorMsg *string
		if execErr != nil {
			msg := execErr.Error()
			errorMsg = &msg
		}
		err = database.UpdateTaskStatus(task.ID, true, errorMsg)
		if err != nil {
			panic(err)
		}

		if execErr != nil {
			panic(fmt.Errorf("seed task execution failed: %w", execErr))
		}

		runLogger.Successf("Seed task executed successfully")
		
		// Stop FUSE server (waits for all files to be closed and processed)
		runLogger.Printf("Waiting for seed outputs to be processed...")
		if err := fuseWatcher.Stop(); err != nil {
			runLogger.Printf("Error stopping FUSE: %v", err)
		}
		close(fuseOutputs)
		
		// Verify seed resources were created
		resourceCount := 0
		for range database.GetResourcesProducedByStep(startStep.ID) {
			resourceCount++
		}
		runLogger.Successf("Processed %d seed resources", resourceCount)
	} else {
		runLogger.Printf("Seed resources already exist, skipping seed task execution")
	}

	totalExecCount := int64(0)
	execCount := int64(1)
	iterationNum := 0
	for execCount > 0 {
		iterationNum++
		runLogger.Printf("======= Execution Round %d =======", iterationNum)
		c := pipeline.Execute(startStepName, parallel)
		runLogger.Successf("Round %d completed: %d tasks processed", iterationNum, c)
		execCount = c
		totalExecCount += c
	}

	elapsed := time.Since(startTime)

	PrintSummary("Pipeline Execution Summary", map[string]interface{}{
		"Total Tasks":      totalExecCount,
		"Total Rounds":     iterationNum,
		"Parallel Workers": parallel,
		"Execution Time":   elapsed.Round(time.Millisecond),
		"Avg Task Rate":    fmt.Sprintf("%.2f tasks/sec", float64(totalExecCount)/elapsed.Seconds()),
	})

	runLogger.Successf("Pipeline completed successfully!")
}
