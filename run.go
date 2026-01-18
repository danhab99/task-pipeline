package main

import (
	"log"
	"os"
	"strings"
)

var runLogger = log.New(os.Stderr, "[RUN] ", log.Ldate|log.Ltime|log.Lmsgprefix)

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
	allSteps := make([]Step, len(manifest.Steps))

	_, err := database.CreateStep(Step{
		Name:     "done",
		Script:   "",
		IsStart:  false,
		Parallel: nil,
	})
	if err != nil {
		panic(err)
	}
	runLogger.Println("Stubbed done task")

	for i, step := range manifest.Steps {
		stepID, err := database.CreateStep(Step{
			Name:     step.Name,
			Script:   step.Script,
			IsStart:  step.Start,
			Parallel: step.Parallel,
		})
		if err != nil {
			panic(err)
		}

		step, err := database.GetStep(stepID)
		if err != nil {
			panic(err)
		}

		allSteps[i] = *step
	}

	runLogger.Println("Registered steps", len(manifest.Steps))

	taintedSteps := 0
	for step := range database.GetTaintedSteps() {
		_, err := database.MigrateTaintedStepTasks(step.ID)
		if err != nil {
			panic(err)
		}
		taintedSteps++
		runLogger.Printf("Marked step %s as tainted\n", step.Name)
	}
	runLogger.Printf("Marked %d steps as tainted\n", taintedSteps)

	pipeline := NewPipeline(&database, allSteps)

	totalExecCount := int64(0)
	execCount := int64(1)
	for execCount > 0 {
		runLogger.Println("--- BEGIN EXECUTION ---")
		c := pipeline.Execute(startStepName, parallel)
		runLogger.Printf("Execution finished with %d\n", c)
		execCount = c
		totalExecCount += c
	}

	runLogger.Printf("Completed processing %d tasks", totalExecCount)
}
