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

	for i, step := range manifest.Steps {
		step := database.RegisterStep(step.Name, step.Script, step.Start, step.Parallel)
		allSteps[i] = *step
	}

	runLogger.Println("Registered steps", len(manifest.Steps))

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
