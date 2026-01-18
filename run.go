package main

import (
	"fmt"
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
		es = append(es, s)
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

	runLogger.Verbosef("Stubbed done task")

	pipeline := NewPipeline(&database, es)

	totalExecCount := int64(0)
	execCount := int64(1)
	iterationNum := 0
	for execCount > 0 {
		iterationNum++
		runLogger.Printf("╔══════ Execution Round %d ══════╗", iterationNum)
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
