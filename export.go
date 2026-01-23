package main

import (
	"fmt"
	"os"

	"github.com/fatih/color"
)

var exportLogger = NewColorLogger("[EXPORT] ", color.New(color.FgGreen, color.Bold))

func exportResults(database Database, stepName string) {
	step, err := database.GetStepByName(stepName)
	if err != nil {
		panic(err)
	}
	if step == nil {
		exportLogger.Errorf("Step '%s' not found", stepName)
		return
	}

	exportLogger.Printf("Exporting results for step: %s", color.MagentaString(stepName))

	// Export all resources produced by this step
	for resource := range database.GetResourcesProducedByStep(step.ID) {
		// Write object data to stdout
		data, err := database.GetObject(resource.ObjectHash)
		if err != nil {
			exportLogger.Errorf("Failed to get object %s: %v", resource.ObjectHash[:16], err)
			continue
		}
		fmt.Fprintf(os.Stdout, "=== Resource: %s (hash: %s, %d bytes) ===\\n", resource.Name, resource.ObjectHash[:16], len(data))
		os.Stdout.Write(data)
		fmt.Fprintf(os.Stdout, "\\n")
	}
}
