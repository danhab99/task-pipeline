package main

import (
	"fmt"
	"log"
	"os"
)

var exportLogger = log.New(os.Stderr, "[EXPORT] ", log.Ldate|log.Ltime|log.Lmsgprefix)

func exportResults(database Database, taskName string, mode string) {
	exportLogger.Printf("Exporting %s for task '%s'", mode, taskName)

	results, err := database.GetResultsByTaskName(taskName, mode)
	if err != nil {
		panic(err)
	}

	exportLogger.Printf("Found %d results", len(results))

	for _, result := range results {
		objectPath := database.GetObjectPath(result.ObjectHash)

		if mode == "output" && result.InputResultID != nil {
			// Get the input result to show which specific input produced this output
			inputResult, err := database.GetResultByID(*result.InputResultID)
			if err != nil {
				exportLogger.Printf("Warning: Could not get input result: %v", err)
				fmt.Println(objectPath)
				continue
			}

			inputHash := inputResult.ObjectHash
			if len(inputHash) > 16 {
				inputHash = inputHash[:16] + "..."
			}
			fmt.Printf("%s\t(from input %s)\n", objectPath, inputHash)
		} else {
			fmt.Println(objectPath)
		}
	}
}
