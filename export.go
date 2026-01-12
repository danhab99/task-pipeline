package main

import (
	"fmt"
	"log"
	"os"
)

var exportLogger = log.New(os.Stderr, "[EXPORT] ", log.Ldate|log.Ltime|log.Lmsgprefix)

func exportResults(database Database, stepName string, mode string) {
	exportLogger.Printf("Exporting %s for step '%s'", mode, stepName)

	tasks, err := database.GetTasksByStepName(stepName, mode)
	if err != nil {
		panic(err)
	}

	exportLogger.Printf("Found %d tasks", len(tasks))

	for _, task := range tasks {
		objectPath := database.GetObjectPath(task.ObjectHash)

		if mode == "output" && task.InputTaskID != nil {
			// Get the input task to show which specific input produced this output
			inputTask, err := database.GetTaskByID(*task.InputTaskID)
			if err != nil {
				exportLogger.Printf("Warning: Could not get input task: %v", err)
				fmt.Println(objectPath)
				continue
			}

			inputHash := inputTask.ObjectHash
			if len(inputHash) > 16 {
				inputHash = inputHash[:16] + "..."
			}
			fmt.Printf("%s\t(from input %s)\n", objectPath, inputHash)
		} else {
			fmt.Println(objectPath)
		}
	}
}
