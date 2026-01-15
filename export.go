package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/danhab99/idk/chans"
)

var exportLogger = log.New(os.Stderr, "[EXPORT] ", log.Ldate|log.Ltime|log.Lmsgprefix)

func exportResults(database Database, stepName string, inputPath string) {
	step, err := database.GetStepByName(stepName)
	if err != nil {
		panic(err)
	}
	if step == nil {
		exportLogger.Printf("Step '%s' not found", stepName)
		return
	}

	if inputPath == "" {
		// No input path specified - list all input tasks for this step
		exportLogger.Printf("Listing input tasks for step '%s'", stepName)
		tasks := <-chans.Accumulate(database.GetTasksForStep(step.ID))
		exportLogger.Printf("Found %d tasks", len(tasks))

		for _, task := range tasks {
			objectPath := database.GetObjectPath(task.ObjectHash)
			fmt.Println(objectPath)
		}
	} else {
		// Input path specified - find this task and list its outputs
		exportLogger.Printf("Finding outputs for input path: %s", inputPath)

		// Resolve the absolute path
		absInputPath, err := filepath.Abs(inputPath)
		if err != nil {
			panic(err)
		}

		// Find the task with this object path
		tasks := <-chans.Accumulate(database.GetTasksForStep(step.ID))
		var matchedTask *Task
		for _, task := range tasks {
			objectPath := database.GetObjectPath(task.ObjectHash)
			absObjectPath, _ := filepath.Abs(objectPath)
			if absObjectPath == absInputPath {
				matchedTask = &task
				break
			}
		}

		if matchedTask == nil {
			exportLogger.Printf("No task found for input path: %s", inputPath)
			return
		}

		exportLogger.Printf("Found task %d", matchedTask.ID)

		// Get all outputs for this task
		outputTasks := <-chans.Accumulate(database.GetNextTasks(matchedTask.ID))
		exportLogger.Printf("Found %d outputs", len(outputTasks))

		for _, outputTask := range outputTasks {
			outputPath := database.GetObjectPath(outputTask.ObjectHash)
			fmt.Println(outputPath)
		}
	}
}
