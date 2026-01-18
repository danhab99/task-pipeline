package main

import (
	"fmt"

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

	for task := range database.GetTasksForStep(step.ID) {
		fmt.Printf("%s\n", database.GetObjectPath(task.ObjectHash))

		for nextTask := range database.GetNextTasks(task.ID) {

			thisStep, err := database.GetStep(*nextTask.StepID)
			if err != nil {
				panic(err)
			}

			fmt.Printf("| %s(%d) -> %s\n", thisStep.Name, thisStep.Version, database.GetObjectPath(nextTask.ObjectHash))
		}
	}

	// if inputPath == "" {
	// 	// No input path specified - list all input tasks for this step
	// 	exportLogger.Printf("Listing input tasks for step '%s'", stepName)
	// 	tasks := <-chans.Accumulate(database.GetTasksForStep(step.ID))
	// 	exportLogger.Printf("Found %d tasks", len(tasks))

	// 	for _, task := range tasks {
	// 		objectPath := database.GetObjectPath(task.ObjectHash)
	// 		fmt.Println(objectPath)
	// 	}
	// } else {
	// 	// Input path specified - find this task and list its outputs
	// 	exportLogger.Printf("Finding outputs for input path: %s", inputPath)

	// 	// Resolve the absolute path
	// 	absInputPath, err := filepath.Abs(inputPath)
	// 	if err != nil {
	// 		panic(err)
	// 	}

	// 	// Find the task with this object path
	// 	tasks := <-chans.Accumulate(database.GetTasksForStep(step.ID))
	// 	var matchedTask *Task
	// 	for _, task := range tasks {
	// 		objectPath := database.GetObjectPath(task.ObjectHash)
	// 		absObjectPath, _ := filepath.Abs(objectPath)
	// 		if absObjectPath == absInputPath {
	// 			matchedTask = &task
	// 			break
	// 		}
	// 	}

	// 	if matchedTask == nil {
	// 		exportLogger.Printf("No task found for input path: %s", inputPath)
	// 		return
	// 	}

	// 	exportLogger.Printf("Found task %d", matchedTask.ID)

	// 	// Get all outputs for this task
	// 	outputTasks := <-chans.Accumulate(database.GetNextTasks(matchedTask.ID))
	// 	exportLogger.Printf("Found %d outputs", len(outputTasks))

	// 	for _, outputTask := range outputTasks {
	// 		outputPath := database.GetObjectPath(outputTask.ObjectHash)
	// 		fmt.Println(outputPath)
	// 	}
	// }
}
