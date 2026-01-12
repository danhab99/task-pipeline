package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
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

func run(manifest Manifest, database Database, parallel int, startStepName string) {
	runLogger.Println("Registering steps...")
	for _, step := range manifest.Steps {
		if step.Start {
			runLogger.Printf("Step: %s (START STEP)", step.Name)
		} else {
			runLogger.Printf("Step: %s", step.Name)
		}
		if step.Parallel != nil {
			runLogger.Printf("  Parallel limit: %d", *step.Parallel)
		}
		database.RegisterStep(step.Name, step.Script, step.Start, step.Parallel)
	}

	// Determine which step to start from
	var startStep *Step
	var err error

	if startStepName != "" {
		runLogger.Printf("Starting from step: %s", startStepName)
		startStep, err = database.GetStepByName(startStepName)
		if err != nil {
			panic(err)
		}
		if startStep == nil {
			panic(fmt.Sprintf("Step '%s' not found", startStepName))
		}

		// Mark all tasks for this step as unprocessed to re-run them
		count, err := database.MarkStepTasksUnprocessed(startStepName)
		if err != nil {
			panic(err)
		}
		if count > 0 {
			runLogger.Printf("Marked %d existing tasks as unprocessed for step '%s'", count, startStepName)
		} else {
			// No existing tasks, create an initial empty one
			_, _, err := database.InsertTask("", &startStep.ID, nil)
			if err != nil {
				panic(err)
			}
			runLogger.Printf("Created initial task for step '%s' (no existing tasks found)", startStep.Name)
		}
	} else {
		startStep, err = database.GetStartStep()
		if err != nil {
			panic(err)
		}
		if startStep == nil {
			panic("No start step found in manifest")
		}
		runLogger.Printf("Starting from default start step: %s", startStep.Name)

		// Create initial task for the start step if it doesn't exist
		_, isNew, err := database.InsertTask("", &startStep.ID, nil)
		if err != nil {
			panic(err)
		}
		if isNew {
			runLogger.Printf("Created initial task for step '%s'", startStep.Name)
		} else {
			runLogger.Printf("Initial task for step '%s' already exists", startStep.Name)
		}
	}

	// Track semaphores for per-step parallelism limits
	stepSemaphores := make(map[int64]chan struct{})
	var semMutex sync.Mutex

	var wg sync.WaitGroup
	jobs := make(chan Task, parallel)

	// Worker function that respects per-step parallelism
	processWithLimit := func(task Task, db Database) {
		// Get step to check for parallelism limit
		step, err := db.GetStepByID(*task.StepID)
		if err != nil {
			panic(err)
		}

		// Acquire slot from step-specific semaphore if limit is set
		var sem chan struct{}
		if step.Parallel != nil {
			semMutex.Lock()
			if stepSemaphores[step.ID] == nil {
				stepSemaphores[step.ID] = make(chan struct{}, *step.Parallel)
			}
			sem = stepSemaphores[step.ID]
			semMutex.Unlock()

			sem <- struct{}{}        // Acquire
			defer func() { <-sem }() // Release
		}

		task.deriveTasks(db)
	}

	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range jobs {
				processWithLimit(task, database)
			}
		}()
	}

	totalProcessed := 0
	for {
		unprocessed, err := database.GetUnprocessedTasks()
		if err != nil {
			panic(err)
		}

		if len(unprocessed) == 0 {
			break
		}

		runLogger.Printf("Processing %d unprocessed tasks...", len(unprocessed))
		for _, task := range unprocessed {
			jobs <- task
			totalProcessed++
		}

		close(jobs)
		wg.Wait()

		jobs = make(chan Task, parallel)
		for i := 0; i < parallel; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for task := range jobs {
					processWithLimit(task, database)
				}
			}()
		}
	}

	close(jobs)
	wg.Wait()

	runLogger.Printf("Completed processing %d tasks", totalProcessed)
}

func (t Task) deriveTasks(db Database) {
	step, err := db.GetStepByID(*t.StepID)
	if err != nil {
		panic(err)
	}

	runLogger.Printf("Processing task %d for step '%s'", t.ID, step.Name)

	err = db.MarkTaskProcessed(t.ID)
	if err != nil {
		panic(err)
	}

	inputFile, err := os.CreateTemp("/tmp", "input-*")
	if err != nil {
		panic(err)
	}
	defer os.Remove(inputFile.Name())

	if t.ObjectHash != "" {
		objectPath := db.GetObjectPath(t.ObjectHash)
		data, err := os.ReadFile(objectPath)
		if err != nil {
			panic(err)
		}
		runLogger.Printf("  Input: %d bytes from %s", len(data), t.ObjectHash[:16]+"...")
		_, err = inputFile.Write(data)
		if err != nil {
			panic(err)
		}
	} else {
		runLogger.Println("  Input: (empty - start step)")
	}
	inputFile.Close()

	outputDir, err := os.MkdirTemp("/tmp", "output-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(outputDir)

	runLogger.Printf("  Executing script for step '%s'", step.Name)
	cmd := exec.Command("sh", "-c", step.Script)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("INPUT_FILE=%s", inputFile.Name()),
		fmt.Sprintf("OUTPUT_DIR=%s", outputDir),
	)

	// Capture stdout and stderr
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		panic(err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		panic(err)
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		runLogger.Printf("  Error starting script: %v", err)
		panic(err)
	}

	// Create a script logger for this specific step
	scriptLogger := log.New(os.Stderr, fmt.Sprintf("[SCRIPT:%s] ", step.Name), log.Ldate|log.Ltime|log.Lmsgprefix)

	// Stream stdout
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			scriptLogger.Println(scanner.Text())
		}
	}()

	// Stream stderr
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			scriptLogger.Printf("[stderr] %s", scanner.Text())
		}
	}()

	// Wait for output streaming to complete
	wg.Wait()

	// Wait for command to complete
	if err := cmd.Wait(); err != nil {
		runLogger.Printf("  Error executing script: %v", err)
		// panic(err)
	}

	entries, err := os.ReadDir(outputDir)
	if err != nil {
		panic(err)
	}

	newCount := 0
	skippedCount := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		stepName := extractStepName(filename)
		filePath := fmt.Sprintf("%s/%s", outputDir, filename)

		runLogger.Printf("  Output: %s -> step '%s'", filename, stepName)

		targetStep, err := db.GetStepByName(stepName)
		if err != nil {
			runLogger.Printf("    Warning: Error looking up step '%s': %v", stepName, err)
			continue
		}

		var stepID *int64
		if targetStep != nil {
			stepID = &targetStep.ID
		} else {
			runLogger.Printf("    (terminal output - no step '%s')", stepName)
			// stepID remains nil for terminal results
		}

		hash, err := hashFileSHA256(filePath)
		if err != nil {
			panic(err)
		}

		objectPath := db.GetObjectPath(hash)
		_, err = copyFileWithSHA256(filePath, objectPath)
		if err != nil {
			panic(err)
		}

		_, isNew, err := db.InsertTask(hash, stepID, &t.ID)
		if err != nil {
			panic(err)
		}

		if isNew {
			newCount++
		} else {
			skippedCount++
		}
	}

	runLogger.Printf("  Created %d new tasks, %d already existed", newCount, skippedCount)
}
