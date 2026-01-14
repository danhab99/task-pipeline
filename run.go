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

func run(manifest Manifest, database Database, parallel int, startStepName string) {
	runLogger.Println("Registered steps")

	for _, step := range manifest.Steps {
		database.CreateStep(Step{
			Name:     step.Name,
			Script:   step.Script,
			IsStart:  step.Start,
			Parallel: step.Parallel,
		})
	}

	runLogger.Println("Stubbed done task")
	database.CreateStep(Step{
		Name:     "done",
		Script:   "true",
		IsStart:  false,
		Parallel: nil,
	})

	pipeline := NewPipeline(&database)

	totalExecCount := int64(0)
	execCount := int64(1)
	for execCount > 0 {
		runLogger.Println("--- BEGIN EXECUTION ---")
		c := pipeline.Execute(startStepName, parallel)
		execCount = c
		totalExecCount += c
	}

	runLogger.Printf("Completed processing %d tasks", totalExecCount)
}

// func (t Task) deriveTasks(db Database) {
// 	step, err := db.GetStepByID(*t.StepID)
// 	if err != nil {
// 		panic(err)
// 	}

// 	runLogger.Printf("Processing task %d for step '%s'", t.ID, step.Name)

// 	err = db.MarkTaskProcessed(t.ID)
// 	if err != nil {
// 		panic(err)
// 	}

// 	inputFile, err := os.CreateTemp("/tmp", "input-*")
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer os.Remove(inputFile.Name())

// 	if t.ObjectHash != "" {
// 		objectPath := db.GetObjectPath(t.ObjectHash)
// 		data, err := os.ReadFile(objectPath)
// 		if err != nil {
// 			panic(err)
// 		}
// 		runLogger.Printf("  Input: %d bytes from %s", len(data), t.ObjectHash[:16]+"...")
// 		_, err = inputFile.Write(data)
// 		if err != nil {
// 			panic(err)
// 		}
// 	} else {
// 		runLogger.Println("  Input: (empty - start step)")
// 	}
// 	inputFile.Close()

// 	outputDir, err := os.MkdirTemp("/tmp", "output-*")
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer os.RemoveAll(outputDir)

// 	runLogger.Printf("  Executing script for step '%s'", step.Name)
// 	cmd := exec.Command("sh", "-c", step.Script)
// 	cmd.Env = append(os.Environ(),
// 		fmt.Sprintf("INPUT_FILE=%s", inputFile.Name()),
// 		fmt.Sprintf("OUTPUT_DIR=%s", outputDir),
// 	)

// 	// Capture stdout and stderr
// 	stdoutPipe, err := cmd.StdoutPipe()
// 	if err != nil {
// 		panic(err)
// 	}
// 	stderrPipe, err := cmd.StderrPipe()
// 	if err != nil {
// 		panic(err)
// 	}

// 	// Start the command
// 	if err := cmd.Start(); err != nil {
// 		runLogger.Printf("  Error starting script: %v", err)
// 		panic(err)
// 	}

// 	// Create a script logger for this specific step
// 	scriptLogger := log.New(os.Stderr, fmt.Sprintf("[SCRIPT:%s] ", step.Name), log.Ldate|log.Ltime|log.Lmsgprefix)

// 	// Stream stdout
// 	var wg sync.WaitGroup
// 	wg.Add(2)
// 	go func() {
// 		defer wg.Done()
// 		scanner := bufio.NewScanner(stdoutPipe)
// 		for scanner.Scan() {
// 			scriptLogger.Println(scanner.Text())
// 		}
// 	}()

// 	// Stream stderr
// 	go func() {
// 		defer wg.Done()
// 		scanner := bufio.NewScanner(stderrPipe)
// 		for scanner.Scan() {
// 			scriptLogger.Printf("[stderr] %s", scanner.Text())
// 		}
// 	}()

// 	// Wait for output streaming to complete
// 	wg.Wait()

// 	// Wait for command to complete
// 	if err := cmd.Wait(); err != nil {
// 		runLogger.Printf("  Error executing script: %v", err)
// 		// panic(err)
// 	}

// 	entries, err := os.ReadDir(outputDir)
// 	if err != nil {
// 		panic(err)
// 	}

// 	entriesChan := make(chan os.DirEntry)

// 	go func() {
// 		defer close(entriesChan)
// 		for _, entry := range entries {
// 			entriesChan <- entry
// 		}
// 	}()

// 	newCount := 0
// 	skippedCount := 0

// 	workers.Parallel0(entriesChan, runtime.NumCPU(), func(entry os.DirEntry) {
// 		if entry.IsDir() {
// 			return
// 		}

// 		filename := entry.Name()
// 		stepName := extractStepName(filename)
// 		filePath := fmt.Sprintf("%s/%s", outputDir, filename)

// 		runLogger.Printf("  Output: %s -> step '%s'", filename, stepName)

// 		targetStep, err := db.GetStepByName(stepName)
// 		if err != nil {
// 			runLogger.Printf("    Warning: Error looking up step '%s': %v", stepName, err)
// 			return
// 		}

// 		var stepID *int64
// 		if targetStep != nil {
// 			stepID = &targetStep.ID
// 		} else {
// 			runLogger.Printf("    (terminal output - no step '%s')", stepName)
// 			// stepID remains nil for terminal results
// 		}

// 		hash, err := hashFileSHA256(filePath)
// 		if err != nil {
// 			panic(err)
// 		}

// 		objectPath := db.GetObjectPath(hash)
// 		_, err = copyFileWithSHA256(filePath, objectPath)
// 		if err != nil {
// 			panic(err)
// 		}

// 		_, isNew, err := db.InsertTask(hash, stepID, &t.ID)
// 		if err != nil {
// 			panic(err)
// 		}

// 		if isNew {
// 			newCount++
// 		} else {
// 			skippedCount++
// 		}
// 	})

// 	runLogger.Printf("  Created %d new tasks, %d already existed", newCount, skippedCount)
// }
