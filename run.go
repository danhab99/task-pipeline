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

func extractTaskName(filename string) string {
	base := filename
	if idx := strings.LastIndex(filename, "."); idx != -1 {
		base = filename[:idx]
	}

	if idx := strings.Index(base, "_"); idx != -1 {
		return base[:idx]
	}

	return base
}

func run(manifest Manifest, database Database, parallel int) {
	runLogger.Println("Registering tasks...")
	for _, task := range manifest.Tasks {
		if task.Start {
			runLogger.Printf("Task: %s (START TASK)", task.Name)
		} else {
			runLogger.Printf("Task: %s", task.Name)
		}
		database.RegisterTask(task.Name, task.Script, task.Start)
	}

	startTask, err := database.GetStartTask()
	if err != nil {
		panic(err)
	}

	if startTask != nil {
		unprocessed, err := database.GetUnprocessedResults()
		if err != nil {
			panic(err)
		}

		if len(unprocessed) == 0 {
			runLogger.Printf("Seeding start task: %s", startTask.Name)
			database.InsertResult("", &startTask.ID, nil)
		}
	}

	var wg sync.WaitGroup
	jobs := make(chan Result, parallel)

	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for result := range jobs {
				processResult(result, database)
			}
		}()
	}

	totalProcessed := 0
	for {
		unprocessed, err := database.GetUnprocessedResults()
		if err != nil {
			panic(err)
		}

		if len(unprocessed) == 0 {
			break
		}

		runLogger.Printf("Processing %d results...", len(unprocessed))
		for _, result := range unprocessed {
			jobs <- result
			totalProcessed++
		}

		close(jobs)
		wg.Wait()

		jobs = make(chan Result, parallel)
		for i := 0; i < parallel; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for result := range jobs {
					processResult(result, database)
				}
			}()
		}
	}

	close(jobs)
	wg.Wait()

	runLogger.Printf("Completed processing %d results", totalProcessed)
}

func processResult(r Result, db Database) {
	task, err := db.GetTaskByID(*r.TaskID)
	if err != nil {
		panic(err)
	}

	runLogger.Printf("Processing result %d for task '%s'", r.ID, task.Name)

	err = db.MarkResultProcessed(r.ID)
	if err != nil {
		panic(err)
	}

	inputFile, err := os.CreateTemp("/tmp", "input-*")
	if err != nil {
		panic(err)
	}
	defer os.Remove(inputFile.Name())

	if r.ObjectHash != "" {
		objectPath := db.GetObjectPath(r.ObjectHash)
		data, err := os.ReadFile(objectPath)
		if err != nil {
			panic(err)
		}
		runLogger.Printf("  Input: %d bytes from %s", len(data), r.ObjectHash[:16]+"...")
		_, err = inputFile.Write(data)
		if err != nil {
			panic(err)
		}
	} else {
		runLogger.Println("  Input: (empty - start task)")
	}
	inputFile.Close()

	outputDir, err := os.MkdirTemp("/tmp", "output-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(outputDir)

	runLogger.Printf("  Executing script for task '%s'", task.Name)
	cmd := exec.Command("sh", "-c", task.Script)
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

	// Create a script logger for this specific task
	scriptLogger := log.New(os.Stderr, fmt.Sprintf("[SCRIPT:%s] ", task.Name), log.Ldate|log.Ltime|log.Lmsgprefix)

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
		panic(err)
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
		taskName := extractTaskName(filename)
		filePath := fmt.Sprintf("%s/%s", outputDir, filename)

		runLogger.Printf("  Output: %s -> task '%s'", filename, taskName)

		targetTask, err := db.GetTaskByName(taskName)
		if err != nil {
			runLogger.Printf("    Warning: Error looking up task '%s': %v", taskName, err)
			continue
		}

		var taskID *int64
		if targetTask != nil {
			taskID = &targetTask.ID
		} else {
			runLogger.Printf("    (terminal output - no task '%s')", taskName)
			// taskID remains nil for terminal results
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

		_, isNew, err := db.InsertResult(hash, taskID, &r.ID)
		if err != nil {
			panic(err)
		}

		if isNew {
			newCount++
		} else {
			skippedCount++
		}
	}

	runLogger.Printf("  Created %d new results, %d already existed", newCount, skippedCount)
}
