package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"sync"
)

type ScriptExecutor struct {
	db       *Database
	pipeline *Pipeline
}

func NewScriptExecutor(db *Database, pipeline *Pipeline) *ScriptExecutor {
	return &ScriptExecutor{
		db:       db,
		pipeline: pipeline,
	}
}

var executeLogger = NewLogger("EXEC")

func (e *ScriptExecutor) Execute(task Task, step Step, outputChan chan FileData) error {
	watcher, err := NewTempDirFuseWatcher(outputChan)
	defer watcher.WaitForWrites()

	executeLogger.Printf("Executing task ID=%d for step '%s' (step_id=%d)\n", task.ID, step.Name, task.StepID)

	// Create input file
	inputFile, err := os.CreateTemp("/tmp", "input-*")
	if err != nil {
		return fmt.Errorf("failed to create input file: %w", err)
	}
	defer os.Remove(inputFile.Name())

	// Write input data if exists
	if err := e.prepareInput(task, inputFile); err != nil {
		return err
	}
	inputFile.Close()

	// Execute the script
	executeLogger.Printf("Executing: %s\n", step.Script)
	cmd := e.buildCommand(step, inputFile.Name(), watcher.mountPath)

	// Run script and capture output
	if err := e.runScript(cmd, step); err != nil {
		return err
	}

	return nil
}

func (e *ScriptExecutor) prepareInput(task Task, inputFile *os.File) error {
	// Get input resource if task has one
	if task.InputResourceID != nil {
		inputResource, err := e.db.GetResource(*task.InputResourceID)
		if err != nil {
			return fmt.Errorf("failed to get input resource: %w", err)
		}

		data, err := e.db.GetObject(inputResource.ObjectHash)
		if err != nil {
			return fmt.Errorf("failed to get object: %w", err)
		}

		n, err := inputFile.Write(data)
		if err != nil {
			return fmt.Errorf("failed to write input data: %w", err)
		}
		executeLogger.Printf("Input: %d bytes from resource '%s' (hash: %s)\n", n, inputResource.Name, inputResource.ObjectHash[:16]+"...")
	} else {
		executeLogger.Printf("Input: (empty - start step)\n")
	}

	return nil
}

func (e *ScriptExecutor) buildCommand(step Step, inputFile, outputDir string) *exec.Cmd {
	cmd := exec.Command("sh", "-c", step.Script)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("INPUT_FILE=%s", inputFile),
		fmt.Sprintf("OUTPUT_DIR=%s", outputDir),
	)
	return cmd
}

func (e *ScriptExecutor) runScript(cmd *exec.Cmd, step Step) error {
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		executeLogger.Printf("Error starting script: %v\n", err)
		return fmt.Errorf("failed to start script: %w", err)
	}

	scriptLogger := NewLogger(fmt.Sprintf("SCRIPT:%s ", step.Name))

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			scriptLogger.Printf("[stdout] %s\n", scanner.Text())
		}
	}()

	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			scriptLogger.Printf("[stderr] %s\n", scanner.Text())
		}
	}()

	// Wait for command to finish (closes pipes)
	err = cmd.Wait()

	// Then wait for goroutines to finish reading
	wg.Wait()

	if err != nil {
		executeLogger.Printf("Error executing script: %v\n", err)
		return fmt.Errorf("script execution failed: %w", err)
	}

	return nil
}

func (e *ScriptExecutor) setupFUSEWatcher(cmd *exec.Cmd, step Step) error {
	// TODO: Implement FUSE-based watcher if needed
	return nil
}
