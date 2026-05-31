package exec

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"grit/db"
	"grit/log"
	"grit/types"
	"os"
	"os/exec"
	"syscall"
	"sync"
	"time"
)

type ScriptExecutor struct {
	db *db.Database
}

func NewScriptExecutor(db *db.Database) *ScriptExecutor {
	return &ScriptExecutor{db}
}

var executeLogger = log.NewLogger("EXEC")

// ErrTimeout is returned when a script exceeds its configured timeout.
var ErrTimeout = errors.New("script timed out")

// func (e *ScriptExecutor) ExecuteStep(step types.Step, defaultParallel int) error {
// 	database := e.db
// 	executeLogger.Println("Running unfinished tasks for step", step.Name)

// 	p := defaultParallel
// 	if step.Parallel != nil {
// 		p = min(defaultParallel, *step.Parallel)
// 	}

// 	workers.Parallel0(database.GetUnprocessedTasks(step.ID), p, func(task types.Task) {
// 		err := e.Execute(task, step)
// 		if err != nil {
// 			panic(err)
// 		}
// 	})

// 	return nil
// }

func (e *ScriptExecutor) Execute(task types.Task, step types.Step) error {
	// executeLogger.Printf("Executing task ID=%d for step '%s' (step_id=%d)\n", task.ID, step.Name, task.StepID)
	start := time.Now()

	// Create input file
	inputFile, err := os.CreateTemp("/tmp", "input-*")
	if err != nil {
		return fmt.Errorf("failed to create input file: %w", err)
	}
	defer os.Remove(inputFile.Name())
	defer inputFile.Close()

	// Write input data if exists
	if err := e.prepareInput(task, inputFile); err != nil {
		return err
	}
	inputFile.Close()

	// Create output directory
	outputDir, err := os.MkdirTemp("", "grit-output-*")
	if err != nil {
		return fmt.Errorf("failed to create output dir: %w", err)
	}
	defer os.RemoveAll(outputDir)

	// Execute the script
	executeLogger.Verbosef("Executing: %s\n", step.Script)
	cmd := e.buildCommand(step, inputFile.Name(), outputDir, task.ID)

	// Run script and capture output
	var runErr error
	if step.Timeout != nil && *step.Timeout > 0 {
		runErr = e.runScriptWithTimeout(cmd, step, *step.Timeout)
	} else {
		runErr = e.runScript(cmd, step)
	}

	elapsedTime := time.Since(start)

	// If the script timed out, mark the task as complete with no outputs.
	if errors.Is(runErr, ErrTimeout) {
		executeLogger.Printf("Task ID=%s for step '%s' timed out after %s, killed with SIGKILL\n", task.ID, step.Name, elapsedTime.String())
		return ErrTimeout
	}

	if runErr != nil {
		return runErr
	}

	// Ingest output files synchronously
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		return fmt.Errorf("failed to read output dir: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		path := outputDir + "/" + entry.Name()
		if err := e.db.IngestFile(path, entry.Name(), task.ID); err != nil {
			return fmt.Errorf("failed to ingest output file %s: %w", entry.Name(), err)
		}
	}

	executeLogger.Printf("Executed task ID=%s for step '%s' successfully in %s\n", task.ID, step.Name, elapsedTime.String())
	return nil
}

func (e *ScriptExecutor) prepareInput(task types.Task, inputFile *os.File) error {
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
		executeLogger.Verbosef("Input: %d bytes from resource '%s' (hash: %s)\n", n, inputResource.Name, inputResource.ObjectHash[:16]+"...")
	} else {
		executeLogger.Verbosef("Input: (empty - start step)\n")
	}

	return nil
}

func (e *ScriptExecutor) buildCommand(step types.Step, inputFile, outputDir string, taskID string) *exec.Cmd {
	cmd := exec.Command("sh", "-c", step.Script)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("INPUT_FILE=%s", inputFile),
		fmt.Sprintf("OUTPUT_DIR=%s", outputDir),
	)
	return cmd
}

func (e *ScriptExecutor) runScript(cmd *exec.Cmd, step types.Step) error {
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

	scriptLogger := executeLogger.Context(step.Name)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			scriptLogger.Verbosef("[stdout] %s\n", scanner.Text())
		}
	}()

	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			scriptLogger.Verbosef("[stderr] %s\n", scanner.Text())
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

func (e *ScriptExecutor) runScriptWithTimeout(cmd *exec.Cmd, step types.Step, timeout time.Duration) error {
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

	scriptLogger := executeLogger.Context(step.Name)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			scriptLogger.Verbosef("[stdout] %s\n", scanner.Text())
		}
	}()

	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			scriptLogger.Verbosef("[stderr] %s\n", scanner.Text())
		}
	}()

	// Wait for command to finish with timeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case err := <-done:
		wg.Wait()
		if err != nil {
			executeLogger.Printf("Error executing script: %v\n", err)
			return fmt.Errorf("script execution failed: %w", err)
		}
		return nil
	case <-ctx.Done():
		executeLogger.Printf("Script '%s' exceeded timeout of %s, sending SIGKILL\n", step.Name, timeout)
		// Send SIGKILL to the process group
		if cmd.Process != nil {
			cmd.Process.Signal(syscall.SIGKILL)
			cmd.Process.Wait()
		}
		wg.Wait()
		return ErrTimeout
	}
}


