package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"sync"

	"github.com/fatih/color"
)

type ScriptExecutor struct {
	db       *Database
	pipeline *Pipeline
	tempDir  string
}

func NewScriptExecutor(db *Database, pipeline *Pipeline) *ScriptExecutor {

	tempDir, err := os.MkdirTemp("/tmp", "output-*")
	if err != nil {
		panic(err)
	}

	return &ScriptExecutor{
		db:       db,
		pipeline: pipeline,
		tempDir:  tempDir,
	}
}

func (e *ScriptExecutor) Execute(task Task, step Step) error {
	pipelineLogger.Verbosef("    Executing task ID=%d for step '%s' (step_id=%d)", task.ID, step.Name, task.StepID)

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

	// Create output directory
	outputDir, err := os.MkdirTemp("/tmp", "output-*")
	if err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}
	defer os.RemoveAll(outputDir)

	// Execute the script
	pipelineLogger.Verbosef("    Executing: %s", step.Script)
	cmd := e.buildCommand(step, inputFile.Name(), outputDir)

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
		pipelineLogger.Verbosef("    Input: %d bytes from resource '%s' (hash: %s)", n, inputResource.Name, inputResource.ObjectHash[:16]+"...")
	} else {
		pipelineLogger.Verbosef("    Input: (empty - start step)")
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
		pipelineLogger.Errorf("    Error starting script: %v", err)
		return fmt.Errorf("failed to start script: %w", err)
	}

	scriptLogger := NewColorLogger(fmt.Sprintf("[SCRIPT:%s] ", step.Name), color.New(color.FgYellow))

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			scriptLogger.Verboseln(scanner.Text())
		}
	}()

	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			scriptLogger.Verbosef("[stderr] %s", scanner.Text())
		}
	}()

	wg.Wait()

	if err := cmd.Wait(); err != nil {
		pipelineLogger.Errorf("    Error executing script: %v", err)
		return fmt.Errorf("script execution failed: %w", err)
	}

	return nil
}
