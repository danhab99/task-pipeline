package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/fsnotify/fsnotify"
)

var watcherLogger = NewColorLogger("[WATCHER] ", color.New(color.FgBlue, color.Bold))

type OutputWatcher struct {
	db       *Database
	task     Task
	pipeline *Pipeline
	watcher  *fsnotify.Watcher
	wg       sync.WaitGroup
	done     chan struct{}
	mu       sync.Mutex
	seen     map[string]bool
	stopOnce sync.Once
}

func NewOutputWatcher(db *Database, task Task, pipeline *Pipeline) (*OutputWatcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create watcher: %w", err)
	}

	return &OutputWatcher{
		db:       db,
		task:     task,
		pipeline: pipeline,
		watcher:  watcher,
		done:     make(chan struct{}),
		seen:     make(map[string]bool),
	}, nil
}

func (w *OutputWatcher) Start(outputDir string) error {
	if err := w.watcher.Add(outputDir); err != nil {
		return fmt.Errorf("failed to watch directory: %w", err)
	}

	watcherLogger.Verbosef("Watching directory: %s", outputDir)

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for {
			select {
			case event, ok := <-w.watcher.Events:
				if !ok {
					return
				}
				// Process file when it's closed (written completely)
				if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
					// Wait a tiny bit to ensure file is fully written
					time.Sleep(10 * time.Millisecond)
					w.processFile(event.Name)
				}
			case err, ok := <-w.watcher.Errors:
				if !ok {
					return
				}
				watcherLogger.Errorf("Watcher error: %v", err)
			case <-w.done:
				return
			}
		}
	}()

	return nil
}

func (w *OutputWatcher) processFile(filePath string) {
	w.mu.Lock()
	if w.seen[filePath] {
		w.mu.Unlock()
		return
	}
	w.seen[filePath] = true
	w.mu.Unlock()

	// Check if file exists and is a regular file
	info, err := os.Stat(filePath)
	if err != nil {
		return // File might have been deleted already
	}
	if info.IsDir() {
		return
	}

	// Open and read the file
	file, err := os.Open(filePath)
	if err != nil {
		watcherLogger.Errorf("Failed to open file %s: %v", filePath, err)
		return
	}
	defer file.Close()

	// Calculate hash while reading
	hasher := sha256.New()
	tempDir, err := os.MkdirTemp("", "task-pipeline-temp-*")
	if err != nil {
		watcherLogger.Errorf("Failed to create temp directory: %v", err)
		return
	}
	defer os.RemoveAll(tempDir)
	
	tempPath := filepath.Join(tempDir, filepath.Base(filePath))
	tempFile, err := os.Create(tempPath)
	if err != nil {
		watcherLogger.Errorf("Failed to create temp file: %v", err)
		return
	}
	defer tempFile.Close()

	// Copy to temp file and hash simultaneously
	writer := io.MultiWriter(tempFile, hasher)
	_, err = io.Copy(writer, file)
	if err != nil {
		watcherLogger.Errorf("Failed to copy file: %v", err)
		os.Remove(tempPath)
		return
	}
	file.Close()
	tempFile.Close()

	// Get final hash
	hashBytes := hasher.Sum(nil)
	hash := hex.EncodeToString(hashBytes)

	// Move to final location - GetObjectPath creates directories
	finalPath := w.db.GetObjectPath(hash)

	// Check for deduplication
	if _, err := os.Stat(finalPath); err == nil {
		watcherLogger.Verbosef("Object already exists: %s", hash[:16]+"...")
	} else {
		// Try rename first (fast if same filesystem)
		err := os.Rename(tempPath, finalPath)
		if err != nil {
			// If cross-device, copy instead
			src, err := os.Open(tempPath)
			if err != nil {
				watcherLogger.Errorf("Failed to open temp file for copy: %v", err)
				return
			}
			defer src.Close()

			dst, err := os.Create(finalPath)
			if err != nil {
				watcherLogger.Errorf("Failed to create final file: %v", err)
				return
			}
			defer dst.Close()

			_, err = io.Copy(dst, src)
			if err != nil {
				watcherLogger.Errorf("Failed to copy file: %v", err)
				os.Remove(finalPath)
				return
			}

			// Ensure data is written
			if err := dst.Sync(); err != nil {
				watcherLogger.Errorf("Failed to sync final file: %v", err)
				return
			}
		}
	}

	// Remove original file from output dir
	os.Remove(filePath)

	// Extract step name and create task
	filename := filepath.Base(filePath)
	stepName := extractStepName(filename)
	watcherLogger.Verbosef("Output: %s -> %s (hash: %s)", filename, color.MagentaString(stepName), hash[:16]+"...")

	nextStep, err := w.db.GetStepByName(stepName)
	if err != nil {
		watcherLogger.Errorf("Failed to get next step %s: %v", stepName, err)
		return
	}
	if nextStep == nil {
		watcherLogger.Warnf("No step found for name: %s", stepName)
		return
	}

	// Check if already completed
	isCompleted, err := w.db.IsTaskCompletedInNextStep(nextStep.ID, w.task.ID)
	if err != nil {
		watcherLogger.Errorf("Failed to check completion: %v", err)
		return
	}

	if isCompleted {
		watcherLogger.Verbosef("Task already completed in step %s", stepName)
		return
	}

	// Create task
	var inputTaskID *int64
	if w.task.ID > 0 {
		inputTaskID = &w.task.ID
	}

	pTask := Task{
		ObjectHash:  hash,
		StepID:      &nextStep.ID,
		InputTaskID: inputTaskID,
		Processed:   false,
	}

	_, err = w.db.CreateTask(pTask)
	if err != nil {
		watcherLogger.Errorf("Failed to create task: %v", err)
		return
	}

	watcherLogger.Verbosef("Created task for %s in step %s", hash[:16]+"...", stepName)
}

func (w *OutputWatcher) Stop() {
	w.stopOnce.Do(func() {
		close(w.done)
		w.watcher.Close()
		w.wg.Wait()
		watcherLogger.Verbosef("Watcher stopped")
	})
}
