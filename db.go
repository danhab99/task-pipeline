package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"

	"github.com/danhab99/idk/workers"
	badger "github.com/dgraph-io/badger/v4"
	_ "github.com/mattn/go-sqlite3"
)

var dbLogger = NewLogger("DB")

const schema string = `
CREATE TABLE IF NOT EXISTS step (
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  name      TEXT NOT NULL,
  script    TEXT NOT NULL,
  is_start  INTEGER DEFAULT 0,
  parallel  INTEGER,
  inputs    TEXT,
  version   INTEGER DEFAULT 1,
  UNIQUE(name, version)
);

CREATE TABLE IF NOT EXISTS task (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  step_id          INTEGER NOT NULL,
  input_resource_id INTEGER,
  processed        INTEGER DEFAULT 0,
  error            TEXT,

  FOREIGN KEY(step_id) REFERENCES step(id),
  FOREIGN KEY(input_resource_id) REFERENCES resource(id),
  UNIQUE(step_id, input_resource_id)
);

CREATE TABLE IF NOT EXISTS resource (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  name             TEXT NOT NULL,
  object_hash      VARCHAR(64) NOT NULL,
  created_at       TEXT DEFAULT (CURRENT_TIMESTAMP),

  UNIQUE(name, object_hash)
);

CREATE INDEX IF NOT EXISTS idx_step_name ON step(name);
CREATE INDEX IF NOT EXISTS idx_task_step ON task(step_id);
CREATE INDEX IF NOT EXISTS idx_task_processed ON task(processed);
CREATE INDEX IF NOT EXISTS idx_resource_name ON resource(name);
CREATE INDEX IF NOT EXISTS idx_task_input_resource ON task(input_resource_id);
`

type Database struct {
	db        *sql.DB
	repo_path string
	badgerDB  *badger.DB
}

type Step struct {
	ID       int64
	Name     string
	Script   string
	IsStart  bool
	Parallel *int
	Inputs   []string
	Version  int
}

type Task struct {
	ID              int64
	StepID          int64
	InputResourceID *int64
	Processed       bool
	Error           *string
}

type Resource struct {
	ID         int64
	Name       string
	ObjectHash string
	CreatedAt  string
}

func (t Task) String() string {
	var e string
	if t.Error == nil {
		e = "NIL"
	} else {
		e = *t.Error
	}

	return fmt.Sprintf("Task(id=%d step_id=%d processed=%v error=%s)", t.ID, t.StepID, t.Processed, e)
}

func NewDatabase(repo_path string) (Database, error) {
	err := os.MkdirAll(repo_path, 0755)
	if err != nil {
		return Database{}, err
	}

	err = os.MkdirAll(repo_path+"/sqlite", 0755)
	if err != nil {
		return Database{}, err
	}

	dbLogger.Verbosef("Opening database at %s/db\n", repo_path)
	db, err := sql.Open("sqlite3", fmt.Sprintf("%s/sqlite/db?timeout=600000", repo_path))
	if err != nil {
		return Database{}, err
	}

	// Set connection pool to reduce lock contention during checkpoint
	// Use multiple connections so readers/writers can make progress in parallel.
	numConns := runtime.NumCPU()
	db.SetMaxOpenConns(numConns)
	db.SetMaxIdleConns(numConns)

	// Force WAL checkpoint to clear the 173GB log before proceeding
	dbLogger.Println("Checkpointing WAL file (this may take a moment)...")
	// _, err = db.Exec("PRAGMA busy_timeout = 600000;")
	_, err = db.Exec("PRAGMA busy_timeout = 6;")
	if err != nil {
		return Database{}, err
	}

	// Checkpoint: restart to clear the wal file
	_, err = db.Exec("PRAGMA wal_autocheckpoint = 0;")
	if err != nil {
		return Database{}, err
	}

	db.Exec("PRAGMA journal_mode=WAL;")
	db.Exec("PRAGMA synchronous=NORMAL;")
	db.Exec("PRAGMA foreign_keys=ON;")

	// Force checkpoint
	_, err = db.Exec("PRAGMA optimize;")
	if err != nil {
		dbLogger.Verbosef("Warning: PRAGMA optimize failed: %v\n", err)
	}

	dbLogger.Println("Initializing database schema")
	_, err = db.Exec(schema)
	if err != nil {
		return Database{}, err
	}

	// Initialize BadgerDB for object storage
	badgerPath := fmt.Sprintf("%s/objects_db", repo_path)
	dbLogger.Verbosef("Opening BadgerDB at %s\n", badgerPath)
	badgerOpts := badger.DefaultOptions(badgerPath)
	badgerOpts.Logger = nil // Disable BadgerDB's default logging

	// Performance tuning for sequential batch operations
	// Objects are written in batches during output processing, then read sequentially during task execution
	badgerOpts.SyncWrites = false           // Don't fsync on every write
	badgerOpts.NumVersionsToKeep = 1        // No version history for immutable data
	badgerOpts.CompactL0OnClose = false     // Faster shutdown
	badgerOpts.ValueLogFileSize = 512 << 20 // Larger value log (512MB) for batch writes
	badgerOpts.MemTableSize = 128 << 20     // Large memtable (128MB) for batch buffering
	badgerOpts.NumMemtables = 3             // More memtables for write-heavy batches
	badgerOpts.NumLevelZeroTables = 5       // Allow more L0 tables before compaction
	badgerOpts.NumLevelZeroTablesStall = 10 // Higher stall threshold
	badgerOpts.ValueThreshold = 1024        // Store larger values in value log for sequential read
	badgerOpts.NumCompactors = 2            // More compactors for background work

	badgerDB, err := badger.Open(badgerOpts)
	if err != nil {
		return Database{}, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	return Database{db, repo_path, badgerDB}, nil
}

// Step CRUD operations

func (d Database) CreateStep(step Step) (int64, error) {
	// Serialize inputs to JSON for storage and comparison
	// Treat nil and empty slices the same way
	var inputsStr string
	if len(step.Inputs) > 0 {
		inputsJSON, err := json.Marshal(step.Inputs)
		if err != nil {
			return 0, fmt.Errorf("failed to marshal inputs: %w", err)
		}
		inputsStr = string(inputsJSON)
	} else {
		inputsStr = "[]"
	}

	// Check if a step with the same name, script, and inputs already exists
	var existingID int64
	var existingInputs sql.NullString
	err := d.db.QueryRow("SELECT id, inputs FROM step WHERE name = ? AND script = ? ORDER BY version DESC LIMIT 1", step.Name, step.Script).Scan(&existingID, &existingInputs)
	if err == nil {
		// Step with same name and script exists, check if inputs match
		existingInputsStr := "[]"
		if existingInputs.Valid && existingInputs.String != "" {
			existingInputsStr = existingInputs.String
		}

		if existingInputsStr == inputsStr {
			// Inputs match, update parallel and is_start flags
			s := 0
			if step.IsStart {
				s = 1
			}

			_, err := d.db.Exec("UPDATE step SET parallel = ?, is_start = ? WHERE id = ?", step.Parallel, s, existingID)
			if err != nil {
				return 0, err
			}

			return existingID, nil
		}
		// Inputs changed, need to create a new version
	}
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}

	// Get the max version for this step name
	var maxVersion sql.NullInt64
	err = d.db.QueryRow("SELECT MAX(version) FROM step WHERE name = ?", step.Name).Scan(&maxVersion)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}

	version := 1
	if maxVersion.Valid {
		version = int(maxVersion.Int64) + 1
	}

	res, err := d.db.Exec(`
INSERT INTO step (name, script, is_start, parallel, inputs, version)
VALUES (?, ?, ?, ?, ?, ?)
`, step.Name, step.Script, step.IsStart, step.Parallel, inputsStr, version)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (d Database) GetStep(id int64) (*Step, error) {
	var step Step
	var parallel sql.NullInt64
	var inputsJSON sql.NullString
	err := d.db.QueryRow("SELECT id, name, script, is_start, parallel, inputs, version FROM step WHERE id = ?", id).Scan(
		&step.ID, &step.Name, &step.Script, &step.IsStart, &parallel, &inputsJSON, &step.Version,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if parallel.Valid {
		val := int(parallel.Int64)
		step.Parallel = &val
	}
	if inputsJSON.Valid && inputsJSON.String != "" {
		if err := json.Unmarshal([]byte(inputsJSON.String), &step.Inputs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal inputs: %w", err)
		}
	}
	return &step, nil
}

func (d Database) GetStepByName(name string) (*Step, error) {
	var step Step
	var parallel sql.NullInt64
	var inputsJSON sql.NullString
	err := d.db.QueryRow("SELECT id, name, script, is_start, parallel, inputs, version FROM step WHERE name = ? ORDER BY version DESC LIMIT 1", name).Scan(
		&step.ID, &step.Name, &step.Script, &step.IsStart, &parallel, &inputsJSON, &step.Version,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if parallel.Valid {
		val := int(parallel.Int64)
		step.Parallel = &val
	}
	if inputsJSON.Valid && inputsJSON.String != "" {
		if err := json.Unmarshal([]byte(inputsJSON.String), &step.Inputs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal inputs: %w", err)
		}
	}
	return &step, nil
}

func (d Database) GetStartingStep() (*Step, error) {
	var step Step
	var parallel sql.NullInt64
	var inputsJSON sql.NullString
	err := d.db.QueryRow("SELECT id, name, script, is_start, parallel, inputs, version FROM step WHERE is_start = 1 ORDER BY version DESC LIMIT 1").Scan(
		&step.ID, &step.Name, &step.Script, &step.IsStart, &parallel, &inputsJSON, &step.Version,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if parallel.Valid {
		val := int(parallel.Int64)
		step.Parallel = &val
	}
	if inputsJSON.Valid && inputsJSON.String != "" {
		if err := json.Unmarshal([]byte(inputsJSON.String), &step.Inputs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal inputs: %w", err)
		}
	}
	return &step, nil
}

func (d Database) DeleteStep(id int64) error {
	_, err := d.db.Exec("DELETE FROM step WHERE id = ?", id)
	return err
}

func (d Database) UpdateStepStatus(id int64, processed bool) error {
	// No-op: step processed status is no longer tracked
	return nil
}

func (d Database) CountSteps() (int64, error) {
	var count int64
	err := d.db.QueryRow("SELECT COUNT(*) FROM step").Scan(&count)
	return count, err
}

func (d Database) CountStepsWithoutParallel() (int64, error) {
	var count int64
	err := d.db.QueryRow("SELECT COUNT(*) FROM step WHERE parallel IS NOT NULL").Scan(&count)
	return count, err
}

func (d Database) ListSteps() chan Step {
	stepChan := make(chan Step)

	go func() {
		defer close(stepChan)

		rows, err := d.db.Query("SELECT id, name, script, is_start, parallel, inputs, version FROM step ORDER BY id")
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		for rows.Next() {
			var step Step
			var parallel sql.NullInt64
			var inputsJSON sql.NullString
			if err := rows.Scan(&step.ID, &step.Name, &step.Script, &step.IsStart, &parallel, &inputsJSON, &step.Version); err != nil {
				panic(err)
			}
			if parallel.Valid {
				val := int(parallel.Int64)
				step.Parallel = &val
			}
			if inputsJSON.Valid && inputsJSON.String != "" {
				if err := json.Unmarshal([]byte(inputsJSON.String), &step.Inputs); err != nil {
					dbLogger.Verbosef("Warning: failed to unmarshal inputs for step %d: %v\n", step.ID, err)
				}
			}
			stepChan <- step
		}

		if err := rows.Err(); err != nil {
			panic(err)
		}
	}()

	return stepChan
}

func (d Database) GetTaintedSteps() chan Step {
	stepChan := make(chan Step)

	go func() {
		defer close(stepChan)

		// Find all steps where there's a newer version with a different script or inputs
		rows, err := d.db.Query(`
			SELECT s1.id, s1.name, s1.script, s1.is_start, s1.parallel, s1.inputs, s1.version
			FROM step s1
			INNER JOIN step s2 ON s1.name = s2.name
			WHERE s1.version < s2.version
			  AND (s1.script != s2.script OR COALESCE(s1.inputs, '') != COALESCE(s2.inputs, ''))
			GROUP BY s1.id
			ORDER BY s1.name, s1.version
		`)
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		for rows.Next() {
			var step Step
			var parallel sql.NullInt64
			var inputsJSON sql.NullString
			if err := rows.Scan(&step.ID, &step.Name, &step.Script, &step.IsStart, &parallel, &inputsJSON, &step.Version); err != nil {
				panic(err)
			}
			if parallel.Valid {
				val := int(parallel.Int64)
				step.Parallel = &val
			}
			if inputsJSON.Valid && inputsJSON.String != "" {
				if err := json.Unmarshal([]byte(inputsJSON.String), &step.Inputs); err != nil {
					dbLogger.Verbosef("Warning: failed to unmarshal inputs for step %d: %v\n", step.ID, err)
				}
			}
			stepChan <- step
		}

		if err := rows.Err(); err != nil {
			panic(err)
		}
	}()

	return stepChan
}

// Resource CRUD operations

// CreateResourceFromReader reads data from an io.Reader, stores it in BadgerDB, and creates a resource record in SQLite.
// Returns the resource ID and the calculated hash.
func (d Database) CreateResourceFromReader(name string, reader io.Reader) (int64, string, error) {
	// Read all data and calculate hash
	data, err := io.ReadAll(reader)
	if err != nil {
		return 0, "", fmt.Errorf("failed to read data: %w", err)
	}

	// Calculate hash
	hasher := sha256.New()
	hasher.Write(data)
	hashBytes := hasher.Sum(nil)
	hash := hex.EncodeToString(hashBytes)

	// Check if object already exists in BadgerDB
	if !d.ObjectExists(hash) {
		// Store in BadgerDB
		if err := d.StoreObject(hash, data); err != nil {
			return 0, "", fmt.Errorf("failed to store object: %w", err)
		}
	}

	// Create resource record in SQLite
	resourceID, err := d.CreateResource(name, hash)
	if err != nil {
		return 0, "", fmt.Errorf("failed to create resource record: %w", err)
	}

	return resourceID, hash, nil
}

func (d Database) CreateResource(name string, objectHash string) (int64, error) {
	// Use an upsert-like pattern to make this safe under concurrency:
	// INSERT ... ON CONFLICT DO NOTHING, then SELECT the id. This avoids
	// races where two goroutines attempt to insert the same resource.
	_, err := d.db.Exec(`
INSERT INTO resource (name, object_hash)
VALUES (?, ?)
ON CONFLICT(name, object_hash) DO NOTHING
`, name, objectHash)
	if err != nil {
		return 0, err
	}

	// Now select the id (should exist either from this insert or a concurrent one)
	var id int64
	err = d.db.QueryRow("SELECT id FROM resource WHERE name = ? AND object_hash = ? LIMIT 1", name, objectHash).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (d Database) GetResource(id int64) (*Resource, error) {
	var r Resource
	err := d.db.QueryRow("SELECT id, name, object_hash, created_at FROM resource WHERE id = ?", id).Scan(
		&r.ID, &r.Name, &r.ObjectHash, &r.CreatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &r, nil
}

func (d Database) GetResourcesByName(name string) chan Resource {
	resourceChan := make(chan Resource)

	go func() {
		defer close(resourceChan)

		rows, err := d.db.Query("SELECT id, name, object_hash, created_at FROM resource WHERE name = ? ORDER BY created_at DESC", name)
		if err != nil {
			dbLogger.Verbosef("Error querying resources by name %s: %v\n", name, err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var r Resource
			if err := rows.Scan(&r.ID, &r.Name, &r.ObjectHash, &r.CreatedAt); err != nil {
				dbLogger.Verbosef("Error scanning resource: %v\n", err)
				return
			}
			resourceChan <- r
		}

		if err := rows.Err(); err != nil {
			dbLogger.Verbosef("Error iterating resources: %v\n", err)
		}
	}()

	return resourceChan
}

func (d Database) CountResources() (int64, error) {
	var count int64
	err := d.db.QueryRow("SELECT COUNT(*) FROM resource").Scan(&count)
	return count, err
}

func (d Database) GetAllResources() chan Resource {
	resourceChan := make(chan Resource)

	go func() {
		defer close(resourceChan)

		rows, err := d.db.Query("SELECT id, name, object_hash, created_at FROM resource ORDER BY created_at DESC")
		if err != nil {
			dbLogger.Verbosef("Error querying all resources: %v\n", err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var r Resource
			if err := rows.Scan(&r.ID, &r.Name, &r.ObjectHash, &r.CreatedAt); err != nil {
				dbLogger.Verbosef("Error scanning resource: %v\n", err)
				return
			}
			resourceChan <- r
		}

		if err := rows.Err(); err != nil {
			dbLogger.Verbosef("Error iterating resources: %v\n", err)
		}
	}()

	return resourceChan
}

func (d Database) GetUnconsumedResourcesByName(name string, consumingStepID int64) chan Resource {
	resourceChan := make(chan Resource)

	go func() {
		defer close(resourceChan)

		// Find resources with this name that don't have a task in the consuming step that uses them as input
		rows, err := d.db.Query(`
			SELECT r.id, r.name, r.object_hash, r.created_at 
			FROM resource r
			WHERE r.name = ?
			AND NOT EXISTS (
				SELECT 1 FROM task t
				WHERE t.input_resource_id = r.id 
				AND t.step_id = ?
			)
			ORDER BY r.created_at DESC
		`, name, consumingStepID)
		if err != nil {
			dbLogger.Verbosef("Error querying unconsumed resources for name %s, step %d: %v\n", name, consumingStepID, err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var r Resource
			if err := rows.Scan(&r.ID, &r.Name, &r.ObjectHash, &r.CreatedAt); err != nil {
				dbLogger.Verbosef("Error scanning resource: %v\n", err)
				return
			}
			resourceChan <- r
		}

		if err := rows.Err(); err != nil {
			dbLogger.Verbosef("Error iterating resources: %v\n", err)
		}
	}()

	return resourceChan
}

func (d Database) DeleteResource(id int64) error {
	_, err := d.db.Exec("DELETE FROM resource WHERE id = ?", id)
	return err
}

// GetTaskInputResource returns the input resource for a task (if any)
func (d Database) GetTaskInputResource(taskID int64) (*Resource, error) {
	var r Resource
	err := d.db.QueryRow(`
		SELECT r.id, r.name, r.object_hash, r.created_at
		FROM resource r
		INNER JOIN task t ON r.id = t.input_resource_id
		WHERE t.id = ?
	`, taskID).Scan(&r.ID, &r.Name, &r.ObjectHash, &r.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &r, nil
}

// Task CRUD operations

func (d Database) CreateTask(task Task) (int64, error) {
	p := 0
	if task.Processed {
		p = 1
	}
	res, err := d.db.Exec(`
INSERT INTO task (step_id, input_resource_id, processed, error)
VALUES (?, ?, ?, ?)
`, task.StepID, task.InputResourceID, p, task.Error)
	if err != nil {
		return 0, err
	}

	return res.LastInsertId()
}

func (d Database) BatchInsertTasks(tasks []Task) ([]Task, error) {
	if len(tasks) == 0 {
		return nil, nil
	}

	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
	INSERT INTO task (step_id, input_resource_id, processed, error)
	VALUES (?, ?, ?, ?)`)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	for i, task := range tasks {
		p := 0
		if task.Processed {
			p = 1
		}
		res, err := stmt.Exec(task.StepID, task.InputResourceID, p, task.Error)
		if err != nil {
			return nil, err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return nil, err
		}
		tasks[i].ID = int64(id)
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return tasks, nil
}

// CreateTasksFromResources creates tasks for a given step from a set of input resources.
// Each resource will create a unique task (step_id, input_resource_id is unique).
// Returns the created task IDs.
func (d Database) CreateTasksFromResources(stepID int64, resourceIDs []int64) ([]int64, error) {
	if len(resourceIDs) == 0 {
		return nil, nil
	}

	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO task (step_id, input_resource_id, processed, error)
		VALUES (?, ?, 0, NULL)
		ON CONFLICT(step_id, input_resource_id) DO NOTHING
	`)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	var taskIDs []int64
	for _, resourceID := range resourceIDs {
		res, err := stmt.Exec(stepID, resourceID)
		if err != nil {
			return nil, err
		}

		id, err := res.LastInsertId()
		if err != nil {
			return nil, err
		}

		// If LastInsertId is 0, the insert was ignored (duplicate)
		if id > 0 {
			taskIDs = append(taskIDs, id)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return taskIDs, nil
}

// ScheduleTasksForStep creates tasks for all unconsumed resources matching the step's inputs.
// Uses a single SQL INSERT to efficiently schedule all tasks at once.
// Returns the number of new tasks created.
func (d Database) ScheduleTasksForStep(stepID int64) (int64, error) {
	step, err := d.GetStep(stepID)
	if err != nil {
		return 0, err
	}

	if len(step.Inputs) == 0 {
		dbLogger.Verbosef("Step %d (%s) has no inputs, skipping scheduling\n", stepID, step.Name)
		return 0, nil
	}

	// Build IN clause for input resource names
	inputsJSON, err := json.Marshal(step.Inputs)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal inputs: %w", err)
	}

	dbLogger.Verbosef("Scheduling tasks for step %d (%s) with inputs: %s\n", stepID, step.Name, string(inputsJSON))

	// Single SQL statement to create tasks for all unconsumed resources
	// that match the step's input names
	result, err := d.db.Exec(`
		INSERT INTO task (step_id, input_resource_id, processed, error)
		SELECT ?, r.id, 0, NULL
		FROM resource r
		WHERE r.name IN (SELECT value FROM json_each(?))
		  AND NOT EXISTS (
		      SELECT 1 FROM task t 
		      WHERE t.step_id = ? 
		        AND t.input_resource_id = r.id
		  )
	`, stepID, string(inputsJSON), stepID)

	if err != nil {
		return 0, fmt.Errorf("failed to schedule tasks: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	if rowsAffected > 0 {
		dbLogger.Verbosef("Scheduled %d new tasks for step %d (%s)\n", rowsAffected, stepID, step.Name)
	} else {
		dbLogger.Verbosef("No new tasks scheduled for step %d (%s) - no matching unconsumed resources\n", stepID, step.Name)
	}

	return rowsAffected, nil
}

func (d Database) GetTask(id int64) (*Task, error) {
	var t Task
	err := d.db.QueryRow("SELECT id, step_id, input_resource_id, processed, error FROM task WHERE id = ?", id).Scan(
		&t.ID, &t.StepID, &t.InputResourceID, &t.Processed, &t.Error,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &t, nil
}

func (d Database) TaskExists(id int64) (bool, error) {
	var exists bool
	err := d.db.QueryRow("SELECT EXISTS(SELECT 1 FROM task WHERE id = ?)", id).Scan(&exists)
	return exists, err
}

func (d Database) UpdateTaskStatus(id int64, processed bool, errorMsg *string) error {
	_, err := d.db.Exec(`
UPDATE task 
SET processed = ?, error = ?
WHERE id = ?
`, processed, errorMsg, id)
	return err
}

func (d Database) MarkStepTasksUnprocessed(stepID int64) error {
	// runtime.Breakpoint()
	_, err := d.db.Exec(`
UPDATE task 
SET processed = 0, error = NULL
WHERE step_id = ?
`, stepID)
	return err
}

func (d Database) MarkStepUndone(stepID int64) error {
	// Delete all tasks and resources for this step
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Delete all tasks for this step
	result, err := tx.Exec("DELETE FROM task WHERE step_id = ?", stepID)
	if err != nil {
		return err
	}

	tasksDeleted, _ := result.RowsAffected()

	if err := tx.Commit(); err != nil {
		return err
	}

	dbLogger.Verbosef("Marked step %d as undone: deleted %d tasks and their resources\n", stepID, tasksDeleted)
	return nil
}

func (d Database) DeleteTask(id int64) error {
	_, err := d.db.Exec("DELETE FROM task WHERE id = ?", id)
	return err
}

func (d Database) ListTasks() chan Task {
	taskChan := make(chan Task)

	go func() {
		defer close(taskChan)

		rows, err := d.db.Query("SELECT id, step_id, input_resource_id, processed, error FROM task ORDER BY id")
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		for rows.Next() {
			var t Task
			if err := rows.Scan(&t.ID, &t.StepID, &t.InputResourceID, &t.Processed, &t.Error); err != nil {
				panic(err)
			}
			taskChan <- t
		}

		if err := rows.Err(); err != nil {
			panic(err)
		}
	}()

	return taskChan
}

// Relational operators

func (d Database) GetTasksForStep(stepID int64) chan Task {
	taskChan := make(chan Task)

	go func() {
		defer close(taskChan)

		rows, err := d.db.Query(`
			SELECT id, step_id, input_resource_id, processed, error 
			FROM task 
			WHERE step_id = ?
			ORDER BY id
		`, stepID)
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		for rows.Next() {
			var t Task
			if err := rows.Scan(&t.ID, &t.StepID, &t.InputResourceID, &t.Processed, &t.Error); err != nil {
				panic(err)
			}
			taskChan <- t
		}

		if err := rows.Err(); err != nil {
			panic(err)
		}
	}()

	return taskChan
}

func (d Database) CountUnprocessedTasks() (int64, error) {
	row := d.db.QueryRow("SELECT COUNT(*) FROM task WHERE processed = 0")
	var count int64
	err := row.Scan(&count)
	return count, err
}

func (d Database) CountTasksForStep(stepID int64) (int64, error) {
	row := d.db.QueryRow("SELECT COUNT(*) FROM task WHERE step_id = ?", stepID)
	var count int64
	err := row.Scan(&count)
	return count, err
}

func (d Database) CountUnprocessedTasksForStep(stepID int64) (int64, error) {
	row := d.db.QueryRow("SELECT COUNT(*) FROM task WHERE step_id = ? AND processed = 0", stepID)
	var count int64
	err := row.Scan(&count)
	return count, err
}

// GetTaskCountsForStep returns (total tasks, processed tasks) for a given step
func (d Database) GetTaskCountsForStep(stepID int64) (int64, int64, error) {
	totalTasks, err := d.CountTasksForStep(stepID)
	if err != nil {
		return 0, 0, err
	}

	unprocessedTasks, err := d.CountUnprocessedTasksForStep(stepID)
	if err != nil {
		return totalTasks, 0, err
	}

	processedTasks := totalTasks - unprocessedTasks
	return totalTasks, processedTasks, nil
}

func (d Database) IsStepComplete(stepID int64) (bool, error) {
	count, err := d.CountUnprocessedTasksForStep(stepID)
	if err != nil {
		return false, err
	}
	return count == 0, nil
}

func (d Database) CheckAndMarkStepComplete(stepID int64) (bool, error) {
	isComplete, err := d.IsStepComplete(stepID)
	if err != nil {
		return false, err
	}

	if isComplete {
		step, err := d.GetStep(stepID)
		if err != nil {
			return false, err
		}

		// Only mark as processed if it wasn't already
		if step != nil {
			err = d.UpdateStepStatus(stepID, true)
			if err != nil {
				return false, err
			}
			dbLogger.Verbosef("Step %d (%s) marked as complete\n", stepID, step.Name)
		}
	}

	return isComplete, nil
}

func (d Database) AreAllStepsComplete() (bool, error) {
	// Step completion is no longer tracked, always return true
	return true, nil
}

func (d Database) GetPipelineStatus() (complete bool, totalTasks int64, processedTasks int64, err error) {
	err = d.db.QueryRow("SELECT COUNT(*) FROM task").Scan(&totalTasks)
	if err != nil {
		return false, 0, 0, err
	}

	err = d.db.QueryRow("SELECT COUNT(*) FROM task WHERE processed = 1").Scan(&processedTasks)
	if err != nil {
		return false, 0, 0, err
	}

	complete = totalTasks > 0 && totalTasks == processedTasks
	return complete, totalTasks, processedTasks, nil
}

func (d Database) GetUnprocessedTasks(stepID int64) chan Task {
	taskChan := make(chan Task)

	go func() {
		defer close(taskChan)
		var taskCount int64 = 0
		defer func() {
			dbLogger.Verbosef("GetUnprocessedTasks(step=%d) found %d unprocessed tasks\n", stepID, taskCount)
		}()

		// Get all unprocessed tasks for this step
		rows, err := d.db.Query(`
			SELECT t.id, t.step_id, t.input_resource_id, t.processed, t.error
			FROM task t
			WHERE t.step_id = ? 
			  AND t.processed = 0
			ORDER BY t.id
		`, stepID)
		if err != nil {
			dbLogger.Verbosef("Error querying unprocessed tasks for step %d: %v\n", stepID, err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var t Task
			if err := rows.Scan(&t.ID, &t.StepID, &t.InputResourceID, &t.Processed, &t.Error); err != nil {
				dbLogger.Verbosef("Error scanning task for step %d: %v\n", stepID, err)
				return
			}
			taskCount++
			taskChan <- t
		}

		if err := rows.Err(); err != nil {
			dbLogger.Verbosef("Error iterating tasks for step %d: %v\n", stepID, err)
		}
	}()

	return taskChan
}

// Utility functions

// StoreObject stores object data in BadgerDB
func (d Database) StoreObject(hash string, data []byte) error {
	// Use WriteBatch for better performance even for single writes
	wb := d.badgerDB.NewWriteBatch()
	defer wb.Cancel()

	if err := wb.Set([]byte(hash), data); err != nil {
		return err
	}

	return wb.Flush()
}

// StoreObjectBatch stores multiple objects in a single batch (much faster)
func (d Database) StoreObjectBatch(objects map[string][]byte) error {
	wb := d.badgerDB.NewWriteBatch()
	defer wb.Cancel()

	for hash, data := range objects {
		if err := wb.Set([]byte(hash), data); err != nil {
			return err
		}
	}

	return wb.Flush()
}

// GetObject retrieves object data from BadgerDB
func (d Database) GetObject(hash string) ([]byte, error) {
	var data []byte
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(hash))
		if err != nil {
			return err
		}
		data, err = item.ValueCopy(nil)
		return err
	})
	return data, err
}

// GetObjectBatch retrieves multiple objects in a single transaction (faster for sequential reads)
func (d Database) GetObjectBatch(hashes []string) (map[string][]byte, error) {
	results := make(map[string][]byte)

	err := d.badgerDB.View(func(txn *badger.Txn) error {
		for _, hash := range hashes {
			item, err := txn.Get([]byte(hash))
			if err != nil {
				return err
			}
			data, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			results[hash] = data
		}
		return nil
	})

	return results, err
}

// ObjectExists checks if an object exists in BadgerDB
func (d Database) ObjectExists(hash string) bool {
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(hash))
		return err
	})
	return err == nil
}

func (d *Database) CreateAndGetTask(t Task) (*Task, error) {
	taskId, err := d.CreateTask(t)
	if err != nil {
		return nil, err
	}

	return d.GetTask(taskId)
}

// ForceSaveWAL performs a WAL checkpoint to ensure data is persisted to the database file
func (d Database) ForceSaveWAL() error {
	dbLogger.Println("Checkpointing WAL...")
	_, err := d.db.Exec("PRAGMA wal_checkpoint(RESTART);")
	if err != nil {
		dbLogger.Verbosef("Error checkpointing WAL: %v\n", err)
		return err
	}
	dbLogger.Println("WAL checkpoint complete")
	return nil
}

// Close closes both SQLite and BadgerDB connections
func (d Database) Close() error {
	if err := d.db.Close(); err != nil {
		return fmt.Errorf("failed to close SQLite: %w", err)
	}
	if err := d.badgerDB.Close(); err != nil {
		return fmt.Errorf("failed to close BadgerDB: %w", err)
	}
	return nil
}

func (db Database) MakeResourceConsumer() chan FileData {
	outputChan := make(chan FileData, 100) // Buffered to prevent deadlock

	// Jobs for background storage and DB insert
	type storeJob struct {
		hash string
		data []byte
		name string
	}
	type dbJob struct {
		name string
		hash string
	}

	storeChan := make(chan storeJob, runtime.NumCPU())
	dbJobChan := make(chan dbJob, 100)

	// Worker pool: read FileData, compute hash, and dispatch store + db jobs using workers.Parallel0
	numWorkers := runtime.NumCPU()
	go func() {
		workers.Parallel0(outputChan, numWorkers, func(fd FileData) {
			resourceName := strings.Split(fd.Name, "_")[0]
			data, err := io.ReadAll(fd.Reader)
			if err != nil {
				pipelineLogger.Verbosef("Error reading file %s: %v\n", fd.Name, err)
				return
			}

			// Compute hash
			hasher := sha256.New()
			hasher.Write(data)
			hash := hex.EncodeToString(hasher.Sum(nil))

			// Enqueue store job; if storeChan is full, spawn a goroutine so the worker doesn't block
			storeChan <- storeJob{hash: hash, data: data, name: fd.Name}

			// Enqueue DB job (should be quick)
			dbJobChan <- dbJob{name: resourceName, hash: hash}
		})

		// When output processing finishes, close the downstream channels
		close(storeChan)
		close(dbJobChan)
	}()

	// Store workers using workers.Parallel0
	numStoreWorkers := 2
	go func() {
		workers.Parallel0(storeChan, numStoreWorkers, func(s storeJob) {
			if !db.ObjectExists(s.hash) {
				if err := db.StoreObject(s.hash, s.data); err != nil {
					pipelineLogger.Verbosef("Error storing object %s: %v\n", s.hash[:16]+"...", err)
				}
			}
		})
	}()

	// DB inserter (parallel workers to improve SQLite concurrency)
	numDBWorkers := runtime.NumCPU()
	go func() {
		workers.Parallel0(dbJobChan, numDBWorkers, func(j dbJob) {
			if _, err := db.CreateResource(j.name, j.hash); err != nil {
				pipelineLogger.Verbosef("Error creating resource %s: %v\n", j.name, err)
				return
			}
			pipelineLogger.Verbosef("Created resource %s (hash: %s)\n", j.name, j.hash[:16]+"...")
		})
	}()

	return outputChan
}
