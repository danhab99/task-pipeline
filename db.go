package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

var dbLogger = log.New(os.Stderr, "[DB] ", log.Ldate|log.Ltime|log.Lmsgprefix)

const schema string = `
CREATE TABLE IF NOT EXISTS step (
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  name     TEXT UNIQUE NOT NULL,
  script   TEXT NOT NULL,
  is_start INTEGER DEFAULT 0,
  parallel INTEGER,
  processed INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS task (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  object_hash      VARCHAR(64) NOT NULL,
  step_id          INTEGER,
  input_task_id    INTEGER,
  processed        INTEGER DEFAULT 0,
  error            TEXT,

  FOREIGN KEY(step_id) REFERENCES step(id),
  FOREIGN KEY(input_task_id) REFERENCES task(id),
  UNIQUE(object_hash, step_id, input_task_id)
);

CREATE INDEX IF NOT EXISTS idx_step_name ON step(name);
CREATE INDEX IF NOT EXISTS idx_task_step ON task(step_id);
CREATE INDEX IF NOT EXISTS idx_task_processed ON task(processed);
CREATE INDEX IF NOT EXISTS idx_task_input ON task(input_task_id);
`

type Database struct {
	db        *sql.DB
	repo_path string
}

type Step struct {
	ID        int64
	Name      string
	Script    string
	IsStart   bool
	Parallel  *int
	Processed bool
}

type Task struct {
	ID          int64
	ObjectHash  string
	StepID      *int64
	InputTaskID *int64
	Processed   bool
	Error       *string
}

func (t Task) String() string {
	var s string
	if t.StepID == nil {
		s = "NIL"
	} else {
		s = fmt.Sprintf("%d", *t.StepID)
	}

	var i string
	if t.InputTaskID == nil {
		i = "NIL"
	} else {
		i = fmt.Sprintf("%d", *t.InputTaskID)
	}

	var e string 
	if t.Error == nil {
		e = "NIL"
	} else {
		e = *t.Error
	}

	return fmt.Sprintf("Task(id=%d object_hash=%s step_id=%s input_task_id=%s processed=%v error=%s)", t.ID, t.ObjectHash, s, i, t.Processed, e)
}

func NewDatabase(repo_path string) (Database, error) {
	err := os.MkdirAll(repo_path, 0755)
	if err != nil {
		return Database{}, err
	}

	dbLogger.Printf("Opening database at %s/db", repo_path)
	db, err := sql.Open("sqlite3", fmt.Sprintf("%s/db", repo_path))
	if err != nil {
		return Database{}, err
	}

	db.Exec("PRAGMA journal_mode=WAL;")
	db.Exec("PRAGMA synchronous=NORMAL;")
	db.Exec("PRAGMA foreign_keys=ON;")

	dbLogger.Println("Initializing database schema")
	_, err = db.Exec(schema)
	if err != nil {
		return Database{}, err
	}

	return Database{db, repo_path}, nil
}

// Step CRUD operations

func (d Database) CreateStep(step Step) (int64, error) {
	res, err := d.db.Exec(`
INSERT INTO step (name, script, is_start, parallel, processed)
VALUES (?, ?, ?, ?, ?)
`, step.Name, step.Script, step.IsStart, step.Parallel, step.Processed)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (d Database) GetStep(id int64) (*Step, error) {
	var step Step
	var parallel sql.NullInt64
	err := d.db.QueryRow("SELECT id, name, script, is_start, parallel, processed FROM step WHERE id = ?", id).Scan(
		&step.ID, &step.Name, &step.Script, &step.IsStart, &parallel, &step.Processed,
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
	return &step, nil
}

func (d Database) GetStepByName(name string) (*Step, error) {
	var step Step
	var parallel sql.NullInt64
	err := d.db.QueryRow("SELECT id, name, script, is_start, parallel, processed FROM step WHERE name = ?", name).Scan(
		&step.ID, &step.Name, &step.Script, &step.IsStart, &parallel, &step.Processed,
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
	return &step, nil
}

func (d Database) GetStartingStep() (*Step, error) {
	var step Step
	var parallel sql.NullInt64
	err := d.db.QueryRow("SELECT id, name, script, is_start, parallel, processed FROM step WHERE is_start = 1 LIMIT 1").Scan(
		&step.ID, &step.Name, &step.Script, &step.IsStart, &parallel, &step.Processed,
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
	return &step, nil
}

func (d Database) DeleteStep(id int64) error {
	_, err := d.db.Exec("DELETE FROM step WHERE id = ?", id)
	return err
}

func (d Database) UpdateStepStatus(id int64, processed bool) error {
	_, err := d.db.Exec(`
UPDATE step 
SET processed = ?
WHERE id = ?
`, processed, id)
	return err
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

		rows, err := d.db.Query("SELECT id, name, script, is_start, parallel, processed FROM step ORDER BY id")
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		for rows.Next() {
			var step Step
			var parallel sql.NullInt64
			if err := rows.Scan(&step.ID, &step.Name, &step.Script, &step.IsStart, &parallel, &step.Processed); err != nil {
				panic(err)
			}
			if parallel.Valid {
				val := int(parallel.Int64)
				step.Parallel = &val
			}
			stepChan <- step
		}

		if err := rows.Err(); err != nil {
			panic(err)
		}
	}()

	return stepChan
}

// Task CRUD operations

func (d Database) CreateTask(task Task) (int64, error) {
	res, err := d.db.Exec(`
INSERT OR IGNORE INTO task (object_hash, step_id, input_task_id, processed, error)
VALUES (?, ?, ?, ?, ?);
`, task.ObjectHash, task.StepID, task.InputTaskID, task.Processed, task.Error)
	if err != nil {
		return 0, err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}

	// If LastInsertId is 0, the insert was ignored (duplicate), so query for existing task
	if id == 0 {
		var existingID int64
		err := d.db.QueryRow(`
			SELECT id FROM task 
			WHERE object_hash = ? AND step_id = ? AND (input_task_id = ? OR (input_task_id IS NULL AND ? IS NULL))
		`, task.ObjectHash, task.StepID, task.InputTaskID, task.InputTaskID).Scan(&existingID)
		if err != nil {
			return 0, fmt.Errorf("failed to find existing task after ignored insert: %w", err)
		}
		return existingID, nil
	}

	return id, nil
}

func (d Database) GetTask(id int64) (*Task, error) {
	var t Task
	err := d.db.QueryRow("SELECT id, object_hash, step_id, input_task_id, processed, error FROM task WHERE id = ?", id).Scan(
		&t.ID, &t.ObjectHash, &t.StepID, &t.InputTaskID, &t.Processed, &t.Error,
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

func (d Database) DeleteTask(id int64) error {
	_, err := d.db.Exec("DELETE FROM task WHERE id = ?", id)
	return err
}

func (d Database) ListTasks() chan Task {
	taskChan := make(chan Task)

	go func() {
		defer close(taskChan)

		rows, err := d.db.Query("SELECT id, object_hash, step_id, input_task_id, processed, error FROM task ORDER BY id")
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		for rows.Next() {
			var t Task
			if err := rows.Scan(&t.ID, &t.ObjectHash, &t.StepID, &t.InputTaskID, &t.Processed, &t.Error); err != nil {
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
			SELECT id, object_hash, step_id, input_task_id, processed, error 
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
			if err := rows.Scan(&t.ID, &t.ObjectHash, &t.StepID, &t.InputTaskID, &t.Processed, &t.Error); err != nil {
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
		if step != nil && !step.Processed {
			err = d.UpdateStepStatus(stepID, true)
			if err != nil {
				return false, err
			}
			dbLogger.Printf("Step %d (%s) marked as complete", stepID, step.Name)
		}
	}

	return isComplete, nil
}

func (d Database) AreAllStepsComplete() (bool, error) {
	var incompleteCount int64
	err := d.db.QueryRow("SELECT COUNT(*) FROM step WHERE processed = 0").Scan(&incompleteCount)
	if err != nil {
		return false, err
	}
	return incompleteCount == 0, nil
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

		rows, err := d.db.Query(`
			SELECT id, object_hash, step_id, input_task_id, processed, error 
			FROM task 
			WHERE step_id = ? 
			  AND processed = 0
			  AND id NOT IN (SELECT DISTINCT input_task_id FROM task WHERE input_task_id IS NOT NULL)
			ORDER BY id
		`, stepID)
		if err != nil {
			dbLogger.Printf("Error querying unprocessed tasks for step %d: %v", stepID, err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var t Task
			if err := rows.Scan(&t.ID, &t.ObjectHash, &t.StepID, &t.InputTaskID, &t.Processed, &t.Error); err != nil {
				dbLogger.Printf("Error scanning task for step %d: %v", stepID, err)
				return
			}
			taskChan <- t
		}

		if err := rows.Err(); err != nil {
			dbLogger.Printf("Error iterating tasks for step %d: %v", stepID, err)
		}
	}()

	return taskChan
}

func (d Database) GetNextTasks(taskID int64) chan Task {
	taskChan := make(chan Task)

	go func() {
		defer close(taskChan)

		rows, err := d.db.Query(`
			SELECT id, object_hash, step_id, input_task_id, processed, error 
			FROM task 
			WHERE input_task_id = ?
			ORDER BY id
		`, taskID)
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		for rows.Next() {
			var t Task
			if err := rows.Scan(&t.ID, &t.ObjectHash, &t.StepID, &t.InputTaskID, &t.Processed, &t.Error); err != nil {
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

func (d Database) IsTaskCompletedInNextStep(nextStepID, taskID int64) (bool, error) {
	var completed bool
	err := d.db.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM task 
			WHERE step_id = ? AND input_task_id = ? AND processed = 1
		)
	`, nextStepID, taskID).Scan(&completed)
	return completed, err
}

// Utility functions

func (d Database) GetObjectPath(hash string) string {
	dir := fmt.Sprintf(
		"%s/objects/%s/%s/%s",
		d.repo_path, hash[0:2], hash[2:4], hash[4:6],
	)

	err := os.MkdirAll(dir, 0755)
	if err != nil {
		panic(err)
	}

	return fmt.Sprintf("%s/%s", dir, hash[6:])
}
