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
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  name      TEXT UNIQUE NOT NULL,
  script    TEXT NOT NULL,
  is_start  INTEGER DEFAULT 0,
  parallel  INTEGER,
  processed INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS task (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  object_hash      VARCHAR(64) NOT NULL,
  step_id          INTEGER,
  input_task_id    INTEGER,
  processed        INTEGER DEFAULT 0,
  error            TEXT,
	runset           TEXT DEFAULT (CURRENT_TIMESTAMP),

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
	runset    string
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

func NewDatabase(repo_path string, runset string) (Database, error) {
	err := os.MkdirAll(repo_path, 0755)
	if err != nil {
		return Database{}, err
	}

	dbLogger.Printf("Opening database at %s/db\n", repo_path)
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

	return Database{db, repo_path, runset}, nil
}

// Step CRUD operations

func (d Database) CreateStep(step Step) (int64, error) {
	dbLogger.Printf("CreateStep: name=%s, is_start=%v, parallel=%v, processed=%v", step.Name, step.IsStart, step.Parallel, step.Processed)
	// Check if step exists first
	existing, err := d.GetStepByName(step.Name)
	if err != nil {
		return 0, err
	}

	if existing != nil {
		// Step exists, update it and return existing ID
		_, err = d.db.Exec(`
UPDATE step 
SET script = ?, is_start = ?, parallel = ?, processed = ?
WHERE id = ?
`, step.Script, step.IsStart, step.Parallel, step.Processed, existing.ID)
		if err != nil {
			return 0, err
		}
		dbLogger.Printf("CreateStep: updated existing step, id=%d", existing.ID)
		return existing.ID, nil
	}

	// Step doesn't exist, create it
	res, err := d.db.Exec(`
INSERT INTO step (name, script, is_start, parallel, processed)
VALUES (?, ?, ?, ?, ?)
`, step.Name, step.Script, step.IsStart, step.Parallel, step.Processed)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	dbLogger.Printf("CreateStep: created new step, id=%d", id)
	return id, err
}

func (d Database) GetStep(id int64) (*Step, error) {
	dbLogger.Printf("GetStep: id=%d", id)
	var step Step
	var parallel sql.NullInt64
	var isStart, processed int
	err := d.db.QueryRow("SELECT id, name, script, is_start, parallel, processed FROM step WHERE id = ?", id).Scan(
		&step.ID, &step.Name, &step.Script, &isStart, &parallel, &processed,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			dbLogger.Printf("GetStep: not found")
			return nil, nil
		}
		return nil, err
	}
	step.IsStart = isStart != 0
	step.Processed = processed != 0
	if parallel.Valid {
		val := int(parallel.Int64)
		step.Parallel = &val
	}
	dbLogger.Printf("GetStep: found name=%s", step.Name)
	return &step, nil
}

func (d Database) GetStepByName(name string) (*Step, error) {
	dbLogger.Printf("GetStepByName: name=%s", name)
	var step Step
	var parallel sql.NullInt64
	var isStart, processed int
	err := d.db.QueryRow("SELECT id, name, script, is_start, parallel, processed FROM step WHERE name = ?", name).Scan(
		&step.ID, &step.Name, &step.Script, &isStart, &parallel, &processed,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			dbLogger.Printf("GetStepByName: not found")
			return nil, nil
		}
		return nil, err
	}
	step.IsStart = isStart != 0
	step.Processed = processed != 0
	if parallel.Valid {
		val := int(parallel.Int64)
		step.Parallel = &val
	}
	dbLogger.Printf("GetStepByName: found id=%d", step.ID)
	return &step, nil
}

func (d Database) GetStartingStep() (*Step, error) {
	dbLogger.Printf("GetStartingStep")
	var step Step
	var parallel sql.NullInt64
	var isStart, processed int
	err := d.db.QueryRow("SELECT id, name, script, is_start, parallel, processed FROM step WHERE is_start = 1 LIMIT 1").Scan(
		&step.ID, &step.Name, &step.Script, &isStart, &parallel, &processed,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			dbLogger.Printf("GetStartingStep: not found")
			return nil, nil
		}
		return nil, err
	}
	step.IsStart = isStart != 0
	step.Processed = processed != 0
	if parallel.Valid {
		val := int(parallel.Int64)
		step.Parallel = &val
	}
	dbLogger.Printf("GetStartingStep: found id=%d, name=%s", step.ID, step.Name)
	return &step, nil
}

func (d Database) DeleteStep(id int64) error {
	dbLogger.Printf("DeleteStep: id=%d", id)
	_, err := d.db.Exec("DELETE FROM step WHERE id = ?", id)
	if err != nil {
		dbLogger.Printf("DeleteStep: error=%v", err)
	} else {
		dbLogger.Printf("DeleteStep: success")
	}
	return err
}

func (d Database) UpdateStepStatus(id int64, processed bool) error {
	dbLogger.Printf("UpdateStepStatus: id=%d, processed=%v", id, processed)
	p := 0
	if processed {
		p = 1
	}

	_, err := d.db.Exec(`
UPDATE step 
SET processed = ?
WHERE id = ?
`, p, id)
	if err != nil {
		dbLogger.Printf("UpdateStepStatus: error=%v", err)
	} else {
		dbLogger.Printf("UpdateStepStatus: success")
	}
	return err
}

func (d Database) CountSteps() (int64, error) {
	dbLogger.Printf("CountSteps")
	var count int64
	err := d.db.QueryRow("SELECT COUNT(*) FROM step").Scan(&count)
	dbLogger.Printf("CountSteps: count=%d", count)
	return count, err
}

func (d Database) CountStepsWithoutParallel() (int64, error) {
	dbLogger.Printf("CountStepsWithoutParallel")
	var count int64
	err := d.db.QueryRow("SELECT COUNT(*) FROM step WHERE parallel IS NOT NULL").Scan(&count)
	dbLogger.Printf("CountStepsWithoutParallel: count=%d", count)
	return count, err
}

func (d Database) ListSteps() chan Step {
	dbLogger.Printf("ListSteps")
	stepChan := make(chan Step)

	go func() {
		defer close(stepChan)
		count := 0

		rows, err := d.db.Query("SELECT id, name, script, is_start, parallel, processed FROM step ORDER BY id")
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		for rows.Next() {
			var step Step
			var parallel sql.NullInt64
			var isStart, processed int
			if err := rows.Scan(&step.ID, &step.Name, &step.Script, &isStart, &parallel, &processed); err != nil {
				panic(err)
			}
			step.IsStart = isStart != 0
			step.Processed = processed != 0
			if parallel.Valid {
				val := int(parallel.Int64)
				step.Parallel = &val
			}
			stepChan <- step
			count++
		}

		if err := rows.Err(); err != nil {
			panic(err)
		}
		dbLogger.Printf("ListSteps: returned %d steps", count)
	}()

	return stepChan
}

// Task CRUD operations

func (d Database) CreateTask(task Task) (int64, error) {
	dbLogger.Printf("CreateTask: object_hash=%s, step_id=%v, input_task_id=%v, processed=%v", task.ObjectHash, task.StepID, task.InputTaskID, task.Processed)
	p := 0
	if task.Processed {
		p = 1
	}
	res, err := d.db.Exec(`
INSERT OR IGNORE INTO task (object_hash, step_id, input_task_id, processed, error, runset)
VALUES (?, ?, ?, ?, ?, ?);
`, task.ObjectHash, task.StepID, task.InputTaskID, p, task.Error, d.runset)
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
		dbLogger.Printf("CreateTask: found existing task, id=%d", existingID)
		return existingID, nil
	}

	dbLogger.Printf("CreateTask: created new task, id=%d", id)
	return id, nil
}

func (d Database) GetTask(id int64) (*Task, error) {
	dbLogger.Printf("GetTask: id=%d", id)
	var t Task
	var processed int
	err := d.db.QueryRow("SELECT id, object_hash, step_id, input_task_id, processed, error FROM task WHERE id = ?", id).Scan(
		&t.ID, &t.ObjectHash, &t.StepID, &t.InputTaskID, &processed, &t.Error,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			dbLogger.Printf("GetTask: not found")
			return nil, nil
		}
		return nil, err
	}
	t.Processed = processed != 0
	dbLogger.Printf("GetTask: found object_hash=%s", t.ObjectHash)
	return &t, nil
}

func (d Database) TaskExists(id int64) (bool, error) {
	dbLogger.Printf("TaskExists: id=%d", id)
	var exists bool
	err := d.db.QueryRow("SELECT EXISTS(SELECT 1 FROM task WHERE id = ?)", id).Scan(&exists)
	dbLogger.Printf("TaskExists: exists=%v", exists)
	return exists, err
}

func (d Database) UpdateTaskStatus(id int64, processed bool, errorMsg *string) error {
	dbLogger.Printf("UpdateTaskStatus: id=%d, processed=%v, error=%v", id, processed, errorMsg)
	p := 0
	if processed {
		p = 1
	}
	_, err := d.db.Exec(`
UPDATE task 
SET processed = ?, error = ?
WHERE id = ?
`, p, errorMsg, id)
	if err != nil {
		dbLogger.Printf("UpdateTaskStatus: error=%v", err)
	} else {
		dbLogger.Printf("UpdateTaskStatus: success")
	}
	return err
}

func (d Database) DeleteTask(id int64) error {
	dbLogger.Printf("DeleteTask: id=%d", id)
	_, err := d.db.Exec("DELETE FROM task WHERE id = ?", id)
	if err != nil {
		dbLogger.Printf("DeleteTask: error=%v", err)
	} else {
		dbLogger.Printf("DeleteTask: success")
	}
	return err
}

func (d Database) ListTasks() chan Task {
	dbLogger.Printf("ListTasks")
	taskChan := make(chan Task)

	go func() {
		defer close(taskChan)
		count := 0

		rows, err := d.db.Query("SELECT id, object_hash, step_id, input_task_id, processed, error FROM task ORDER BY id")
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		for rows.Next() {
			var t Task
			var processed int
			if err := rows.Scan(&t.ID, &t.ObjectHash, &t.StepID, &t.InputTaskID, &processed, &t.Error); err != nil {
				panic(err)
			}
			t.Processed = processed != 0
			taskChan <- t
			count++
		}

		if err := rows.Err(); err != nil {
			panic(err)
		}
		dbLogger.Printf("ListTasks: returned %d tasks", count)
	}()

	return taskChan
}

// Relational operators

func (d Database) GetTasksForStep(stepID int64) chan Task {
	dbLogger.Printf("GetTasksForStep: step_id=%d", stepID)
	taskChan := make(chan Task)

	go func() {
		defer close(taskChan)
		count := 0

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
			var processed int
			if err := rows.Scan(&t.ID, &t.ObjectHash, &t.StepID, &t.InputTaskID, &processed, &t.Error); err != nil {
				panic(err)
			}
			t.Processed = processed != 0
			taskChan <- t
			count++
		}

		if err := rows.Err(); err != nil {
			panic(err)
		}
		dbLogger.Printf("GetTasksForStep: step_id=%d, returned %d tasks", stepID, count)
	}()

	return taskChan
}

func (d Database) CountUnprocessedTasks() (int64, error) {
	dbLogger.Printf("CountUnprocessedTasks")
	row := d.db.QueryRow("SELECT COUNT(*) FROM task WHERE processed = 0")
	var count int64
	err := row.Scan(&count)
	dbLogger.Printf("CountUnprocessedTasks: count=%d", count)
	return count, err
}

func (d Database) CountTasksForStep(stepID int64) (int64, error) {
	dbLogger.Printf("CountTasksForStep: step_id=%d", stepID)
	row := d.db.QueryRow("SELECT COUNT(*) FROM task WHERE step_id = ?", stepID)
	var count int64
	err := row.Scan(&count)
	dbLogger.Printf("CountTasksForStep: step_id=%d, count=%d", stepID, count)
	return count, err
}

func (d Database) CountUnprocessedTasksForStep(stepID int64) (int64, error) {
	dbLogger.Printf("CountUnprocessedTasksForStep: step_id=%d", stepID)
	row := d.db.QueryRow("SELECT COUNT(*) FROM task WHERE step_id = ? AND processed = 0", stepID)
	var count int64
	err := row.Scan(&count)
	dbLogger.Printf("CountUnprocessedTasksForStep: step_id=%d, count=%d", stepID, count)
	return count, err
}

func (d Database) IsStepComplete(stepID int64) (bool, error) {
	dbLogger.Printf("IsStepComplete: step_id=%d", stepID)
	count, err := d.CountUnprocessedTasksForStep(stepID)
	if err != nil {
		return false, err
	}
	isComplete := count == 0
	dbLogger.Printf("IsStepComplete: step_id=%d, complete=%v", stepID, isComplete)
	return isComplete, nil
}

func (d Database) CheckAndMarkStepComplete(stepID int64) (bool, error) {
	dbLogger.Printf("CheckAndMarkStepComplete: step_id=%d", stepID)
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
			dbLogger.Printf("Step %d (%s) marked as complete\n", stepID, step.Name)
		}
	}

	dbLogger.Printf("CheckAndMarkStepComplete: step_id=%d, complete=%v", stepID, isComplete)
	return isComplete, nil
}

func (d Database) AreAllStepsComplete() (bool, error) {
	dbLogger.Printf("AreAllStepsComplete")
	var incompleteCount int64
	err := d.db.QueryRow("SELECT COUNT(*) FROM step WHERE processed = 0").Scan(&incompleteCount)
	if err != nil {
		return false, err
	}
	allComplete := incompleteCount == 0
	dbLogger.Printf("AreAllStepsComplete: complete=%v, incomplete_count=%d", allComplete, incompleteCount)
	return allComplete, nil
}

func (d Database) GetPipelineStatus() (complete bool, totalTasks int64, processedTasks int64, err error) {
	dbLogger.Printf("GetPipelineStatus")
	err = d.db.QueryRow("SELECT COUNT(*) FROM task").Scan(&totalTasks)
	if err != nil {
		return false, 0, 0, err
	}

	err = d.db.QueryRow("SELECT COUNT(*) FROM task WHERE processed = 1").Scan(&processedTasks)
	if err != nil {
		return false, 0, 0, err
	}

	complete = totalTasks > 0 && totalTasks == processedTasks
	dbLogger.Printf("GetPipelineStatus: complete=%v, total=%d, processed=%d", complete, totalTasks, processedTasks)
	return complete, totalTasks, processedTasks, nil
}

func (d Database) GetUnprocessedTasks(stepID int64) chan Task {
	dbLogger.Printf("GetUnprocessedTasks: step_id=%d", stepID)
	taskChan := make(chan Task)

	go func() {
		defer close(taskChan)
		count := 0

		rows, err := d.db.Query(`
			SELECT id, object_hash, step_id, input_task_id, processed, error 
			FROM task 
			WHERE step_id = ? 
			  AND processed = 0
			  AND id NOT IN (SELECT DISTINCT input_task_id FROM task WHERE input_task_id IS NOT NULL)
			ORDER BY id
		`, stepID)
		if err != nil {
			dbLogger.Printf("Error querying unprocessed tasks for step %d: %v\n", stepID, err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var t Task
			var processed int
			if err := rows.Scan(&t.ID, &t.ObjectHash, &t.StepID, &t.InputTaskID, &processed, &t.Error); err != nil {
				dbLogger.Printf("Error scanning task for step %d: %v\n", stepID, err)
				return
			}
			t.Processed = processed != 0
			taskChan <- t
			count++
		}

		if err := rows.Err(); err != nil {
			dbLogger.Printf("Error iterating tasks for step %d: %v\n", stepID, err)
		}
		dbLogger.Printf("GetUnprocessedTasks: step_id=%d, returned %d tasks", stepID, count)
	}()

	return taskChan
}

func (d Database) GetNextTasks(taskID int64) chan Task {
	dbLogger.Printf("GetNextTasks: task_id=%d", taskID)
	taskChan := make(chan Task)

	go func() {
		defer close(taskChan)
		count := 0

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
			var processed int
			if err := rows.Scan(&t.ID, &t.ObjectHash, &t.StepID, &t.InputTaskID, &processed, &t.Error); err != nil {
				panic(err)
			}
			t.Processed = processed != 0
			taskChan <- t
			count++
		}

		if err := rows.Err(); err != nil {
			panic(err)
		}
		dbLogger.Printf("GetNextTasks: task_id=%d, returned %d tasks", taskID, count)
	}()

	return taskChan
}

func (d Database) IsTaskCompletedInNextStep(nextStepID, taskID int64) (bool, error) {
	dbLogger.Printf("IsTaskCompletedInNextStep: next_step_id=%d, task_id=%d", nextStepID, taskID)
	var completed bool
	err := d.db.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM task 
			WHERE step_id = ? AND input_task_id = ? AND processed = 1
		)
	`, nextStepID, taskID).Scan(&completed)
	dbLogger.Printf("IsTaskCompletedInNextStep: next_step_id=%d, task_id=%d, completed=%v", nextStepID, taskID, completed)
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

func (d *Database) CreateAndGetTask(t Task) (*Task, error) {
	dbLogger.Printf("CreateAndGetTask: object_hash=%s, step_id=%v, input_task_id=%v", t.ObjectHash, t.StepID, t.InputTaskID)
	taskId, err := d.CreateTask(t)
	if err != nil {
		return nil, err
	}

	task, err := d.GetTask(taskId)
	if err == nil && task != nil {
		dbLogger.Printf("CreateAndGetTask: returned task id=%d", task.ID)
	}
	return task, err
}
