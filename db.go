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
  parallel INTEGER
);

CREATE TABLE IF NOT EXISTS task (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  object_hash      VARCHAR(64) NOT NULL,
  step_id          INTEGER,
  input_task_id    INTEGER,
  processed        INTEGER DEFAULT 0,

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
	ID       int64
	Name     string
	Script   string
	IsStart  bool
	Parallel *int
}

type Task struct {
	ID          int64
	ObjectHash  string
	StepID      *int64
	InputTaskID *int64
	Processed   bool
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

func (d Database) RegisterStep(name string, script string, isStart bool, parallel *int) (int64, error) {
	_, err := d.db.Exec(`
INSERT INTO step (name, script, is_start, parallel)
VALUES (?, ?, ?, ?)
ON CONFLICT(name)
DO UPDATE SET script = excluded.script, is_start = excluded.is_start, parallel = excluded.parallel;
`, name, script, isStart, parallel)
	if err != nil {
		return 0, err
	}

	var stepID int64
	err = d.db.QueryRow("SELECT id FROM step WHERE name = ?", name).Scan(&stepID)
	return stepID, err
}

func (d Database) GetStepByName(name string) (*Step, error) {
	var step Step
	var parallel sql.NullInt64
	err := d.db.QueryRow("SELECT id, name, script, is_start, parallel FROM step WHERE name = ?", name).Scan(
		&step.ID,
		&step.Name,
		&step.Script,
		&step.IsStart,
		&parallel,
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

func (d Database) GetStepByID(id int64) (*Step, error) {
	var step Step
	var parallel sql.NullInt64
	err := d.db.QueryRow("SELECT id, name, script, is_start, parallel FROM step WHERE id = ?", id).Scan(
		&step.ID,
		&step.Name,
		&step.Script,
		&step.IsStart,
		&parallel,
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

func (d Database) GetTaskByID(id int64) (*Task, error) {
	var t Task
	err := d.db.QueryRow("SELECT id, object_hash, step_id, input_task_id, processed FROM task WHERE id = ?", id).Scan(
		&t.ID,
		&t.ObjectHash,
		&t.StepID,
		&t.InputTaskID,
		&t.Processed,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &t, nil
}

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

func (d Database) InsertTask(objectHash string, stepID *int64, inputTaskID *int64) (int64, bool, error) {
	res, err := d.db.Exec(`
INSERT OR IGNORE INTO task (object_hash, step_id, input_task_id, processed)
VALUES (?, ?, ?, 0);
`, objectHash, stepID, inputTaskID)
	if err != nil {
		return 0, false, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return 0, false, err
	}

	if rowsAffected > 0 {
		id, err := res.LastInsertId()
		return id, true, err
	}

	// Already exists, get the ID
	var id int64
	err = d.db.QueryRow(`
		SELECT id FROM task 
		WHERE object_hash = ? AND step_id IS ? AND input_task_id IS ?
	`, objectHash, stepID, inputTaskID).Scan(&id)
	return id, false, err
}

func (d Database) GetUnprocessedTasks() ([]Task, error) {
	rows, err := d.db.Query(`
		SELECT t.id, t.object_hash, t.step_id, t.input_task_id
		FROM task t
		WHERE t.processed = 0 AND t.step_id IS NOT NULL
		ORDER BY t.id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []Task
	for rows.Next() {
		var t Task
		var inputID sql.NullInt64
		err := rows.Scan(&t.ID, &t.ObjectHash, &t.StepID, &inputID)
		if err != nil {
			return nil, err
		}
		if inputID.Valid {
			val := inputID.Int64
			t.InputTaskID = &val
		}
		tasks = append(tasks, t)
	}

	return tasks, rows.Err()
}

func (d Database) MarkTaskProcessed(taskID int64) error {
	_, err := d.db.Exec("UPDATE task SET processed = 1 WHERE id = ?", taskID)
	return err
}

func (d Database) GetTasksByStepName(name string, mode string) ([]Task, error) {
	var query string
	if mode == "input" {
		query = `
			SELECT t.id, t.object_hash, t.step_id, t.input_task_id, t.processed
			FROM task t
			JOIN step s ON t.step_id = s.id
			WHERE s.name = ?
			ORDER BY t.id
		`
	} else {
		// output mode - get tasks created by this step
		query = `
			SELECT t2.id, t2.object_hash, t2.step_id, t2.input_task_id, t2.processed
			FROM task t1
			JOIN step s ON t1.step_id = s.id
			JOIN task t2 ON t2.input_task_id = t1.id
			WHERE s.name = ?
			ORDER BY t2.id
		`
	}

	rows, err := d.db.Query(query, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []Task
	for rows.Next() {
		var t Task
		if err := rows.Scan(&t.ID, &t.ObjectHash, &t.StepID, &t.InputTaskID, &t.Processed); err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}

	return tasks, rows.Err()
}

func (d Database) MarkStepTasksUnprocessed(stepName string) (int64, error) {
	res, err := d.db.Exec(`
		UPDATE task
		SET processed = 0
		WHERE step_id = (SELECT id FROM step WHERE name = ?)
	`, stepName)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (d Database) GetStartStep() (*Step, error) {
	var step Step
	var parallel sql.NullInt64
	err := d.db.QueryRow("SELECT id, name, script, is_start, parallel FROM step WHERE is_start = 1 LIMIT 1").Scan(
		&step.ID,
		&step.Name,
		&step.Script,
		&step.IsStart,
		&parallel,
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
