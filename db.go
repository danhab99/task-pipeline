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
CREATE TABLE IF NOT EXISTS task (
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  name     TEXT UNIQUE NOT NULL,
  script   TEXT NOT NULL,
  is_start INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS result (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  object_hash      VARCHAR(64) NOT NULL,
  task_id          INTEGER,
  input_result_id  INTEGER,
  processed        INTEGER DEFAULT 0,

  FOREIGN KEY(task_id) REFERENCES task(id),
  FOREIGN KEY(input_result_id) REFERENCES result(id),
  UNIQUE(object_hash, task_id, input_result_id)
);

CREATE INDEX IF NOT EXISTS idx_task_name ON task(name);
CREATE INDEX IF NOT EXISTS idx_result_task ON result(task_id);
CREATE INDEX IF NOT EXISTS idx_result_processed ON result(processed);
CREATE INDEX IF NOT EXISTS idx_result_input ON result(input_result_id);
`

type Database struct {
	db        *sql.DB
	repo_path string
}

type Task struct {
	ID      int64
	Name    string
	Script  string
	IsStart bool
}

type Result struct {
	ID            int64
	ObjectHash    string
	TaskID        *int64
	InputResultID *int64
	Processed     bool
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

func (d Database) RegisterTask(name string, script string, isStart bool) (int64, error) {
	_, err := d.db.Exec(`
INSERT INTO task (name, script, is_start)
VALUES (?, ?, ?)
ON CONFLICT(name)
DO UPDATE SET script = excluded.script, is_start = excluded.is_start;
`, name, script, isStart)
	if err != nil {
		return 0, err
	}

	var taskID int64
	err = d.db.QueryRow("SELECT id FROM task WHERE name = ?", name).Scan(&taskID)
	return taskID, err
}

func (d Database) GetTaskByName(name string) (*Task, error) {
	var task Task
	err := d.db.QueryRow("SELECT id, name, script, is_start FROM task WHERE name = ?", name).Scan(
		&task.ID,
		&task.Name,
		&task.Script,
		&task.IsStart,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &task, nil
}

func (d Database) GetTaskByID(id int64) (*Task, error) {
	var task Task
	err := d.db.QueryRow("SELECT id, name, script, is_start FROM task WHERE id = ?", id).Scan(
		&task.ID,
		&task.Name,
		&task.Script,
		&task.IsStart,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &task, nil
}

func (d Database) GetResultByID(id int64) (*Result, error) {
	var r Result
	err := d.db.QueryRow("SELECT id, object_hash, task_id, input_result_id, processed FROM result WHERE id = ?", id).Scan(
		&r.ID,
		&r.ObjectHash,
		&r.TaskID,
		&r.InputResultID,
		&r.Processed,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &r, nil
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

func (d Database) InsertResult(objectHash string, taskID *int64, inputResultID *int64) (int64, bool, error) {
	res, err := d.db.Exec(`
INSERT OR IGNORE INTO result (object_hash, task_id, input_result_id, processed)
VALUES (?, ?, ?, 0);
`, objectHash, taskID, inputResultID)
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
		SELECT id FROM result 
		WHERE object_hash = ? AND task_id IS ? AND input_result_id IS ?
	`, objectHash, taskID, inputResultID).Scan(&id)
	return id, false, err
}

func (d Database) GetUnprocessedResults() ([]Result, error) {
	rows, err := d.db.Query(`
		SELECT r.id, r.object_hash, r.task_id, r.input_result_id
		FROM result r
		WHERE r.processed = 0 AND r.task_id IS NOT NULL
		ORDER BY r.id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []Result
	for rows.Next() {
		var r Result
		var inputID sql.NullInt64
		err := rows.Scan(&r.ID, &r.ObjectHash, &r.TaskID, &inputID)
		if err != nil {
			return nil, err
		}
		if inputID.Valid {
			val := inputID.Int64
			r.InputResultID = &val
		}
		results = append(results, r)
	}

	return results, rows.Err()
}

func (d Database) MarkResultProcessed(resultID int64) error {
	_, err := d.db.Exec("UPDATE result SET processed = 1 WHERE id = ?", resultID)
	return err
}

func (d Database) GetResultsByTaskName(name string, mode string) ([]Result, error) {
	var query string
	if mode == "input" {
		query = `
			SELECT r.id, r.object_hash, r.task_id, r.input_result_id, r.processed
			FROM result r
			JOIN task t ON r.task_id = t.id
			WHERE t.name = ?
			ORDER BY r.id
		`
	} else {
		// output mode - get results created by this task
		query = `
			SELECT r2.id, r2.object_hash, r2.task_id, r2.input_result_id, r2.processed
			FROM result r1
			JOIN task t ON r1.task_id = t.id
			JOIN result r2 ON r2.input_result_id = r1.id
			WHERE t.name = ?
			ORDER BY r2.id
		`
	}

	rows, err := d.db.Query(query, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []Result
	for rows.Next() {
		var r Result
		if err := rows.Scan(&r.ID, &r.ObjectHash, &r.TaskID, &r.InputResultID, &r.Processed); err != nil {
			return nil, err
		}
		results = append(results, r)
	}

	return results, rows.Err()
}

func (d Database) GetStartTask() (*Task, error) {
	var task Task
	err := d.db.QueryRow("SELECT id, name, script, is_start FROM task WHERE is_start = 1 LIMIT 1").Scan(
		&task.ID,
		&task.Name,
		&task.Script,
		&task.IsStart,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &task, nil
}
