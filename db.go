package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
)

const schema string = `
CREATE TABLE IF NOT EXISTS task(
  id     INTEGER PRIMARY KEY AUTOINCREMENT,
	name   TEXT UNIQUE,
	script TEXT,
);

CREATE TABLE IF NOT EXISTS step (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
	object_hash VARCHAR(64) UNIQUE,
	task        INTEGER,

	FOREIGN KEY(task)      REFERENCES task(id)
	FOREIGN KEY(next_step) REFERENCES step(id)
);

CREATE TABLE IF NOT EXISTS step_link (
  from_step_id INTEGER NOT NULL,
  to_step_id   INTEGER NOT NULL,

  PRIMARY KEY (from_step_id, to_step_id),

  FOREIGN KEY (from_step_id) REFERENCES step(id),
  FOREIGN KEY (to_step_id)   REFERENCES step(id)
);

CREATE INDEX IF NOT EXISTS idx_task_name ON task(name);
CREATE INDEX IF NOT EXISTS idx_step_task ON step(task);
CREATE INDEX IF NOT EXISTS idx_step_link_from ON step_link(from_step_id);
`

type Database struct {
	db        *sql.DB
	repo_path string
	bus       map[string]chan Step
}

func NewDatabase(repo_path string) (Database, error) {
	err := os.MkdirAll(repo_path, os.ModeDir)
	if err != nil {
		return Database{}, err
	}

	db, err := sql.Open("sqlite", fmt.Sprintf("%s/db", repo_path))
	if err != nil {
		log.Fatal(err)
	}

	db.Exec("PRAGMA journal_mode=WAL;")
	db.Exec("PRAGMA synchronous=NORMAL;")
	db.Exec("PRAGMA foreign_keys=ON;")

	_, err = db.Exec(schema)
	if err != nil {
		return Database{}, err
	}

	return Database{db, repo_path, make(map[string]chan Step)}, nil
}

func (d Database) RegisterTask(name string, script string) {
	d.db.Exec(`
INSERT INTO task (name, script)
VALUES ('?1', '?2')
ON CONFLICT(name)
DO NOTHING;
`)
}

func (d Database) getObjectPath(h string) string {
	dir := fmt.Sprintf(
		"%s/objects/%s/%s",
		d.repo_path, h[0:2], h[2:4],
	)

	err := os.MkdirAll(dir, os.ModeDir)
	if err != nil {
		panic(err)
	}

	return fmt.Sprintf("%s/%s", dir, h[4:])
}

type Task struct {
	ID     int64
	Name   string
	Script sql.NullString
}

type Step struct {
	ID         int64
	TaskID     int64
	ObjectHash sql.NullString
	Object     []byte

	PrevStep *Step
	Task     *Task
}

func (d Database) IterateTasks(name string) (chan Step, error) {
	rows, err := d.db.Query(`
SELECT
  s.id,
  s.object_hash,
  s.task,

  ps.id,
  ps.object_hash,

  t.id,
  t.name,
  t.script
FROM step s
JOIN task t ON t.id = s.task
LEFT JOIN step_link sl_prev
  ON sl_prev.to_step_id = s.id
LEFT JOIN step ps
  ON ps.id = sl_prev.from_step_id
WHERE t.name = ?
  AND NOT EXISTS (
    SELECT 1
    FROM step_link sl
    WHERE sl.from_step_id = s.id
  );
`, name)

	if err != nil {
		return nil, err
	}

	out := make(chan Step)
	d.bus[name] = out

	go func() {
		defer close(out)
		defer close(d.bus[name])
		defer rows.Close()

		for rows.Next() {

			select {
			case x := <-d.bus[name]:
				out <- x
			default:
			}

			var (
				step Step

				prevID   sql.NullInt64
				prevHash sql.NullString

				task Task
			)

			if err := rows.Scan(
				&step.ID,
				&step.ObjectHash,
				&step.TaskID,

				&prevID,
				&prevHash,

				&task.ID,
				&task.Name,
				&task.Script,
			); err != nil {
				panic(err)
			}

			step.Task = &task

			if step.ObjectHash.Valid {
				obj, err := os.ReadFile(d.getObjectPath(step.ObjectHash.String))
				if err != nil {
					panic(err)
				}
				step.Object = obj
			}

			if prevID.Valid {
				prev := &Step{
					ID:         prevID.Int64,
					ObjectHash: prevHash,
				}

				if prev.ObjectHash.Valid {
					obj, err := os.ReadFile(d.getObjectPath(prev.ObjectHash.String))
					if err != nil {
						panic(err)
					}
					prev.Object = obj
				}

				step.PrevStep = prev
			}

			out <- step
		}

		if err := rows.Err(); err != nil {
			panic(err)
		}
	}()

	return out, nil
}

func hash(body []byte) string {
	b := sha256.Sum256(body)
	return hex.EncodeToString(b[:])
}

func (d Database) InsertStep(s Step) error {
	hash := hash(s.Object)

	p := d.getObjectPath(hash)
	err := os.WriteFile(p, s.Object, os.ModeAppend)
	if err != nil {
		return err
	}

	_, err = d.db.Exec(`
INSERT INTO step (hash, task)
VALUES ('?1', '?2');
`, hash, s.TaskID)

	d.bus[s.Task.Name] <- s

	return err
}
