package db

import (
	"database/sql"
	"fmt"
)

type TaskStatusUpdate struct {
	ID        string
	Processed bool
	Error     *string
}

func (d Database) CreateTask(task Task) (string, error) {
	id := newULID()
	task.ID = id
	processed := 0
	if task.Processed {
		processed = 1
	}
	_, err := d.db.Exec(`INSERT INTO tasks(id, step_id, input_resource_id, processed, error) VALUES(?, ?, ?, ?, ?)`, task.ID, task.StepID, nullableStringValue(task.InputResourceID), processed, nullableStringValue(task.Error))
	return id, err
}

func (d *Database) CreateAndGetTask(t Task) (*Task, error) {
	id, err := d.CreateTask(t)
	if err != nil {
		return nil, err
	}
	return d.GetTask(id)
}

func (d Database) CreateTasksFromResources(stepID string, resourceIDs []string) ([]string, error) {
	if len(resourceIDs) == 0 {
		return nil, nil
	}
	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT OR IGNORE INTO tasks(id, step_id, input_resource_id, processed, error) VALUES(?, ?, ?, 0, NULL)`)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	var ids []string
	for _, resourceID := range resourceIDs {
		taskID := newULID()
		result, err := stmt.Exec(taskID, stepID, resourceID)
		if err != nil {
			return nil, err
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return nil, err
		}
		if rowsAffected > 0 {
			ids = append(ids, taskID)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return ids, nil
}

func (d Database) GetTask(id string) (*Task, error) {
	row := d.db.QueryRow(`SELECT id, step_id, input_resource_id, processed, error FROM tasks WHERE id = ?`, id)
	task, err := taskFromScanner(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return task, err
}

func (d Database) GetTasksForStep(stepID string) chan Task {
	ch := make(chan Task)
	go func() {
		defer close(ch)
		lastID := ""
		for {
			rows, err := d.db.Query(`
				SELECT id, step_id, input_resource_id, processed, error
				FROM tasks
				WHERE step_id = ? AND id > ?
				ORDER BY id
				LIMIT ?`, stepID, lastID, scanBatchSize)
			if err != nil {
				dbLogger.Verbosef("Error querying tasks for step %s: %v\n", stepID, err)
				return
			}
			batch := make([]Task, 0, scanBatchSize)
			for rows.Next() {
				task, err := taskFromScanner(rows)
				if err != nil {
					rows.Close()
					dbLogger.Verbosef("Error querying tasks for step %s: %v\n", stepID, err)
					return
				}
				batch = append(batch, *task)
				lastID = task.ID
			}
			rows.Close()
			for _, task := range batch {
				ch <- task
			}
			if len(batch) < scanBatchSize {
				return
			}
		}
	}()
	return ch
}

func (d Database) GetUnprocessedTasks(stepID string) chan Task {
	ch := make(chan Task)
	go func() {
		defer close(ch)
		var taskCount int64
		defer func() {
			dbLogger.Verbosef("GetUnprocessedTasks(step=%s) found %d unprocessed tasks\n", stepID, taskCount)
		}()
		lastID := ""
		for {
			rows, err := d.db.Query(`
				SELECT id, step_id, input_resource_id, processed, error
				FROM tasks
				WHERE step_id = ? AND processed = 0 AND id > ?
				ORDER BY id
				LIMIT ?`, stepID, lastID, scanBatchSize)
			if err != nil {
				dbLogger.Verbosef("Error querying unprocessed tasks for step %s: %v\n", stepID, err)
				return
			}
			batch := make([]Task, 0, scanBatchSize)
			for rows.Next() {
				task, err := taskFromScanner(rows)
				if err != nil {
					rows.Close()
					dbLogger.Verbosef("Error querying unprocessed tasks for step %s: %v\n", stepID, err)
					return
				}
				batch = append(batch, *task)
				lastID = task.ID
			}
			rows.Close()
			taskCount += int64(len(batch))
			for _, task := range batch {
				ch <- task
			}
			if len(batch) < scanBatchSize {
				return
			}
		}
	}()
	return ch
}

func (d Database) GetTaskInputResource(taskID string) (*Resource, error) {
	row := d.db.QueryRow(`
		SELECT r.id, r.name, r.object_hash, r.created_at, r.created_by_task_id, r.storage_backend
		FROM tasks t
		JOIN resources r ON r.id = t.input_resource_id
		WHERE t.id = ?`, taskID)
	resource, err := resourceFromScanner(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return resource, err
}

func (d Database) UpdateTaskStatus(id string, processed bool, errorMsg *string) error {
	processedValue := 0
	if processed {
		processedValue = 1
	}
	_, err := d.db.Exec(`UPDATE tasks SET processed = ?, error = ? WHERE id = ?`, processedValue, nullableStringValue(errorMsg), id)
	return err
}

func (d Database) BatchUpdateTaskStatus(updates []TaskStatusUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`UPDATE tasks SET processed = ?, error = ? WHERE id = ?`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, update := range updates {
		processedValue := 0
		if update.Processed {
			processedValue = 1
		}
		if _, err := stmt.Exec(processedValue, nullableStringValue(update.Error), update.ID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (d Database) CountTasksForStep(stepID string) (int64, error) {
	var count int64
	err := d.db.QueryRow(`SELECT COUNT(*) FROM tasks WHERE step_id = ?`, stepID).Scan(&count)
	return count, err
}

func (d Database) CountUnprocessedTasks() (int64, error) {
	var count int64
	err := d.db.QueryRow(`SELECT COUNT(*) FROM tasks WHERE processed = 0`).Scan(&count)
	return count, err
}

func (d Database) CountUnprocessedTasksForStep(stepID string) (int64, error) {
	var count int64
	err := d.db.QueryRow(`SELECT COUNT(*) FROM tasks WHERE step_id = ? AND processed = 0`, stepID).Scan(&count)
	return count, err
}

func (d Database) GetTaskCountsForStep(stepID string) (total int64, processed int64, err error) {
	total, err = d.CountTasksForStep(stepID)
	if err != nil {
		return 0, 0, err
	}
	unprocessed, err := d.CountUnprocessedTasksForStep(stepID)
	if err != nil {
		return total, 0, err
	}
	return total, total - unprocessed, nil
}

func (d Database) DeleteTask(id string) error {
	_, err := d.db.Exec(`DELETE FROM tasks WHERE id = ?`, id)
	return err
}

func (d Database) TaskExists(id string) (bool, error) {
	var exists int
	err := d.db.QueryRow(`SELECT 1 FROM tasks WHERE id = ? LIMIT 1`, id).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

func (d Database) ListTasks() chan Task {
	ch := make(chan Task)
	go func() {
		defer close(ch)
		lastID := ""
		for {
			rows, err := d.db.Query(`
				SELECT id, step_id, input_resource_id, processed, error
				FROM tasks
				WHERE id > ?
				ORDER BY id
				LIMIT ?`, lastID, scanBatchSize)
			if err != nil {
				panic(err)
			}
			batch := make([]Task, 0, scanBatchSize)
			for rows.Next() {
				task, err := taskFromScanner(rows)
				if err != nil {
					rows.Close()
					panic(err)
				}
				batch = append(batch, *task)
				lastID = task.ID
			}
			rows.Close()
			for _, task := range batch {
				ch <- task
			}
			if len(batch) < scanBatchSize {
				return
			}
		}
	}()
	return ch
}

func (d Database) MarkStepTasksUnprocessed(stepID string) error {
	_, err := d.db.Exec(`UPDATE tasks SET processed = 0, error = NULL WHERE step_id = ?`, stepID)
	return err
}

func (d Database) MarkStepUndone(stepID string) error {
	result, err := d.db.Exec(`DELETE FROM tasks WHERE step_id = ?`, stepID)
	if err != nil {
		return err
	}
	deleted, _ := result.RowsAffected()
	dbLogger.Verbosef("Marked step %s as undone: deleted %d tasks\n", stepID, deleted)
	return nil
}

func (d Database) IsStepComplete(stepID string) (bool, error) {
	count, err := d.CountUnprocessedTasksForStep(stepID)
	if err != nil {
		return false, err
	}
	return count == 0, nil
}

func (d Database) CheckAndMarkStepComplete(stepID string) (bool, error) {
	isComplete, err := d.IsStepComplete(stepID)
	if err != nil {
		return false, err
	}
	if isComplete {
		step, err := d.GetStep(stepID)
		if err != nil {
			return false, err
		}
		if step != nil {
			dbLogger.Verbosef("Step %s (%s) marked as complete\n", stepID, step.Name)
		}
	}
	return isComplete, nil
}

func (d Database) GetPipelineStatus() (complete bool, totalTasks int64, processedTasks int64, err error) {
	err = d.db.QueryRow(`SELECT COUNT(*) FROM tasks`).Scan(&totalTasks)
	if err != nil {
		return
	}
	var unprocessed int64
	err = d.db.QueryRow(`SELECT COUNT(*) FROM tasks WHERE processed = 0`).Scan(&unprocessed)
	if err != nil {
		return
	}
	processedTasks = totalTasks - unprocessed
	complete = totalTasks > 0 && totalTasks == processedTasks
	return
}

func idxTaskByStepProcPrefix(stepID string) []byte {
	return []byte(idxTaskByStepProc + stepID + "\x00")
}

func (d Database) ScheduleTasksForStep(stepID string) (int64, error) {
	step, err := d.GetStep(stepID)
	if err != nil {
		return 0, err
	}
	if step == nil || step.Input == "" {
		dbLogger.Verbosef("Step %s (%s) has no input, skipping scheduling\n", stepID, step.Name)
		return 0, nil
	}

	dbLogger.Verbosef("Scheduling tasks for step %s (%s) with input: %s\n", stepID, step.Name, step.Input)

	const scheduleBatchSize = 5000
	var totalScheduled int64
	var cursor sql.NullString
	if err := d.db.QueryRow(`SELECT MAX(input_resource_id) FROM tasks WHERE step_id = ? AND input_resource_id IS NOT NULL`, stepID).Scan(&cursor); err != nil {
		return 0, err
	}
	cursorID := ""
	if cursor.Valid {
		cursorID = cursor.String
	}

	dbLogger.Verbosef("ScheduleTasksForStep: step=%s input=%s scanning\n", stepID, step.Input)
	for {
		rows, err := d.db.Query(`
			SELECT id
			FROM resources
			WHERE name = ? AND id > ?
			ORDER BY id
			LIMIT ?`, step.Input, cursorID, scheduleBatchSize)
		if err != nil {
			return totalScheduled, fmt.Errorf("failed to scan resources for step %s: %w", stepID, err)
		}
		batch := make([]string, 0, scheduleBatchSize)
		for rows.Next() {
			var resourceID string
			if err := rows.Scan(&resourceID); err != nil {
				rows.Close()
				return totalScheduled, fmt.Errorf("failed to scan resources for step %s: %w", stepID, err)
			}
			batch = append(batch, resourceID)
			cursorID = resourceID
		}
		rows.Close()

		dbLogger.Verbosef("ScheduleTasksForStep: step=%s input=%s scan_window=%d exhausted=%v\n", stepID, step.Input, len(batch), len(batch) < scheduleBatchSize)

		if len(batch) == 0 {
			break
		}

		tx, err := d.db.Begin()
		if err != nil {
			return totalScheduled, err
		}
		stmt, err := tx.Prepare(`INSERT OR IGNORE INTO tasks(id, step_id, input_resource_id, processed, error) VALUES(?, ?, ?, 0, NULL)`)
		if err != nil {
			_ = tx.Rollback()
			return totalScheduled, err
		}
		written := 0
		for _, resourceID := range batch {
			taskID := newULID()
			result, err := stmt.Exec(taskID, stepID, resourceID)
			if err != nil {
				stmt.Close()
				_ = tx.Rollback()
				return totalScheduled, fmt.Errorf("failed to write task batch for step %s: %w", stepID, err)
			}
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				stmt.Close()
				_ = tx.Rollback()
				return totalScheduled, fmt.Errorf("failed to write task batch for step %s: %w", stepID, err)
			}
			written += int(rowsAffected)
		}
		stmt.Close()
		if err := tx.Commit(); err != nil {
			return totalScheduled, fmt.Errorf("failed to write task batch for step %s: %w", stepID, err)
		}
		totalScheduled += int64(written)
		if len(batch) < scheduleBatchSize {
			break
		}
	}

	dbLogger.Verbosef("ScheduleTasksForStep: step=%s scheduled_total=%d\n", stepID, totalScheduled)
	return totalScheduled, nil
}