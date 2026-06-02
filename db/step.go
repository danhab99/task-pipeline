package db

import (
	"database/sql"
	"fmt"
	"time"
)

func (d Database) CreateStep(step Step) (string, error) {
	tx, err := d.db.Begin()
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	var latest Step
	var latestParallel sql.NullInt64
	var latestTimeout sql.NullInt64
	err = tx.QueryRow(`
		SELECT id, name, script, parallel, input, timeout_ns, version
		FROM steps
		WHERE name = ?
		ORDER BY version DESC
		LIMIT 1`, step.Name).Scan(&latest.ID, &latest.Name, &latest.Script, &latestParallel, &latest.Input, &latestTimeout, &latest.Version)
	if err != nil && err != sql.ErrNoRows {
		return "", err
	}
	if err == nil {
		if latestParallel.Valid {
			parallel := int(latestParallel.Int64)
			latest.Parallel = &parallel
		}
		if latestTimeout.Valid {
			timeout := timeDurationFromNS(latestTimeout.Int64)
			latest.Timeout = &timeout
		}
		if latest.Script == step.Script && latest.Input == step.Input {
			if _, err := tx.Exec(`UPDATE steps SET parallel = ?, timeout_ns = ? WHERE id = ?`, nullableParallelValue(step.Parallel), nullableTimeoutValue(step.Timeout), latest.ID); err != nil {
				return "", err
			}
			if err := tx.Commit(); err != nil {
				return "", err
			}
			return latest.ID, nil
		}
	}

	version := latest.Version + 1
	if version == 0 {
		version = 1
	}
	step.ID = newULID()
	step.Version = version
	if _, err := tx.Exec(`INSERT INTO steps(id, name, script, parallel, input, timeout_ns, version) VALUES(?, ?, ?, ?, ?, ?, ?)`, step.ID, step.Name, step.Script, nullableParallelValue(step.Parallel), step.Input, nullableTimeoutValue(step.Timeout), step.Version); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return step.ID, nil
}

func (d Database) GetStep(id string) (*Step, error) {
	row := d.db.QueryRow(`SELECT id, name, script, parallel, input, timeout_ns, version FROM steps WHERE id = ?`, id)
	step, err := stepFromScanner(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return step, err
}

func (d Database) GetStepByName(name string) (*Step, error) {
	row := d.db.QueryRow(`
		SELECT id, name, script, parallel, input, timeout_ns, version
		FROM steps
		WHERE name = ?
		ORDER BY version DESC
		LIMIT 1`, name)
	step, err := stepFromScanner(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return step, err
}

func (d Database) GetStepsWithZeroInputs() chan Step {
	ch := make(chan Step)
	go func() {
		defer close(ch)
		lastID := ""
		for {
			rows, err := d.db.Query(`
				SELECT id, name, script, parallel, input, timeout_ns, version
				FROM steps
				WHERE input = '' AND id > ?
				ORDER BY id
				LIMIT ?`, lastID, scanBatchSize)
			if err != nil {
				dbLogger.Verbosef("Error in GetStepsWithZeroInputs: %v\n", err)
				return
			}
			batch := make([]Step, 0, scanBatchSize)
			for rows.Next() {
				step, err := stepFromScanner(rows)
				if err != nil {
					rows.Close()
					dbLogger.Verbosef("Error in GetStepsWithZeroInputs: %v\n", err)
					return
				}
				batch = append(batch, *step)
				lastID = step.ID
			}
			rows.Close()
			for _, step := range batch {
				ch <- step
			}
			if len(batch) < scanBatchSize {
				return
			}
		}
	}()
	return ch
}

func (d Database) GetStarterSteps() chan Step {
	return d.GetStepsWithZeroInputs()
}

func (d Database) ListSteps() chan Step {
	ch := make(chan Step)
	go func() {
		defer close(ch)
		lastID := ""
		for {
			rows, err := d.db.Query(`
				SELECT id, name, script, parallel, input, timeout_ns, version
				FROM steps
				WHERE id > ?
				ORDER BY id
				LIMIT ?`, lastID, scanBatchSize)
			if err != nil {
				dbLogger.Verbosef("Error in ListSteps: %v\n", err)
				return
			}
			batch := make([]Step, 0, scanBatchSize)
			for rows.Next() {
				step, err := stepFromScanner(rows)
				if err != nil {
					rows.Close()
					dbLogger.Verbosef("Error in ListSteps: %v\n", err)
					return
				}
				batch = append(batch, *step)
				lastID = step.ID
			}
			rows.Close()
			for _, step := range batch {
				ch <- step
			}
			if len(batch) < scanBatchSize {
				return
			}
		}
	}()
	return ch
}

func (d Database) CountSteps() (int64, error) {
	var count int64
	err := d.db.QueryRow(`SELECT COUNT(*) FROM steps`).Scan(&count)
	return count, err
}

func (d Database) CountStepsWithoutParallel() (int64, error) {
	var count int64
	err := d.db.QueryRow(`SELECT COUNT(*) FROM steps WHERE parallel IS NOT NULL`).Scan(&count)
	return count, err
}

func (d Database) DeleteStep(id string) error {
	for task := range d.GetTasksForStep(id) {
		if err := d.DeleteTask(task.ID); err != nil {
			return err
		}
	}
	_, err := d.db.Exec(`DELETE FROM steps WHERE id = ?`, id)
	return err
}

func (d Database) UpdateStepStatus(id string, processed bool) error {
	return nil
}

func (d Database) GetStepVersions(name string) chan Step {
	ch := make(chan Step)
	go func() {
		defer close(ch)
		lastVersion := 0
		for {
			rows, err := d.db.Query(`
				SELECT id, name, script, parallel, input, timeout_ns, version
				FROM steps
				WHERE name = ? AND version > ?
				ORDER BY version
				LIMIT ?`, name, lastVersion, scanBatchSize)
			if err != nil {
				fmt.Printf("Error in GetStepVersions: %v\n", err)
				return
			}
			batch := make([]Step, 0, scanBatchSize)
			for rows.Next() {
				step, err := stepFromScanner(rows)
				if err != nil {
					rows.Close()
					fmt.Printf("Error in GetStepVersions: %v\n", err)
					return
				}
				batch = append(batch, *step)
				lastVersion = step.Version
			}
			rows.Close()
			for _, step := range batch {
				ch <- step
			}
			if len(batch) < scanBatchSize {
				return
			}
		}
	}()
	return ch
}

func (d Database) GetTaintedSteps() chan Step {
	ch := make(chan Step)
	go func() {
		defer close(ch)
		stepsByName := make(map[string][]Step)
		lastID := ""
		for {
			rows, err := d.db.Query(`
				SELECT id, name, script, parallel, input, timeout_ns, version
				FROM steps
				WHERE id > ?
				ORDER BY id
				LIMIT ?`, lastID, scanBatchSize)
			if err != nil {
				fmt.Printf("Error in GetTaintedSteps: %v\n", err)
				return
			}
			count := 0
			for rows.Next() {
				step, err := stepFromScanner(rows)
				if err != nil {
					rows.Close()
					fmt.Printf("Error in GetTaintedSteps: %v\n", err)
					return
				}
				stepsByName[step.Name] = append(stepsByName[step.Name], *step)
				lastID = step.ID
				count++
			}
			rows.Close()
			if count < scanBatchSize {
				break
			}
		}

		for _, steps := range stepsByName {
			if len(steps) < 2 {
				continue
			}
			maxStep := steps[0]
			for _, step := range steps[1:] {
				if step.Version > maxStep.Version {
					maxStep = step
				}
			}
			for _, step := range steps {
				if step.Version < maxStep.Version && (step.Script != maxStep.Script || step.Input != maxStep.Input) {
					ch <- step
				}
			}
		}
	}()
	return ch
}

func timeDurationFromNS(value int64) time.Duration {
	return time.Duration(value)
}