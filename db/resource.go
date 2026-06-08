package db

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
)

type ResourceDeleteResult struct {
	ResourceID          string
	Name                string
	ObjectHash          string
	ResourceDeleted     bool
	ObjectDeleted       bool
	RemainingObjectRefs int64
}

func (d Database) CreateResource(name string, objectHash string) (string, error) {
	return d.CreateResourceWithTask(name, objectHash, nil)
}

func (d Database) CreateResourceWithTask(name string, objectHash string, createdByTaskID *string) (string, error) {
	tx, err := d.db.Begin()
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	var resourceID string
	err = tx.QueryRow(`SELECT id FROM resources WHERE name = ? AND object_hash = ?`, name, objectHash).Scan(&resourceID)
	if err == nil {
		if commitErr := tx.Commit(); commitErr != nil {
			return "", commitErr
		}
		return resourceID, nil
	}
	if err != nil && err != sql.ErrNoRows {
		return "", err
	}

	resourceID = newULID()
	if _, err := tx.Exec(`INSERT INTO resources(id, name, object_hash, created_at, created_by_task_id, storage_backend) VALUES(?, ?, ?, ?, ?, ?)`, resourceID, name, objectHash, nowTimestamp(), nullableStringValue(createdByTaskID), typesStorageBackendFS()); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return resourceID, nil
}

func (d Database) CreateResourceFromReader(name string, reader io.Reader) (string, string, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return "", "", fmt.Errorf("failed to read data: %w", err)
	}

	hasher := sha256.New()
	hasher.Write(data)
	hashBytes := hasher.Sum(nil)
	hash := hex.EncodeToString(hashBytes)

	if !d.ObjectExists(hash) {
		if err := d.StoreObject(hash, data); err != nil {
			return "", "", fmt.Errorf("failed to store object: %w", err)
		}
	}

	resourceID, err := d.CreateResource(name, hash)
	if err != nil {
		return "", "", fmt.Errorf("failed to create resource record: %w", err)
	}

	return resourceID, hash, nil
}

func (d Database) GetResource(id string) (*Resource, error) {
	row := d.db.QueryRow(`SELECT id, name, object_hash, created_at, created_by_task_id, storage_backend FROM resources WHERE id = ?`, id)
	resource, err := resourceFromScanner(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return resource, err
}

func (d Database) GetResourcesByName(name string) chan Resource {
	ch := make(chan Resource)
	go func() {
		defer close(ch)
		lastID := ""
		for {
			rows, err := d.db.Query(`
				SELECT id, name, object_hash, created_at, created_by_task_id, storage_backend
				FROM resources
			WHERE name = ? AND (? = '' OR id > ?)
			ORDER BY id ASC
				LIMIT ?`, name, lastID, lastID, scanBatchSize)
			if err != nil {
				dbLogger.Verbosef("Error querying resources by name %s: %v\n", name, err)
				return
			}
			batch := make([]Resource, 0, scanBatchSize)
			for rows.Next() {
				resource, err := resourceFromScanner(rows)
				if err != nil {
					rows.Close()
					dbLogger.Verbosef("Error scanning resources by name %s: %v\n", name, err)
					return
				}
				batch = append(batch, *resource)
				lastID = resource.ID
			}
			rows.Close()
			for _, resource := range batch {
				ch <- resource
			}
			if len(batch) < scanBatchSize {
				return
			}
		}
	}()
	return ch
}

func (d Database) GetAllResources() chan Resource {
	ch := make(chan Resource)
	go func() {
		defer close(ch)
		lastID := ""
		for {
			rows, err := d.db.Query(`
				SELECT id, name, object_hash, created_at, created_by_task_id, storage_backend
				FROM resources
				WHERE id > ?
				ORDER BY id
				LIMIT ?`, lastID, scanBatchSize)
			if err != nil {
				dbLogger.Verbosef("Error querying all resources: %v\n", err)
				return
			}
			batch := make([]Resource, 0, scanBatchSize)
			for rows.Next() {
				resource, err := resourceFromScanner(rows)
				if err != nil {
					rows.Close()
					dbLogger.Verbosef("Error scanning all resources: %v\n", err)
					return
				}
				batch = append(batch, *resource)
				lastID = resource.ID
			}
			rows.Close()
			for _, resource := range batch {
				ch <- resource
			}
			if len(batch) < scanBatchSize {
				return
			}
		}
	}()
	return ch
}

func (d Database) GetAllResourceNames() chan string {
	ch := make(chan string)
	go func() {
		defer close(ch)
		lastName := ""
		for {
			rows, err := d.db.Query(`
				SELECT DISTINCT name
				FROM resources
				WHERE name > ?
				ORDER BY name
				LIMIT ?`, lastName, scanBatchSize)
			if err != nil {
				dbLogger.Verbosef("Error querying resource names: %v\n", err)
				return
			}
			batch := make([]string, 0, scanBatchSize)
			for rows.Next() {
				var name string
				if err := rows.Scan(&name); err != nil {
					rows.Close()
					dbLogger.Verbosef("Error scanning resource names: %v\n", err)
					return
				}
				batch = append(batch, name)
				lastName = name
			}
			rows.Close()
			for _, name := range batch {
				ch <- name
			}
			if len(batch) < scanBatchSize {
				return
			}
		}
	}()
	return ch
}

func (d Database) GetUnconsumedResourcesByName(name string, consumingStepID string) chan Resource {
	ch := make(chan Resource)
	go func() {
		defer close(ch)
		lastID := ""
		for {
			rows, err := d.db.Query(`
				SELECT r.id, r.name, r.object_hash, r.created_at, r.created_by_task_id, r.storage_backend
				FROM resources r
				WHERE r.name = ?
			  AND (? = '' OR r.id > ?)
			  AND NOT EXISTS (
				SELECT 1 FROM tasks t WHERE t.step_id = ? AND t.input_resource_id = r.id
			  )
			ORDER BY r.id ASC
				LIMIT ?`, name, lastID, lastID, consumingStepID, scanBatchSize)
			if err != nil {
				dbLogger.Verbosef("Error querying unconsumed resources for name %s, step %s: %v\n", name, consumingStepID, err)
				return
			}
			batch := make([]Resource, 0, scanBatchSize)
			for rows.Next() {
				resource, err := resourceFromScanner(rows)
				if err != nil {
					rows.Close()
					dbLogger.Verbosef("Error scanning unconsumed resources for name %s, step %s: %v\n", name, consumingStepID, err)
					return
				}
				batch = append(batch, *resource)
				lastID = resource.ID
			}
			rows.Close()
			for _, resource := range batch {
				ch <- resource
			}
			if len(batch) < scanBatchSize {
				return
			}
		}
	}()
	return ch
}

func (d Database) CountResources() (int64, error) {
	var count int64
	err := d.db.QueryRow(`SELECT COUNT(*) FROM resources`).Scan(&count)
	return count, err
}

func (d Database) DeleteResource(id string) error {
	_, err := d.db.Exec(`DELETE FROM resources WHERE id = ?`, id)
	return err
}

func (d Database) DeleteResourceHard(id string) (ResourceDeleteResult, error) {
	result := ResourceDeleteResult{ResourceID: id}
	tx, err := d.db.Begin()
	if err != nil {
		return result, err
	}
	defer tx.Rollback()

	row := tx.QueryRow(`SELECT id, name, object_hash, created_at, created_by_task_id, storage_backend FROM resources WHERE id = ?`, id)
	resource, err := resourceFromScanner(row)
	if err == sql.ErrNoRows {
		if commitErr := tx.Commit(); commitErr != nil {
			return result, commitErr
		}
		return result, nil
	}
	if err != nil {
		return result, err
	}

	result.Name = resource.Name
	result.ObjectHash = resource.ObjectHash
	if _, err := tx.Exec(`DELETE FROM resources WHERE id = ?`, id); err != nil {
		return result, err
	}
	result.ResourceDeleted = true

	if err := tx.QueryRow(`SELECT COUNT(*) FROM resources WHERE object_hash = ?`, resource.ObjectHash).Scan(&result.RemainingObjectRefs); err != nil {
		return result, err
	}
	if err := tx.Commit(); err != nil {
		return result, err
	}

	if result.RemainingObjectRefs == 0 {
		if err := removeObjectFileIfExists(d.objectFilePath(resource.ObjectHash)); err != nil {
			return result, err
		}
		result.ObjectDeleted = true
	}

	return result, nil
}

func countResourcesByObjectHashTxn(txn *sql.Tx, objectHash string) (int64, error) {
	var count int64
	err := txn.QueryRow(`SELECT COUNT(*) FROM resources WHERE object_hash = ?`, objectHash).Scan(&count)
	return count, err
}
