package db

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
)

// IngestFile reads a file from disk, hashes it, routes blob storage by size,
// and creates a Resource record in BadgerDB. Idempotent: duplicate (name, hash)
// pairs are silently skipped.
func (d *Database) IngestFile(path, name, taskID string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read output file %s: %w", path, err)
	}

	h := sha256.Sum256(data)
	hash := hex.EncodeToString(h[:])

	if err := d.StoreObject(hash, data); err != nil {
		return fmt.Errorf("failed to store object for %s: %w", name, err)
	}

	backend := d.StorageBackendForSize(len(data))
	return d.insertResource(name, hash, taskID, backend)
}

func (d *Database) insertResource(name, hash, taskID, backend string) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var existingID string
	err = tx.QueryRow(`SELECT id FROM resources WHERE name = ? AND object_hash = ?`, name, hash).Scan(&existingID)
	if err == nil {
		return tx.Commit()
	}
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	resourceID := newULID()
	createdByTaskID := any(nil)
	if taskID != "" {
		createdByTaskID = taskID
	}
	if _, err := tx.Exec(`INSERT INTO resources(id, name, object_hash, created_at, created_by_task_id, storage_backend) VALUES(?, ?, ?, ?, ?, ?)`, resourceID, name, hash, nowTimestamp(), createdByTaskID, backend); err != nil {
		return err
	}
	return tx.Commit()
}
