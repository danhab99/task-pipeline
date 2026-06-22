package db

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"

	badger "github.com/dgraph-io/badger/v4"
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
	var createdID string
	err := d.resourcePoolDB.Update(func(txn *badger.Txn) error {
		hashIdxKey := idxResourceHashKey(name, hash)
		existing, err := getVal(txn, hashIdxKey)
		if err != nil {
			return err
		}
		if existing != nil {
			return nil // already exists, idempotent
		}

		id := newULID()
		res := Resource{
			ID:              id,
			Name:            name,
			ObjectHash:      hash,
			CreatedAt:       nowTimestamp(),
			CreatedByTaskID: &taskID,
			StorageBackend:  backend,
		}

		if err := putEntity(txn, resourceKey(id), &res); err != nil {
			return err
		}
		if err := txn.Set(idxResourceByNameKey(name, id), nil); err != nil {
			return err
		}
		if err := txn.Set(hashIdxKey, []byte(id)); err != nil {
			return err
		}
		createdID = id
		return nil
	})
	if err != nil || createdID == "" {
		return err
	}

	return d.stepEngineDB.Update(func(txn *badger.Txn) error {
		return txn.Set(idxResourceRouteKey(name, ResourceStatusUnprocessed, createdID), nil)
	})
}
