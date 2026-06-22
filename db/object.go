package db

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	badger "github.com/dgraph-io/badger/v4"
)

// fsObjectThreshold is the size above which blobs are stored on the filesystem
// rather than inline in BadgerDB. 64 KiB keeps small values (domain names,
// short text) in the LSM tree while shunting large blobs (HTML, images) to disk.
const fsObjectThreshold = 64 * 1024

// fsSentinel is stored as the BadgerDB value for objects routed to the filesystem.
const fsSentinel = "fs"

func (d Database) objectFilePath(hash string) string {
	return filepath.Join(d.repoPath, "objects", hash[0:3], hash[3:6], hash[6:9], hash[9:])
}

func (d Database) StoreObject(hash string, data []byte) error {
	if len(data) >= fsObjectThreshold {
		return d.storeObjectFS(hash, data)
	}
	return d.storeObjectBadger(hash, data)
}

func (d Database) storeObjectBadger(hash string, data []byte) error {
	hashBytes, err := hex.DecodeString(hash)
	if err != nil {
		return err
	}
	wb := d.resourcePoolDB.NewWriteBatch()
	defer wb.Cancel()
	if err := wb.Set(objectKey(hashBytes), data); err != nil {
		return err
	}
	return wb.Flush()
}

func (d Database) storeObjectFS(hash string, data []byte) error {
	path := d.objectFilePath(hash)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("failed to create object dir: %w", err)
	}
	// Write to temp file then rename for atomicity.
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0444); err != nil {
		return fmt.Errorf("failed to write object file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("failed to rename object file: %w", err)
	}
	// Record sentinel in BadgerDB so we know where to fetch from.
	hashBytes, err := hex.DecodeString(hash)
	if err != nil {
		return err
	}
	wb := d.resourcePoolDB.NewWriteBatch()
	defer wb.Cancel()
	if err := wb.Set(objectKey(hashBytes), []byte(fsSentinel)); err != nil {
		return err
	}
	return wb.Flush()
}

func (d Database) StorageBackendForSize(size int) string {
	if size >= fsObjectThreshold {
		return "fs"
	}
	return "inline"
}

func (d Database) StoreObjectAndGetHash(data []byte) (string, error) {
	h := sha256.Sum256(data)
	hashStr := hex.EncodeToString(h[:])
	if err := d.StoreObject(hashStr, data); err != nil {
		return "", err
	}
	return hashStr, nil
}

func (d Database) GetObject(hash string) ([]byte, error) {
	hashBytes, err := hex.DecodeString(hash)
	if err != nil {
		return nil, err
	}
	var val []byte
	err = d.resourcePoolDB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(objectKey(hashBytes))
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, err
	}
	if string(val) == fsSentinel {
		return os.ReadFile(d.objectFilePath(hash))
	}
	return val, nil
}

func (d Database) ObjectExists(hash string) bool {
	hashBytes, err := hex.DecodeString(hash)
	if err != nil {
		return false
	}
	err = d.resourcePoolDB.View(func(txn *badger.Txn) error {
		_, err := txn.Get(objectKey(hashBytes))
		return err
	})
	return err == nil
}

func removeObjectFileIfExists(path string) error {
	err := os.Remove(path)
	if err == nil || os.IsNotExist(err) {
		return nil
	}
	return err
}
