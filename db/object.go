package db

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
)

// Objects now always live on the filesystem. Keep the legacy sentinel so Badger
// migration can detect file-backed values from older databases.
const fsSentinel = "fs"

func (d Database) objectFilePath(hash string) string {
	return filepath.Join(d.repo_path, "objects", hash[0:3], hash[3:6], hash[6:9], hash[9:])
}

func (d Database) StoreObject(hash string, data []byte) error {
	return d.storeObjectFS(hash, data)
}

func (d Database) storeObjectFS(hash string, data []byte) error {
	path := d.objectFilePath(hash)
	if _, err := os.Stat(path); err == nil {
		return nil
	}
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
	return nil
}

func (d Database) StorageBackendForSize(size int) string {
	return typesStorageBackendFS()
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
	return os.ReadFile(d.objectFilePath(hash))
}

func (d Database) ObjectExists(hash string) bool {
	_, err := os.Stat(d.objectFilePath(hash))
	return err == nil
}

func removeObjectFileIfExists(path string) error {
	err := os.Remove(path)
	if err == nil || os.IsNotExist(err) {
		return nil
	}
	return err
}
