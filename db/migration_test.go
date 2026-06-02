package db

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
)

func TestNewDatabaseMigratesLegacyBadgerMetadata(t *testing.T) {
	tmp := t.TempDir()
	legacy, err := badger.Open(badger.DefaultOptions(tmp + "/db").WithLogger(nil))
	if err != nil {
		t.Fatalf("open legacy badger: %v", err)
	}

	stepID := newULID()
	resourceID := newULID()
	taskID := newULID()
	payload := []byte("legacy inline object")
	hashBytes := sha256Bytes(payload)
	hash := hex.EncodeToString(hashBytes)
	createdByTaskID := taskID
	inputResourceID := resourceID
	step := Step{ID: stepID, Name: "legacy-step", Script: "echo hi", Input: "legacy-input", Version: 1}
	resource := Resource{ID: resourceID, Name: "legacy-input", ObjectHash: hash, CreatedAt: nowTimestamp(), CreatedByTaskID: &createdByTaskID}
	task := Task{ID: taskID, StepID: stepID, InputResourceID: &inputResourceID, Processed: false}

	err = legacy.Update(func(txn *badger.Txn) error {
		if err := putEntity(txn, stepKey(stepID), &step); err != nil {
			return err
		}
		if err := putEntity(txn, resourceKey(resourceID), &resource); err != nil {
			return err
		}
		if err := putEntity(txn, taskKey(taskID), &task); err != nil {
			return err
		}
		if err := txn.Set(objectKey(hashBytes), payload); err != nil {
			return err
		}
		return txn.Set(metaCsvHashKey("legacy.csv"), []byte("hash-value"))
	})
	if err != nil {
		legacy.Close()
		t.Fatalf("seed legacy badger: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy badger: %v", err)
	}

	database, err := NewDatabase(tmp)
	if err != nil {
		t.Fatalf("NewDatabase() migrate legacy badger: %v", err)
	}
	defer database.Close()

	migratedStep, err := database.GetStep(stepID)
	if err != nil || migratedStep == nil || migratedStep.Name != step.Name {
		t.Fatalf("GetStep() = %#v, %v", migratedStep, err)
	}
	migratedTask, err := database.GetTask(taskID)
	if err != nil || migratedTask == nil || migratedTask.StepID != stepID {
		t.Fatalf("GetTask() = %#v, %v", migratedTask, err)
	}
	migratedResource, err := database.GetResource(resourceID)
	if err != nil || migratedResource == nil || migratedResource.ObjectHash != hash {
		t.Fatalf("GetResource() = %#v, %v", migratedResource, err)
	}
	if migratedResource.StorageBackend != typesStorageBackendFS() {
		t.Fatalf("expected migrated resource backend fs, got %q", migratedResource.StorageBackend)
	}
	objectData, err := database.GetObject(hash)
	if err != nil {
		t.Fatalf("GetObject() error = %v", err)
	}
	if !bytes.Equal(objectData, payload) {
		t.Fatalf("migrated object mismatch: got %q want %q", objectData, payload)
	}

	resources := database.GetResourcesByName("legacy-input")
	resourceFromChannel, ok := <-resources
	if !ok {
		t.Fatalf("expected migrated resource from channel getter")
	}
	if resourceFromChannel.ID != resourceID {
		t.Fatalf("unexpected resource from channel getter: %#v", resourceFromChannel)
	}
	if _, ok := <-resources; ok {
		t.Fatalf("expected exactly one migrated resource")
	}

	taskInputResource, err := database.GetTaskInputResource(taskID)
	if err != nil || taskInputResource == nil || taskInputResource.ID != resourceID {
		t.Fatalf("GetTaskInputResource() = %#v, %v", taskInputResource, err)
	}

	storedHash, err := database.getCsvFileHash("legacy.csv")
	if err != nil || storedHash != "hash-value" {
		t.Fatalf("getCsvFileHash() = %q, %v", storedHash, err)
	}
}

func sha256Bytes(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}