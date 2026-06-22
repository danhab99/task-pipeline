package db

import (
	"bytes"
	"testing"

	"grit/types"

	badger "github.com/dgraph-io/badger/v4"
)

func TestResourceRouterSchedulesByTypedUnprocessedPrefix(t *testing.T) {
	database, err := NewDatabase(t.TempDir())
	if err != nil {
		t.Fatalf("NewDatabase() error = %v", err)
	}
	defer database.Close()

	_, _, err = database.CreateResourceFromReader("html", bytes.NewReader([]byte("<html>ok</html>")))
	if err != nil {
		t.Fatalf("CreateResourceFromReader() error = %v", err)
	}

	stepID, err := database.CreateStep(types.Step{
		Name:   "extract-links",
		Script: "true",
		Input:  "html",
	})
	if err != nil {
		t.Fatalf("CreateStep() error = %v", err)
	}

	scheduled, err := database.ScheduleTasksForStep(stepID)
	if err != nil {
		t.Fatalf("ScheduleTasksForStep() error = %v", err)
	}
	if scheduled != 1 {
		t.Fatalf("expected 1 scheduled task, got %d", scheduled)
	}
}

func TestResourceRouterKeysCreatedAndDeleted(t *testing.T) {
	database, err := NewDatabase(t.TempDir())
	if err != nil {
		t.Fatalf("NewDatabase() error = %v", err)
	}
	defer database.Close()

	resourceID, _, err := database.CreateResourceFromReader("careerpage", bytes.NewReader([]byte("body")))
	if err != nil {
		t.Fatalf("CreateResourceFromReader() error = %v", err)
	}

	var routerCount int64
	err = database.stepEngineDB.View(func(txn *badger.Txn) error {
		routerCount, err = prefixCount(txn, idxResourceRoutePrefix("careerpage", ResourceStatusUnprocessed))
		return err
	})
	if err != nil {
		t.Fatalf("prefixCount() error = %v", err)
	}
	if routerCount != 1 {
		t.Fatalf("expected 1 unprocessed router key, got %d", routerCount)
	}

	if err := database.DeleteResource(resourceID); err != nil {
		t.Fatalf("DeleteResource() error = %v", err)
	}

	err = database.stepEngineDB.View(func(txn *badger.Txn) error {
		routerCount, err = prefixCount(txn, idxResourceRoutePrefix("careerpage", ResourceStatusUnprocessed))
		return err
	})
	if err != nil {
		t.Fatalf("prefixCount() after delete error = %v", err)
	}
	if routerCount != 0 {
		t.Fatalf("expected 0 unprocessed router keys after delete, got %d", routerCount)
	}
}
