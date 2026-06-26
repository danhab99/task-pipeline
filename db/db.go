package db

import (
	"fmt"
	"grit/log"
	"os"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

var dbLogger = log.NewLogger("DB")

type Database struct {
	repoPath       string
	stepEngineDB   *badger.DB
	taskQueueDB    *badger.DB
	resourcePoolDB *badger.DB
}

func baseBadgerOptions(path string) badger.Options {
	opts := badger.DefaultOptions(path)
	opts.Logger = nil
	opts.SyncWrites = false
	opts.BlockCacheSize = 32 << 20
	opts.NumVersionsToKeep = 1
	opts.CompactL0OnClose = false
	opts.NumLevelZeroTables = 5
	opts.NumLevelZeroTablesStall = 10
	opts.NumCompactors = 2 // Badger v4 requires at least 2 compactors.
	return opts
}

func openBadgerAt(path string, opts badger.Options) (*badger.DB, error) {
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB at %s: %w", path, err)
	}
	return db, nil
}

func NewDatabase(repo_path string) (Database, error) {
	err := os.MkdirAll(repo_path, 0755)
	if err != nil {
		return Database{}, err
	}

	stepPath := repo_path + "/db/step_engine"
	taskPath := repo_path + "/db/task_queue"
	resourcePath := repo_path + "/db/resource_pool"

	if err := os.MkdirAll(stepPath, 0755); err != nil {
		return Database{}, err
	}
	if err := os.MkdirAll(taskPath, 0755); err != nil {
		return Database{}, err
	}
	if err := os.MkdirAll(resourcePath, 0755); err != nil {
		return Database{}, err
	}

	dbLogger.Verbosef("Opening BadgerDB step_engine at %s\n", stepPath)
	stepOpts := baseBadgerOptions(stepPath)
	stepOpts.ValueThreshold = 1024
	stepOpts.MemTableSize = 32 << 20
	stepOpts.BaseTableSize = 2 << 20
	stepOpts.ValueLogFileSize = 64 << 20
	stepOpts.NumMemtables = 2
	stepEngineDB, err := openBadgerAt(stepPath, stepOpts)
	if err != nil {
		return Database{}, err
	}

	dbLogger.Verbosef("Opening BadgerDB task_queue at %s\n", taskPath)
	taskOpts := baseBadgerOptions(taskPath)
	taskOpts.ValueThreshold = 1024
	taskOpts.MemTableSize = 32 << 20
	taskOpts.BaseTableSize = 2 << 20
	taskOpts.ValueLogFileSize = 64 << 20
	taskOpts.NumMemtables = 2
	taskQueueDB, err := openBadgerAt(taskPath, taskOpts)
	if err != nil {
		_ = stepEngineDB.Close()
		return Database{}, err
	}

	dbLogger.Verbosef("Opening BadgerDB resource_pool at %s\n", resourcePath)
	resourceOpts := baseBadgerOptions(resourcePath)
	resourceOpts.ValueThreshold = 1024
	resourceOpts.MemTableSize = 64 << 20
	resourceOpts.BaseTableSize = 8 << 20
	resourceOpts.ValueLogFileSize = 512 << 20
	resourceOpts.NumMemtables = 3
	resourcePoolDB, err := openBadgerAt(resourcePath, resourceOpts)
	if err != nil {
		_ = taskQueueDB.Close()
		_ = stepEngineDB.Close()
		return Database{}, err
	}

	dbLogger.Println("Database ready")
	return Database{
		repoPath:       repo_path,
		stepEngineDB:   stepEngineDB,
		taskQueueDB:    taskQueueDB,
		resourcePoolDB: resourcePoolDB,
	}, nil
}

func (d Database) Close() error {
	if err := d.stepEngineDB.Close(); err != nil {
		return fmt.Errorf("failed to close step_engine DB: %w", err)
	}
	if err := d.taskQueueDB.Close(); err != nil {
		return fmt.Errorf("failed to close task_queue DB: %w", err)
	}
	if err := d.resourcePoolDB.Close(); err != nil {
		return fmt.Errorf("failed to close resource_pool DB: %w", err)
	}
	return nil
}
