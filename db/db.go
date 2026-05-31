package db

import (
	"fmt"
	"grit/log"
	"os"
	"runtime"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

var dbLogger = log.NewLogger("DB")

func NewDatabase(repo_path string) (Database, error) {
	err := os.MkdirAll(repo_path, 0755)
	if err != nil {
		return Database{}, err
	}

	if err := os.MkdirAll(repo_path+"/objects", 0755); err != nil {
		return Database{}, err
	}

	dbLogger.Verbosef("Opening BadgerDB at %s\n", repo_path)
	badgerOpts := badger.DefaultOptions(repo_path + "/db")
	badgerOpts.Logger = nil

	// Keep writes durable but light on memory.
	badgerOpts.SyncWrites = false
	badgerOpts.NumVersionsToKeep = 1
	badgerOpts.CompactL0OnClose = false

	// Memory footprint: default block cache is 256 MB (z.Calloc off-heap).
	// Reduce to 32 MB; we don't need a large read cache for a write-heavy pipeline.
	badgerOpts.BlockCacheSize = 32 << 20

	// Memtable: default is 64 MB. Keep it at 32 MB × 2 = 64 MB max instead of
	// the previous 128 MB × 3 = 384 MB.
	badgerOpts.MemTableSize = 32 << 20
	badgerOpts.NumMemtables = 2

	// Vlog: limit file size so dead space is reclaimed more frequently.
	badgerOpts.ValueLogFileSize = 64 << 20
	badgerOpts.NumLevelZeroTables = 5
	badgerOpts.NumLevelZeroTablesStall = 10
	badgerOpts.ValueThreshold = 1024
	// Minimum allowed by badger is 2; keep at minimum to halve parallelism vs default.
	badgerOpts.NumCompactors = 2
	// Smaller SST target size → each table.Builder holds smaller z.Allocator buffers.
	// Default is 2MB; 512KB keeps compaction peak memory proportionally lower.
	badgerOpts.BaseTableSize = 512 << 10

	badgerDB, err := badger.Open(badgerOpts)
	if err != nil {
		return Database{}, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	dbLogger.Println("Flattening database")
	badgerDB.Flatten(max(2, runtime.NumCPU()-2))

	dbLogger.Println("Database ready")
	return Database{repo_path, badgerDB}, nil
}

func (d Database) Close() error {
	if err := d.badgerDB.Close(); err != nil {
		return fmt.Errorf("failed to close BadgerDB: %w", err)
	}
	return nil
}

// ForceSaveWAL is a no-op. BadgerDB handles compaction automatically.
func (d Database) ForceSaveWAL() error {
	return nil
}

// StartValueLogGC runs badger's value-log garbage collector in a background
// goroutine. It fires every interval and keeps running until the stop channel
// is closed. Call this once after opening the database.
//
// Without periodic GC, vlog files accumulate dead versions from overwritten
// keys (task status updates, etc.) and stay mmap'd, growing RSS without bound.
func (d Database) StartValueLogGC(interval time.Duration, stop <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// Reclaim vlogs where >30% of space is dead.
				for {
					if err := d.badgerDB.RunValueLogGC(0.3); err != nil {
						break // ErrNoRewrite means nothing left to reclaim
					}
				}
				dbLogger.Verbosef("Value log GC complete\n")
			case <-stop:
				return
			}
		}
	}()
}
