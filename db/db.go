package db

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"grit/log"
	"os"
	"path/filepath"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	_ "modernc.org/sqlite"
)

var dbLogger = log.NewLogger("DB")

const sqliteFileName = "metadata.sqlite"

type rowScanner interface {
	Scan(dest ...any) error
}

func NewDatabase(repo_path string) (Database, error) {
	err := os.MkdirAll(repo_path, 0755)
	if err != nil {
		return Database{}, err
	}

	if err := os.MkdirAll(repo_path+"/objects", 0755); err != nil {
		return Database{}, err
	}

	sqliteDB, err := sql.Open("sqlite", sqlitePath(repo_path))
	if err != nil {
		return Database{}, fmt.Errorf("failed to open SQLite database: %w", err)
	}
	if err := configureSQLite(sqliteDB); err != nil {
		_ = sqliteDB.Close()
		return Database{}, err
	}
	if err := initSQLiteSchema(sqliteDB); err != nil {
		_ = sqliteDB.Close()
		return Database{}, err
	}

	database := Database{repo_path: repo_path, db: sqliteDB}
	if err := database.ensureMigrated(); err != nil {
		_ = sqliteDB.Close()
		return Database{}, err
	}

	dbLogger.Println("Database ready")
	return database, nil
}

func (d Database) Close() error {
	if d.badgerDB != nil {
		if err := d.badgerDB.Close(); err != nil {
			return fmt.Errorf("failed to close BadgerDB: %w", err)
		}
	}
	if d.db != nil {
		if err := d.db.Close(); err != nil {
			return fmt.Errorf("failed to close SQLite DB: %w", err)
		}
	}
	return nil
}

// ForceSaveWAL checkpoints and truncates SQLite's WAL so long scans do not leave
// stale pages on disk across command boundaries.
func (d Database) ForceSaveWAL() error {
	if d.db == nil {
		return nil
	}
	_, err := d.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
	return err
}

// StartValueLogGC now periodically checkpoints SQLite WAL during long-running
// pipeline execution. The name stays for API compatibility.
func (d Database) StartValueLogGC(interval time.Duration, stop <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if d.db != nil {
					if _, err := d.db.Exec(`PRAGMA wal_checkpoint(PASSIVE)`); err == nil {
						dbLogger.Verbosef("SQLite WAL checkpoint complete\n")
					}
				}
			case <-stop:
				return
			}
		}
	}()
}

func (d *Database) ensureMigrated() error {
	complete, err := getMetaValue(d.db, "migration_complete")
	if err != nil {
		return fmt.Errorf("failed to read migration state: %w", err)
	}
	if complete == "1" {
		return nil
	}

	badgerPath := d.repo_path + "/db"
	if hasBadgerData(badgerPath) {
		if err := d.MigrateFromBadger(); err != nil {
			return err
		}
		return nil
	}

	return setMetaValue(d.db, "migration_complete", "1")
}

func (d *Database) openMigrationBadger() (*badger.DB, error) {
	badgerPath := d.repo_path + "/db"
	if !hasBadgerData(badgerPath) {
		return nil, nil
	}
	options := badger.DefaultOptions(badgerPath)
	options.Logger = nil
	options.ReadOnly = true
	options.BypassLockGuard = true
	return badger.Open(options)
}

func sqlitePath(repoPath string) string {
	return filepath.Join(repoPath, sqliteFileName)
}

func configureSQLite(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA wal_autocheckpoint=4096",
		"PRAGMA journal_size_limit=268435456",
		"PRAGMA temp_store=MEMORY",
		"PRAGMA cache_size=-262144",
		"PRAGMA busy_timeout=5000",
		"PRAGMA foreign_keys=OFF",
	}
	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			return fmt.Errorf("failed to apply %q: %w", pragma, err)
		}
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	return nil
}

func initSQLiteSchema(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS steps (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			script TEXT NOT NULL,
			parallel INTEGER,
			input TEXT NOT NULL DEFAULT '',
			timeout_ns INTEGER,
			version INTEGER NOT NULL
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_steps_name_version ON steps(name, version)`,
		`CREATE INDEX IF NOT EXISTS idx_steps_name_latest ON steps(name, version DESC, id DESC)`,
		`CREATE TABLE IF NOT EXISTS tasks (
			id TEXT PRIMARY KEY,
			step_id TEXT NOT NULL,
			input_resource_id TEXT,
			processed INTEGER NOT NULL DEFAULT 0,
			error TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_step_all ON tasks(step_id, id)`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_step_processed ON tasks(step_id, processed, id)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_step_resource_unique ON tasks(step_id, input_resource_id) WHERE input_resource_id IS NOT NULL`,
		`CREATE TABLE IF NOT EXISTS resources (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			object_hash TEXT NOT NULL,
			created_at TEXT NOT NULL,
			created_by_task_id TEXT,
			storage_backend TEXT NOT NULL DEFAULT 'fs'
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_resources_name_hash ON resources(name, object_hash)`,
		`CREATE INDEX IF NOT EXISTS idx_resources_name_id ON resources(name, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_resources_hash ON resources(object_hash)`,
		`CREATE TABLE IF NOT EXISTS meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("failed to initialize sqlite schema: %w", err)
		}
	}
	return nil
}

func stepFromScanner(scanner rowScanner) (*Step, error) {
	var step Step
	var parallel sql.NullInt64
	var input string
	var timeoutNS sql.NullInt64
	if err := scanner.Scan(&step.ID, &step.Name, &step.Script, &parallel, &input, &timeoutNS, &step.Version); err != nil {
		return nil, err
	}
	if parallel.Valid {
		value := int(parallel.Int64)
		step.Parallel = &value
	}
	step.Input = input
	if timeoutNS.Valid {
		duration := time.Duration(timeoutNS.Int64)
		step.Timeout = &duration
	}
	return &step, nil
}

func taskFromScanner(scanner rowScanner) (*Task, error) {
	var task Task
	var inputResourceID sql.NullString
	var processed int64
	var errMsg sql.NullString
	if err := scanner.Scan(&task.ID, &task.StepID, &inputResourceID, &processed, &errMsg); err != nil {
		return nil, err
	}
	task.InputResourceID = nullableStringPtr(inputResourceID)
	task.Processed = processed != 0
	task.Error = nullableStringPtr(errMsg)
	return &task, nil
}

func resourceFromScanner(scanner rowScanner) (*Resource, error) {
	var resource Resource
	var createdByTaskID sql.NullString
	if err := scanner.Scan(&resource.ID, &resource.Name, &resource.ObjectHash, &resource.CreatedAt, &createdByTaskID, &resource.StorageBackend); err != nil {
		return nil, err
	}
	resource.CreatedByTaskID = nullableStringPtr(createdByTaskID)
	if resource.StorageBackend == "" {
		resource.StorageBackend = typesStorageBackendFS()
	}
	return &resource, nil
}

func nullableStringPtr(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	str := value.String
	return &str
}

func nullableStringValue(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableParallelValue(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableTimeoutValue(value *time.Duration) any {
	if value == nil {
		return nil
	}
	return int64(*value)
}

func setMetaValue(db *sql.DB, key, value string) error {
	_, err := db.Exec(`INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	return err
}

func getMetaValue(db *sql.DB, key string) (string, error) {
	var value string
	err := db.QueryRow(`SELECT value FROM meta WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return value, err
}

func deleteMetaValue(db *sql.DB, key string) error {
	_, err := db.Exec(`DELETE FROM meta WHERE key = ?`, key)
	return err
}

func hasBadgerData(path string) bool {
	entries, err := os.ReadDir(path)
	if err != nil {
		return false
	}
	return len(entries) > 0
}

func typesStorageBackendFS() string {
	return "fs"
}

func (d *Database) MigrateFromBadger() error {
	source, err := d.openMigrationBadger()
	if err != nil {
		return fmt.Errorf("failed to open legacy Badger database: %w", err)
	}
	if source == nil {
		return setMetaValue(d.db, "migration_complete", "1")
	}
	defer source.Close()

	dbLogger.Printf("Migrating legacy Badger metadata to SQLite at %s\n", sqlitePath(d.repo_path))

	if err := d.resetSQLiteForMigration(); err != nil {
		return err
	}
	if err := d.migrateBadgerObjects(source); err != nil {
		return err
	}
	if err := d.migrateBadgerSteps(source); err != nil {
		return err
	}
	if err := d.migrateBadgerTasks(source); err != nil {
		return err
	}
	if err := d.migrateBadgerResources(source); err != nil {
		return err
	}
	if err := d.migrateBadgerMeta(source); err != nil {
		return err
	}

	if err := setMetaValue(d.db, "migration_complete", "1"); err != nil {
		return fmt.Errorf("failed to finalize migration state: %w", err)
	}

	if _, err := d.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		return fmt.Errorf("failed to checkpoint migrated SQLite database: %w", err)
	}

	dbLogger.Println("Legacy Badger migration complete")
	return nil
}

func (d *Database) resetSQLiteForMigration() error {
	stmts := []string{
		`DELETE FROM meta`,
		`DELETE FROM tasks`,
		`DELETE FROM resources`,
		`DELETE FROM steps`,
	}
	for _, stmt := range stmts {
		if _, err := d.db.Exec(stmt); err != nil {
			return fmt.Errorf("failed to reset sqlite metadata: %w", err)
		}
	}
	return nil
}

func (d *Database) migrateBadgerObjects(source *badger.DB) error {
	return source.View(func(txn *badger.Txn) error {
		return prefixScan(txn, []byte(prefixObject), func(key, val []byte) (bool, error) {
			hash := hex.EncodeToString(key[len(prefixObject):])
			if string(val) == fsSentinel {
				if _, err := os.Stat(d.objectFilePath(hash)); err != nil {
					return false, fmt.Errorf("legacy object file missing for %s: %w", hash, err)
				}
				return true, nil
			}
			if err := d.storeObjectFS(hash, val); err != nil {
				return false, fmt.Errorf("failed to migrate inline object %s: %w", hash, err)
			}
			return true, nil
		})
	})
}

func (d *Database) migrateBadgerSteps(source *badger.DB) error {
	var batch []Step
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		tx, err := d.db.Begin()
		if err != nil {
			return err
		}
		stmt, err := tx.Prepare(`INSERT INTO steps(id, name, script, parallel, input, timeout_ns, version) VALUES(?, ?, ?, ?, ?, ?, ?)`)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		defer stmt.Close()
		for _, step := range batch {
			if _, err := stmt.Exec(step.ID, step.Name, step.Script, nullableParallelValue(step.Parallel), step.Input, nullableTimeoutValue(step.Timeout), step.Version); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}
	if err := source.View(func(txn *badger.Txn) error {
		return prefixScan(txn, []byte(prefixStep), func(_ []byte, val []byte) (bool, error) {
			var step Step
			if err := decode(val, &step); err != nil {
				return false, err
			}
			batch = append(batch, step)
			if len(batch) >= writeBatchSize {
				return true, flush()
			}
			return true, nil
		})
	}); err != nil {
		return fmt.Errorf("failed migrating steps: %w", err)
	}
	return flush()
}

func (d *Database) migrateBadgerTasks(source *badger.DB) error {
	var batch []Task
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		tx, err := d.db.Begin()
		if err != nil {
			return err
		}
		stmt, err := tx.Prepare(`INSERT INTO tasks(id, step_id, input_resource_id, processed, error) VALUES(?, ?, ?, ?, ?)`)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		defer stmt.Close()
		for _, task := range batch {
			processed := 0
			if task.Processed {
				processed = 1
			}
			if _, err := stmt.Exec(task.ID, task.StepID, nullableStringValue(task.InputResourceID), processed, nullableStringValue(task.Error)); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}
	if err := source.View(func(txn *badger.Txn) error {
		return prefixScan(txn, []byte(prefixTask), func(_ []byte, val []byte) (bool, error) {
			var task Task
			if err := decode(val, &task); err != nil {
				return false, err
			}
			batch = append(batch, task)
			if len(batch) >= writeBatchSize {
				return true, flush()
			}
			return true, nil
		})
	}); err != nil {
		return fmt.Errorf("failed migrating tasks: %w", err)
	}
	return flush()
}

func (d *Database) migrateBadgerResources(source *badger.DB) error {
	var batch []Resource
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		tx, err := d.db.Begin()
		if err != nil {
			return err
		}
		stmt, err := tx.Prepare(`INSERT INTO resources(id, name, object_hash, created_at, created_by_task_id, storage_backend) VALUES(?, ?, ?, ?, ?, ?)`)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		defer stmt.Close()
		for _, resource := range batch {
			if _, err := stmt.Exec(resource.ID, resource.Name, resource.ObjectHash, resource.CreatedAt, nullableStringValue(resource.CreatedByTaskID), typesStorageBackendFS()); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}
	if err := source.View(func(txn *badger.Txn) error {
		return prefixScan(txn, []byte(prefixResource), func(_ []byte, val []byte) (bool, error) {
			var resource Resource
			if err := decode(val, &resource); err != nil {
				return false, err
			}
			batch = append(batch, resource)
			if len(batch) >= writeBatchSize {
				return true, flush()
			}
			return true, nil
		})
	}); err != nil {
		return fmt.Errorf("failed migrating resources: %w", err)
	}
	return flush()
}

func (d *Database) migrateBadgerMeta(source *badger.DB) error {
	var batch [][2]string
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		tx, err := d.db.Begin()
		if err != nil {
			return err
		}
		stmt, err := tx.Prepare(`INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		defer stmt.Close()
		for _, pair := range batch {
			if _, err := stmt.Exec(pair[0], pair[1]); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}
	if err := source.View(func(txn *badger.Txn) error {
		return prefixScan(txn, []byte(prefixMeta), func(key, val []byte) (bool, error) {
			batch = append(batch, [2]string{string(key), string(val)})
			if len(batch) >= writeBatchSize {
				return true, flush()
			}
			return true, nil
		})
	}); err != nil {
		return fmt.Errorf("failed migrating meta keys: %w", err)
	}
	return flush()
}
