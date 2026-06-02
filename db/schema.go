package db

import (
	"database/sql"
	"grit/types"

	badger "github.com/dgraph-io/badger/v4"
)

type Database struct {
	repo_path string
	db        *sql.DB
	badgerDB  *badger.DB
}

// Type aliases so existing db internals compile unchanged until rewrite.
type Step = types.Step
type Task = types.Task
type Resource = types.Resource


