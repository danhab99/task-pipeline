package wal

import (
	"path"
)

type WalSet struct {
	dir  string
	logs map[string]IndexedWriteAheadLog
}

func NewWalSet(dir string) WalSet {
	return WalSet{
		dir:  dir,
		logs: make(map[string]IndexedWriteAheadLog),
	}
}

func (w WalSet) Close() {
	for _, l := range w.logs {
		l.Close()
	}
}

func (w WalSet) Get(name string) IndexedWriteAheadLog {
	_, exists := w.logs[name]
	if !exists {
		w.logs[name] = NewIndexedWriteAheadLog(path.Join(w.dir, name))
	}
	return w.logs[name]
}
