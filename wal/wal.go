package wal

import (
	"os"
	"path"

	"github.com/vmihailenco/msgpack/v5"
)

type WriteAheadLog struct {
	file *os.File
}

func NewWriteAheadLog(dir string) WriteAheadLog {
	f, err := os.OpenFile(dir+".wal", os.O_APPEND+os.O_CREATE, os.ModeAppend)
	if err != nil {
		panic(err)
	}

	return WriteAheadLog{f}
}

func (w WriteAheadLog) Close() {
	w.Close()
}

func (w WriteAheadLog) Append(obj any) int64 {
	startPos, err := w.file.Seek(0, 0)
	err = msgpack.NewEncoder(w.file).Encode(obj)
	if err != nil {
		panic(err)
	}

	return startPos
}

func (w WriteAheadLog) Iterate() func(obj any) error {
	decoder := msgpack.NewDecoder(w.file)

	decoder.Skip()

	return func(obj any) error {
		return decoder.Decode(obj)
	}
}

func (w WriteAheadLog) Count() (c int) {
	decoder := msgpack.NewDecoder(w.file)

	for err := decoder.Skip(); err != nil; {
		c++
	}

	return
}

type IndexedWriteAheadLog struct {
	values WriteAheadLog
	index  map[string]WriteAheadLog
	dir    string
}

func NewIndexedWriteAheadLog(dir string) IndexedWriteAheadLog {
	return IndexedWriteAheadLog{
		dir:    dir,
		values: NewWriteAheadLog(path.Join(dir, "values")),
		index:  make(map[string]WriteAheadLog),
	}
}

func (irw IndexedWriteAheadLog) Close() {
	irw.values.Close()
	for _, w := range irw.index {
		w.Close()
	}
}

type indexKeyPair struct {
	Value    string
	Position int64
}

func (irw IndexedWriteAheadLog) Append(index map[string]string, obj any) {
	valuePos := irw.values.Append(obj)

	for key, value := range index {
		indexWal, exists := irw.index[key]
		if !exists {
			irw.index[key] = NewWriteAheadLog(path.Join(irw.dir, "index", key))
			indexWal = irw.index[key]
		}

		indexWal.Append(indexKeyPair{
			Value:    value,
			Position: valuePos,
		})
	}
}

func (irw IndexedWriteAheadLog) Iterate(key, value string) func(obj any) error {
	if key == "" {
		return irw.values.Iterate()
	}

	pos := irw.index[key].Iterate()
	iter := irw.values.Iterate()

	return func(obj any) error {
		var ikp indexKeyPair

		for ikp.Value != value {
			pos(&ikp)
		}

		_, err := irw.values.file.Seek(ikp.Position, 0)
		if err != nil {
			return err
		}

		return iter(obj)
	}
}

func (irw IndexedWriteAheadLog) Count() (c int) {
	return irw.values.Count()
}
