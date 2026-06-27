package wal

import (
	"io"
	"os"
	"path"

	"github.com/vmihailenco/msgpack/v5"
)

type WriteAheadLog struct {
	file *os.File
}

func NewWriteAheadLog(dir string) WriteAheadLog {
	f, err := os.OpenFile(dir+".wal", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}

	return WriteAheadLog{f}
}

func (w WriteAheadLog) Close() {
	w.file.Close()
}

func (w WriteAheadLog) Append(obj any) int64 {
	startPos, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		panic(err)
	}
	if err = msgpack.NewEncoder(w.file).Encode(obj); err != nil {
		panic(err)
	}
	if err = w.file.Sync(); err != nil {
		panic(err)
	}

	return startPos
}

func (w WriteAheadLog) Iterate() func(obj any) error {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		panic(err)
	}
	decoder := msgpack.NewDecoder(w.file)

	return func(obj any) error {
		return decoder.Decode(obj)
	}
}

func (w WriteAheadLog) Count() (c int) {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		panic(err)
	}
	decoder := msgpack.NewDecoder(w.file)

	for err := decoder.Skip(); err == nil; err = decoder.Skip() {
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
		w.file.Close()
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

	indexWal, exists := irw.index[key]
	if !exists {
		return func(obj any) error { return io.EOF }
	}

	pos := indexWal.Iterate()

	return func(obj any) error {
		var ikp indexKeyPair
		for {
			if err := pos(&ikp); err != nil {
				return err
			}
			if ikp.Value == value {
				break
			}
		}

		if _, err := irw.values.file.Seek(ikp.Position, io.SeekStart); err != nil {
			return err
		}

		// Fresh decoder after seek — stale decoder buffers would corrupt reads.
		return msgpack.NewDecoder(irw.values.file).Decode(obj)
	}
}

func (irw IndexedWriteAheadLog) Count() (c int) {
	return irw.values.Count()
}
