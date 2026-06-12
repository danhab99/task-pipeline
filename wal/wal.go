package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"strconv"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
)

type WriteAheadLog struct {
	bdb     *badger.DB
	walFile *os.File
	mtx     *sync.Mutex

	appendKeyCache map[string]int64
}

func NewWriteAheadLog(dir string) (out WriteAheadLog) {
	var err error
	out.bdb, err = badger.Open(badger.DefaultOptions(path.Join(dir, "bdb")))
	if err != nil {
		panic(err)
	}

	out.walFile, err = os.Open(path.Join(dir, "wal"))

	return
}

func (w WriteAheadLog) Close() {
	w.bdb.Close()
	w.walFile.Close()
}

func (w WriteAheadLog) Append(obj any, index map[string]string) {
	defer w.mtx.Unlock()
	w.mtx.Lock()

	pos, err := w.walFile.Seek(0, 1)
	if err != nil {
		panic(err)
	}

	err = msgpack.NewEncoder(w.walFile).Encode(obj)
	if err != nil {
		panic(err)
	}

	posBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(posBytes, uint64(pos))

	err = w.bdb.Update(func(txn *badger.Txn) error {
		for key, value := range index {
			k := key + value

			count, hit := w.appendKeyCache[k]
			if hit {
				w.appendKeyCache[k]++
			} else {
				for iter := txn.NewIterator(badger.DefaultIteratorOptions); iter.Valid(); iter.Seek([]byte(k)) {
					count++
				}

				w.appendKeyCache[k] = count
			}

			k += fmt.Sprintf("%d", count)

			err := txn.Set([]byte(k), posBytes)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		panic(err)
	}
}

func (w WriteAheadLog) Fetch(index, value string, out any) bool {
	next, done := w.Iterate(index, value)
	r := next(out)
	done()
	return r
}

func (w WriteAheadLog) Iterate(index, value string) (next func(out any) bool, done func()) {
	_, err := w.walFile.Seek(0, 0)
	if err != nil {
		panic(err)
	}

	txn := w.bdb.NewTransaction(false)
	if err != nil {
		panic(err)
	}

	iter := txn.NewIterator(badger.DefaultIteratorOptions)

	done = func() {
		iter.Close()
		txn.Discard()
	}

	next = func(obj any) bool {
		defer w.mtx.Unlock()
		w.mtx.Lock()

		iter.Seek([]byte(index + value))

		if iter.Valid() {
			posItem := iter.Item()
			var pos uint64
			err := posItem.Value(func(val []byte) error {
				pos = binary.BigEndian.Uint64(val)
				return nil
			})
			if err != nil {
				done()
				return false
			}

			_, err = w.walFile.Seek(int64(pos), 0)
			if err != nil {
				done()
				return false
			}

			err = msgpack.NewDecoder(w.walFile).Decode(obj)
			if err != nil {
				done()
				return false
			}

			return true
		}

		done()
		return false
	}

	return
}
