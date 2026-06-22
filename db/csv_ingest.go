package db

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	badger "github.com/dgraph-io/badger/v4"
)

// getCsvFileHash retrieves the stored hash for a CSV file path, or "" if none.
func (d *Database) getCsvFileHash(path string) (string, error) {
	var hash string
	err := d.resourcePoolDB.View(func(txn *badger.Txn) error {
		val, err := getVal(txn, metaCsvHashKey(path))
		if err != nil {
			return err
		}
		if val != nil {
			hash = string(val)
		}
		return nil
	})
	return hash, err
}

// setCsvFileHash stores the hash for a CSV file path.
func (d *Database) setCsvFileHash(path, hash string) error {
	return d.resourcePoolDB.Update(func(txn *badger.Txn) error {
		return txn.Set(metaCsvHashKey(path), []byte(hash))
	})
}

// getCsvFileOffset returns the byte offset of the last committed batch for path, or 0.
func (d *Database) getCsvFileOffset(path string) (int64, error) {
	var offset int64
	err := d.resourcePoolDB.View(func(txn *badger.Txn) error {
		val, err := getVal(txn, metaCsvOffsetKey(path))
		if err != nil {
			return err
		}
		if val != nil {
			n, err := strconv.ParseInt(string(val), 10, 64)
			if err != nil {
				return fmt.Errorf("invalid CSV offset value: %w", err)
			}
			offset = n
		}
		return nil
	})
	return offset, err
}

// setCsvFileOffset persists the committed byte offset for crash recovery.
func (d *Database) setCsvFileOffset(path string, offset int64) error {
	return d.resourcePoolDB.Update(func(txn *badger.Txn) error {
		return txn.Set(metaCsvOffsetKey(path), []byte(strconv.FormatInt(offset, 10)))
	})
}

// deleteCsvFileOffset removes the in-progress offset once ingestion completes.
func (d *Database) deleteCsvFileOffset(path string) error {
	return d.resourcePoolDB.Update(func(txn *badger.Txn) error {
		return txn.Delete(metaCsvOffsetKey(path))
	})
}

// hashFile streams through a file computing its SHA-256 without loading it all into memory.
func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("failed to open file for hashing: %w", err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("failed to hash file: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// IngestCsvFile reads a CSV file and creates a resource for each row.
// If columns is non-empty, the CSV header is parsed and only those named
// columns are kept; rows with any empty value in the requested columns are
// skipped. Otherwise the raw line is stored as-is.
// The file is hashed first to skip re-ingestion when content is unchanged.
// Crash recovery: a byte offset is checkpointed after each batch so a restart
// can resume mid-file without re-processing rows.
func (d *Database) IngestCsvFile(path string, outputName string, columns []string) (int64, error) {
	dbLogger.Printf("Ingesting CSV file: %s → output name: %s\n", path, outputName)

	filtering := len(columns) > 0

	fileHash, err := hashFile(path)
	if err != nil {
		return 0, err
	}

	storedHash, err := d.getCsvFileHash(path)
	if err != nil {
		return 0, fmt.Errorf("failed to check stored CSV hash: %w", err)
	}
	if storedHash == fileHash {
		dbLogger.Printf("CSV file %s unchanged (hash %s), skipping\n", path, fileHash[:16])
		return 0, nil
	}

	startOffset, err := d.getCsvFileOffset(path)
	if err != nil {
		return 0, fmt.Errorf("failed to read CSV offset: %w", err)
	}
	if startOffset > 0 {
		dbLogger.Printf("CSV file %s: resuming from byte offset %d\n", path, startOffset)
	} else {
		dbLogger.Printf("CSV file %s changed or new, ingesting rows...\n", path)
	}

	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer f.Close()

	reader := bufio.NewReaderSize(f, 64*1024)
	bytePos := int64(0)

	var count int64
	const batchSize = 500

	var objectBatch []struct {
		hash string
		data []byte
	}

	appendRow := func(data []byte) {
		h := sha256.Sum256(data)
		objectBatch = append(objectBatch, struct {
			hash string
			data []byte
		}{hex.EncodeToString(h[:]), data})
		count++
	}

	flushBatch := func() error {
		if len(objectBatch) == 0 {
			return nil
		}
		for _, item := range objectBatch {
			if err := d.StoreObject(item.hash, item.data); err != nil {
				return fmt.Errorf("failed to store object: %w", err)
			}
			backend := d.StorageBackendForSize(len(item.data))
			if err := d.insertResource(outputName, item.hash, "", backend); err != nil {
				return fmt.Errorf("failed to create resource: %w", err)
			}
		}
		if err := d.setCsvFileOffset(path, bytePos); err != nil {
			return fmt.Errorf("failed to save CSV offset: %w", err)
		}
		objectBatch = objectBatch[:0]
		return nil
	}

	readLine := func() ([]byte, error) {
		raw, err := reader.ReadBytes('\n')
		bytePos += int64(len(raw))
		return bytes.TrimRight(raw, "\r\n"), err
	}

	if filtering {
		// Parse header to resolve column indices.
		headerLine, err := readLine()
		if err != nil && err != io.EOF {
			return 0, fmt.Errorf("failed to read CSV header: %w", err)
		}
		header, err := csv.NewReader(strings.NewReader(string(headerLine))).Read()
		if err != nil {
			return 0, fmt.Errorf("failed to parse CSV header: %w", err)
		}

		colIdx := make(map[string]int, len(header))
		for i, name := range header {
			colIdx[strings.TrimSpace(name)] = i
		}
		keepIndices := make([]int, 0, len(columns))
		for _, col := range columns {
			idx, ok := colIdx[col]
			if !ok {
				return 0, fmt.Errorf("column %q not found in CSV header %v", col, header)
			}
			keepIndices = append(keepIndices, idx)
		}

		// Emit the filtered header as the first resource (fresh start only).
		if startOffset == 0 {
			filteredHeader := make([]string, len(keepIndices))
			for i, idx := range keepIndices {
				filteredHeader[i] = header[idx]
			}
			var hdrBuf strings.Builder
			hw := csv.NewWriter(&hdrBuf)
			hw.Write(filteredHeader)
			hw.Flush()
			appendRow([]byte(strings.TrimRight(hdrBuf.String(), "\n")))
		}

		// Seek to resume point (past header) if recovering from a crash.
		if startOffset > bytePos {
			if _, err := f.Seek(startOffset, io.SeekStart); err != nil {
				return 0, fmt.Errorf("failed to seek to offset %d: %w", startOffset, err)
			}
			reader.Reset(f)
			bytePos = startOffset
		}

		filtered := make([]string, len(keepIndices))
		var skipped int64
		for {
			line, err := readLine()
			if len(line) > 0 {
				record, parseErr := csv.NewReader(strings.NewReader(string(line))).Read()
				if parseErr == nil {
					incomplete := false
					for _, idx := range keepIndices {
						if idx >= len(record) || strings.TrimSpace(record[idx]) == "" {
							incomplete = true
							break
						}
					}
					if incomplete {
						skipped++
					} else {
						for i, idx := range keepIndices {
							filtered[i] = record[idx]
						}
						var buf strings.Builder
						w := csv.NewWriter(&buf)
						w.Write(filtered)
						w.Flush()
						appendRow([]byte(strings.TrimRight(buf.String(), "\n")))

						if len(objectBatch) >= batchSize {
							if err := flushBatch(); err != nil {
								return count, err
							}
							dbLogger.Verbosef("CSV ingest: %d rows processed so far\n", count)
						}
					}
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				return count, fmt.Errorf("error reading CSV file: %w", err)
			}
		}
		if skipped > 0 {
			dbLogger.Printf("CSV ingest: skipped %d rows with missing column values\n", skipped)
		}
	} else {
		// Raw line-by-line path (no column filtering).
		if startOffset > 0 {
			if _, err := f.Seek(startOffset, io.SeekStart); err != nil {
				return 0, fmt.Errorf("failed to seek to offset %d: %w", startOffset, err)
			}
			reader.Reset(f)
			bytePos = startOffset
		}

		for {
			line, err := readLine()
			if len(line) > 0 {
				data := make([]byte, len(line))
				copy(data, line)
				appendRow(data)

				if len(objectBatch) >= batchSize {
					if err := flushBatch(); err != nil {
						return count, err
					}
					dbLogger.Verbosef("CSV ingest: %d rows processed so far\n", count)
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				return count, fmt.Errorf("error reading CSV file: %w", err)
			}
		}
	}

	if err := flushBatch(); err != nil {
		return count, err
	}

	if err := d.setCsvFileHash(path, fileHash); err != nil {
		return count, fmt.Errorf("failed to store CSV file hash: %w", err)
	}
	if err := d.deleteCsvFileOffset(path); err != nil {
		return count, fmt.Errorf("failed to clear CSV offset: %w", err)
	}

	dbLogger.Printf("CSV ingest complete: %d rows from %s\n", count, path)
	return count, nil
}
