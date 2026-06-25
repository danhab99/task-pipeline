package db

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	badger "github.com/dgraph-io/badger/v4"
)

type ResourceDeleteResult struct {
	ResourceID          string
	Name                string
	ObjectHash          string
	ResourceDeleted     bool
	ObjectDeleted       bool
	RemainingObjectRefs int64
}

func (d Database) CreateResource(name string, objectHash string) (string, error) {
	return d.CreateResourceWithTask(name, objectHash, nil)
}

func (d Database) CreateResourceWithTask(name string, objectHash string, createdByTaskID *string) (string, error) {
	var resultID string
	err := d.resourcePoolDB.Update(func(txn *badger.Txn) error {
		// Check unique constraint: (name, object_hash)
		hashKey := idxResourceHashKey(name, objectHash)
		existing, err := getVal(txn, hashKey)
		if err != nil {
			return err
		}
		if existing != nil {
			resultID = string(existing)
			return nil // already exists
		}

		id := newULID()
		res := Resource{
			ID:              id,
			Name:            name,
			ObjectHash:      objectHash,
			CreatedAt:       nowTimestamp(),
			CreatedByTaskID: createdByTaskID,
		}

		if err := putEntity(txn, resourceKey(id), &res); err != nil {
			return err
		}

		// Indexes
		if err := txn.Set(idxResourceByNameKey(name, id), nil); err != nil {
			return err
		}
		if err := txn.Set(hashKey, []byte(id)); err != nil {
			return err
		}

		resultID = id
		return nil
	})
	if err != nil || resultID == "" {
		return resultID, err
	}

	err = d.stepEngineDB.Update(func(txn *badger.Txn) error {
		return txn.Set(idxResourceRouteKey(name, ResourceStatusUnprocessed, resultID), nil)
	})
	return resultID, err
}

func (d Database) CreateResourceFromReader(name string, reader io.Reader) (string, string, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return "", "", fmt.Errorf("failed to read data: %w", err)
	}

	hasher := sha256.New()
	hasher.Write(data)
	hashBytes := hasher.Sum(nil)
	hash := hex.EncodeToString(hashBytes)

	if !d.ObjectExists(hash) {
		if err := d.StoreObject(hash, data); err != nil {
			return "", "", fmt.Errorf("failed to store object: %w", err)
		}
	}

	resourceID, err := d.CreateResource(name, hash)
	if err != nil {
		return "", "", fmt.Errorf("failed to create resource record: %w", err)
	}

	return resourceID, hash, nil
}

func (d Database) GetResource(id string) (*Resource, error) {
	var res *Resource
	err := d.resourcePoolDB.View(func(txn *badger.Txn) error {
		var err error
		res, err = getEntity[Resource](txn, resourceKey(id))
		return err
	})
	return res, err
}

func (d Database) GetResourcesByName(name string) chan Resource {
	ch := make(chan Resource)
	go func() {
		defer close(ch)
		prefix := idxResourceByNamePrefix(name)
		// Reverse scan: ULIDs are time-sorted, reverse gives newest first.
		// cursor starts at the top of the range; skipFirst skips the already-seen
		// cursor key on each subsequent batch (Seek in reverse lands on the key itself).
		cursor := append(append([]byte{}, prefix...), 0xFF)
		skipFirst := false
		for {
			var resources []Resource
			var lastKey []byte
			exhausted := false
			err := d.resourcePoolDB.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.Prefix = prefix
				opts.Reverse = true
				opts.PrefetchValues = false
				it := txn.NewIterator(opts)
				defer it.Close()
				it.Seek(cursor)
				if skipFirst && it.ValidForPrefix(prefix) {
					it.Next()
				}
				var scanned int
				for ; it.ValidForPrefix(prefix); it.Next() {
					key := it.Item().KeyCopy(nil)
					lastKey = key
					scanned++
					resID := string(key[len(prefix):])
					r, err := getEntity[Resource](txn, resourceKey(resID))
					if err != nil {
						return err
					}
					if r != nil {
						resources = append(resources, *r)
					}
					if scanned >= scanBatchSize {
						return nil
					}
				}
				exhausted = true
				return nil
			})
			if err != nil {
				dbLogger.Verbosef("Error querying resources by name %s: %v\n", name, err)
				break
			}
			for _, r := range resources {
				ch <- r
			}
			if exhausted || lastKey == nil {
				break
			}
			cursor = lastKey
			skipFirst = true
		}
	}()
	return ch
}

func (d Database) GetAllResources() chan Resource {
	ch := make(chan Resource)
	go func() {
		defer close(ch)
		prefix := []byte(prefixResource)
		cursor := append([]byte{}, prefix...)
		for {
			var resources []Resource
			var lastKey []byte
			exhausted := false
			err := d.resourcePoolDB.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.Prefix = prefix
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Seek(cursor); it.ValidForPrefix(prefix); it.Next() {
					lastKey = it.Item().KeyCopy(nil)
					var r Resource
					err := it.Item().Value(func(v []byte) error { return decode(v, &r) })
					if err == nil {
						resources = append(resources, r)
					}
					if len(resources) >= scanBatchSize {
						return nil
					}
				}
				exhausted = true
				return nil
			})
			if err != nil {
				dbLogger.Verbosef("Error querying all resources: %v\n", err)
				break
			}
			for _, r := range resources {
				ch <- r
			}
			if exhausted || lastKey == nil {
				break
			}
			cursor = append(lastKey, 0x00)
		}
	}()
	return ch
}

func (d Database) GetAllResourceNames() chan string {
	ch := make(chan string)
	go func() {
		defer close(ch)
		prefix := []byte(prefixResource)
		cursor := append([]byte{}, prefix...)
		seen := make(map[string]bool) // global dedup across batches
		for {
			var names []string
			var lastKey []byte
			exhausted := false
			var scanned int
			err := d.resourcePoolDB.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.Prefix = prefix
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Seek(cursor); it.ValidForPrefix(prefix); it.Next() {
					lastKey = it.Item().KeyCopy(nil)
					scanned++
					var r Resource
					err := it.Item().Value(func(v []byte) error { return decode(v, &r) })
					if err == nil && !seen[r.Name] {
						seen[r.Name] = true
						names = append(names, r.Name)
					}
					if scanned >= scanBatchSize {
						return nil
					}
				}
				exhausted = true
				return nil
			})
			if err != nil {
				dbLogger.Verbosef("Error querying resource names: %v\n", err)
				break
			}
			for _, n := range names {
				ch <- n
			}
			if exhausted || lastKey == nil {
				break
			}
			cursor = append(lastKey, 0x00)
		}
	}()
	return ch
}

func (d Database) GetUnconsumedResourcesByName(name string, consumingStepID string) chan Resource {
	ch := make(chan Resource)
	go func() {
		defer close(ch)
		prefix := idxResourceByNamePrefix(name)
		cursor := append(append([]byte{}, prefix...), 0xFF)
		skipFirst := false
		for {
			var resources []Resource
			var lastKey []byte
			exhausted := false
			err := d.resourcePoolDB.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.Prefix = prefix
				opts.Reverse = true
				opts.PrefetchValues = false
				it := txn.NewIterator(opts)
				defer it.Close()
				it.Seek(cursor)
				if skipFirst && it.ValidForPrefix(prefix) {
					it.Next()
				}
				var scanned int
				for ; it.ValidForPrefix(prefix); it.Next() {
					key := it.Item().KeyCopy(nil)
					lastKey = key
					scanned++
					resID := string(key[len(prefix):])
					uniqueKey := idxTaskUniqueKey(consumingStepID, resID)

					consumed := false
					err := d.taskQueueDB.View(func(taskTxn *badger.Txn) error {
						consumed = keyExists(taskTxn, uniqueKey)
						return nil
					})
					if err != nil {
						return err
					}

					if consumed {
						if scanned >= scanBatchSize {
							return nil
						}
						continue
					}
					r, err := getEntity[Resource](txn, resourceKey(resID))
					if err != nil {
						return err
					}
					if r != nil {
						resources = append(resources, *r)
					}
					if scanned >= scanBatchSize {
						return nil
					}
				}
				exhausted = true
				return nil
			})
			if err != nil {
				dbLogger.Verbosef("Error querying unconsumed resources for name %s, step %s: %v\n", name, consumingStepID, err)
				break
			}
			for _, r := range resources {
				ch <- r
			}
			if exhausted || lastKey == nil {
				break
			}
			cursor = lastKey
			skipFirst = true
		}
	}()
	return ch
}

func (d Database) CountResources() (int64, error) {
	var count int64
	err := d.resourcePoolDB.View(func(txn *badger.Txn) error {
		var err error
		count, err = prefixCount(txn, []byte(prefixResource))
		return err
	})
	return count, err
}

func (d Database) DeleteResource(id string) error {
	var deletedResource *Resource
	err := d.resourcePoolDB.Update(func(txn *badger.Txn) error {
		r, err := getEntity[Resource](txn, resourceKey(id))
		if err != nil || r == nil {
			return err
		}
		deletedResource = r
		_ = txn.Delete(resourceKey(id))
		_ = txn.Delete(idxResourceByNameKey(r.Name, id))
		_ = txn.Delete(idxResourceHashKey(r.Name, r.ObjectHash))
		return nil
	})
	if err != nil || deletedResource == nil {
		return err
	}

	return d.stepEngineDB.Update(func(txn *badger.Txn) error {
		_ = txn.Delete(idxResourceRouteKey(deletedResource.Name, ResourceStatusUnprocessed, id))
		_ = txn.Delete(idxResourceRouteKey(deletedResource.Name, ResourceStatusProcessing, id))
		return nil
	})
}

func (d Database) DeleteResourceHard(id string) (ResourceDeleteResult, error) {
	res := ResourceDeleteResult{ResourceID: id}
	var deleteFSFile bool
	err := d.resourcePoolDB.Update(func(txn *badger.Txn) error {
		r, err := getEntity[Resource](txn, resourceKey(id))
		if err != nil {
			return err
		}
		if r == nil {
			return nil
		}

		res.Name = r.Name
		res.ObjectHash = r.ObjectHash

		if err := txn.Delete(resourceKey(id)); err != nil {
			return err
		}
		if err := txn.Delete(idxResourceByNameKey(r.Name, id)); err != nil {
			return err
		}
		if err := txn.Delete(idxResourceHashKey(r.Name, r.ObjectHash)); err != nil {
			return err
		}
		res.ResourceDeleted = true

		remainingRefs, err := countResourcesByObjectHashTxn(txn, r.ObjectHash)
		if err != nil {
			return err
		}
		res.RemainingObjectRefs = remainingRefs

		if remainingRefs > 0 {
			return nil
		}

		hashBytes, err := hex.DecodeString(r.ObjectHash)
		if err != nil {
			return err
		}

		item, err := txn.Get(objectKey(hashBytes))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		deleteFSFile = string(val) == fsSentinel

		if err := txn.Delete(objectKey(hashBytes)); err != nil {
			return err
		}
		res.ObjectDeleted = true
		return nil
	})
	if err != nil {
		return res, err
	}

	if res.ResourceDeleted {
		if err := d.stepEngineDB.Update(func(txn *badger.Txn) error {
			_ = txn.Delete(idxResourceRouteKey(res.Name, ResourceStatusUnprocessed, id))
			_ = txn.Delete(idxResourceRouteKey(res.Name, ResourceStatusProcessing, id))
			return nil
		}); err != nil {
			return res, err
		}
	}

	if deleteFSFile {
		if err := removeObjectFileIfExists(d.objectFilePath(res.ObjectHash)); err != nil {
			return res, err
		}
	}

	return res, nil
}

type BulkResourceDeleteResult struct {
	ResourcesDeleted int
	ObjectsDeleted   int
	DeletedResources []ResourceDeleteResult
}

func (d Database) DeleteResourcesByName(name string) (BulkResourceDeleteResult, error) {
	var result BulkResourceDeleteResult
	var deleteFSFiles []string

	dbLogger.Verbosef("DeleteResourcesByName: scanning prefix for name=%s", name)
	err := d.resourcePoolDB.Update(func(txn *badger.Txn) error {
		prefix := idxResourceByNamePrefix(name)
		return prefixScanKeys(txn, prefix, func(key []byte) (bool, error) {
			resID := string(key[len(prefix):])
			r, err := getEntity[Resource](txn, resourceKey(resID))
			if err != nil {
				return false, err
			}
			if r == nil {
				return true, nil
			}

			dbLogger.Verbosef("  deleting resource id=%s hash=%s", resID, r.ObjectHash)

			if err := txn.Delete(resourceKey(resID)); err != nil {
				return false, err
			}
			if err := txn.Delete(idxResourceByNameKey(r.Name, resID)); err != nil {
				return false, err
			}
			if err := txn.Delete(idxResourceHashKey(r.Name, r.ObjectHash)); err != nil {
				return false, err
			}

			dbLogger.Verbosef("  counting remaining refs for hash=%s", r.ObjectHash)
			remainingRefs, err := countResourcesByObjectHashTxn(txn, r.ObjectHash)
			if err != nil {
				return false, err
			}
			dbLogger.Verbosef("  remaining refs for hash=%s: %d", r.ObjectHash, remainingRefs)

			rd := ResourceDeleteResult{
				ResourceID:          resID,
				Name:                r.Name,
				ObjectHash:          r.ObjectHash,
				ResourceDeleted:     true,
				RemainingObjectRefs: remainingRefs,
			}

			if remainingRefs == 0 {
				hashBytes, err := hex.DecodeString(r.ObjectHash)
				if err != nil {
					return false, err
				}

				item, err := txn.Get(objectKey(hashBytes))
				if err == badger.ErrKeyNotFound {
					// Object already gone (e.g. previously deleted)
					rd.ObjectDeleted = true
					result.DeletedResources = append(result.DeletedResources, rd)
					result.ResourcesDeleted++
					return true, nil
				}
				if err != nil {
					return false, err
				}

				val, err := item.ValueCopy(nil)
				if err != nil {
					return false, err
				}

				if err := txn.Delete(objectKey(hashBytes)); err != nil {
					return false, err
				}
				rd.ObjectDeleted = true
				result.ObjectsDeleted++

				if string(val) == fsSentinel {
					deleteFSFiles = append(deleteFSFiles, d.objectFilePath(r.ObjectHash))
				}
			}

			result.DeletedResources = append(result.DeletedResources, rd)
			result.ResourcesDeleted++
			if len(result.DeletedResources)%1000 == 0 {
				dbLogger.Verbosef("Deleted %d resources\n", result.ResourcesDeleted)
			}
			return true, nil
		})
	})
	if err != nil {
		return result, err
	}

	// Clean up router keys in stepEngineDB for all deleted resources
	if result.ResourcesDeleted > 0 {
		err = d.stepEngineDB.Update(func(txn *badger.Txn) error {
			for _, rd := range result.DeletedResources {
				_ = txn.Delete(idxResourceRouteKey(rd.Name, ResourceStatusUnprocessed, rd.ResourceID))
				_ = txn.Delete(idxResourceRouteKey(rd.Name, ResourceStatusProcessing, rd.ResourceID))
			}
			return nil
		})
		if err != nil {
			return result, err
		}
	}

	// Delete filesystem objects outside the transaction
	for _, path := range deleteFSFiles {
		if err := removeObjectFileIfExists(path); err != nil {
			return result, err
		}
	}

	return result, nil
}

func countResourcesByObjectHashTxn(txn *badger.Txn, objectHash string) (int64, error) {
	var count int64
	prefix := []byte(prefixResource)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = true
	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		var r Resource
		err := it.Item().Value(func(v []byte) error { return decode(v, &r) })
		if err != nil {
			return 0, err
		}
		if r.ObjectHash == objectHash {
			count++
		}
	}

	return count, nil
}
