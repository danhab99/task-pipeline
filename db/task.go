package db

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

func (d Database) CreateTask(task Task) (string, error) {
	var resultID string
	err := d.badgerDB.Update(func(txn *badger.Txn) error {
		id := newULID()
		task.ID = id

		if err := putEntity(txn, taskKey(id), &task); err != nil {
			return err
		}

		// Status index
		if task.Processed {
			if err := txn.Set(idxTaskByStepProcKey(task.StepID, id), nil); err != nil {
				return err
			}
		} else {
			if err := txn.Set(idxTaskByStepUnprocKey(task.StepID, id), nil); err != nil {
				return err
			}
		}

		// All-tasks-for-step index
		if err := txn.Set(idxTaskByStepAllKey(task.StepID, id), nil); err != nil {
			return err
		}

 // Unique constraint index
 		if task.InputResourceID != nil {
 			if err := txn.Set(idxTaskUniqueKey(task.StepID, *task.InputResourceID), []byte(id)); err != nil {
 				return err
 			}
 		}

		resultID = id
		return nil
	})
	return resultID, err
}

func (d *Database) CreateAndGetTask(t Task) (*Task, error) {
	id, err := d.CreateTask(t)
	if err != nil {
		return nil, err
	}
	return d.GetTask(id)
}

func (d Database) CreateTasksFromResources(stepID string, resourceIDs []string) ([]string, error) {
	if len(resourceIDs) == 0 {
		return nil, nil
	}

	var taskIDs []string
	for i := 0; i < len(resourceIDs); i += writeBatchSize {
		end := i + writeBatchSize
		if end > len(resourceIDs) {
			end = len(resourceIDs)
		}
		chunk := resourceIDs[i:end]
		err := d.badgerDB.Update(func(txn *badger.Txn) error {
			for _, resourceID := range chunk {
				// Check unique constraint
				uniqueKey := idxTaskUniqueKey(stepID, resourceID)
				if keyExists(txn, uniqueKey) {
					continue // already exists
				}

				id := newULID()
				resID := resourceID
				task := Task{
					ID:              id,
					StepID:          stepID,
					InputResourceID: &resID,
				}

				if err := putEntity(txn, taskKey(id), &task); err != nil {
					return err
				}
				if err := txn.Set(idxTaskByStepUnprocKey(stepID, id), nil); err != nil {
					return err
				}
				if err := txn.Set(idxTaskByStepAllKey(stepID, id), nil); err != nil {
					return err
				}
				if err := txn.Set(uniqueKey, []byte(id)); err != nil {
					return err
				}

				taskIDs = append(taskIDs, id)
			}
			return nil
		})
		if err != nil {
			return taskIDs, err
		}
	}
	return taskIDs, nil
}

func (d Database) GetTask(id string) (*Task, error) {
	var task *Task
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		var err error
		task, err = getEntity[Task](txn, taskKey(id))
		return err
	})
	return task, err
}

func (d Database) GetTasksForStep(stepID string) chan Task {
	ch := make(chan Task)
	go func() {
		defer close(ch)
		prefix := idxTaskByStepAllPrefix(stepID)
		cursor := append([]byte{}, prefix...)
		for {
			var tasks []Task
			var lastKey []byte
			exhausted := false
			err := d.badgerDB.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.Prefix = prefix
				opts.PrefetchValues = false
				it := txn.NewIterator(opts)
				defer it.Close()
				var scanned int
				for it.Seek(cursor); it.ValidForPrefix(prefix); it.Next() {
					key := it.Item().KeyCopy(nil)
					lastKey = key
					scanned++
					taskID := string(key[len(prefix):])
					t, err := getEntity[Task](txn, taskKey(taskID))
					if err != nil {
						return err
					}
					if t != nil {
						tasks = append(tasks, *t)
					}
					if scanned >= scanBatchSize {
						return nil
					}
				}
				exhausted = true
				return nil
			})
			if err != nil {
				panic(err)
			}
			for _, t := range tasks {
				ch <- t
			}
			if exhausted || lastKey == nil {
				break
			}
			cursor = append(lastKey, 0x00)
		}
	}()
	return ch
}

func (d Database) GetUnprocessedTasks(stepID string) chan Task {
	ch := make(chan Task)
	go func() {
		defer close(ch)
		prefix := idxTaskByStepUnprocPrefix(stepID)
		cursor := append([]byte{}, prefix...)
		var total int
		for {
			var tasks []Task
			var lastKey []byte
			exhausted := false
			err := d.badgerDB.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.Prefix = prefix
				opts.PrefetchValues = false
				it := txn.NewIterator(opts)
				defer it.Close()
				var scanned int
				for it.Seek(cursor); it.ValidForPrefix(prefix); it.Next() {
					key := it.Item().KeyCopy(nil)
					lastKey = key
					scanned++
					taskID := string(key[len(prefix):])
					t, err := getEntity[Task](txn, taskKey(taskID))
					if err != nil {
						return err
					}
					if t != nil {
						tasks = append(tasks, *t)
					}
					if scanned >= scanBatchSize {
						return nil
					}
				}
				exhausted = true
				return nil
			})
			if err != nil {
				dbLogger.Verbosef("Error querying unprocessed tasks for step %s: %v\n", stepID, err)
				break
			}
			total += len(tasks)
			for _, t := range tasks {
				ch <- t
			}
			if exhausted || lastKey == nil {
				break
			}
			cursor = append(lastKey, 0x00)
		}
		dbLogger.Verbosef("GetUnprocessedTasks(step=%s) found %d unprocessed tasks\n", stepID, total)
	}()
	return ch
}

func (d Database) GetTaskInputResource(taskID string) (*Resource, error) {
	var resource *Resource
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		t, err := getEntity[Task](txn, taskKey(taskID))
		if err != nil || t == nil || t.InputResourceID == nil {
			return err
		}
		resource, err = getEntity[Resource](txn, resourceKey(*t.InputResourceID))
		return err
	})
	return resource, err
}

// TaskStatusUpdate holds a single deferred status change for BatchUpdateTaskStatus.
type TaskStatusUpdate struct {
	ID        string
	Processed bool
	Error     *string
}

// BatchUpdateTaskStatus writes a slice of task status updates in chunks of
// writeBatchSize, so no single transaction is unbounded.
func (d Database) BatchUpdateTaskStatus(updates []TaskStatusUpdate) error {
	for i := 0; i < len(updates); i += writeBatchSize {
		end := i + writeBatchSize
		if end > len(updates) {
			end = len(updates)
		}
		chunk := updates[i:end]
		err := d.badgerDB.Update(func(txn *badger.Txn) error {
			for _, u := range chunk {
				t, err := getEntity[Task](txn, taskKey(u.ID))
				if err != nil || t == nil {
					continue
				}
				wasProcessed := t.Processed
				t.Processed = u.Processed
				t.Error = u.Error
				if err := putEntity(txn, taskKey(u.ID), t); err != nil {
					return err
				}
				if wasProcessed != u.Processed {
					if u.Processed {
						_ = txn.Delete(idxTaskByStepUnprocKey(t.StepID, u.ID))
						if err := txn.Set(idxTaskByStepProcKey(t.StepID, u.ID), nil); err != nil {
							return err
						}
					} else {
						_ = txn.Delete(idxTaskByStepProcKey(t.StepID, u.ID))
						if err := txn.Set(idxTaskByStepUnprocKey(t.StepID, u.ID), nil); err != nil {
							return err
						}
					}
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (d Database) UpdateTaskStatus(id string, processed bool, errorMsg *string) error {
	return d.badgerDB.Update(func(txn *badger.Txn) error {
		t, err := getEntity[Task](txn, taskKey(id))
		if err != nil || t == nil {
			return err
		}

		wasProcessed := t.Processed
		t.Processed = processed
		t.Error = errorMsg

		if err := putEntity(txn, taskKey(id), t); err != nil {
			return err
		}

		// Move between status indexes
		if wasProcessed != processed {
			if processed {
				_ = txn.Delete(idxTaskByStepUnprocKey(t.StepID, id))
				return txn.Set(idxTaskByStepProcKey(t.StepID, id), nil)
			} else {
				_ = txn.Delete(idxTaskByStepProcKey(t.StepID, id))
				return txn.Set(idxTaskByStepUnprocKey(t.StepID, id), nil)
			}
		}
		return nil
	})
}

func (d Database) CountTasksForStep(stepID string) (int64, error) {
	var count int64
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		var err error
		count, err = prefixCount(txn, idxTaskByStepAllPrefix(stepID))
		return err
	})
	return count, err
}

func (d Database) CountUnprocessedTasks() (int64, error) {
	var count int64
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		// Sum unprocessed across all steps by scanning entire unprocessed index
		var err error
		count, err = prefixCount(txn, []byte(idxTaskByStepUnproc))
		return err
	})
	return count, err
}

func (d Database) CountUnprocessedTasksForStep(stepID string) (int64, error) {
	var count int64
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		var err error
		count, err = prefixCount(txn, idxTaskByStepUnprocPrefix(stepID))
		return err
	})
	return count, err
}

func (d Database) GetTaskCountsForStep(stepID string) (total int64, processed int64, err error) {
	total, err = d.CountTasksForStep(stepID)
	if err != nil {
		return 0, 0, err
	}
	unprocessed, err := d.CountUnprocessedTasksForStep(stepID)
	if err != nil {
		return total, 0, err
	}
	return total, total - unprocessed, nil
}

func (d Database) DeleteTask(id string) error {
	return d.badgerDB.Update(func(txn *badger.Txn) error {
		t, err := getEntity[Task](txn, taskKey(id))
		if err != nil || t == nil {
			return err
		}

		if err := txn.Delete(taskKey(id)); err != nil {
			return err
		}
		_ = txn.Delete(idxTaskByStepAllKey(t.StepID, id))
		_ = txn.Delete(idxTaskByStepUnprocKey(t.StepID, id))
		_ = txn.Delete(idxTaskByStepProcKey(t.StepID, id))
		if t.InputResourceID != nil {
			_ = txn.Delete(idxTaskUniqueKey(t.StepID, *t.InputResourceID))
		}
		return nil
	})
}

func (d Database) TaskExists(id string) (bool, error) {
	var exists bool
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		exists = keyExists(txn, taskKey(id))
		return nil
	})
	return exists, err
}

func (d Database) ListTasks() chan Task {
	ch := make(chan Task)
	go func() {
		defer close(ch)
		prefix := []byte(prefixTask)
		cursor := append([]byte{}, prefix...)
		for {
			var tasks []Task
			var lastKey []byte
			exhausted := false
			err := d.badgerDB.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.Prefix = prefix
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Seek(cursor); it.ValidForPrefix(prefix); it.Next() {
					lastKey = it.Item().KeyCopy(nil)
					var t Task
					err := it.Item().Value(func(v []byte) error { return decode(v, &t) })
					if err == nil {
						tasks = append(tasks, t)
					}
					if len(tasks) >= scanBatchSize {
						return nil
					}
				}
				exhausted = true
				return nil
			})
			if err != nil {
				panic(err)
			}
			for _, t := range tasks {
				ch <- t
			}
			if exhausted || lastKey == nil {
				break
			}
			cursor = append(lastKey, 0x00)
		}
	}()
	return ch
}

func (d Database) MarkStepTasksUnprocessed(stepID string) error {
	prefix := idxTaskByStepProcPrefix(stepID)
	cursor := append([]byte{}, prefix...)
	for {
		var taskIDs []string
		var lastKey []byte
		var exhausted bool
		err := d.badgerDB.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Seek(cursor); it.ValidForPrefix(prefix); it.Next() {
				key := it.Item().KeyCopy(nil)
				taskIDs = append(taskIDs, string(key[len(prefix):]))
				lastKey = key
				if len(taskIDs) >= scanBatchSize {
					return nil
				}
			}
			exhausted = true
			return nil
		})
		if err != nil {
			return err
		}
		if len(taskIDs) == 0 {
			break
		}
		for i := 0; i < len(taskIDs); i += writeBatchSize {
			end := i + writeBatchSize
			if end > len(taskIDs) {
				end = len(taskIDs)
			}
			chunk := taskIDs[i:end]
			err = d.badgerDB.Update(func(txn *badger.Txn) error {
				for _, taskID := range chunk {
					t, err := getEntity[Task](txn, taskKey(taskID))
					if err != nil || t == nil {
						continue
					}
					t.Processed = false
					t.Error = nil
					if err := putEntity(txn, taskKey(taskID), t); err != nil {
						return err
					}
					_ = txn.Delete(idxTaskByStepProcKey(stepID, taskID))
					if err := txn.Set(idxTaskByStepUnprocKey(stepID, taskID), nil); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		if exhausted || lastKey == nil {
			break
		}
		cursor = append(lastKey, 0x00)
	}
	return nil
}

func (d Database) MarkStepUndone(stepID string) error {
	prefix := idxTaskByStepAllPrefix(stepID)
	cursor := append([]byte{}, prefix...)
	var totalDeleted int
	for {
		var taskIDs []string
		var lastKey []byte
		var exhausted bool
		err := d.badgerDB.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Seek(cursor); it.ValidForPrefix(prefix); it.Next() {
				key := it.Item().KeyCopy(nil)
				taskIDs = append(taskIDs, string(key[len(prefix):]))
				lastKey = key
				if len(taskIDs) >= scanBatchSize {
					return nil
				}
			}
			exhausted = true
			return nil
		})
		if err != nil {
			return err
		}
		if len(taskIDs) == 0 {
			break
		}
		for i := 0; i < len(taskIDs); i += writeBatchSize {
			end := i + writeBatchSize
			if end > len(taskIDs) {
				end = len(taskIDs)
			}
			chunk := taskIDs[i:end]
			err = d.badgerDB.Update(func(txn *badger.Txn) error {
				for _, taskID := range chunk {
					t, err := getEntity[Task](txn, taskKey(taskID))
					if err != nil || t == nil {
						continue
					}
					// Delete task and all its indexes
					_ = txn.Delete(taskKey(taskID))
					_ = txn.Delete(idxTaskByStepAllKey(stepID, taskID))
					_ = txn.Delete(idxTaskByStepUnprocKey(stepID, taskID))
					_ = txn.Delete(idxTaskByStepProcKey(stepID, taskID))
					if t.InputResourceID != nil {
						_ = txn.Delete(idxTaskUniqueKey(stepID, *t.InputResourceID))
					}
					totalDeleted++
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		if exhausted || lastKey == nil {
			break
		}
		cursor = append(lastKey, 0x00)
	}
	dbLogger.Verbosef("Marked step %s as undone: deleted %d tasks\n", stepID, totalDeleted)
	return nil
}

func (d Database) IsStepComplete(stepID string) (bool, error) {
	count, err := d.CountUnprocessedTasksForStep(stepID)
	if err != nil {
		return false, err
	}
	return count == 0, nil
}

func (d Database) CheckAndMarkStepComplete(stepID string) (bool, error) {
	isComplete, err := d.IsStepComplete(stepID)
	if err != nil {
		return false, err
	}
	if isComplete {
		step, err := d.GetStep(stepID)
		if err != nil {
			return false, err
		}
		if step != nil {
			dbLogger.Verbosef("Step %s (%s) marked as complete\n", stepID, step.Name)
		}
	}
	return isComplete, nil
}

func (d Database) GetPipelineStatus() (complete bool, totalTasks int64, processedTasks int64, err error) {
	err = d.badgerDB.View(func(txn *badger.Txn) error {
		totalTasks, err = prefixCount(txn, []byte(prefixTask))
		if err != nil {
			return err
		}
		var unprocessed int64
		unprocessed, err = prefixCount(txn, []byte(idxTaskByStepUnproc))
		if err != nil {
			return err
		}
		processedTasks = totalTasks - unprocessed
		return nil
	})
	complete = totalTasks > 0 && totalTasks == processedTasks
	return
}

// idxTaskByStepProcPrefix returns the prefix for processed tasks of a step.
func idxTaskByStepProcPrefix(stepID string) []byte {
	return []byte(idxTaskByStepProc + stepID + "\x00")
}

// ScheduleTasksForStep schedules tasks for a step by finding resources produced by input steps
// that haven't already been consumed by this step.
//
// Memory is bounded to scheduleBatchSize resource IDs at a time by using a seek-resume cursor:
// scan up to batchSize resources in a View, write them in an Update, then resume the scan from
// the key immediately after the last one processed.
func (d Database) ScheduleTasksForStep(stepID string) (int64, error) {
	step, err := d.GetStep(stepID)
	if err != nil {
		return 0, err
	}
	if step == nil || step.Input == "" {
		dbLogger.Verbosef("Step %s (%s) has no input, skipping scheduling\n", stepID, step.Name)
		return 0, nil
	}

	dbLogger.Verbosef("Scheduling tasks for step %s (%s) with input: %s\n", stepID, step.Name, step.Input)

	const scheduleBatchSize = scanBatchSize
	var totalScheduled int64

	prefix := idxResourceByNamePrefix(step.Input)
	cursor := append([]byte{}, prefix...) // start at beginning of prefix; may be advanced by watermark below

	// Fast-forward cursor past resources already scheduled for this step.
	// The unique index (ix:tu:{stepID}\x00{resourceID}) is ULID-ordered, so
	// its last key is the highest resourceID already scheduled.  A single
	// reverse seek gives us the resume point in O(1) instead of re-scanning
	// the entire resource index on every startup.
	uniquePrefix := []byte(idxTaskUnique + stepID + "\x00")
	wmErr := d.badgerDB.View(func(txn *badger.Txn) error {
		return prefixScanReverse(txn, uniquePrefix, func(key, _ []byte) (bool, error) {
			lastResourceID := string(key[len(uniquePrefix):])
			// Set cursor to just after the last scheduled resource key.
			cursor = append(idxResourceByNameKey(step.Input, lastResourceID), 0x00)
			return false, nil // stop after first (highest) result
		})
	})
	if wmErr != nil {
		dbLogger.Verbosef("ScheduleTasksForStep: step=%s watermark lookup error (proceeding from start): %v\n", stepID, wmErr)
		cursor = append([]byte{}, prefix...) // reset to safe default
	}

	dbLogger.Verbosef("ScheduleTasksForStep: step=%s input=%s scanning\n", stepID, step.Input)

	for {
		var batch []string
		var lastKey []byte
		var exhausted bool
		var scanTotal int

		err = d.badgerDB.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			defer it.Close()

			for it.Seek(cursor); it.ValidForPrefix(prefix); it.Next() {
				key := it.Item().KeyCopy(nil)
				resourceID := string(key[len(prefix):])
				scanTotal++
				batch = append(batch, resourceID)
				lastKey = key
				if len(batch) >= scheduleBatchSize {
					return nil // stop early; resume from lastKey next iteration
				}
			}
			exhausted = true
			return nil
		})
		if err != nil {
			return totalScheduled, fmt.Errorf("failed to scan resources for step %s: %w", stepID, err)
		}

		dbLogger.Verbosef("ScheduleTasksForStep: step=%s input=%s scan_window=%d exhausted=%v\n",
			stepID, step.Input, scanTotal, exhausted)

		if len(batch) > 0 {
			var batchWritten int
			for j := 0; j < len(batch); j += writeBatchSize {
				wEnd := j + writeBatchSize
				if wEnd > len(batch) {
					wEnd = len(batch)
				}
				chunk := batch[j:wEnd]
				err = d.badgerDB.Update(func(txn *badger.Txn) error {
					for _, resourceID := range chunk {
						uniqueKey := idxTaskUniqueKey(stepID, resourceID)
						if keyExists(txn, uniqueKey) {
							continue
						}
						id := newULID()
						resID := resourceID
						task := Task{
							ID:              id,
							StepID:          stepID,
							InputResourceID: &resID,
						}
						if err := putEntity(txn, taskKey(id), &task); err != nil {
							return err
						}
						if err := txn.Set(idxTaskByStepUnprocKey(stepID, id), nil); err != nil {
							return err
						}
						if err := txn.Set(idxTaskByStepAllKey(stepID, id), nil); err != nil {
							return err
						}
						if err := txn.Set(uniqueKey, []byte(id)); err != nil {
							return err
						}
						batchWritten++
					}
					return nil
				})
				if err != nil {
					return totalScheduled, fmt.Errorf("failed to write task batch for step %s: %w", stepID, err)
				}
			}
			totalScheduled += int64(batchWritten)
			dbLogger.Verbosef("ScheduleTasksForStep: step=%s input=%s batch_written=%d (race_skipped=%d) total_scheduled=%d\n",
				stepID, step.Input, batchWritten, len(batch)-batchWritten, totalScheduled)
		}

		if exhausted || len(lastKey) == 0 {
			break
		}
		cursor = append(lastKey, 0x00)
	}

	dbLogger.Verbosef("ScheduleTasksForStep: step=%s input=%s done: scheduled=%d\n",
		stepID, step.Input, totalScheduled)

	if totalScheduled > 0 {
		dbLogger.Verbosef("Scheduled %d new tasks for step %s (%s)\n", totalScheduled, stepID, step.Name)
	}
	return totalScheduled, nil
}
