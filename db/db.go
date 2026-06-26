package db

import (
	"crypto/sha256"
	"encoding/hex"
	"grit/types"
	"grit/wal"
	"path"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/oklog/ulid/v2"
)

type Database struct {
	resources        wal.WalSet
	unprocessedTasks wal.WalSet
	processedTasks   wal.WalSet
	dir              string
}

func NewDatabase(dir string) Database {
	return Database{
		resources:        wal.NewWalSet(path.Join(dir, "resources")),
		unprocessedTasks: wal.NewWalSet(path.Join(dir, "unprocessed_tasks")),
		processedTasks:   wal.NewWalSet(path.Join(dir, "processed_tasks")),
		dir:              dir,
	}
}

func (d Database) Close() {
	d.resources.Close()
	d.processedTasks.Close()
	d.unprocessedTasks.Close()
}

func (d Database) CreateTask(step types.Step, resource types.Resource) types.Task {
	t := types.Task{
		ID:              ulid.Make().String(),
		StepID:          step.ID,
		InputResourceID: &resource.ID,
		Processed:       false,
		Error:           nil,
	}

	tl := d.unprocessedTasks.Get(step.Name)

	tl.Append(map[string]string{
		"step_id":     step.ID,
		"resource_id": resource.ID,
	}, t)

	return t
}

func (d Database) CreateResource(step types.Step, task types.Task, name string, data []byte) types.Resource {
	h := sha256.Sum256(data)

	r := types.Resource{
		ID:              ulid.Make().String(),
		Name:            name,
		CreatedByTaskID: &task.ID,
		CreatedAt:       time.Now().String(),
		ObjectHash:      hex.EncodeToString(h[:]),
		Data:            data,
	}

	l := d.resources.Get(name)
	l.Append(map[string]string{
		"name":       name,
		"input_task": task.ID,
	}, r)

	d.processedTasks.Get(step.Name).Append(nil, task.ID)

	return r
}

func (d Database) IterateUnprocessedTasks(name string, out chan<- types.Task) {
	processed_tasks := d.processedTasks.Get(name)
	processed_filter := bloom.NewWithEstimates(uint(processed_tasks.Count()), 0.01)
	iter := processed_tasks.Iterate("", "")

	for {
		var t string
		err := iter(&t)
		if err == nil {
			processed_filter.Add([]byte(t))
		} else {
			break
		}
	}

	iter = d.unprocessedTasks.Get(name).Iterate("", "")

	for {
		var t types.Task
		err := iter(&t)

		if err != nil {
			break
		}

		if !processed_filter.Test([]byte(t.ID)) {
			out <- t
		}
	}
}
