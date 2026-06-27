package db

import (
	"crypto/sha256"
	"encoding/hex"
	"grit/manifest"
	"grit/stepmanager"
	"grit/types"
	"grit/wal"
	"path"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/oklog/ulid/v2"
)

type DataSet struct {
	resources        wal.IndexedWriteAheadLog
	unprocessedTasks wal.IndexedWriteAheadLog
	processedTasks   wal.IndexedWriteAheadLog
}

type Database struct {
	datasets map[types.Step]DataSet
	dir      string
}

func NewDatabase(dir string, m manifest.Manifest) Database {
	datasets := make(map[types.Step]DataSet)
	for _, mstep := range m.Steps {
		manager := stepmanager.NewManagedStep(dir, mstep)

		step := manager.Step()

		datasets[step] = DataSet{
			resources:        wal.NewIndexedWriteAheadLog(path.Join(manager.Dir(), "resources")),
			unprocessedTasks: wal.NewIndexedWriteAheadLog(path.Join(manager.Dir(), "unprocessedTasks")),
			processedTasks:   wal.NewIndexedWriteAheadLog(path.Join(manager.Dir(), "processedTasks")),
		}
	}

	return Database{
		datasets: datasets,
		dir:      dir,
	}
}

func (d Database) Close() {
	for _, dataset := range d.datasets {
		dataset.processedTasks.Close()
		dataset.unprocessedTasks.Close()
		dataset.resources.Close()
	}
}

func (d Database) CreateTask(step types.Step, resource *types.Resource) types.Task {
	rid := resource.ID()

	t := types.Task{
		Id:              ulid.Make().String(),
		StepID:          step.ID(),
		InputResourceID: &rid,
		Processed:       false,
		Error:           nil,
	}

	d.datasets[step].unprocessedTasks.Append(map[string]string{
		"step_id":     step.ID(),
		"resource_id": resource.ID(),
	}, t)

	return t
}

func (d Database) CreateResource(step types.Step, task types.Task, name string, data []byte) types.Resource {
	h := sha256.Sum256(data)

	id := task.ID()

	r := types.Resource{
		Name:            name,
		CreatedByTaskID: &id,
		CreatedAt:       time.Now().String(),
		ObjectHash:      hex.EncodeToString(h[:]),
		Data:            data,
	}

	d.datasets[step].unprocessedTasks.Append(map[string]string{
		"name":       name,
		"input_task": task.ID(),
	}, r)

	d.datasets[step].processedTasks.Append(nil, task.ID)

	return r
}

func (d Database) GetUnprocessedTasks(step types.Step) chan types.Task {
	processed_tasks := d.datasets[step].processedTasks
	processed_filter := bloom.NewWithEstimates(uint(processed_tasks.Count()), 0.01)
	iter := processed_tasks.Iterate("", "")
	out := make(chan types.Task)

	go func() {
		defer close(out)

		for {
			var t string
			err := iter(&t)
			if err == nil {
				processed_filter.Add([]byte(t))
			} else {
				break
			}
		}

		iter = d.datasets[step].unprocessedTasks.Iterate("", "")

		for {
			var t types.Task
			err := iter(&t)

			if err != nil {
				break
			}

			if !processed_filter.Test([]byte(t.ID())) {
				out <- t
			}
		}
	}()

	return out
}

func (d Database) CountTasksForStep(step types.Step) int {
	return d.datasets[step].unprocessedTasks.Count()
}

func (d Database) GetTasksForStep(step types.Step) chan types.Task {
	out := make(chan types.Task)

	go func() {
		defer close(out)

		iter := d.datasets[step].unprocessedTasks.Iterate("", "")

		for {
			var t types.Task
			err := iter(&t)

			if err != nil {
				break
			}
		}
	}()

	return out
}

func (d Database) ScheduleTasksForStep(step types.Step) int {
	iter := d.datasets[step].resources.Iterate("name", step.Input)
	count := 0

	for {
		var r types.Resource
		err := iter(&r)
		if err != nil {
			break
		}

		d.CreateTask(step, &r)
		count++
	}

	return count
}

func (d Database) ListSteps() []types.Step {
	keys := make([]types.Step, 0, len(d.datasets))
	for s := range d.datasets {
		keys = append(keys, s)
	}

	return keys
}
