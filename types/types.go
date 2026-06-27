package types

import (
	"fmt"
	"time"
)

type Step struct {
	Name       string         `msgpack:"name"`
	Script     string         `msgpack:"script"`
	ScriptHash string         `msgpack:"script_hash"`
	Parallel   *int           `msgpack:"parallel,omitempty"`
	Input      string         `msgpack:"input,omitempty"`
	Timeout    *time.Duration `msgpack:"timeout,omitempty"`
	Version    int            `msgpack:"version"`
}

func (s Step) ID() string {
	return s.Name + s.ScriptHash
}

type Task struct {
	Id              string  `msgpack:"id"`
	StepID          string  `msgpack:"step_id"`
	InputResourceID *string `msgpack:"input_resource_id,omitempty"`
	Processed       bool    `msgpack:"processed"`
	Error           *string `msgpack:"error,omitempty"`
}

func (t Task) ID() string {
	return t.Id
}

const (
	StorageBackendInline = "inline"
	StorageBackendFS     = "fs"
)

type Resource struct {
	Name            string  `msgpack:"name"`
	ObjectHash      string  `msgpack:"object_hash"`
	CreatedAt       string  `msgpack:"created_at"`
	CreatedByTaskID *string `msgpack:"created_by_task_id,omitempty"`
	Data            []byte  `msgpack:"data,omitempty"`
}

func (r Resource) ID() string {
	return *r.CreatedByTaskID + r.ObjectHash
}

func (t Task) String() string {
	var e string
	if t.Error == nil {
		e = "NIL"
	} else {
		e = *t.Error
	}
	return fmt.Sprintf("Task(id=%s step_id=%s processed=%v error=%s)", t.ID, t.StepID, t.Processed, e)
}
