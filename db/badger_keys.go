package db

import "fmt"

// Primary entity key prefixes
const (
	prefixStep     = "s:"
	prefixTask     = "t:"
	prefixResource = "r:"
	prefixObject   = "o:"
	prefixMeta     = "m:"
)

// Index key prefixes.
//
// Key formats use \x00 as a field separator so that prefix scans terminate
// cleanly at the boundary of each segment.  Versions are zero-padded to 8
// digits so lexicographic order equals numeric order.
const (
	// idxStepByName indexes all versions of a step by name.
	// Scan the prefix "ix:sn:{name}\x00" to find all versions; the last key
	// (highest zero-padded version) is the current one.
	// Key: ix:sn:{name}\x00{version_08d}\x00{step_ulid}
	idxStepByName = "ix:sn:"

	// idxTaskByStepUnproc is the scheduler's work queue: all tasks for a step
	// that have not yet been executed.  Deleted from when a task is marked done.
	// Key: ix:tsu:{step_ulid}\x00{task_ulid}
	idxTaskByStepUnproc = "ix:tsu:"

	// idxTaskByStepProc mirrors idxTaskByStepUnproc for completed tasks.
	// Key: ix:tsp:{step_ulid}\x00{task_ulid}
	idxTaskByStepProc = "ix:tsp:"

	// idxTaskByStepAll covers every task for a step regardless of status.
	// Used for counts and bulk operations (e.g. MarkStepUndone).
	// Key: ix:tsa:{step_ulid}\x00{task_ulid}
	idxTaskByStepAll = "ix:tsa:"

	// idxTaskUnique enforces the constraint that a step processes each input
	// resource at most once.  Value is the task ULID.
	// Key: ix:tu:{step_ulid}\x00{resource_ulid}  →  task_ulid
	idxTaskUnique = "ix:tu:"

	// idxResourceByName lists all resources with a given name ordered by ULID
	// (creation time).  This is the index ScheduleTasksForStep pages through.
	// Key: ix:rn:{name}\x00{resource_ulid}
	idxResourceByName = "ix:rn:"

	// idxResourceHash deduplicates resources: same (name, content) reuses the
	// existing record.  Value is the resource ULID.
	// Key: ix:rh:{name}\x00{object_hash}  →  resource_ulid
	idxResourceHash = "ix:rh:"

	// idxResourceRoute is the typed resource router used by step polling.
	// Key: ix:rr:{resource_type}\x00{status}\x00{resource_ulid}
	idxResourceRoute = "ix:rr:"

)

const (
	ResourceStatusUnprocessed = "unprocessed"
	ResourceStatusProcessing  = "processing"
)

// --- Primary key builders ---

func stepKey(id string) []byte     { return []byte(prefixStep + id) }
func taskKey(id string) []byte     { return []byte(prefixTask + id) }
func resourceKey(id string) []byte { return []byte(prefixResource + id) }
func objectKey(hash []byte) []byte { return append([]byte(prefixObject), hash...) }

// --- Index key builders ---

func idxStepByNameKey(name string, version int, id string) []byte {
	return []byte(fmt.Sprintf("%s%s\x00%08d\x00%s", idxStepByName, name, version, id))
}

func idxTaskByStepUnprocKey(stepID, taskID string) []byte {
	return []byte(idxTaskByStepUnproc + stepID + "\x00" + taskID)
}

func idxTaskByStepProcKey(stepID, taskID string) []byte {
	return []byte(idxTaskByStepProc + stepID + "\x00" + taskID)
}

func idxTaskByStepAllKey(stepID, taskID string) []byte {
	return []byte(idxTaskByStepAll + stepID + "\x00" + taskID)
}

func idxTaskUniqueKey(stepID, resourceID string) []byte {
	return []byte(idxTaskUnique + stepID + "\x00" + resourceID)
}

func idxResourceByNameKey(name, id string) []byte {
	return []byte(idxResourceByName + name + "\x00" + id)
}

func idxResourceHashKey(name, objectHash string) []byte {
	return []byte(idxResourceHash + name + "\x00" + objectHash)
}

func idxResourceRouteKey(resourceType, status, id string) []byte {
	return []byte(idxResourceRoute + resourceType + "\x00" + status + "\x00" + id)
}

// --- Prefix builders for scans ---

func idxStepByNamePrefix(name string) []byte {
	return []byte(idxStepByName + name + "\x00")
}

func idxTaskByStepUnprocPrefix(stepID string) []byte {
	return []byte(idxTaskByStepUnproc + stepID + "\x00")
}

func idxTaskByStepAllPrefix(stepID string) []byte {
	return []byte(idxTaskByStepAll + stepID + "\x00")
}

func idxResourceByNamePrefix(name string) []byte {
	return []byte(idxResourceByName + name + "\x00")
}

func idxResourceRoutePrefix(resourceType, status string) []byte {
	return []byte(idxResourceRoute + resourceType + "\x00" + status + "\x00")
}

// --- Meta key builders ---

func metaCsvHashKey(path string) []byte {
	return []byte(prefixMeta + "csvhash:" + path)
}

func metaCsvOffsetKey(path string) []byte {
	return []byte(prefixMeta + "csvoffset:" + path)
}
