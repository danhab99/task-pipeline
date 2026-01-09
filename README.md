# Task Pipeline

A Go-based task pipeline system that executes shell scripts in a managed workflow with persistent state tracking.

## Overview

Task Pipeline is a workflow automation tool that:
- Executes tasks defined in a TOML manifest
- Stores task execution state in a SQLite database
- Manages task inputs/outputs through a content-addressable object store
- Supports task chaining and dependencies through step linking
- Processes multiple task streams concurrently

## Architecture

The system consists of four main components:

### 1. **Main (`main.go`)**
Entry point that:
- Parses command-line flags for manifest and database paths
- Loads and parses the TOML manifest
- Initializes the database
- Registers tasks and creates processing channels
- Merges task streams and executes steps

### 2. **Manifest (`manifest.go`)**
Defines the task structure from TOML configuration files.

### 3. **Database (`db.go`)**
Manages persistent storage with:
- **Tasks table**: Stores task definitions (name, script)
- **Steps table**: Tracks individual task executions with content hashes
- **Step links table**: Manages dependencies between steps
- **Object store**: Content-addressable storage using SHA-256 hashes

The object store uses a sharded directory structure: `objects/AB/CD/EFGH...` where `ABCDEFGH...` is the full hash.

### 4. **Executor (`run.go`)**
Runs individual steps by:
- Writing input objects to temporary files
- Executing the task's shell script with environment variables
- Reading output files from a designated directory
- Inserting new steps into the database for downstream processing

## Usage

```bash
task-pipeline -manifest <path-to-manifest.toml> -db <database-directory>
```

### Command-Line Flags

- `-manifest` (required): Path to the TOML manifest file defining tasks
- `-db` (default: `./db`): Directory for database and object storage

### Manifest Format

Create a TOML file with task definitions:

```toml
[[task]]
name = "process-data"
script = """
#!/bin/bash
cat $INPUT_FILE | process-tool > $OUTPUT_DIR/result.txt
"""

[[task]]
name = "transform"
script = """
#!/bin/bash
transform-tool < $INPUT_FILE > $OUTPUT_DIR/transformed.json
"""
```

### Environment Variables for Scripts

Each task script receives:
- `INPUT_FILE`: Path to the input file (previous step's output or initial input)
- `OUTPUT_DIR`: Directory where the script should write output files

## How It Works

1. **Initialization**: Loads manifest and initializes SQLite database with schema
2. **Task Registration**: Registers each task from the manifest
3. **Step Iteration**: Queries for unprocessed steps (leaf nodes in the dependency graph)
4. **Concurrent Processing**: Merges multiple task channels and processes steps as they become available
5. **Execution**: For each step:
   - Writes input object to temporary file
   - Executes shell script with environment variables
   - Collects output files from output directory
   - Inserts new steps with output objects, creating dependencies
6. **Continuation**: New steps are queued for processing, enabling pipeline progression

## Database Schema

### Tables

- **task**: Task definitions
  - `id`: Auto-increment primary key
  - `name`: Unique task name
  - `script`: Shell script to execute

- **step**: Execution instances
  - `id`: Auto-increment primary key
  - `object_hash`: SHA-256 hash of the output object
  - `task`: Foreign key to task table

- **step_link**: Task dependencies
  - `from_step_id`: Source step
  - `to_step_id`: Destination step
  - Composite primary key ensures unique links

### Indexes

- `idx_task_name`: Fast task lookup by name
- `idx_step_task`: Efficient step filtering by task
- `idx_step_link_from`: Quick dependency traversal

## Features

- **Content-Addressable Storage**: Deduplicates outputs using SHA-256 hashing
- **Concurrent Processing**: Handles multiple task streams simultaneously
- **Persistent State**: Maintains execution history in SQLite
- **Dependency Management**: Links steps to form execution chains
- **WAL Mode**: Enables concurrent reads/writes to the database
- **Shell Script Flexibility**: Execute any shell command or script

## Dependencies

- `github.com/danhab99/idk/chans`: Channel utilities for stream merging
- `github.com/pelletier/go-toml`: TOML parsing
- `database/sql`: SQLite database access
- Standard Go libraries

## Building

```bash
go build -o task-pipeline
```

## Example Workflow

1. Create a manifest file `workflow.toml`:
```toml
[[task]]
name = "fetch"
script = "curl -o $OUTPUT_DIR/data.json https://api.example.com/data"

[[task]]
name = "process"
script = "jq '.items[]' < $INPUT_FILE > $OUTPUT_DIR/processed.json"
```

2. Run the pipeline:
```bash
./task-pipeline -manifest workflow.toml -db ./my-pipeline-db
```

3. Monitor execution through printed logs showing:
   - Tasks being registered
   - Steps being executed
   - Output files being generated
   - Database operations

## Output

The program prints detailed logs during execution:
- Manifest loading and task count
- Database initialization
- Task registration
- Step execution with IDs and task names
- Script execution details
- Output file generation counts
- Total steps processed

## License

[Add your license here]
