# Task Pipeline

A Go-based task pipeline system that executes shell scripts in a managed workflow with persistent state tracking.

## Quick Start

### Using Nix Flakes

#### Build with Nix
```bash
nix build
./result/bin/task-pipeline --help
```

#### Run directly
```bash
nix run . -- -manifest workflow.toml -db ./db -run
```

#### Add to your NixOS configuration

Add this flake as an input to your NixOS configuration flake:

```nix
# In your NixOS flake.nix
{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    task-pipeline = {
      url = "path:/home/dan/Documents/go/src/task-pipeline";
      # Or use a git repository:
      # url = "github:yourusername/task-pipeline";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, task-pipeline, ... }: {
    nixosConfigurations.yourhostname = nixpkgs.lib.nixosSystem {
      system = "x86_64-linux";
      modules = [
        ./configuration.nix
        {
          # Add to system packages
          environment.systemPackages = [
            task-pipeline.packages.x86_64-linux.default
          ];
        }
      ];
    };
  };
}
```

After rebuilding your system, `task-pipeline` will be available system-wide:
```bash
sudo nixos-rebuild switch --flake .#yourhostname
task-pipeline --help
```

Alternatively, add it to your home-manager configuration:
```nix
# In home.nix or similar
{ inputs, ... }:
{
  home.packages = [
    inputs.task-pipeline.packages.x86_64-linux.default
  ];
}
```

### Build without Nix
```bash
go build -o task-pipeline
```

### Run a Pipeline
```bash
./task-pipeline -manifest workflow.toml -db ./my-pipeline-db -run
```

### Create a Manifest (workflow.toml)
```toml
[[step]]
name = "start"
start = true
script = '''
echo "Input data" > $OUTPUT_DIR/process
echo "More data" > $OUTPUT_DIR/transform
'''

[[step]]
name = "process"
script = '''
cat $INPUT_FILE | tr '[:lower:]' '[:upper:]' > $OUTPUT_DIR/next
'''

[[step]]
name = "transform"
script = '''
cat $INPUT_FILE | sort > $OUTPUT_DIR/next
'''
```

### Common Commands
```bash
# Run the pipeline
./task-pipeline -manifest manifest.toml --db ./db -run

# Detect and migrate tainted steps (when a step's script changes)
./task-pipeline -manifest manifest.toml --db ./db -migrate-tainted

# Run with parallel limit
./task-pipeline -manifest manifest.toml --db ./db -run -parallel 4

# Specify starting step
./task-pipeline -manifest manifest.toml --db ./db -run -start process_name
```

## Overview

Task Pipeline is a workflow automation tool that:
- Executes tasks defined in a TOML manifest
- Stores task execution state in a SQLite database
- Manages task inputs/outputs through a content-addressable object store
- Supports task chaining and dependencies through step linking
- Processes multiple task streams concurrently
- Automatically detects and handles step versioning/tainted steps

## Development

### Nix Development Shell
```bash
# Enter development environment with all dependencies
nix develop

# Now you have go, gopls, delve, sqlite, etc.
go build
go test
```

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
Defines the step structure from TOML configuration files.

### 3. **Database (`db.go`)**
Manages persistent storage with:
- **Steps table**: Stores step definitions (name, script, version)
- **Tasks table**: Tracks individual task executions with content hashes and processing status
- **Object store**: Content-addressable storage using SHA-256 hashes
- **Step versioning**: Automatically tracks script changes as new versions

The object store uses a sharded directory structure: `objects/AB/CD/EFGH...` where `ABCDEFGH...` is the full hash.

### 4. **Executor (`pipeline.go`)**
Runs individual tasks by:
- Writing input objects to temporary files
- Executing the step's shell script with environment variables
- Reading output files from a designated directory
- Creating new tasks in the database for downstream steps
- Handling step versioning and tainted task migration

## Usage

```bash
task-pipeline -manifest <path-to-manifest.toml> -db <database-directory> [options]
```

### Command-Line Flags

- `-manifest` (required): Path to the TOML manifest file defining steps
- `-db` (default: `./db`): Directory for database and object storage
- `-run`: Execute the pipeline
- `-migrate-tainted`: Detect steps with changed scripts and migrate their tasks to new versions
- `-parallel` (default: number of CPUs): Maximum concurrent tasks to execute
- `-start`: Name of the step to start from (defaults to step with `start=true`)

### Manifest Format

Create a TOML file with step definitions:

```toml
[[step]]
name = "extract"
start = true
script = """
# Initial step - generates output files
curl https://api.example.com/data > $OUTPUT_DIR/data
"""

[[step]]
name = "process"
script = """
# Process the data and generate output
process-tool < $INPUT_FILE > $OUTPUT_DIR/result
"""

[[step]]
name = "transform"
script = """
# Transform and output to next step
transform-tool < $INPUT_FILE > $OUTPUT_DIR/final
"""
```

### Environment Variables for Scripts

Each step script receives:
- `INPUT_FILE`: Path to the input file (previous step's output or empty for start step)
- `OUTPUT_DIR`: Directory where the script should write output files

Output filenames determine which step processes them next. For example:
- `result_next.txt` → routes to `next` step
- `final_transform.txt` → routes to `transform` step
- `done.txt` → final output (no further processing)

## Step Versioning & Tainted Steps

When you modify a step's script in your manifest and run it again, Task Pipeline:

1. Detects the script change and creates a new step version
2. Identifies "tainted" tasks (those running the old script)
3. Can automatically migrate and reprocess them with the new script

### Workflow
```bash
# Initial run
./task-pipeline -manifest workflow.toml --db ./db -run

# Edit workflow.toml - change a step's script

# Detect and migrate tainted steps
./task-pipeline -manifest workflow.toml --db ./db -migrate-tainted

# Run again - reprocesses with new script
./task-pipeline -manifest workflow.toml --db ./db -run
```

Old tasks are preserved as historical records while new tasks reprocess with the updated logic.

## Database Schema

### Tables

- **step**: Step definitions and versions
  - `id`: Auto-increment primary key
  - `name`: Step name
  - `script`: Shell script to execute
  - `version`: Auto-incrementing version when script changes
  - `is_start`: Whether this is the starting step
  - `parallel`: Maximum parallel execution limit

- **task**: Task execution instances
  - `id`: Auto-increment primary key
  - `object_hash`: SHA-256 hash of the input object
  - `step_id`: Foreign key to step table
  - `input_task_id`: Foreign key linking to parent task
  - `processed`: 0/1 flag for completion status
  - `error`: Error message if task failed
  - `runset`: Timestamp grouping related tasks

### Indexes

- `idx_step_name`: Fast step lookup by name
- `idx_task_step`: Efficient task filtering by step
- `idx_task_processed`: Quick filtering of unprocessed tasks
- `idx_task_input`: Find downstream tasks by input task ID

## Features

- **Content-Addressable Storage**: Deduplicates outputs using SHA-256 hashing
- **Concurrent Processing**: Handles multiple task streams simultaneously
- **Persistent State**: Maintains execution history in SQLite
- **Step Versioning**: Automatically tracks script changes
- **Tainted Step Migration**: Detects and reprocesses tasks when steps change
- **Dependency Management**: Links tasks through input/output relationships
- **WAL Mode**: Enables concurrent reads/writes to the database
- **Shell Script Flexibility**: Execute any shell command or script

## Dependencies

- `github.com/danhab99/idk/chans`: Channel utilities for stream merging
- `github.com/danhab99/idk/workers`: Worker pool for parallel processing
- `github.com/pelletier/go-toml`: TOML parsing
- `database/sql`: SQLite database access
- `github.com/mattn/go-sqlite3`: SQLite driver
- Standard Go libraries

## Example Workflow

1. Create a manifest file `workflow.toml`:
```toml
[[step]]
name = "fetch"
start = true
script = "echo 'sample data' > $OUTPUT_DIR/process"

[[step]]
name = "process"
script = "tr '[:lower:]' '[:upper:]' < $INPUT_FILE > $OUTPUT_DIR/done"
```

2. Run the pipeline:
```bash
./task-pipeline -manifest workflow.toml -db ./my-db -run
```

3. Modify the process step in workflow.toml:
```toml
[[step]]
name = "process"
script = "tr '[:lower:]' '[:upper:]' < $INPUT_FILE | sort > $OUTPUT_DIR/done"
```

4. Migrate tainted tasks:
```bash
./task-pipeline -manifest workflow.toml -db ./my-db -migrate-tainted
```

5. Run again to reprocess with new logic:
```bash
./task-pipeline -manifest workflow.toml -db ./my-db -run
```

## Output

The program prints detailed logs during execution:
- Manifest loading and step count
- Database initialization
- Tainted step detection and migration counts
- Step execution with IDs and step names
- Script execution details
- Output file generation
- Task processing counts
- Total steps processed
