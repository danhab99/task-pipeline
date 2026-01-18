# Task Pipeline Visibility Features

## Overview
Enhanced visibility features including verbose logging, color-coded output, and progress bars to better understand pipeline execution.

## New Command-Line Flags

### `--verbose` / `-verbose`
Enable detailed logging to see everything happening in the pipeline:
- Detailed step registration
- Input/output file operations
- Script execution details
- Script stdout/stderr output
- Task completion details

**Example:**
```bash
./task-pipeline --manifest notes/example.toml --run --verbose
```

### `--quiet` / `-quiet`
Minimal output mode for automation and scripts:
- Suppresses progress bars
- Only shows critical errors
- Suitable for CI/CD pipelines

**Example:**
```bash
./task-pipeline --manifest notes/example.toml --run --quiet
```

## Output Features

### 1. **Color-Coded Logging**
Different components use distinct colors for easy identification:
- 🟣 **Magenta**: Main program operations
- 🔵 **Blue**: Run controller
- 🟢 **Cyan**: Pipeline execution
- 🟡 **Yellow**: Script output
- 🟢 **Green**: Export operations and success messages
- 🔴 **Red**: Errors

### 2. **Progress Bars**
Real-time progress indicators for each step showing:
- Current/total tasks
- Completion percentage
- Tasks per second
- Elapsed time
- Visual progress bar with green indicators

**Example output:**
```
  Processing fetch █████████████████████████-----  75% [3/4] [2.1 it/s]
```

### 3. **Execution Summary**
At the end of each run, a formatted summary box displays:
- Total tasks processed
- Total execution rounds
- Number of parallel workers
- Total execution time
- Average task processing rate

**Example output:**
```
╔══════════════════════════════════════════════════════════╗
║ Pipeline Execution Summary                               ║
╠══════════════════════════════════════════════════════════╣
║ Total Tasks:         42                                  ║
║ Total Rounds:        3                                   ║
║ Parallel Workers:    8                                   ║
║ Execution Time:      2.345s                              ║
║ Avg Task Rate:       17.91 tasks/sec                     ║
╚══════════════════════════════════════════════════════════╝
```

### 4. **Step Indicators**
Clear visual indicators for step execution:
```
▶ Step: fetch
  → Task 1 | Step: fetch
    Processing fetch ███████████████████ 100% [10/10]
  Step 'fetch' complete: 10/10 tasks
```

## Log Levels

### Normal Mode (default)
- Shows step execution
- Task progress bars
- Important operations
- Warnings and errors
- Summary statistics

### Verbose Mode (`--verbose`)
- Everything in Normal mode PLUS:
- Detailed task information
- Input/output file paths and sizes
- Script commands being executed
- Script stdout/stderr in real-time
- Database operations

### Quiet Mode (`--quiet`)
- Minimal output
- Only critical errors
- No progress bars
- Suitable for automation

## Usage Examples

### Standard Run
```bash
./task-pipeline --manifest notes/example.toml --run --parallel 4
```

### Verbose Debugging
```bash
./task-pipeline --manifest notes/example.toml --run --verbose
```

### Automation-Friendly
```bash
./task-pipeline --manifest notes/example.toml --run --quiet
```

### Exporting with Verbose Output
```bash
./task-pipeline --manifest notes/example.toml --export done --verbose
```

## Implementation Details

### New Files
- **logger.go**: Colored, leveled logging system with progress bar support

### Modified Files
- **main.go**: Added verbose/quiet flags and log level configuration
- **run.go**: Enhanced with summary statistics and execution tracking
- **pipeline.go**: Added progress bars and improved task logging
- **export.go**: Colorized export output

### Dependencies
- `github.com/fatih/color`: Terminal color output
- `github.com/schollz/progressbar/v3`: Progress bar visualization

## Tips

1. **Use verbose mode** when developing or debugging manifests to see exactly what's happening
2. **Use quiet mode** in scripts and automation where you only want error output
3. **Normal mode** provides good visibility without overwhelming detail for regular usage
4. Progress bars automatically adapt to terminal width
5. All log output goes to stderr, keeping stdout clean for piping results
