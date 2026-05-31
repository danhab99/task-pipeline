// Description: Run the pipeline
package run

import (
	"bufio"
	"flag"
	"fmt"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"
	"time"

	"grit/db"
	"grit/exec"
	"grit/log"
	"grit/manifest"
	"grit/pipeline"
	"grit/types"
	"grit/utils"

	"github.com/pelletier/go-toml"
)

var runLogger = log.NewLogger("RUN")

// Command flags
var (
	manifestPath    *string
	dbPath          *string
	parallel        *int
	enabledSteps    stringSlice
	pprofAddr       *string
	profileDir      *string
	profileInterval *time.Duration
)

type stringSlice []string

func (s *stringSlice) String() string {
	return fmt.Sprintf("%v", *s)
}

func (s *stringSlice) Set(value string) error {
	*s = append(*s, value)
	return nil
}

// RegisterFlags sets up the flags for the run command
func RegisterFlags(fs *flag.FlagSet) {
	manifestPath = fs.String("manifest", "", "manifest path (required)")
	dbPath = fs.String("db", "./db", "database path")
	parallel = fs.Int("parallel", runtime.NumCPU(), "number of processes to run in parallel")
	fs.Var(&enabledSteps, "step", "steps to run (can be specified multiple times)")
}

// Execute runs the pipeline
func Execute() {
	// Set before any allocations so every allocation is sampled.
	runtime.MemProfileRate = 1

	if *manifestPath == "" {
		fmt.Fprintf(os.Stderr, "Error: -manifest is required\n")
		os.Exit(1)
	}

	runLogger.Printf("Loading manifest from: %s\n", *manifestPath)

	manifestToml, err := os.ReadFile(*manifestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading manifest: %v\n", err)
		os.Exit(1)
	}

	var m manifest.Manifest
	err = toml.Unmarshal(manifestToml, &m)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing manifest: %v\n", err)
		os.Exit(1)
	}
	runLogger.Printf("Loaded %d steps from manifest\n", len(m.Steps))

	// Check disk space before opening database
	utils.CheckDiskSpace(*dbPath)

	runLogger.Printf("Initializing database at: %s\n", *dbPath)
	database, err := db.NewDatabase(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	// Limit the Go heap to 512 MB so the GC scavenger returns idle pages to
	// the OS aggressively instead of sitting on hundreds of MB indefinitely.
	debug.SetMemoryLimit(512 << 20)

	stopGC := make(chan struct{})
	database.StartValueLogGC(30*time.Second, stopGC)
	defer close(stopGC)

	run(m, database, *parallel, enabledSteps)
}

// readRSSKB returns the current process RSS in kilobytes by reading
// /proc/self/status (Linux only). Returns 0 on any error.
func readRSSKB() int64 {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return 0
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				v, _ := strconv.ParseInt(fields[1], 10, 64)
				return v
			}
		}
	}
	return 0
}

// Each tick produces:
//
//	profiles/heap_001.pprof  – inuse/alloc heap snapshot
//	profiles/cpu_001.pprof   – CPU activity over the interval
//
// Returns a stop func that flushes one final snapshot before returning.
func startPeriodicProfiles(dir string, interval time.Duration) func() {
	seq := 0
	writeSnapshot := func(label string) {
		seq++
		tag := fmt.Sprintf("%s%03d", label, seq)

		// heap
		heapPath := filepath.Join(dir, tag+"_heap.pprof")
		if f, err := os.Create(heapPath); err == nil {
			runtime.GC()
			if err := pprof.WriteHeapProfile(f); err != nil {
				runLogger.Printf("heap profile write error: %v\n", err)
			}
			f.Close()
			runLogger.Printf("heap profile → %s\n", heapPath)
		} else {
			runLogger.Printf("heap profile create error: %v\n", err)
		}

		// goroutine snapshot (cheap; useful for leak detection alongside heap)
		gorPath := filepath.Join(dir, tag+"_goroutines.txt")
		if f, err := os.Create(gorPath); err == nil {
			pprof.Lookup("goroutine").WriteTo(f, 1)
			f.Close()
		}
	}

	// CPU profile runs for one interval, then rotates.
	startCPU := func(tag string) (*os.File, error) {
		cpuPath := filepath.Join(dir, tag+"_cpu.pprof")
		f, err := os.Create(cpuPath)
		if err != nil {
			return nil, err
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			f.Close()
			return nil, err
		}
		return f, nil
	}

	cpuSeq := 0
	nextCPUTag := func() string {
		cpuSeq++
		return fmt.Sprintf("cpu%03d", cpuSeq)
	}

	// Start first CPU interval immediately.
	cpuFile, cpuErr := startCPU(nextCPUTag())
	if cpuErr != nil {
		runLogger.Printf("CPU profile start error: %v\n", cpuErr)
	}

	runLogger.Printf("Periodic profiling every %s → %s\n", interval, dir)

	ticker := time.NewTicker(interval)
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-ticker.C:
				// Stop current CPU interval and save it.
				if cpuFile != nil {
					pprof.StopCPUProfile()
					cpuFile.Close()
					runLogger.Printf("cpu profile → %s\n", cpuFile.Name())
					cpuFile = nil
				}
				writeSnapshot("snap")
				// Begin next CPU interval.
				cpuFile, cpuErr = startCPU(nextCPUTag())
				if cpuErr != nil {
					runLogger.Printf("CPU profile start error: %v\n", cpuErr)
				}
			case <-done:
				return
			}
		}
	}()

	stop := func() {
		ticker.Stop()
		close(done)
		if cpuFile != nil {
			pprof.StopCPUProfile()
			cpuFile.Close()
			runLogger.Printf("cpu profile → %s\n", cpuFile.Name())
		}
		writeSnapshot("final")
	}
	return stop
}

func constructRunnerPipeline(m manifest.Manifest, database db.Database, enabledSteps []string) ([]types.Step, *pipeline.Pipeline, func()) {
	steps := m.RegisterSteps(&database, enabledSteps)

	runLogger.Printf("Registered %d steps\n", len(steps))

	executor := exec.NewScriptExecutor(&database)

	// Create pipeline
	pipeline, err := pipeline.NewPipeline(executor, &database)
	if err != nil {
		panic(err)
	}

	stop := func() {
		// database.Close() is handled by the caller's defer
	}

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGABRT, os.Interrupt, os.Kill, syscall.SIGTERM)
		<-c
		stop()
	}()

	return steps, pipeline, stop
}

func run(m manifest.Manifest, database db.Database, parallel int, enabledSteps []string) {
	startTime := time.Now()

	// Ingest CSV files before pipeline execution
	if len(m.CsvFiles) > 0 {
		csvCount, err := m.IngestCsvFiles(&database)
		if err != nil {
			runLogger.Printf("Error ingesting CSV files: %v\n", err)
			panic(err)
		}
		if csvCount > 0 {
			runLogger.Printf("Ingested %d rows from %d CSV file(s)\n", csvCount, len(m.CsvFiles))
		}
	}

	steps, pipeline, stop := constructRunnerPipeline(m, database, enabledSteps)
	defer stop()

	// Check if we need to seed
	resourceCount, err := database.CountResources()
	if err != nil {
		panic(err)
	}

	// Execute all steps
	var totalStepExecutions int64

	if resourceCount == 0 {
		runLogger.Printf("No resources found, running seed steps\n")

		for step := range database.GetStepsWithZeroInputs() {
			totalStepExecutions += pipeline.ExecuteStep(step, parallel)
		}
	}

	resourceCount, err = database.CountResources()
	if err != nil {
		panic(err)
	}

	if resourceCount == 0 {
		panic("No resources were seeded")
	}

	// run twice to check that everything is done
	for range 2 {
		for _, step := range steps {
			executions := pipeline.ExecuteStep(step, parallel)
			totalStepExecutions += executions

			if executions > 0 {
				runLogger.Printf("Step %s: executed %d tasks\n", step.Name, executions)
			}

			// After each step, return idle Go heap pages to the OS so that
			// long multi-day runs don't accumulate RSS from previous steps.
			runtime.GC()
			debug.FreeOSMemory()
		}
	}

	duration := time.Since(startTime)
	runLogger.Printf("Pipeline complete: %d step tasks executed in %s\n", totalStepExecutions, duration.Round(time.Millisecond))
}
