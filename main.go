package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/pelletier/go-toml"
)

const LOG_FLAGS = log.Lshortfile | log.Lmicroseconds | log.Ldate

type stringSlice []string

func (s *stringSlice) String() string {
	return fmt.Sprintf("%v", *s)
}

func (s *stringSlice) Set(value string) error {
	*s = append(*s, value)
	return nil
}

var mainLogger = NewLogger("MAIN")

func main() {
	manifest_path := flag.String("manifest", "", "manifest path")
	db_path := flag.String("db", "./db", "database path")
	parallel := flag.Int("parallel", runtime.NumCPU(), "number of processes to run in parallel")
	exportName := flag.String("export", "", "list resource hashes by name")
	exportHash := flag.String("export-hash", "", "export file content by hash")
	runPipeline := flag.Bool("run", false, "run the pipeline")
	startStep := flag.String("start", "", "step to start from (optional, defaults to start step in manifest)")

	var enabledSteps stringSlice
	flag.Var(&enabledSteps, "step", "steps to run")

	flag.Parse()

	mainLogger.Printf("Loading manifest from: %s\n", *manifest_path)

	manifest_toml, err := os.ReadFile(*manifest_path)
	if err != nil {
		panic(err)
	}

	var manifest Manifest
	err = toml.Unmarshal(manifest_toml, &manifest)
	if err != nil {
		panic(err)
	}
	mainLogger.Printf("Loaded %d steps from manifest\n", len(manifest.Steps))

	// Check disk space before opening database
	checkDiskSpace(*db_path)

	mainLogger.Printf("Initializing database at: %s\n", *db_path)
	database, err := NewDatabase(*db_path)
	if err != nil {
		panic(err)
	}

	if *runPipeline {
		run(manifest, database, *parallel, *startStep, enabledSteps)
	} else if exportName != nil && *exportName != "" {
		exportResourcesByName(database, *exportName)
	} else if exportHash != nil && *exportHash != "" {
		exportResourceByHash(database, *exportHash)
	}
}
