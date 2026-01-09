package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/danhab99/idk/chans"
	"github.com/pelletier/go-toml"
)

func main() {
	manifest_path := flag.String("manifest", "", "manifest path")
	db_path := flag.String("db", "./db", "database path")
	parallel := flag.Int("parallel", runtime.NumCPU(), "number of processes to run in parallel")

	flag.Parse()

	fmt.Printf("Starting task-pipeline...\n")
	fmt.Printf("Loading manifest from: %s\n", *manifest_path)

	manifest_toml, err := os.ReadFile(*manifest_path)
	if err != nil {
		panic(err)
	}

	var manifest Manifest
	err = toml.Unmarshal(manifest_toml, &manifest)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Loaded %d tasks from manifest\n", len(manifest.Tasks))

	fmt.Printf("Initializing database at: %s\n", *db_path)
	database, err := NewDatabase(*db_path)
	if err != nil {
		panic(err)
	}

	bus := make([]chan Step, len(manifest.Tasks))

	fmt.Println("Registering tasks...")
	for i, task := range manifest.Tasks {
		fmt.Printf("  - %s\n", task.Name)
		database.RegisterTask(task.Name, task.Script.String)
		bus[i], err = database.IterateTasks(task.Name)
	}

	fmt.Println("Processing steps...")
	stepCount := 0

	tasks := chans.Merge(bus...)

	for goid := 0; goid < *parallel; goid++ {
		go func() {
			for task := range tasks {
				stepCount++
				runStep(&task, database)
			}
		}()
	}

	fmt.Printf("Completed processing %d steps\n", stepCount)
}
