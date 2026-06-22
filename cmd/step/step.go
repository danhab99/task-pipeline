package step

import (
	"flag"
	"fmt"
	"os"
	"sort"

	"grit/db"
)

var (
	dbPath *string
)

func RegisterFlags(fs *flag.FlagSet) {
	dbPath = fs.String("db", "./db", "database path")
}

func Execute() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "Error: missing subcommand (list, versions, delete)")
		printUsage()
		os.Exit(1)
	}

	subcommand := os.Args[2]

	switch subcommand {
	case "list":
		listCmd := flag.NewFlagSet("list", flag.ExitOnError)
		listCmd.Parse(os.Args[3:])
		listSteps()
	case "versions":
		versionsCmd := flag.NewFlagSet("versions", flag.ExitOnError)
		name := versionsCmd.String("name", "", "step name to list versions for")
		versionsCmd.Parse(os.Args[3:])
		if *name == "" {
			fmt.Fprintln(os.Stderr, "Error: -name is required for versions subcommand")
			os.Exit(1)
		}
		listVersions(*name)
	case "delete":
		deleteCmd := flag.NewFlagSet("delete", flag.ExitOnError)
		name := deleteCmd.String("name", "", "step name to delete")
		version := deleteCmd.Int("version", 0, "specific version to delete (0 = all versions)")
		deleteCmd.Parse(os.Args[3:])
		if *name == "" {
			fmt.Fprintln(os.Stderr, "Error: -name is required for delete subcommand")
			os.Exit(1)
		}
		deleteStep(*name, *version)
	default:
		fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n\n", subcommand)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: grit step <subcommand> [flags]")
	fmt.Println()
	fmt.Println("Available subcommands:")
	fmt.Println("  list       List all steps")
	fmt.Println("  versions   List all versions of a step (requires -name)")
	fmt.Println("  delete     Delete a step or specific version (requires -name)")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  -db string")
	fmt.Println("    database path (default \"./db\")")
}

func listSteps() {
	database, err := db.NewDatabase(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	steps := database.ListSteps()
	var allSteps []db.Step
	for s := range steps {
		allSteps = append(allSteps, s)
	}

	if len(allSteps) == 0 {
		fmt.Println("No steps found")
		return
	}

	// Group by name and sort versions
	stepsByName := make(map[string][]db.Step)
	for _, s := range allSteps {
		stepsByName[s.Name] = append(stepsByName[s.Name], s)
	}

	// Sort names
	var names []string
	for name := range stepsByName {
		names = append(names, name)
	}
	sort.Strings(names)

	// Find max name length for formatting
	maxNameLen := 0
	for _, name := range names {
		if len(name) > maxNameLen {
			maxNameLen = len(name)
		}
	}

	for _, name := range names {
		versions := stepsByName[name]
		// Sort by version descending
		sort.Slice(versions, func(i, j int) bool {
			return versions[i].Version > versions[j].Version
		})

		for _, s := range versions {
			parallel := "-"
			if s.Parallel != nil {
				parallel = fmt.Sprintf("%d", *s.Parallel)
			}
			fmt.Printf("  %-*s  v%-4d  parallel=%s  id=%s\n", maxNameLen, name, s.Version, parallel, s.ID)
		}
	}
}

func listVersions(name string) {
	database, err := db.NewDatabase(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	versions := database.GetStepVersions(name)
	var allVersions []db.Step
	for v := range versions {
		allVersions = append(allVersions, v)
	}

	if len(allVersions) == 0 {
		fmt.Printf("No versions found for step %s\n", name)
		return
	}

	// Sort by version descending
	sort.Slice(allVersions, func(i, j int) bool {
		return allVersions[i].Version > allVersions[j].Version
	})

	fmt.Printf("Versions for step %s:\n", name)
	for _, v := range allVersions {
		parallel := "-"
		if v.Parallel != nil {
			parallel = fmt.Sprintf("%d", *v.Parallel)
		}
		fmt.Printf("  v%-4d  parallel=%s  id=%s\n", v.Version, parallel, v.ID)
	}
}

func deleteStep(name string, version int) {
	database, err := db.NewDatabase(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	versions := database.GetStepVersions(name)
	var allVersions []db.Step
	for v := range versions {
		allVersions = append(allVersions, v)
	}

	if len(allVersions) == 0 {
		fmt.Printf("No step found with name %s\n", name)
		return
	}

	if version == 0 {
		// Delete all versions
		deleted := 0
		for _, v := range allVersions {
			if err := database.DeleteStep(v.ID); err != nil {
				fmt.Fprintf(os.Stderr, "Error deleting step version %d: %v\n", v.Version, err)
				os.Exit(1)
			}
			deleted++
		}
		fmt.Printf("Deleted %d version(s) of step %s\n", deleted, name)
	} else {
		// Delete specific version
		var found *db.Step
		for _, v := range allVersions {
			if v.Version == version {
				found = &v
				break
			}
		}
		if found == nil {
			fmt.Printf("Version %d not found for step %s\n", version, name)
			return
		}
		if err := database.DeleteStep(found.ID); err != nil {
			fmt.Fprintf(os.Stderr, "Error deleting step version %d: %v\n", version, err)
			os.Exit(1)
		}
		fmt.Printf("Deleted version %d of step %s\n", version, name)
	}
}
