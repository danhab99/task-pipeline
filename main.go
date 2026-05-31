package main

import (
	"flag"
	"fmt"
	"os"

	"grit/cmd/delete_resource"
	"grit/cmd/export"
	"grit/cmd/progress"
	"grit/cmd/prune_resources"
	"grit/cmd/resource"
	"grit/cmd/run"
	"grit/cmd/step"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "run":
		runCmd := flag.NewFlagSet("run", flag.ExitOnError)
		run.RegisterFlags(runCmd)
		runCmd.Parse(os.Args[2:])
		run.Execute()

	case "export":
		exportCmd := flag.NewFlagSet("export", flag.ExitOnError)
		export.RegisterFlags(exportCmd)
		exportCmd.Parse(os.Args[2:])
		export.Execute()

	case "progress":
		progressCmd := flag.NewFlagSet("progress", flag.ExitOnError)
		progress.RegisterFlags(progressCmd)
		progressCmd.Parse(os.Args[2:])
		progress.Execute()

	case "delete":
		deleteResourceCmd := flag.NewFlagSet("delete", flag.ExitOnError)
		delete_resource.RegisterFlags(deleteResourceCmd)
		deleteResourceCmd.Parse(os.Args[2:])
		delete_resource.Execute()

	case "prune":
		pruneResourcesCmd := flag.NewFlagSet("prune", flag.ExitOnError)
		prune_resources.RegisterFlags(pruneResourcesCmd)
		pruneResourcesCmd.Parse(os.Args[2:])
		prune_resources.Execute()

	case "help", "-h", "--help":
		printUsage()

	case "resource":
		resourceCmd := flag.NewFlagSet("resource", flag.ExitOnError)
		resource.RegisterFlags(resourceCmd)
		resourceCmd.Parse(os.Args[2:])
		resource.Execute()

	case "step":
		stepCmd := flag.NewFlagSet("step", flag.ExitOnError)
		step.RegisterFlags(stepCmd)
		stepCmd.Parse(os.Args[2:])
		step.Execute()

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("grit - Task pipeline management system")
	fmt.Println("\nUsage:")
	fmt.Println("  grit <command> [flags]")
	fmt.Println("\nAvailable commands:")
	fmt.Println("  run         Run the pipeline")
	fmt.Println("  export      Export resources from the database")
	fmt.Println("  progress    Show pipeline progress and statistics")
	fmt.Println("  delete      Delete resources and unreferenced object blobs")
	fmt.Println("  resource    CRUD operations for resources")
	fmt.Println("  step        List, view versions, and delete steps")
	fmt.Println("  prune    Prune old resource versions by keeping newest N")
	fmt.Println("\nUse 'grit <command> -h' for more information about a command.")
}
