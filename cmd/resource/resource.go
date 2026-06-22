package resource

import (
	"flag"
	"fmt"
	"os"
	"sort"

	"grit/db"
)

var (
	dbPath string
)

func RegisterFlags(fs *flag.FlagSet) {
	fs.StringVar(&dbPath, "db", "./db", "database path")
}

func Execute() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "Error: missing subcommand (names, list, get, create, delete)")
		printUsage()
		os.Exit(1)
	}

	subcommand := os.Args[2]

	switch subcommand {
	case "names":
		namesCmd := flag.NewFlagSet("names", flag.ContinueOnError)
		namesCmd.StringVar(&dbPath, "db", dbPath, "database path")
		namesCmd.Parse(os.Args[3:])
		listResourceNames()
	case "list":
		listCmd := flag.NewFlagSet("list", flag.ContinueOnError)
		listCmd.StringVar(&dbPath, "db", dbPath, "database path")
		name := listCmd.String("name", "", "filter by resource name")
		listCmd.Parse(os.Args[3:])
		listResources(*name)
	case "get":
		getCmd := flag.NewFlagSet("get", flag.ContinueOnError)
		getCmd.StringVar(&dbPath, "db", dbPath, "database path")
		id := getCmd.String("id", "", "resource ID to get")
		getCmd.Parse(os.Args[3:])
		if *id == "" {
			fmt.Fprintln(os.Stderr, "Error: -id is required for get subcommand")
			os.Exit(1)
		}
		getResource(*id)
	case "create":
		createCmd := flag.NewFlagSet("create", flag.ContinueOnError)
		createCmd.StringVar(&dbPath, "db", dbPath, "database path")
		name := createCmd.String("name", "", "resource name")
		hash := createCmd.String("hash", "", "object hash")
		createCmd.Parse(os.Args[3:])
		if *name == "" || *hash == "" {
			fmt.Fprintln(os.Stderr, "Error: -name and -hash are required for create subcommand")
			os.Exit(1)
		}
		createResource(*name, *hash)
	case "delete":
		deleteCmd := flag.NewFlagSet("delete", flag.ContinueOnError)
		deleteCmd.StringVar(&dbPath, "db", dbPath, "database path")
		id := deleteCmd.String("id", "", "resource ID to delete")
		name := deleteCmd.String("name", "", "delete all resources with this name")
		deleteCmd.Parse(os.Args[3:])
		if *id == "" && *name == "" {
			fmt.Fprintln(os.Stderr, "Error: specify exactly one of -id or -name for delete subcommand")
			os.Exit(1)
		}
		if *id != "" && *name != "" {
			fmt.Fprintln(os.Stderr, "Error: specify exactly one of -id or -name for delete subcommand")
			os.Exit(1)
		}
		deleteResource(*id, *name)
	default:
		fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n\n", subcommand)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: grit resource <subcommand> [flags]")
	fmt.Println()
	fmt.Println("Available subcommands:")
	fmt.Println("  names    List unique resource names with counts")
	fmt.Println("  list     List all resources (optionally filter by -name)")
	fmt.Println("  get      Get a specific resource by ID (requires -id)")
	fmt.Println("  create   Create a new resource (requires -name and -hash)")
	fmt.Println("  delete   Delete a resource by ID or name (requires -id or -name)")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  -db string")
	fmt.Println("    database path (default \"./db\")")
}

func listResources(filterName string) {
	database, err := db.NewDatabase(	dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	var resources []db.Resource
	if filterName != "" {
		for r := range database.GetResourcesByName(filterName) {
			resources = append(resources, r)
		}
	} else {
		for r := range database.GetAllResources() {
			resources = append(resources, r)
		}
	}

	if len(resources) == 0 {
		if filterName != "" {
			fmt.Printf("No resources found for name=%s\n", filterName)
		} else {
			fmt.Println("No resources found")
		}
		return
	}

	// Sort by creation time (newest first)
	sort.Slice(resources, func(i, j int) bool {
		return resources[i].CreatedAt > resources[j].CreatedAt
	})

	// Find max name length for formatting
	maxNameLen := 0
	for _, r := range resources {
		if len(r.Name) > maxNameLen {
			maxNameLen = len(r.Name)
		}
	}

	for _, r := range resources {
		createdBy := "-"
		if r.CreatedByTaskID != nil {
			createdBy = *r.CreatedByTaskID
		}
		fmt.Printf("  %-*s  id=%s  hash=%s  created=%s  by_task=%s\n",
			maxNameLen, r.Name, r.ID, r.ObjectHash, r.CreatedAt, createdBy)
	}
}

func listResourceNames() {
	database, err := db.NewDatabase(	dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	counts := make(map[string]int)
	for r := range database.GetAllResources() {
		counts[r.Name]++
	}

	if len(counts) == 0 {
		fmt.Println("No resources found")
		return
	}

	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Strings(names)

	maxNameLen := 0
	for _, name := range names {
		if len(name) > maxNameLen {
			maxNameLen = len(name)
		}
	}

	total := 0
	for _, name := range names {
		fmt.Printf("  %-*s  %d\n", maxNameLen, name, counts[name])
		total += counts[name]
	}
	fmt.Printf("  %d unique names, %d total resources\n", len(names), total)
}

func getResource(id string) {
	database, err := db.NewDatabase(	dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	r, err := database.GetResource(id)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting resource: %v\n", err)
		os.Exit(1)
	}
	if r == nil {
		fmt.Printf("No resource found for id=%s\n", id)
		return
	}

	createdBy := "-"
	if r.CreatedByTaskID != nil {
		createdBy = *r.CreatedByTaskID
	}
	fmt.Printf("  name=%s\n", r.Name)
	fmt.Printf("  id=%s\n", r.ID)
	fmt.Printf("  hash=%s\n", r.ObjectHash)
	fmt.Printf("  created=%s\n", r.CreatedAt)
	fmt.Printf("  by_task=%s\n", createdBy)
}

func createResource(name, hash string) {
	database, err := db.NewDatabase(	dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	resourceID, err := database.CreateResource(name, hash)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating resource: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created resource id=%s name=%s hash=%s\n", resourceID, name, hash)
}

func deleteResource(id, name string) {
	database, err := db.NewDatabase(	dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	if id != "" {
		result, err := database.DeleteResourceHard(id)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error deleting resource %s: %v\n", id, err)
			os.Exit(1)
		}
		if !result.ResourceDeleted {
			fmt.Printf("No resource found for id=%s\n", id)
			return
		}

		fmt.Printf("Deleted resource id=%s name=%s hash=%s\n", result.ResourceID, result.Name, result.ObjectHash)
		if result.ObjectDeleted {
			fmt.Printf("Deleted object hash=%s (remaining_refs=%d)\n", result.ObjectHash, result.RemainingObjectRefs)
		} else {
			fmt.Printf("Kept object hash=%s (remaining_refs=%d)\n", result.ObjectHash, result.RemainingObjectRefs)
		}
		return
	}

	resourceIDs := make([]string, 0)
	for r := range database.GetResourcesByName(name) {
		resourceIDs = append(resourceIDs, r.ID)
	}

	if len(resourceIDs) == 0 {
		fmt.Printf("No resources found for name=%s\n", name)
		return
	}

	resourcesDeleted := 0
	objectsDeleted := 0
	for _, resourceID := range resourceIDs {
		result, err := database.DeleteResourceHard(resourceID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error deleting resource %s: %v\n", resourceID, err)
			os.Exit(1)
		}
		if !result.ResourceDeleted {
			continue
		}
		resourcesDeleted++
		if result.ObjectDeleted {
			objectsDeleted++
		}
	}

	fmt.Printf("Deleted %d resources for name=%s\n", resourcesDeleted, name)
	fmt.Printf("Deleted %d unreferenced objects\n", objectsDeleted)
}
