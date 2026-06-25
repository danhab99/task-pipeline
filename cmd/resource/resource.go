package resource

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"grit/db"
	"grit/log"
)

var (
	dbPath    string
	resLogger = log.NewLogger("resource")
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
		names := deleteCmd.String("names", "", "delete all resources with these names (comma-separated)")
		deleteCmd.Parse(os.Args[3:])
		if *id == "" && *names == "" {
			fmt.Fprintln(os.Stderr, "Error: specify exactly one of -id or -names for delete subcommand")
			os.Exit(1)
		}
		if *id != "" && *names != "" {
			fmt.Fprintln(os.Stderr, "Error: specify exactly one of -id or -names for delete subcommand")
			os.Exit(1)
		}
		deleteResource(*id, *names)
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
	fmt.Println("  delete   Delete a resource by ID or names (requires -id or -names)")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  -db string")
	fmt.Println("    database path (default \"./db\")")
}

func listResources(filterName string) {
	resLogger.Verbosef("listing resources, filterName=%q", filterName)

	database, err := db.NewDatabase(dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	var resources []db.Resource
	if filterName != "" {
		resLogger.Verbosef("querying resources by name: %s", filterName)
		for r := range database.GetResourcesByName(filterName) {
			resources = append(resources, r)
		}
	} else {
		resLogger.Verbosef("querying all resources")
		for r := range database.GetAllResources() {
			resources = append(resources, r)
		}
	}
	resLogger.Verbosef("found %d resources", len(resources))

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
		resLogger.Verbosef("resource: name=%s id=%s hash=%s", r.Name, r.ID, r.ObjectHash)
		fmt.Printf("  %-*s  id=%s  hash=%s  created=%s  by_task=%s\n",
			maxNameLen, r.Name, r.ID, r.ObjectHash, r.CreatedAt, createdBy)
	}
}

func listResourceNames() {
	resLogger.Verbosef("listing resource names")

	database, err := db.NewDatabase(dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	counts := make(map[string]int)
	for r := range database.GetAllResources() {
		counts[r.Name]++
	}
	resLogger.Verbosef("found %d unique resource names", len(counts))

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
	resLogger.Verbosef("getting resource id=%s", id)

	database, err := db.NewDatabase(dbPath)
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
		resLogger.Verbosef("resource not found for id=%s", id)
		fmt.Printf("No resource found for id=%s\n", id)
		return
	}
	resLogger.Verbosef("found resource: name=%s hash=%s", r.Name, r.ObjectHash)

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
	resLogger.Verbosef("creating resource name=%s hash=%s", name, hash)

	database, err := db.NewDatabase(dbPath)
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

	resLogger.Verbosef("created resource id=%s", resourceID)
	fmt.Printf("Created resource id=%s name=%s hash=%s\n", resourceID, name, hash)
}

func deleteResource(id, names string) {
	database, err := db.NewDatabase(dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	if id != "" {
		resLogger.Verbosef("deleting resource by id=%s", id)
		result, err := database.DeleteResourceHard(id)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error deleting resource %s: %v\n", id, err)
			os.Exit(1)
		}
		if !result.ResourceDeleted {
			resLogger.Verbosef("resource not found for id=%s", id)
			fmt.Printf("No resource found for id=%s\n", id)
			return
		}

		fmt.Printf("Deleted resource id=%s name=%s hash=%s\n", result.ResourceID, result.Name, result.ObjectHash)
		if result.ObjectDeleted {
			resLogger.Verbosef("deleted object hash=%s", result.ObjectHash)
			fmt.Printf("Deleted object hash=%s (remaining_refs=%d)\n", result.ObjectHash, result.RemainingObjectRefs)
		} else {
			resLogger.Verbosef("kept object hash=%s (still referenced)", result.ObjectHash)
			fmt.Printf("Kept object hash=%s (remaining_refs=%d)\n", result.ObjectHash, result.RemainingObjectRefs)
		}
		return
	}

	resourceNames := strings.Split(names, ",")
	for i := range resourceNames {
		resourceNames[i] = strings.TrimSpace(resourceNames[i])
	}

	totalResources := 0
	totalObjects := 0
	for i, name := range resourceNames {
		if name == "" {
			continue
		}
		resLogger.Verbosef("[%d/%d] deleting resources by name=%s", i+1, len(resourceNames), name)
		result, err := database.DeleteResourcesByName(name)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error deleting resources for name %s: %v\n", name, err)
			os.Exit(1)
		}
		if result.ResourcesDeleted == 0 {
			resLogger.Verbosef("[%d/%d] no resources found for name=%s", i+1, len(resourceNames), name)
			fmt.Printf("No resources found for name=%s\n", name)
			continue
		}
		resLogger.Verbosef("[%d/%d] deleted %d resources, %d objects for name=%s", i+1, len(resourceNames), result.ResourcesDeleted, result.ObjectsDeleted, name)
		fmt.Printf("Deleted %d resources for name=%s\n", result.ResourcesDeleted, name)
		totalResources += result.ResourcesDeleted
		totalObjects += result.ObjectsDeleted
	}

	if totalResources > 0 {
		fmt.Printf("Deleted %d unreferenced objects\n", totalObjects)
		resLogger.Verbosef("deleted %d resources, %d objects total across %d names", totalResources, totalObjects, len(resourceNames))
		resLogger.Verbosef("compaction handled automatically by Badger background goroutines")
	}
}
