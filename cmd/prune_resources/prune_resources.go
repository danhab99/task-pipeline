// Description: Prune old resource versions by keeping newest N per name
package prune_resources

import (
	"flag"
	"fmt"
	"log"
	"os"

	"grit/db"
)

var (
	dbPath *string
	name   *string
	keep   *int
	dryRun *bool
)

func RegisterFlags(fs *flag.FlagSet) {
	dbPath = fs.String("db", "./db", "database path")
	name = fs.String("name", "", "prune only this resource name")
	keep = fs.Int("keep", 1, "number of newest versions to keep per resource name")
	dryRun = fs.Bool("dry-run", false, "show what would be deleted without writing changes")
}

func Execute() {
	if *keep < 0 {
		fmt.Fprintln(os.Stderr, "Error: -keep must be >= 0")
		os.Exit(1)
	}

	log.Println("Opening database")
	database, err := db.NewDatabase(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()
	log.Println("Opened database")

	names := make([]string, 0)
	if *name != "" {
		names = append(names, *name)
	} else {
		for n := range database.GetAllResourceNames() {
			names = append(names, n)
		}
	}

	namesProcessed := 0
	resourcesKept := 0
	resourcesDeleted := 0
	objectsDeleted := 0

	for _, resourceName := range names {
		namesProcessed++
		idx := 0
		for r := range database.GetResourcesByName(resourceName) {
			if idx < *keep {
				resourcesKept++
				idx++
				continue
			}

			if *dryRun {
				fmt.Printf("Would delete resource id=%s name=%s hash=%s\n", r.ID, r.Name, r.ObjectHash)
				resourcesDeleted++
				idx++
				continue
			}

			log.Printf("Deleting resources %s:%s\n", resourceName, r.ObjectHash)

			result, err := database.DeleteResourceHard(r.ID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error deleting resource %s: %v\n", r.ID, err)
				os.Exit(1)
			}
			if result.ResourceDeleted {
				resourcesDeleted++
			}
			if result.ObjectDeleted {
				objectsDeleted++
			}
			idx++
		}
	}

	fmt.Printf("Processed %d resource names\n", namesProcessed)
	fmt.Printf("Kept %d resources\n", resourcesKept)
	if *dryRun {
		fmt.Printf("Would delete %d resources\n", resourcesDeleted)
		return
	}
	fmt.Printf("Deleted %d resources\n", resourcesDeleted)
	fmt.Printf("Deleted %d unreferenced objects\n", objectsDeleted)
}
