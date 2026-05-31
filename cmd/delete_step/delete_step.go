package delete_step

import (
	"flag"
	"fmt"
	"os"

	"grit/db"
	"grit/log"
)

var logger = log.NewLogger("delete_step")

var (
	dbPath *string
	id     *string
	name   *string
)

func RegisterFlags(fs *flag.FlagSet) {
	dbPath = fs.String("db", "./db", "database path")
	name = fs.String("name", "", "delete step with this name")
}

// Execute runs the command
func Execute() {
	if (name == nil) || (*name == "") {
		fmt.Fprintln(os.Stderr, "Error: specify -name")
		os.Exit(1)
	}

	database, err := db.NewDatabase(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	stepId, err := database.GetStepByName(*name)
	if err != nil {
		panic(err)
	}

	database.DeleteStep(stepId.ID)

	fmt.Printf("Deleted step %s\n", *name)
}
