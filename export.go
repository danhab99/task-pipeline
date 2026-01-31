package main

import (
	"fmt"
	"os"

	"github.com/fatih/color"
)

var exportLogger = NewLogger("EXPORT")

func exportResourcesByName(database Database, resourceName string) {
	exportLogger.Printf("Listing resources with name: %s\n", color.MagentaString(resourceName))

	// List all resources with the given name
	resourceCount := 0
	for resource := range database.GetResourcesByName(resourceName) {
		resourceCount++
		// Output hash and metadata
		fmt.Fprintf(os.Stdout, "%s\t%s\t%s\n", resource.ObjectHash, resource.Name, resource.CreatedAt)
	}

	if resourceCount == 0 {
		exportLogger.Printf("No resources found with name '%s'\n", resourceName)
		os.Exit(1)
	} else {
		exportLogger.Printf("Listed %d resource(s)\n", resourceCount)
	}
}

func exportResourceByHash(database Database, hash string) {
	exportLogger.Printf("Exporting resource with hash: %s\n", color.MagentaString(hash[:16]+"..."))

	// Check if object exists
	if !database.ObjectExists(hash) {
		exportLogger.Printf("Object with hash '%s' not found\n", hash)
		os.Exit(1)
	}

	// Get object data
	data, err := database.GetObject(hash)
	if err != nil {
		exportLogger.Printf("Failed to get object %s: %v\n", hash[:16], err)
		os.Exit(1)
	}

	// Write raw content to stdout
	os.Stdout.Write(data)
	
	exportLogger.Printf("Exported %d bytes\n", len(data))
}
