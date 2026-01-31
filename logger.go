package main

import (
	"fmt"
	"log"
	"os"
)

func NewLogger(prefix string) *log.Logger {
	return log.New(os.Stderr, fmt.Sprintf("[%s] ", prefix), log.Ldate|log.Ltime|log.Lshortfile)
}
