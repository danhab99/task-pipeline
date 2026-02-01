package main

import (
	"fmt"
	"log"
	"os"
)

type MyLogger struct {
	logger *log.Logger
	prefix string
}

func NewLogger(prefix string) MyLogger {
	l := log.New(os.Stderr, fmt.Sprintf("[%s] ", prefix), log.Ldate|log.Lmicroseconds)

	return MyLogger{l, prefix}
}

func (m MyLogger) Printf(format string, v ...any) {
	m.logger.Printf(format, v...)
}

func (m MyLogger) Println(v ...any) {
	m.logger.Println(v...)
}

func (m MyLogger) Verbosef(format string, v ...any) {
	if len(os.Getenv("VERBOSE")) >= 1 {
		m.Printf(format, v...)
	}
}

func (m MyLogger) Verboseln(v ...any) {
	if len(os.Getenv("VERBOSE")) >= 1 {
		m.logger.Println(v...)
	}
}

func (m MyLogger) Context(c string) MyLogger {
	p := fmt.Sprintf("[%s:%s] ", m.prefix, c)
	l := log.New(os.Stderr, p, log.Ldate|log.Lmicroseconds|log.Lshortfile)

	return MyLogger{l, p}
}
