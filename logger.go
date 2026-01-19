package main

import (
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/schollz/progressbar/v3"
)

// LogLevel represents the verbosity level
type LogLevel int

const (
	LogLevelQuiet LogLevel = iota
	LogLevelNormal
	LogLevelVerbose
)

var (
	currentLogLevel = LogLevelNormal
	logLevelMutex   sync.RWMutex
)

// SetLogLevel sets the global log level
func SetLogLevel(level LogLevel) {
	logLevelMutex.Lock()
	defer logLevelMutex.Unlock()
	currentLogLevel = level
}

// GetLogLevel returns the current log level
func GetLogLevel() LogLevel {
	logLevelMutex.RLock()
	defer logLevelMutex.RUnlock()
	return currentLogLevel
}

// ColorLogger provides colored, leveled logging
type ColorLogger struct {
	prefix     string
	verbose    *log.Logger
	normal     *log.Logger
	quiet      *log.Logger
	color      *color.Color
	errorLog   *log.Logger
	warnLog    *log.Logger
	successLog *log.Logger
}

// NewColorLogger creates a new colored logger
func NewColorLogger(prefix string, c *color.Color) *ColorLogger {
	flags := log.Ltime | log.Lmsgprefix
	_, file, line, _ := runtime.Caller(, 1)

	return &ColorLogger{
		prefix:     fmt.Sprintf("%s:%d@%s", file, line, prefix),
		color:      c,
		verbose:    log.New(os.Stderr, c.Sprint(prefix), flags),
		normal:     log.New(os.Stderr, c.Sprint(prefix), flags),
		quiet:      log.New(io.Discard, "", 0),
		errorLog:   log.New(os.Stderr, color.RedString(prefix), flags),
		warnLog:    log.New(os.Stderr, color.YellowString(prefix), flags),
		successLog: log.New(os.Stderr, color.GreenString(prefix), flags),
	}
}

// Printf logs at normal level
func (cl *ColorLogger) Printf(format string, v ...interface{}) {
	if GetLogLevel() >= LogLevelNormal {
		cl.normal.Printf(format, v...)
	}
}

// Println logs at normal level
func (cl *ColorLogger) Println(v ...interface{}) {
	if GetLogLevel() >= LogLevelNormal {
		cl.normal.Println(v...)
	}
}

// Verbosef logs only in verbose mode
func (cl *ColorLogger) Verbosef(format string, v ...interface{}) {
	if GetLogLevel() >= LogLevelVerbose {
		cl.verbose.Printf(format, v...)
	}
}

// Verboseln logs only in verbose mode
func (cl *ColorLogger) Verboseln(v ...interface{}) {
	if GetLogLevel() >= LogLevelVerbose {
		cl.verbose.Println(v...)
	}
}

// Errorf logs errors in red
func (cl *ColorLogger) Errorf(format string, v ...interface{}) {
	if GetLogLevel() >= LogLevelNormal {
		cl.errorLog.Printf(format, v...)
	}
}

// Warnf logs warnings in yellow
func (cl *ColorLogger) Warnf(format string, v ...interface{}) {
	if GetLogLevel() >= LogLevelNormal {
		cl.warnLog.Printf(format, v...)
	}
}

// Successf logs success messages in green
func (cl *ColorLogger) Successf(format string, v ...interface{}) {
	if GetLogLevel() >= LogLevelNormal {
		cl.successLog.Printf(format, v...)
	}
}

// ProgressBar wraps schollz/progressbar with our visibility settings
type ProgressBar struct {
	bar *progressbar.ProgressBar
}

// NewProgressBar creates a progress bar that respects log level
func NewProgressBar(max int64, description string) *ProgressBar {
	if GetLogLevel() == LogLevelQuiet {
		return &ProgressBar{
			bar: progressbar.NewOptions64(
				max,
				progressbar.OptionSetWriter(io.Discard),
			),
		}
	}

	bar := progressbar.NewOptions64(
		max,
		progressbar.OptionSetDescription(color.CyanString(description)),
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionThrottle(65*time.Millisecond),
		progressbar.OptionShowElapsedTimeOnFinish(),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        color.GreenString("█"),
			SaucerHead:    color.GreenString("█"),
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)

	return &ProgressBar{bar: bar}
}

// Add increments the progress bar
func (pb *ProgressBar) Add(num int) error {
	if pb.bar != nil {
		return pb.bar.Add(num)
	}
	return nil
}

// Finish completes the progress bar
func (pb *ProgressBar) Finish() error {
	if pb.bar != nil {
		return pb.bar.Finish()
	}
	return nil
}

// Set sets the progress bar to a specific value
func (pb *ProgressBar) Set(num int) error {
	if pb.bar != nil {
		return pb.bar.Set(num)
	}
	return nil
}

// PrintSummary prints a summary box
func PrintSummary(title string, items map[string]interface{}) {
	if GetLogLevel() == LogLevelQuiet {
		return
	}

	color.New(color.Bold, color.FgCyan).Fprintf(os.Stderr, "\n╔══════════════════════════════════════════════════════════╗\n")
	color.New(color.Bold, color.FgCyan).Fprintf(os.Stderr, "║ %-56s ║\n", title)
	color.New(color.Bold, color.FgCyan).Fprintf(os.Stderr, "╠══════════════════════════════════════════════════════════╣\n")

	for k, v := range items {
		color.New(color.FgWhite).Fprintf(os.Stderr, "║ ")
		color.New(color.FgYellow).Fprintf(os.Stderr, "%-20s", k+":")
		color.New(color.FgWhite).Fprintf(os.Stderr, " %-34v", v)
		color.New(color.FgWhite).Fprintf(os.Stderr, " ║\n")
	}

	color.New(color.Bold, color.FgCyan).Fprintf(os.Stderr, "╚══════════════════════════════════════════════════════════╝\n\n")
}
