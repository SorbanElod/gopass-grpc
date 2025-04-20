package server

import (
	"io"
	"log"
	"os"
)

// Logger interface defines the logging methods
type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// DefaultLogger implements the Logger interface
type DefaultLogger struct {
	debug        *log.Logger
	info         *log.Logger
	warn         *log.Logger
	error        *log.Logger
	debugEnabled bool
}

// NewDefaultLogger creates a new DefaultLogger instance
func NewDefaultLogger(debug bool) *DefaultLogger {
	flags := log.LstdFlags | log.Lshortfile
	return &DefaultLogger{
		debug:        log.New(os.Stdout, "DEBUG: ", flags),
		info:         log.New(os.Stdout, "INFO: ", flags),
		warn:         log.New(os.Stdout, "WARN: ", flags),
		error:        log.New(os.Stderr, "ERROR: ", flags),
		debugEnabled: debug,
	}
}

func (l *DefaultLogger) Debugf(format string, args ...interface{}) {
	if l.debugEnabled {
		l.debug.Printf(format, args...)
	}
}

func (l *DefaultLogger) Infof(format string, args ...interface{}) {
	l.info.Printf(format, args...)
}

func (l *DefaultLogger) Warnf(format string, args ...interface{}) {
	l.warn.Printf(format, args...)
}

func (l *DefaultLogger) Errorf(format string, args ...interface{}) {
	l.error.Printf(format, args...)
}

// SetOutput sets the output destination for all loggers
func (l *DefaultLogger) SetOutput(w io.Writer) {
	l.debug.SetOutput(w)
	l.info.SetOutput(w)
	l.warn.SetOutput(w)
	l.error.SetOutput(w)
}
