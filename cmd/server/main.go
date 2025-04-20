package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/gopasspw/gopass/proto"
	"github.com/gopasspw/gopass/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	// Parse command line flags
	port := flag.Int("port", 50051, "The gRPC server port")
	gopassPath := flag.String("gopass", "", "Path to gopass binary")
	debugLog := flag.Bool("debug", false, "Enable debug logging")
	logFile := flag.String("log", "", "Path to log file (default: stdout)")
	flag.Parse()

	// Create logger
	logger := server.NewDefaultLogger(*debugLog)

	// Set up log file if specified
	if *logFile != "" {
		logOutput, err := os.OpenFile(*logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			logger.Errorf("Failed to open log file: %v", err)
			os.Exit(1)
		}
		defer logOutput.Close()
		logger.SetOutput(logOutput)
	}

	logger.Infof("Starting gopass gRPC server (debug: %v)", *debugLog)

	// Find gopass binary if not specified
	binaryPath := *gopassPath
	if binaryPath == "" {
		logger.Debugf("No gopass path specified, searching in PATH")
		var err error
		binaryPath, err = exec.LookPath("gopass")
		if err != nil {
			exePath, err := os.Executable()
			if err != nil {
				logger.Errorf("Failed to get executable path: %v", err)
				os.Exit(1)
			}

			dirPath := filepath.Dir(exePath)
			possiblePath := filepath.Join(dirPath, "gopass")
			if _, err := os.Stat(possiblePath); err == nil {
				binaryPath = possiblePath
				logger.Debugf("Found gopass binary in same directory: %s", possiblePath)
			} else {
				logger.Errorf("Could not find gopass binary: %v", err)
				os.Exit(1)
			}
		}
	}

	// Verify gopass binary
	if _, err := os.Stat(binaryPath); err != nil {
		logger.Errorf("Gopass binary not found at %s: %v", binaryPath, err)
		os.Exit(1)
	}

	logger.Infof("Using gopass binary: %s", binaryPath)

	// Create listener
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		logger.Errorf("Failed to listen on port %d: %v", *port, err)
		os.Exit(1)
	}

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Create and register gopass server
	gopassServer := server.NewGopassServer(binaryPath, logger)
	proto.RegisterGopassServiceServer(grpcServer, gopassServer)
	reflection.Register(grpcServer)

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Infof("Received shutdown signal, gracefully stopping server...")
		grpcServer.GracefulStop()
	}()

	// Start server
	logger.Infof("Server listening on :%d", *port)
	if err := grpcServer.Serve(lis); err != nil {
		logger.Errorf("Server failed: %v", err)
		os.Exit(1)
	}
}
