package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/gopasspw/gopass/proto"
	"github.com/gopasspw/gopass/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	// Parse command line flags
	port := flag.Int("port", 50051, "The gRPC server port")
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

	// Create listener
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		logger.Errorf("Failed to listen on port %d: %v", *port, err)
		os.Exit(1)
	}

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Create and register gopass server
	gopassServer, err := server.NewGopassServer(logger)
	if err != nil {
		logger.Errorf("Failed to create gopass server: %v", err)
		os.Exit(1)
	}
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
