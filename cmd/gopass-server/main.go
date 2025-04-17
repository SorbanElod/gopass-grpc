package main

import (
	"flag"
	"fmt"
	"log"
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
	flag.Parse()

	// Find gopass binary if not specified
	binaryPath := *gopassPath
	if binaryPath == "" {
		// Try to find it in PATH
		var err error
		binaryPath, err = exec.LookPath("gopass")
		if err != nil {
			// If not in PATH, try to use the same binary as this server
			exePath, err := os.Executable()
			if err != nil {
				log.Fatalf("Failed to get executable path: %v", err)
			}

			// Try to find gopass in the same directory
			dirPath := filepath.Dir(exePath)
			possiblePath := filepath.Join(dirPath, "gopass")
			if _, err := os.Stat(possiblePath); err == nil {
				binaryPath = possiblePath
			} else {
				log.Fatalf("Could not find gopass binary. Please specify with -gopass flag.")
			}
		}
	}

	// Verify gopass binary exists and is executable
	if _, err := os.Stat(binaryPath); err != nil {
		log.Fatalf("Gopass binary not found at %s: %v", binaryPath, err)
	}

	log.Printf("Using gopass binary: %s", binaryPath)

	// Create listener
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen on port %d: %v", *port, err)
	}

	// Create a new server
	grpcServer := grpc.NewServer()

	// Create the gopass server implementation
	gopassServer := server.NewGopassServer(binaryPath)

	// Register the server with gRPC
	proto.RegisterGopassServiceServer(grpcServer, gopassServer)

	// Register reflection service (optional, helps with debugging)
	reflection.Register(grpcServer)

	// Handle shutdown signals gracefully
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutting down gRPC server...")
		grpcServer.GracefulStop()
	}()

	// Start server
	log.Printf("Starting gopass gRPC server on :%d", *port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
