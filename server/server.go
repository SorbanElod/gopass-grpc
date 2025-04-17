package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// GopassServer handles gRPC requests and interfaces with the gopass CLI
type GopassServer struct {
	GopassBinary    string
	RunningCommands map[string]*exec.Cmd
	CommandsMutex   sync.Mutex
	UnimplementedGopassServiceServer
}

// UnimplementedGopassServiceServer is an interface that you'll replace with the
// actual generated code from your proto file after protobuf compilation
type UnimplementedGopassServiceServer interface{}

// NewGopassServer creates a new server instance
func NewGopassServer(gopassPath string) *GopassServer {
	return &GopassServer{
		GopassBinary:    gopassPath,
		RunningCommands: make(map[string]*exec.Cmd),
	}
}

// ExecuteCommand runs a gopass command and returns the result
func (s *GopassServer) ExecuteCommand(ctx context.Context, req *CommandRequest) (*CommandResponse, error) {
	startTime := time.Now()

	// Validate request
	if len(req.Args) == 0 {
		return nil, fmt.Errorf("no command arguments provided")
	}

	// Prepare the command
	cmd := exec.CommandContext(ctx, s.GopassBinary, req.Args...)

	// Set working directory if specified
	if req.WorkingDir != "" {
		cmd.Dir = req.WorkingDir
	}

	// Set environment variables if needed
	if len(req.EnvVars) > 0 {
		cmd.Env = append(cmd.Env, formatEnvVars(req.EnvVars)...)
	}

	// Capture stdout and stderr
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Make an ID for this command for tracking
	commandID := fmt.Sprintf("%p", cmd)

	// Track the command
	s.CommandsMutex.Lock()
	s.RunningCommands[commandID] = cmd
	s.CommandsMutex.Unlock()

	// Execute the command
	err := cmd.Run()
	executionTime := time.Since(startTime).Milliseconds()

	// Remove from tracking
	s.CommandsMutex.Lock()
	delete(s.RunningCommands, commandID)
	s.CommandsMutex.Unlock()

	// Process results
	resp := &CommandResponse{
		Stdout:          stdout.String(),
		Stderr:          stderr.String(),
		ExecutionTimeMs: executionTime,
		Success:         err == nil,
	}

	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			resp.ExitCode = exitErr.ExitCode()
			resp.ErrorMessage = stderr.String()
		} else {
			resp.ErrorMessage = err.Error()
		}
	}

	return resp, nil
}

// ExecuteCommandStream executes a command and streams the output
func (s *GopassServer) ExecuteCommandStream(req *CommandRequest, stream CommandStreamServer) error {
	// Validate request
	if len(req.Args) == 0 {
		return fmt.Errorf("no command arguments provided")
	}

	ctx := stream.Context()

	// Prepare the command
	cmd := exec.CommandContext(ctx, s.GopassBinary, req.Args...)

	// Set working directory if specified
	if req.WorkingDir != "" {
		cmd.Dir = req.WorkingDir
	}

	// Set environment variables if needed
	if len(req.EnvVars) > 0 {
		cmd.Env = append(cmd.Env, formatEnvVars(req.EnvVars)...)
	}

	// Set up pipes for stdout and stderr
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %v", err)
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %v", err)
	}

	// Make an ID for this command for tracking
	commandID := fmt.Sprintf("%p", cmd)

	// Track the command
	s.CommandsMutex.Lock()
	s.RunningCommands[commandID] = cmd
	s.CommandsMutex.Unlock()

	// Clean up when done
	defer func() {
		s.CommandsMutex.Lock()
		delete(s.RunningCommands, commandID)
		s.CommandsMutex.Unlock()
	}()

	// Start the command
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start command: %v", err)
	}

	// Use WaitGroup to track when stdout and stderr readers are done
	var wg sync.WaitGroup
	wg.Add(2)

	// Process stdout
	go func() {
		defer wg.Done()
		buffer := make([]byte, 4096)
		for {
			n, err := stdoutPipe.Read(buffer)
			if n > 0 {
				if err := stream.Send(&CommandOutput{
					Data:     buffer[:n],
					IsStderr: false,
				}); err != nil {
					log.Printf("Error sending stdout: %v", err)
					return
				}
			}
			if err != nil {
				if err != io.EOF {
					log.Printf("Error reading stdout: %v", err)
				}
				break
			}
		}
	}()

	// Process stderr
	go func() {
		defer wg.Done()
		buffer := make([]byte, 4096)
		for {
			n, err := stderrPipe.Read(buffer)
			if n > 0 {
				if err := stream.Send(&CommandOutput{
					Data:     buffer[:n],
					IsStderr: true,
				}); err != nil {
					log.Printf("Error sending stderr: %v", err)
					return
				}
			}
			if err != nil {
				if err != io.EOF {
					log.Printf("Error reading stderr: %v", err)
				}
				break
			}
		}
	}()

	// Wait for readers to finish
	wg.Wait()

	// Wait for command to complete
	err = cmd.Wait()

	// Send final message with exit code
	exitCode := 0
	errorMessage := ""
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			errorMessage = err.Error()
		}
	}

	return stream.Send(&CommandOutput{
		IsFinal:      true,
		ExitCode:     exitCode,
		ErrorMessage: errorMessage,
	})
}

// ExecuteInteractiveCommand handles bidirectional streaming for interactive commands
func (s *GopassServer) ExecuteInteractiveCommand(stream InteractiveCommandServer) error {
	var cmd *exec.Cmd
	var stdin io.WriteCloser
	var commandID string

	ctx := stream.Context()

	// Function to clean up resources
	cleanup := func() {
		if stdin != nil {
			stdin.Close()
		}
		if commandID != "" {
			s.CommandsMutex.Lock()
			delete(s.RunningCommands, commandID)
			s.CommandsMutex.Unlock()
		}
	}
	defer cleanup()

	// Receive the first message with command details
	firstMsg, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive initial command: %v", err)
	}

	if len(firstMsg.Args) == 0 {
		return fmt.Errorf("no command arguments provided")
	}

	// Prepare the command
	cmd = exec.CommandContext(ctx, s.GopassBinary, firstMsg.Args...)

	// Set working directory if specified
	if firstMsg.WorkingDir != "" {
		cmd.Dir = firstMsg.WorkingDir
	}

	// Set environment variables if needed
	if len(firstMsg.EnvVars) > 0 {
		cmd.Env = append(cmd.Env, formatEnvVars(firstMsg.EnvVars)...)
	}

	// Set up pipes
	var err1, err2 error
	stdin, err = cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdin pipe: %v", err)
	}

	stdout, err1 := cmd.StdoutPipe()
	if err1 != nil {
		return fmt.Errorf("failed to create stdout pipe: %v", err1)
	}

	stderr, err2 := cmd.StderrPipe()
	if err2 != nil {
		return fmt.Errorf("failed to create stderr pipe: %v", err2)
	}

	// Create command ID and track it
	commandID = fmt.Sprintf("%p", cmd)
	s.CommandsMutex.Lock()
	s.RunningCommands[commandID] = cmd
	s.CommandsMutex.Unlock()

	// Start the command
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start command: %v", err)
	}

	// Write initial input if provided
	if len(firstMsg.Input) > 0 {
		if _, err := stdin.Write(firstMsg.Input); err != nil {
			return fmt.Errorf("failed to write to stdin: %v", err)
		}
	}

	if firstMsg.CloseStdin {
		stdin.Close()
		stdin = nil
	}

	// Start goroutine to read command output
	outputDone := make(chan struct{})
	go func() {
		defer close(outputDone)

		// Read from stdout and stderr
		stdoutBuffer := make([]byte, 4096)
		stderrBuffer := make([]byte, 4096)

		for {
			// Check if context is done
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Read from stdout
			n, err := stdout.Read(stdoutBuffer)
			if n > 0 {
				if err := stream.Send(&CommandOutput{
					Data:     stdoutBuffer[:n],
					IsStderr: false,
				}); err != nil {
					log.Printf("Error sending stdout: %v", err)
					return
				}
			}

			// Read from stderr
			n, err = stderr.Read(stderrBuffer)
			if n > 0 {
				if err := stream.Send(&CommandOutput{
					Data:     stderrBuffer[:n],
					IsStderr: true,
				}); err != nil {
					log.Printf("Error sending stderr: %v", err)
					return
				}
			}

			// Check for EOF
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("Error reading output: %v", err)
				break
			}
		}
	}()

	// Process input from client
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error receiving message: %v", err)
			break
		}

		// Handle cancel request
		if msg.Cancel {
			cmd.Process.Kill()
			break
		}

		// Write input if provided
		if len(msg.Input) > 0 && stdin != nil {
			if _, err := stdin.Write(msg.Input); err != nil {
				log.Printf("Error writing to stdin: %v", err)
				break
			}
		}

		// Close stdin if requested
		if msg.CloseStdin && stdin != nil {
			stdin.Close()
			stdin = nil
		}
	}

	// Wait for command to complete
	err = cmd.Wait()

	// Send final result
	exitCode := 0
	errorMessage := ""
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			errorMessage = err.Error()
		}
	}

	return stream.Send(&CommandOutput{
		IsFinal:      true,
		ExitCode:     exitCode,
		ErrorMessage: errorMessage,
	})
}

// Helper function to format environment variables
func formatEnvVars(envVars map[string]string) []string {
	result := make([]string, 0, len(envVars))
	for k, v := range envVars {
		result = append(result, fmt.Sprintf("%s=%s", k, v))
	}
	return result
}

// These are placeholder types that will be replaced by your generated protobuf code
// You'll remove these when you integrate with the real generated code

type CommandRequest struct {
	Args       []string
	WorkingDir string
	EnvVars    map[string]string
	Timeout    int32
}

type CommandResponse struct {
	Stdout          string
	Stderr          string
	ExitCode        int
	Success         bool
	ErrorMessage    string
	ExecutionTimeMs int64
}

type CommandOutput struct {
	Data         []byte
	IsStderr     bool
	IsFinal      bool
	ExitCode     int
	ErrorMessage string
}

type CommandInput struct {
	Args       []string
	Input      []byte
	CloseStdin bool
	WorkingDir string
	EnvVars    map[string]string
	Cancel     bool
}

type CommandStreamServer interface {
	Send(*CommandOutput) error
	Context() context.Context
}

type InteractiveCommandServer interface {
	Send(*CommandOutput) error
	Recv() (*CommandInput, error)
	Context() context.Context
}
