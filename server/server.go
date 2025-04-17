package server

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/gopasspw/gopass/proto"
)

// GopassServer handles gRPC requests and interfaces with the gopass CLI
type GopassServer struct {
	GopassBinary    string
	RunningCommands map[string]*exec.Cmd
	CommandsMutex   sync.Mutex
	proto.UnimplementedGopassServiceServer
}

// NewGopassServer creates a new server instance
func NewGopassServer(gopassPath string) *GopassServer {
	return &GopassServer{
		GopassBinary:    gopassPath,
		RunningCommands: make(map[string]*exec.Cmd),
	}
}

// ExecuteCommand runs a gopass command and returns the result
func (s *GopassServer) ExecuteCommand(ctx context.Context, req *proto.CommandRequest) (*proto.CommandResponse, error) {
	startTime := time.Now()

	if len(req.Args) == 0 {
		return nil, fmt.Errorf("no command arguments provided")
	}

	cmd := exec.CommandContext(ctx, s.GopassBinary, req.Args...)

	if req.WorkingDir != "" {
		cmd.Dir = req.WorkingDir
	}

	if len(req.EnvVars) > 0 {
		cmd.Env = append(cmd.Env, formatEnvVars(req.EnvVars)...)
	}

	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	commandID := fmt.Sprintf("%p", cmd)

	s.CommandsMutex.Lock()
	s.RunningCommands[commandID] = cmd
	s.CommandsMutex.Unlock()

	err := cmd.Run()
	executionTime := time.Since(startTime).Milliseconds()

	s.CommandsMutex.Lock()
	delete(s.RunningCommands, commandID)
	s.CommandsMutex.Unlock()

	resp := &proto.CommandResponse{
		Stdout:          stdout.String(),
		Stderr:          stderr.String(),
		ExecutionTimeMs: executionTime,
		Success:         err == nil,
	}

	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			resp.ExitCode = int32(exitErr.ExitCode())
			resp.ErrorMessage = stderr.String()
		} else {
			resp.ErrorMessage = err.Error()
		}
	}

	return resp, nil
}

// ExecuteCommandStream executes a command and streams the output
func (s *GopassServer) ExecuteCommandStream(req *proto.CommandRequest, stream proto.GopassService_ExecuteCommandStreamServer) error {
	if len(req.Args) == 0 {
		return fmt.Errorf("no command arguments provided")
	}

	ctx := stream.Context()
	cmd := exec.CommandContext(ctx, s.GopassBinary, req.Args...)

	if req.WorkingDir != "" {
		cmd.Dir = req.WorkingDir
	}

	if len(req.EnvVars) > 0 {
		cmd.Env = append(cmd.Env, formatEnvVars(req.EnvVars)...)
	}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %v", err)
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %v", err)
	}

	commandID := fmt.Sprintf("%p", cmd)

	s.CommandsMutex.Lock()
	s.RunningCommands[commandID] = cmd
	s.CommandsMutex.Unlock()

	defer func() {
		s.CommandsMutex.Lock()
		delete(s.RunningCommands, commandID)
		s.CommandsMutex.Unlock()
	}()

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start command: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		buffer := make([]byte, 4096)
		for {
			n, err := stdoutPipe.Read(buffer)
			if n > 0 {
				_ = stream.Send(&proto.CommandOutput{
					Data:     buffer[:n],
					IsStderr: false,
				})
			}
			if err != nil {
				break
			}
		}
	}()

	go func() {
		defer wg.Done()
		buffer := make([]byte, 4096)
		for {
			n, err := stderrPipe.Read(buffer)
			if n > 0 {
				_ = stream.Send(&proto.CommandOutput{
					Data:     buffer[:n],
					IsStderr: true,
				})
			}
			if err != nil {
				break
			}
		}
	}()

	wg.Wait()
	err = cmd.Wait()

	exitCode := 0
	errorMessage := ""
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			errorMessage = err.Error()
		}
	}

	return stream.Send(&proto.CommandOutput{
		IsFinal:      true,
		ExitCode:     int32(exitCode),
		ErrorMessage: errorMessage,
	})
}

// ExecuteInteractiveCommand handles bidirectional streaming for interactive commands
func (s *GopassServer) ExecuteInteractiveCommand(stream proto.GopassService_ExecuteInteractiveCommandServer) error {
	var cmd *exec.Cmd
	var stdin io.WriteCloser
	var commandID string

	ctx := stream.Context()

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

	firstMsg, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive initial command: %v", err)
	}

	if len(firstMsg.Args) == 0 {
		return fmt.Errorf("no command arguments provided")
	}

	cmd = exec.CommandContext(ctx, s.GopassBinary, firstMsg.Args...)

	if firstMsg.WorkingDir != "" {
		cmd.Dir = firstMsg.WorkingDir
	}

	if len(firstMsg.EnvVars) > 0 {
		cmd.Env = append(cmd.Env, formatEnvVars(firstMsg.EnvVars)...)
	}

	stdin, err = cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdin pipe: %v", err)
	}

	stdout, err1 := cmd.StdoutPipe()
	stderr, err2 := cmd.StderrPipe()
	if err1 != nil || err2 != nil {
		return fmt.Errorf("failed to create stdout/stderr pipes")
	}

	commandID = fmt.Sprintf("%p", cmd)
	s.CommandsMutex.Lock()
	s.RunningCommands[commandID] = cmd
	s.CommandsMutex.Unlock()

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start command: %v", err)
	}

	if len(firstMsg.Input) > 0 {
		if _, err := stdin.Write(firstMsg.Input); err != nil {
			return fmt.Errorf("failed to write to stdin: %v", err)
		}
	}

	if firstMsg.CloseStdin {
		stdin.Close()
		stdin = nil
	}

	outputDone := make(chan struct{})
	go func() {
		defer close(outputDone)
		stdoutBuffer := make([]byte, 4096)
		stderrBuffer := make([]byte, 4096)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			n, err := stdout.Read(stdoutBuffer)
			if n > 0 {
				_ = stream.Send(&proto.CommandOutput{
					Data:     stdoutBuffer[:n],
					IsStderr: false,
				})
			}

			n, err = stderr.Read(stderrBuffer)
			if n > 0 {
				_ = stream.Send(&proto.CommandOutput{
					Data:     stderrBuffer[:n],
					IsStderr: true,
				})
			}

			if err == io.EOF {
				break
			}
			if err != nil {
				break
			}
		}
	}()

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}

		if msg.Cancel {
			cmd.Process.Kill()
			break
		}

		if len(msg.Input) > 0 && stdin != nil {
			if _, err := stdin.Write(msg.Input); err != nil {
				break
			}
		}

		if msg.CloseStdin && stdin != nil {
			stdin.Close()
			stdin = nil
		}
	}

	err = cmd.Wait()

	exitCode := 0
	errorMessage := ""
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			errorMessage = err.Error()
		}
	}

	return stream.Send(&proto.CommandOutput{
		IsFinal:      true,
		ExitCode:     int32(exitCode),
		ErrorMessage: errorMessage,
	})
}

func formatEnvVars(envVars map[string]string) []string {
	result := make([]string, 0, len(envVars))
	for k, v := range envVars {
		result = append(result, fmt.Sprintf("%s=%s", k, v))
	}
	return result
}
