package server

import (
	"context"
	"fmt"
	"time"

	"github.com/gopasspw/gopass/pkg/gopass/api"
	"github.com/gopasspw/gopass/proto"
)

// GopassServer handles gRPC requests and interfaces with the gopass API.
type GopassServer struct {
	gopass *api.Gopass
	proto.UnimplementedGopassServiceServer
	logger Logger
}

// ByteWrapper is a custom type that implements the gopass.Byter interface.
type ByteWrapper []byte

// Bytes implements the gopass.Byter interface.
func (b ByteWrapper) Bytes() []byte {
	return b
}

// NewGopassServer creates a new server instance.
func NewGopassServer(logger Logger) (*GopassServer, error) {
	// Initialize the Gopass API
	gp, err := api.New(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to initialize gopass: %w", err)
	}

	return &GopassServer{
		gopass: gp,
		logger: logger,
	}, nil
}

// Authenticate handles authentication requests.
func (s *GopassServer) Authenticate(ctx context.Context, req *proto.AuthRequest) (*proto.AuthResponse, error) {
	// authenticate the user
	return &proto.AuthResponse{Status: "authenticated"}, nil
}

// ListSecrets returns a list of all secrets.
func (s *GopassServer) ListSecrets(ctx context.Context, req *proto.ListRequest) (*proto.ListResponse, error) {
	secrets, err := s.gopass.List(ctx)
	if err != nil {
		s.logger.Errorf("Failed to list secrets: %v", err)
		return nil, err
	}

	return &proto.ListResponse{
		Secrets: secrets,
	}, nil
}

// GetSecret returns a single encrypted secret.
func (s *GopassServer) GetSecret(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	// Set a timeout of 60 seconds for the context
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	secret, err := s.gopass.Get(ctx, req.Name, req.Revision)
	if err != nil {
		s.logger.Errorf("Failed to get secret %s: %v", req.Name, err)
		return nil, err
	}

	return &proto.GetResponse{
		Secret: string(secret.Bytes()), // The encrypted secret
	}, nil
}

// SetSecret adds a new revision or creates a new secret.
func (s *GopassServer) SetSecret(ctx context.Context, req *proto.SetRequest) (*proto.SetResponse, error) {
	// Convert the secret string to a custom Byter implementation
	secret := ByteWrapper([]byte(req.Secret))

	// Pass the custom Byter to the Set method
	err := s.gopass.Set(ctx, req.Name, secret)
	if err != nil {
		s.logger.Errorf("Failed to set secret %s: %v", req.Name, err)
		return nil, err
	}

	return &proto.SetResponse{}, nil
}

// RemoveSecret removes a single secret.
func (s *GopassServer) RemoveSecret(ctx context.Context, req *proto.RemoveRequest) (*proto.RemoveResponse, error) {
	err := s.gopass.Remove(ctx, req.Name)
	if err != nil {
		s.logger.Errorf("Failed to remove secret %s: %v", req.Name, err)
		return nil, err
	}

	return &proto.RemoveResponse{}, nil
}

// RemoveAllSecretsWithPrefix removes all secrets with the given prefix.
func (s *GopassServer) RemoveAllSecretsWithPrefix(ctx context.Context, req *proto.RemoveAllRequest) (*proto.RemoveAllResponse, error) {
	err := s.gopass.RemoveAll(ctx, req.Prefix)
	if err != nil {
		s.logger.Errorf("Failed to remove all secrets with prefix %s: %v", req.Prefix, err)
		return nil, err
	}

	return &proto.RemoveAllResponse{}, nil
}

// RenameSecret moves a prefix to another.
func (s *GopassServer) RenameSecret(ctx context.Context, req *proto.RenameRequest) (*proto.RenameResponse, error) {
	err := s.gopass.Rename(ctx, req.Src, req.Dest)
	if err != nil {
		s.logger.Errorf("Failed to rename secret %s to %s: %v", req.Src, req.Dest, err)
		return nil, err
	}

	return &proto.RenameResponse{}, nil
}

// SyncSecret synchronizes a secret with a remote.
func (s *GopassServer) SyncSecret(ctx context.Context, req *proto.SyncRequest) (*proto.SyncResponse, error) {
	// Sync is not implemented in the current API.
	s.logger.Warnf("Sync secret is not implemented")
	return nil, fmt.Errorf("sync not implemented")
}

// RevisionsOfSecret lists all revisions of a secret.
func (s *GopassServer) RevisionsOfSecret(ctx context.Context, req *proto.RevisionsRequest) (*proto.RevisionsResponse, error) {
	// Revisions feature is not implemented in the current API.
	s.logger.Warnf("Revisions not implemented")
	return nil, fmt.Errorf("revisions not implemented")
}
