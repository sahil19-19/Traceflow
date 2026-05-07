package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/sahil19-19/Traceflow/internal/grpc/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// WorkerClient wraps the generated gRPC client with a friendlier API
// the Ingestion Service holds one instance of this, created at startup
type WorkerClient struct {
	conn   *grpc.ClientConn
	client gen.LogServiceClient
}

func NewWorkerClient(addr string) (*WorkerClient, error) {
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection to worker at %s: %w", addr, err)
	}

	slog.Info("grpc worker client created", "addr", addr)

	return &WorkerClient{
		conn:   conn,
		client: gen.NewLogServiceClient(conn),
	}, nil
}

// NotifyBatchReady signals the Worker that new logs are waiting in Redis.

func (c *WorkerClient) NotifyBatchReady(ctx context.Context, traceID string, count int32) error {
	callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	resp, err := c.client.NotifyBatchReady(callCtx, &gen.NotifyRequest{
		TraceId: traceID,
		Count:   count,
	})
	if err != nil {
		return fmt.Errorf("grpc NotifyBatchReady failed: %w", err)
	}

	slog.Debug("grpc NotifyBatchReady response",
		"accepted", resp.GetAccepted(),
		"message", resp.GetMessage(),
		"trace_id", traceID,
	)

	return nil
}

// Close releases the underlying gRPC connection.
// Defer this in main.go after creating the client.
func (c *WorkerClient) Close() error {
	return c.conn.Close()
}
