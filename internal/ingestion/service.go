package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/sahil19-19/Traceflow/internal/grpc"
	"github.com/sahil19-19/Traceflow/internal/queue"
)

// Service orchestrates the ingestion pipeline: validate > serialise > enqueue > signal worker
type Service struct {
	producer     *queue.Producer
	workerClient *grpc.WorkerClient
}

// newService creates an ingestion Service with its dependencies
func NewService(producer *queue.Producer, workerClient *grpc.WorkerClient) *Service {
	return &Service{
		producer:     producer,
		workerClient: workerClient,
	}
}

// ingest validates a log event, pushes it to redis, and signals the worker
// returns an error only if the event is invalid or redis is unavailable
func (s *Service) Ingest(ctx context.Context, event *LogEvent) error {
	// Step 1: Validate and fill defaults (timestamp, trace_id).
	if err := Validate(event); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal log event to JSON: %w", err)
	}

	// Push to Redis queue
	if err := s.producer.Push(ctx, data); err != nil {
		return fmt.Errorf("failed to push log event to Redis queue: %w", err)
	}

	slog.Info("log event enqueued",
		"service", event.Service,
		"level", event.Level,
		"trace_id", event.TraceID,
	)

	// signal the worker via gRPC that a new log is ready
	// if this fails, no issues cause queue is source of truth
	if err := s.workerClient.NotifyBatchReady(ctx, event.TraceID, 1); err != nil {
		// Log the error but do not return it: ingestion succeeded.
		slog.Warn("grpc signal to worker failed (non-fatal, worker will poll)",
			"error", err,
			"trace_id", event.TraceID,
		)
	}

	return nil
}
