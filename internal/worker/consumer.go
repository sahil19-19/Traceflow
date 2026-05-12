package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/sahil19-19/Traceflow/internal/queue"
)

// StartConsumerLoop is main loop of the Worker Service
// triggerChan is written to by the gRPC server (LogServer.NotifyBatchReady)
func StartConsumerLoop(
	ctx context.Context,
	consumer *queue.Consumer,
	processor *Processor,
	triggerChan <-chan struct{},
) {
	slog.Info("worker consumer loop started")

	for {
		select {
		case <-ctx.Done():
			// graceful shutdown
			slog.Info("consumer loop shutting down, draining remaining messages")
			drainAndProcess(context.Background(), consumer, processor) // drain whatever is left in Redis
			slog.Info("consumer loop stopped")
			return

		case <-triggerChan:
			// The Ingestion Service signalled via gRPC that new logs are ready
			// process immediatly without waiting for the timer
			slog.Debug("triggered by gRPC signal, draining queue")
			drainAndProcess(ctx, consumer, processor)

		case <-time.After(5 * time.Second):
			// Fallback polling — drain the queue on a fixed interval
			slog.Debug("polling timer fired, draining queue")
			drainAndProcess(ctx, consumer, processor)
		}
	}
}

// drainAndProcess reads up to batchSize messages from Redis and processes them
const batchSize = 20

func drainAndProcess(ctx context.Context, consumer *queue.Consumer, processor *Processor) {
	for {
		if ctx.Err() != nil {
			return
		}

		messages, err := consumer.PopBatch(ctx, batchSize)
		if err != nil {
			slog.Error("failed to pop batch from Redis", "error:", err)
			return
		}

		if len(messages) == 0 {
			// queue empty
			return
		}

		slog.Info("processing batch", "count", len(messages))

		if err := processor.ProcessBatch(ctx, messages); err != nil {
			slog.Error("batch processing failed", "error:", err)
		}
	}
}
