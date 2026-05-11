// contains the worker service logic:
// gRPC server, redis consumer loop, and batch processor
package worker

import (
	"context"
	"log/slog"

	"github.com/sahil19-19/Traceflow/internal/grpc/gen"
)

type LogServer struct {
	gen.UnimplementedLogServiceServer

	triggerChan chan<- struct{}
}

func NewLogServer(triggerChan chan<- struct{}) *LogServer {
	return &LogServer{
		triggerChan: triggerChan,
	}
}

func (s *LogServer) NotifyBatchReady(ctx context.Context, req *gen.NotifyRequest) (*gen.NotifyResponse, error) {
	slog.Info("grpc NotifyBatchReady received",
		"trace_id", req.GetTraceId(),
		"count", req.GetCount(),
	)

	select {
	case s.triggerChan <- struct{}{}:
		slog.Debug("drain signal sent to consumer loop")
	default:
		slog.Debug("drain signal dropped (consumer loop already active or channel full)")
	}

	return &gen.NotifyResponse{
		Accepted: true,
		Message:  "signal received",
	}, nil
}
