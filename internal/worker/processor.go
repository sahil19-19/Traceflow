package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/sahil19-19/Traceflow/internal/clickhouse"
	"github.com/sahil19-19/Traceflow/internal/ingestion"
)

// Processor converts raw Redis messages (josn bytes) into clickHouse logEntries and performs the batch insert
type Processor struct {
	repo      *clickhouse.LogRepo
	batchSize int
}

// NewProcessor creates a processor with the given clickHouse repository.
func NewProcessor(repo *clickhouse.LogRepo, batchSize int) *Processor {
	if batchSize <= 0 {
		batchSize = 20 // default batch size
	}
	return &Processor{
		repo:      repo,
		batchSize: batchSize,
	}
}

func (p *Processor) ProcessBatch(ctx context.Context, rawMessages [][]byte) error {
	entries := make([]clickhouse.LogEntry, 0, len(rawMessages))

	for i, raw := range rawMessages {
		var event ingestion.LogEvent
		if err := json.Unmarshal(raw, &event); err != nil {
			slog.Error("failed to unmarshal log event from Redis",
				"error", err,
				"index", i,
				"raw_preview", truncate(string(raw), 200), // log just 200 bytes of the raw message
			)
			continue
		}

		// Parse the timestamp string back to time.Time
		ts, err := time.Parse(time.RFC3339, event.Timestamp)
		if err != nil {
			// fallback to current time
			slog.Warn("failed to parse event timestamp, using now",
				"timestamp", event.Timestamp,
				"trace_id", event.TraceID,
				"error", err,
			)
			ts = time.Now().UTC()
		}

		// convert metadata map to JSON string for ClickHouse storage.
		metadataJSON := "{}"
		if len(event.Metadata) > 0 {
			if b, err := json.Marshal(event.Metadata); err == nil {
				metadataJSON = string(b)
			}
		}

		entries = append(entries, clickhouse.LogEntry{
			Timestamp: ts,
			Service:   event.Service,
			Level:     event.Level,
			Message:   event.Message,
			TraceID:   event.TraceID,
			Metadata:  metadataJSON,
		})
	}

	if len(entries) == 0 {
		slog.Warn("no valid entries to insert after unmarshalling batch")
		return nil
	}

	if err := p.repo.BatchInsert(ctx, entries); err != nil {
		return err
	}

	slog.Info("batch processed successfully",
		"total_received", len(rawMessages),
		"total_inserted", len(entries),
		"skipped", len(rawMessages)-len(entries),
	)

	return nil
}

// truncate shortens s to at most n characters, appending "..." if truncated.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
