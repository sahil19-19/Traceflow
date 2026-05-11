package ingestion

import (
	"fmt"
	"strings"
	"time"

	"github.com/sahil19-19/Traceflow/internal/traceid"
)

var validLevels = map[string]bool{
	"INFO":  true,
	"WARN":  true,
	"ERROR": true,
	"DEBUG": true,
}

// checks required fields, adds default values for optional fields
func Validate(event *LogEvent) error {
	if strings.TrimSpace(event.Service) == "" {
		return fmt.Errorf("service is required and cannot be empty")
	}

	if strings.TrimSpace(event.Message) == "" {
		return fmt.Errorf("message is required and cannot be empty")
	}

	event.Level = strings.ToUpper(event.Level)
	if !validLevels[event.Level] {
		return fmt.Errorf("level must be one of INFO, WARN, ERROR, DEBUG — got %q", event.Level)
	}

	// timestamp: if the client didn't provide one, set it to server time.
	// if the client did provide one, validate it is in the correct format
	if event.Timestamp == "" {
		event.Timestamp = time.Now().UTC().Format(time.RFC3339)
	} else {
		if _, err := time.Parse(time.RFC3339, event.Timestamp); err != nil {
			return fmt.Errorf("timestamp must be RFC3339 format (e.g. 2026-05-05T10:00:00Z), got %q", event.Timestamp)
		}
	}

	// traceID: if the client didn't provide one,
	// generate a new random trace ID - ensures every log in ClickHouse has a trace_id
	if event.TraceID == "" {
		event.TraceID = traceid.New()
	}

	return nil
}
