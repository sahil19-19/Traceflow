// Package ingestion handles the HTTP ingestion path
// receive log events from clients, validate them, and enqueue them for processing
package ingestion

// LogEvent is the structure clients POST to /ingest
type LogEvent struct {
	// name of the service emitting the log
	Service string `json:"service"`

	// severity
	// one of: INFO, WARN, ERROR, DEBUG.
	Level string `json:"level"`

	// human-readable log message
	Message string `json:"message"`

	// timestamp(optional) - if empty, the server sets it to time.Now().UTC()
	// format: "2026-05-05T10:00:00Z"
	Timestamp string `json:"timestamp,omitempty"`

	// TraceID(optional) - if empty, the server generates one
	TraceID string `json:"trace_id,omitempty"`

	// Metadata (optional) map of arbitrary key-value pairs.
	Metadata map[string]string `json:"metadata,omitempty"`
}
