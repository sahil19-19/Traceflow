package clickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type LogEntry struct {
	Timestamp time.Time
	Service   string
	Level     string
	Message   string
	TraceID   string
	Metadata  string // JSON string, {"user_id":"42"}
}

// for the GET /logs query
type QueryFilter struct {
	Service string
	Level   string
	From    time.Time
	To      time.Time
	Limit   int // default 100, max 1000
	Offset  int
}

// LogRepo provides database access for the logs table.
type LogRepo struct {
	conn driver.Conn
	db   string // database name
}

// NewLogRepo creates a LogRepo with the given ClickHouse connection.
func NewLogRepo(conn driver.Conn, db string) *LogRepo {
	return &LogRepo{conn: conn, db: db}
}

// BatchInsert inserts multiple log entries in a single ClickHouse batch
func (r *LogRepo) BatchInsert(ctx context.Context, entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	table := fmt.Sprintf("%s.logs", r.db)

	// PrepareBatch opens a batch INSERT statement
	// The columns must match the table schema exactly (same order as Append calls below)
	batch, err := r.conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", table))
	if err != nil {
		return fmt.Errorf("clickhouse PrepareBatch failed: %w", err)
	}

	for _, e := range entries {
		if err := batch.Append(
			e.Timestamp,
			e.Service,
			e.Level,
			e.Message,
			e.TraceID,
			e.Metadata,
		); err != nil {
			return fmt.Errorf("clickhouse batch.Append failed: %w", err)
		}
	}

	// Send flushes the batch to ClickHouse in a single network round-trip.
	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse batch.Send failed: %w", err)
	}

	slog.Info("batch insert complete", "count", len(entries), "table", table)
	return nil
}

// query retrieves log entries matching the given filter
// only non-zero filter fields are included in the WHERE clause
func (r *LogRepo) Query(ctx context.Context, filter QueryFilter) ([]LogEntry, error) {
	// Set defaults
	if filter.Limit <= 0 {
		filter.Limit = 100
	}
	if filter.Limit > 1000 {
		filter.Limit = 1000
	}

	// named parameters (@param syntax) > prevents SQL injection
	var conditions []string
	args := make(map[string]interface{})

	if filter.Service != "" {
		conditions = append(conditions, "service = @service")
		args["service"] = filter.Service
	}
	if filter.Level != "" {
		conditions = append(conditions, "level = @level")
		args["level"] = filter.Level
	}
	if !filter.From.IsZero() {
		conditions = append(conditions, "timestamp >= @from")
		args["from"] = filter.From
	}
	if !filter.To.IsZero() {
		conditions = append(conditions, "timestamp <= @to")
		args["to"] = filter.To
	}

	query := fmt.Sprintf("SELECT timestamp, service, level, message, trace_id, metadata FROM %s.logs", r.db)
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	query += " ORDER BY timestamp DESC"
	query += fmt.Sprintf(" LIMIT %d OFFSET %d", filter.Limit, filter.Offset)

	// Build the named parameter list for ClickHouse
	var queryArgs []interface{}
	for k, v := range args {
		queryArgs = append(queryArgs, driver.NamedValue{Name: k, Value: v})
	}

	rows, err := r.conn.Query(ctx, query, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse query failed: %w", err)
	}
	defer rows.Close()

	var results []LogEntry
	for rows.Next() {
		var e LogEntry
		if err := rows.Scan(
			&e.Timestamp,
			&e.Service,
			&e.Level,
			&e.Message,
			&e.TraceID,
			&e.Metadata,
		); err != nil {
			return nil, fmt.Errorf("clickhouse row scan failed: %w", err)
		}
		results = append(results, e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse rows iteration error: %w", err)
	}

	return results, nil
}
