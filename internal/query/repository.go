// Package query handles the HTTP query path: receive filter params, query ClickHouse, return results.
package query

import (
	"context"

	"github.com/sahil19-19/Traceflow/internal/clickhouse"
)

// Repository is an alias for the ClickHouse log query operations
type Repository struct {
	repo *clickhouse.LogRepo
}

// NewRepository creates a query Repository
func NewRepository(repo *clickhouse.LogRepo) *Repository {
	return &Repository{repo: repo}
}

// FindLogs queries ClickHouse with the given filter and returns matching log entries.
func (r *Repository) FindLogs(ctx context.Context, filter clickhouse.QueryFilter) ([]clickhouse.LogEntry, error) {
	return r.repo.Query(ctx, filter)
}
