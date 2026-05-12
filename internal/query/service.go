package query

import (
	"context"

	"github.com/sahil19-19/Traceflow/internal/clickhouse"
)

// Service contains the query business logic- building filters, enforcing limits
type Service struct {
	repo *Repository
}

// NewService creates a query service
func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

// GetLogs retrieves logs matching the given filter from ClickHouse.
func (s *Service) GetLogs(ctx context.Context, filter clickhouse.QueryFilter) ([]clickhouse.LogEntry, error) {
	// min and max limits
	if filter.Limit <= 0 {
		filter.Limit = 100
	}
	if filter.Limit > 1000 {
		filter.Limit = 1000
	}

	return s.repo.FindLogs(ctx, filter)
}
