// Package clickhouse provides the ClickHouse database connection and repository.
package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// NewClient opens a native TCP connection to ClickHouse
func NewClient(addr, database, username, password string) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},

		DialTimeout: 5 * time.Second, // initial wait for db connection

		// limits concurrent connections to clickHouse
		MaxOpenConns: 10,

		// connections older than this are closed and recreated.
		ConnMaxLifetime: 1 * time.Hour,

		// LZ4 compression reduces network transfer for bulk log inserts
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},

		// Debug mode logs every query — disable in production
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse.Open failed for addr %q: %w", addr, err)
	}

	if err := conn.Ping(context.TODO()); err != nil { // not to pass nil context?
		return nil, fmt.Errorf("clickhouse ping failed: %w", err)
	}

	return conn, nil
}
