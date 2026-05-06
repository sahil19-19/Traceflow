// Package config loads environment variables into a typed Config struct
package config

import (
	"log/slog"
	"os"
)

type Config struct {
	//redis
	RedisAddr     string
	RedisQueueKey string

	//	clickhouse
	ClickhouseAddr string
	ClickhouseDB   string
	ClickhouseUser string
	ClickhousePass string

	IngestionPort string
	QueryPort     string
	GRPCPort      string
}

// main.go calls godotenv.Load(), then
// call config.Load() once at startup in each cmd/*/main.go.

func Load() *Config {
	cfg := &Config{
		RedisAddr:      getEnv("REDIS_ADDR", "localhost:6379"),
		RedisQueueKey:  getEnv("REDIS_QUEUE_KEY", "logs_queue"),
		ClickhouseAddr: getEnv("CLICKHOUSE_ADDR", "localhost:9000"),
		ClickhouseDB:   getEnv("CLICKHOUSE_DB", "observability"),
		ClickhouseUser: getEnv("CLICKHOUSE_USER", "default"),
		ClickhousePass: getEnv("CLICKHOUSE_PASSWORD", ""),
		IngestionPort:  getEnv("INGESTION_PORT", "8080"),
		QueryPort:      getEnv("QUERY_PORT", "8081"),
		GRPCPort:       getEnv("GRPC_PORT", "50051"),
	}

	slog.Info("config loaded",
		"redis_addr", cfg.RedisAddr,
		"clickhouse_addr", cfg.ClickhouseAddr,
		"grpc_port", cfg.GRPCPort,
	)

	return cfg
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
