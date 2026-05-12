// Worker Service entry point
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"github.com/sahil19-19/Traceflow/internal/clickhouse"
	"github.com/sahil19-19/Traceflow/internal/config"
	"github.com/sahil19-19/Traceflow/internal/grpc/gen"
	"github.com/sahil19-19/Traceflow/internal/queue"
	"github.com/sahil19-19/Traceflow/internal/worker"
	"google.golang.org/grpc"
)

func main() {
	// Load .env file. Ignore error if .env doesn't exist
	if err := godotenv.Load(); err != nil {
		slog.Warn("no .env file found, using environment variables")
	}

	cfg := config.Load()

	// redis
	redisClient := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})
	// verify Redis connection at startup
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		// panic on startup failures- causes container to restart
		panic(fmt.Sprintf("cannot connect to Redis at %s: %v", cfg.RedisAddr, err))
	}
	defer redisClient.Close()
	slog.Info("redis connected", "addr", cfg.RedisAddr)

	queueConsumer := queue.NewConsumer(redisClient, cfg.RedisQueueKey, 2*time.Second)

	// clickHouse
	chConn, err := clickhouse.NewClient(
		cfg.ClickhouseAddr,
		cfg.ClickhouseDB,
		cfg.ClickhouseUser,
		cfg.ClickhousePass,
	)
	if err != nil {
		panic(fmt.Sprintf("cannot connect to ClickHouse at %s: %v", cfg.ClickhouseAddr, err))
	}
	defer chConn.Close()
	slog.Info("clickhouse connected", "addr", cfg.ClickhouseAddr)

	logRepo := clickhouse.NewLogRepo(chConn, cfg.ClickhouseDB)
	processor := worker.NewProcessor(logRepo, 20)

	// triggerChan connects the gRPC server to the consumer loop
	// Buffered so NotifyBatchReady doesn't block if the loop is busy
	triggerChan := make(chan struct{}, 10)

	// gRPC Server
	grpcAddr := ":" + cfg.GRPCPort
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		panic(fmt.Sprintf("failed to listen on gRPC port %s: %v", grpcAddr, err))
	}

	grpcServer := grpc.NewServer()
	logServer := worker.NewLogServer(triggerChan)
	gen.RegisterLogServiceServer(grpcServer, logServer)

	// run gRPC server in a background goroutine - it blocks internally.
	go func() {
		slog.Info("grpc server started", "addr", grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("grpc server failed", "error", err)
		}
	}()

	// context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// handle OS sinals for graceful shutdown
	// SIGINT = Ctrl+C, SIGTERM = docker stop
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		slog.Info("shutdown signal received", "signal", sig)
		cancel() // cancel the consumer loop context

		grpcServer.GracefulStop()
	}()

	// Consumer Loop (blocking)
	// This blocks until ctx is cancelled (by the signal handler above).
	worker.StartConsumerLoop(ctx, queueConsumer, processor, triggerChan)

	slog.Info("worker service stopped")
}
