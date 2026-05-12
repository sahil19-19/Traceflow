// ingestion service entry point
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/gofiber/fiber/v2"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"github.com/sahil19-19/Traceflow/internal/config"
	"github.com/sahil19-19/Traceflow/internal/grpc"
	"github.com/sahil19-19/Traceflow/internal/ingestion"
	"github.com/sahil19-19/Traceflow/internal/queue"
	"github.com/sahil19-19/Traceflow/pkg/middleware"
)

func main() {
	// Load .env file
	if err := godotenv.Load(); err != nil {
		slog.Warn("no .env file found, using environment variables")
	}

	cfg := config.Load()

	// Redis
	redisClient := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		panic(fmt.Sprintf("cannot connect to Redis at %s: %v", cfg.RedisAddr, err))
	}
	defer redisClient.Close()
	slog.Info("redis connected", "addr", cfg.RedisAddr)

	producer := queue.NewProducer(redisClient, cfg.RedisQueueKey)

	// gRPC Worker Client
	workerAddr := "localhost:" + cfg.GRPCPort

	if override := os.Getenv("GRPC_WORKER_ADDR"); override != "" {
		workerAddr = override
	}

	workerClient, err := grpc.NewWorkerClient(workerAddr)
	if err != nil {
		// non-fatal at startup - the ingestion service can operate without gRPC
		slog.Warn("failed to create gRPC worker client (non-fatal)",
			"addr", workerAddr,
			"error", err,
		)
	} else {
		defer workerClient.Close()
	}

	// ingestion service and handler
	svc := ingestion.NewService(producer, workerClient)
	handler := ingestion.NewHandler(svc)

	// fiber http server
	app := fiber.New(fiber.Config{
		DisableStartupMessage: false, // can disable in prod
		AppName:               "Traceflow",
	})

	// middleware: attach/generate trace id for every request
	app.Use(middleware.TraceID())

	// Routes
	app.Post("/ingest", handler.Handle)
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	// shutdown is graceful
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		slog.Info("shutdown signal received", "signal", sig)
		// shutdown() waits for in-flight requests to complete
		if err := app.Shutdown(); err != nil {
			slog.Error("error during fiber shutdown", "error", err)
		}
	}()

	addr := ":" + cfg.IngestionPort
	slog.Info("ingestion service starting", "addr", addr)

	// listenAndServe blocks until Shutdown() is called
	if err := app.Listen(addr); err != nil {
		slog.Error("fiber listen error", "error", err)
		os.Exit(1)
	}

	slog.Info("ingestion service stopped")
}
