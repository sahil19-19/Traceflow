// query service entry point
// exposes GET /logs with optional filters, reading directly from ClickHouse
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
	"github.com/sahil19-19/Traceflow/internal/clickhouse"
	"github.com/sahil19-19/Traceflow/internal/config"
	"github.com/sahil19-19/Traceflow/internal/query"
	"github.com/sahil19-19/Traceflow/pkg/middleware"
)

func main() {
	if err := godotenv.Load(); err != nil {
		slog.Warn("no .env file found, using environment variables")
	}

	cfg := config.Load()

	chConn, err := clickhouse.NewClient(
		cfg.ClickhouseAddr,
		cfg.ClickhouseDB,
		cfg.ClickhouseUser,
		cfg.ClickhousePass,
	)
	if err != nil {
		panic(fmt.Sprintf("cannot connect to ClickHouse at %s: %v", cfg.ClickhouseAddr, err))
	}

	// verify the connection is alive before starting the HTTP server
	if err := chConn.Ping(context.Background()); err != nil {
		panic(fmt.Sprintf("clickhouse ping failed: %v", err))
	}
	defer chConn.Close()
	slog.Info("clickhouse connected", "addr", cfg.ClickhouseAddr)

	// Wire up query service and handler
	logRepo := clickhouse.NewLogRepo(chConn, cfg.ClickhouseDB)
	repo := query.NewRepository(logRepo)
	svc := query.NewService(repo)
	handler := query.NewHandler(svc)

	// fiber http server
	app := fiber.New(fiber.Config{
		AppName: "observability-platform-query",
	})

	app.Use(middleware.TraceID())

	app.Get("/logs", handler.Handle)
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	// graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		slog.Info("shutdown signal received", "signal", sig)
		if err := app.Shutdown(); err != nil {
			slog.Error("error during fiber shutdown", "error", err)
		}
	}()

	addr := ":" + cfg.QueryPort
	slog.Info("query service starting", "addr", addr)

	if err := app.Listen(addr); err != nil {
		slog.Error("fiber listen error", "error", err)
		os.Exit(1)
	}

	slog.Info("query service stopped")
}
