package query

import (
	"log/slog"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/sahil19-19/Traceflow/internal/clickhouse"
	"github.com/sahil19-19/Traceflow/pkg/middleware"
)

// Handler serves GET /logs HTTP requests
type Handler struct {
	service *Service
}

// NewHandler creates a query HTTP handler
func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

// handle the GET request
func (h *Handler) Handle(c *fiber.Ctx) error {
	traceID, _ := c.Locals(middleware.TraceIDKey).(string)

	filter, err := parseFilter(c)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	logs, err := h.service.GetLogs(c.Context(), filter)
	if err != nil {
		slog.Error("query service error",
			"error", err,
			"trace_id", traceID,
		)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "query failed: " + err.Error(),
		})
	}

	// Normalise nil slice to empty slice for consistent JSON output
	if logs == nil {
		logs = []clickhouse.LogEntry{}
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"count": len(logs),
		"logs":  logs,
	})
}

// parseFilter extracts and validates query parameters into a QueryFilter.
func parseFilter(c *fiber.Ctx) (clickhouse.QueryFilter, error) {
	filter := clickhouse.QueryFilter{
		Service: c.Query("service"),
		Level:   c.Query("level"),
		Limit:   100,
		Offset:  0,
	}

	// from (optional)
	if fromStr := c.Query("from"); fromStr != "" {
		t, err := time.Parse(time.RFC3339, fromStr)
		if err != nil {
			return filter, fiber.NewError(fiber.StatusBadRequest,
				"from must be RFC3339 format, e.g. 2026-05-05T00:00:00Z")
		}
		filter.From = t
	}

	// to (optional)
	if toStr := c.Query("to"); toStr != "" {
		t, err := time.Parse(time.RFC3339, toStr)
		if err != nil {
			return filter, fiber.NewError(fiber.StatusBadRequest,
				"to must be RFC3339 format, e.g. 2026-05-05T23:59:59Z")
		}
		filter.To = t
	}
	// limit (optional)
	if limitStr := c.Query("limit"); limitStr != "" {
		n, err := strconv.Atoi(limitStr)
		if err != nil {
			return filter, fiber.NewError(fiber.StatusBadRequest, "limit must be an integer")
		}
		filter.Limit = n
	}

	// offset (optional)
	if offsetStr := c.Query("offset"); offsetStr != "" {
		n, err := strconv.Atoi(offsetStr)
		if err != nil {
			return filter, fiber.NewError(fiber.StatusBadRequest, "offset must be an integer")
		}
		filter.Offset = n
	}

	return filter, nil
}
