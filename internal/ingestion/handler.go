package ingestion

import (
	"log/slog"

	"github.com/gofiber/fiber/v2"
	"github.com/sahil19-19/Traceflow/pkg/middleware"
)

// handler parses the request body, reading trace ID and forming the response
type Handler struct {
	service *Service
}

// NewHandler creates an HTTP handler backed by the given service
func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

func (h *Handler) Handle(c *fiber.Ctx) error {
	// read the trace ID that the middleware attached to locals
	// type assertion: Locals returns interface{}, we know it's a string
	traceID, _ := c.Locals(middleware.TraceIDKey).(string)

	var event LogEvent
	if err := c.BodyParser(&event); err != nil {
		slog.Error("failed to parse request body",
			"error", err,
			"trace_id", traceID,
		)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid JSON body: " + err.Error(),
		})
	}

	if event.TraceID == "" {
		event.TraceID = traceID
	}

	if err := h.service.Ingest(c.Context(), &event); err != nil {
		slog.Error("ingestion failed",
			"error", err,
			"trace_id", traceID,
			"service", event.Service,
		)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{
		"status":   "accepted",
		"trace_id": event.TraceID,
	})
}
