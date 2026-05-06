package middleware

import (
	"github.com/gofiber/fiber/v2"
	"github.com/sahil19-19/Traceflow/internal/traceid"
)

const (
	// HTTP header used to propagate trace idss
	TraceIDHeader = "X-Trace-ID"

	// TraceIDKey is the key used to store the trace id in Fiber's locals.
	// Handlers retrieve it via: c.Locals(TraceIDKey).(string)
	TraceIDKey = "trace_id"
)

// a middleware that ensures every request has a trace id
func TraceID() fiber.Handler {
	return func(c *fiber.Ctx) error {
		id := c.Get(TraceIDHeader)

		if id == "" {
			id = traceid.New()
		}

		c.Locals(TraceIDKey, id)
		c.Set(TraceIDHeader, id)
		return c.Next()
	}
}
