package traceid

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
)

func New() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// log and return a fixed fallback so the system keeps running
		slog.Error("failed to generate trace_id", "error", err)
		return "0000000000000000fallback00000000"
	}
	return hex.EncodeToString(b)
}
