package queue

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Producer pushes log event payloads (JSON bytes) into a redis list.
type Producer struct {
	client *redis.Client
	key    string // redis list key
}

// NewProducer creates a Producer connected to Redis.
func NewProducer(client *redis.Client, key string) *Producer {
	return &Producer{
		client: client,
		key:    key,
	}
}

// Push serialises a log event (as JSON bytes) and appends it to the Redis queue
func (p *Producer) Push(ctx context.Context, data []byte) error {
	if err := p.client.LPush(ctx, p.key, data).Err(); err != nil {
		return fmt.Errorf("redis LPUSH to key %q failed: %w", p.key, err)
	}
	return nil
}
