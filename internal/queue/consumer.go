package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// reads log event payloads from a redis list
type Consumer struct {
	client  *redis.Client
	key     string        // Redis list key
	timeout time.Duration // how long BRPOP blocks before returning empty
}

func NewConsumer(client *redis.Client, key string, timeout time.Duration) *Consumer {
	return &Consumer{
		client:  client,
		key:     key,
		timeout: timeout,
	}
}

// Pop is of blocking nature
func (c *Consumer) Pop(ctx context.Context) ([]byte, error) {
	result, err := c.client.BRPop(ctx, c.timeout, c.key).Result()
	if err != nil {
		if err == redis.Nil {
			// timeout elapsed - no message available, not an error
			return nil, nil
		}
		return nil, fmt.Errorf("redis BRPOP from key %q failed: %w", c.key, err)
	}

	// BRPop returns [key, value],we want the value (index 1)
	if len(result) < 2 {
		return nil, fmt.Errorf("unexpected BRPOP result length: %d", len(result))
	}

	return []byte(result[1]), nil
}

// reads up to n messages from the queue without blocking
// If queue.size < n , returns however many are available
// if queue empty, returns an empty slice (not an error)
func (c *Consumer) PopBatch(ctx context.Context, n int) ([][]byte, error) {
	// LRANGE 0 n-1 reads the last n itemm
	results, err := c.client.LRange(ctx, c.key, -int64(n), -1).Result()
	if err != nil {
		return nil, fmt.Errorf("redis LRANGE from key %q failed: %w", c.key, err)
	}

	if len(results) == 0 {
		return nil, nil
	}

	// LTRIM removes the items we fetched
	if err := c.client.LTrim(ctx, c.key, 0, -int64(len(results))-1).Err(); err != nil {
		return nil, fmt.Errorf("redis LTRIM on key %q failed: %w", c.key, err)
	}

	// convert []string to [][]byte
	messages := make([][]byte, len(results))
	for i, r := range results {
		messages[i] = []byte(r)
	}

	return messages, nil
}
