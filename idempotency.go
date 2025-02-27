package idempotency

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"log/slog"
	"os"
	"time"
)

type IdempotencyKey struct {
	separator       string
	revalidateCache bool
	redisClient     *redis.Conn
	logger          *slog.Logger
}

func NewIdempotencyKey(conn *redis.Conn, revalidateCache bool, separator string) *IdempotencyKey {
	if separator == "" {
		separator = "-"
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	return &IdempotencyKey{
		separator:       separator,
		redisClient:     conn,
		revalidateCache: revalidateCache,
		logger:          logger,
	}
}

func (ik IdempotencyKey) Once(ctx context.Context, fn func(ctx context.Context) error, TTL time.Duration, actionName string, args ...string) error {
	key, alreadyExist := ik.createIdempotencyKey(ctx, TTL, actionName, args...)

	if !alreadyExist {
		if err := fn(ctx); err != nil {
			ik.rollback(ctx, key)
			return err
		}
	}

	return nil
}

func (ik IdempotencyKey) createIdempotencyKey(ctx context.Context, TTL time.Duration, actionName string, args ...string) (string, bool) {
	var key string
	key = "idempotency" + ik.separator
	key += actionName
	for i := range args {
		key += ik.separator + args[i]
	}

	if ik.alreadyExecuted(ctx, key, TTL) {
		return "", true
	}

	if err := ik.redisClient.Set(ctx, key, time.Now().String(), TTL).Err(); err != nil {
		ik.logger.Error("createIdempotencyKey error", "error", err)
		return "", false
	}

	return key, false
}

func (ik IdempotencyKey) alreadyExecuted(ctx context.Context, key string, TTL time.Duration) bool {
	v, err := ik.redisClient.Get(ctx, key).Result()

	if errors.Is(err, redis.Nil) {
		return false
	} else if err != nil {
		ik.logger.Error("alreadyExecuted error", "error", err)
		return false
	}

	if ik.revalidateCache {
		if err := ik.redisClient.Set(ctx, key, v, TTL).Err(); err != nil {
			ik.logger.Error("error revalidate ttl", "error", err)
		}
	}

	return true
}

func (ik IdempotencyKey) rollback(ctx context.Context, key string) {
	if err := ik.redisClient.Del(ctx, key).Err(); err != nil {
		ik.logger.Error("", "commit error", err)
	}
}
