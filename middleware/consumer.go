package middleware

import (
	"context"

	"github.com/wins1908/redis-queue"
)

type ConsumeFunc func(ctx context.Context, d redisq.Delivery) error
type ConsumeFuncMiddleware func(next ConsumeFunc) ConsumeFunc
type CreateTxnNameFunc func(d redisq.Delivery) string

var _ redisq.Consumer = &consumer{}

type consumer struct {
	consumerType string
	consumeFn    ConsumeFunc
}

func (c consumer) Type() string                                         { return c.consumerType }
func (c consumer) Consume(ctx context.Context, d redisq.Delivery) error { return c.consumeFn(ctx, d) }

type ConsumeFuncMiddlewares []ConsumeFuncMiddleware

func (ms ConsumeFuncMiddlewares) Consumer(main redisq.Consumer) redisq.Consumer {
	return &consumer{main.Type(), buildConsumeFunc(ms, main.Consume)}
}

func (ms ConsumeFuncMiddlewares) Consumers(consumers ...redisq.Consumer) []redisq.Consumer {
	cs := make([]redisq.Consumer, len(consumers))
	for idx, consumer := range consumers {
		cs[idx] = ms.Consumer(consumer)
	}
	return cs
}

func buildConsumeFunc(ms []ConsumeFuncMiddleware, end ConsumeFunc) ConsumeFunc {
	total := len(ms)
	if total == 0 {
		return end
	}

	head := end
	for i := total - 1; i >= 0; i-- {
		head = ms[i](head)
	}

	return head
}
