package middleware

import (
	"context"

	"github.com/wins1908/redis-queue"
)

type (
	PublishFunc           func(ctx context.Context, pub redisq.Publishing) error
	PublishFuncMiddleware func(next PublishFunc) PublishFunc
)

var _ redisq.Publisher = &publisher{}

type publisher struct {
	publishFn PublishFunc
}

func (p publisher) Publish(ctx context.Context, pub redisq.Publishing) error {
	return p.publishFn(ctx, pub)
}

type PublishFuncMiddlewares []PublishFuncMiddleware

func (ms PublishFuncMiddlewares) Publisher(main redisq.Publisher) redisq.Publisher {
	return &publisher{buildPublishFunc(ms, main.Publish)}
}

func (ms PublishFuncMiddlewares) Publishers(publishers ...redisq.Publisher) []redisq.Publisher {
	ps := make([]redisq.Publisher, len(publishers))
	for idx, publisher := range publishers {
		ps[idx] = ms.Publisher(publisher)
	}
	return ps
}

func buildPublishFunc(ms []PublishFuncMiddleware, end PublishFunc) PublishFunc {
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
