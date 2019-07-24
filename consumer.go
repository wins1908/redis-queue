package redisq

import (
	"context"

	"github.com/adjust/rmq"
)

type Consumer interface {
	Type() string
	Consume(ctx context.Context, d Delivery) error
}

type ConsumerAdapter func(delivery rmq.Delivery)

func (fn ConsumerAdapter) Consume(d rmq.Delivery) { fn(d) }
