package middleware

import (
	"context"

	"github.com/wins1908/redis-queue"

	"github.com/newrelic/go-agent"
)

const headerKeyTracePayload = "traceId"

type NewRelicDataStoreConfig interface {
	Host() string
	PortPathOrID() string
	Product() newrelic.DatastoreProduct
}

func NewrelicStartTxn(app newrelic.Application, fn CreateTxnNameFunc) ConsumeFuncMiddleware {
	return func(next ConsumeFunc) ConsumeFunc {
		return func(ctx context.Context, d redisq.Delivery) error {
			txnName := fn(d)

			txn := app.StartTransaction(txnName, nil, nil)
			defer func() {
				redisq.LogErr(txn.End())
			}()

			return next(newrelic.NewContext(ctx, txn), d)
		}
	}
}

func NewrelicAcceptTracePayload(next ConsumeFunc) ConsumeFunc {
	return func(ctx context.Context, d redisq.Delivery) error {
		if txn := newrelic.FromContext(ctx); txn != nil {
			if tracePayload, exists := d.Headers[headerKeyTracePayload]; exists {
				redisq.LogErr(txn.AcceptDistributedTracePayload(newrelic.TransportQueue, tracePayload))
			}
		}
		return next(ctx, d)
	}
}

func NewrelicStartSegment(c NewRelicDataStoreConfig) PublishFuncMiddleware {
	return func(next PublishFunc) PublishFunc {
		return func(ctx context.Context, pub redisq.Publishing) error {
			if txn := newrelic.FromContext(ctx); txn != nil {
				segment := newrelic.DatastoreSegment{
					StartTime:    newrelic.StartSegmentNow(txn),
					Product:      c.Product(),
					Collection:   pub.Type,
					Operation:    "LPUSH",
					DatabaseName: pub.Queue,
					Host:         c.Host(),
					PortPathOrID: c.PortPathOrID(),
				}
				defer func() {
					redisq.LogErr(segment.End())
				}()
			}

			return next(ctx, pub)
		}
	}
}

func NewrelicTracePayloadToPublishing(next PublishFunc) PublishFunc {
	return func(ctx context.Context, pub redisq.Publishing) error {
		if txn := newrelic.FromContext(ctx); txn != nil {
			if len(pub.Headers) == 0 {
				pub.Headers = make(map[string]interface{})
			}
			pub.Headers[headerKeyTracePayload] = txn.CreateDistributedTracePayload().Text()
		}
		return next(ctx, pub)
	}
}
