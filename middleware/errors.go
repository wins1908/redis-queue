package middleware

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/wins1908/redis-queue"
)

var _ error = &PanicError{}

type PanicError struct {
	Err        interface{}
	StackTrace string
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("Panic error:\n%s\n\nStack trace:\n%s", castToError(e.Err).Error(), e.StackTrace)
}

func castToError(err interface{}) error {
	var converted error

	if err, ok := err.(error); ok {
		converted = err
	} else {
		converted = errors.New(fmt.Sprintf("%v", err))
	}

	return converted
}

type HandleErrorFunc func(ctx context.Context, d redisq.Delivery, err error)

func Recoverer(fn HandleErrorFunc) ConsumeFuncMiddleware {
	return func(next ConsumeFunc) ConsumeFunc {
		return func(ctx context.Context, d redisq.Delivery) error {
			defer func() {
				if err := recover(); err != nil {
					fn(ctx, d, &PanicError{Err: err, StackTrace: string(debug.Stack())})
				}
			}()

			if err := next(ctx, d); err != nil {
				fn(ctx, d, err)
			}
			return nil
		}
	}
}
