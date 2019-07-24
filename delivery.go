package redisq

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/adjust/rmq"
)

type Acknowledger interface {
	Ack(ctx context.Context) error
	Reject(ctx context.Context) error
	Delay(ctx context.Context, d Delivery, delayedAt time.Time) error
	Push(ctx context.Context) error
}

type Delivery struct {
	Acknowledger
	Queue    string
	Headers  map[string]interface{}
	Type     string
	Attempts uint
	Body     []byte
}

func (d Delivery) Delay(ctx context.Context, delayedAt time.Time) error {
	return d.Acknowledger.Delay(ctx, d, delayedAt)
}

func NewDelivery(queue string, d rmq.Delivery, p Publisher) (*Delivery, error) {
	m := &message{}
	body := json.RawMessage{}
	m.Data = &body
	if err := json.Unmarshal([]byte(d.Payload()), m); err != nil {
		return nil, err
	}

	return &Delivery{
		Acknowledger: &acknowledger{d, p},
		Queue:        queue,
		Headers:      m.Headers,
		Type:         m.Type,
		Attempts:     m.Attempts,
		Body:         body,
	}, nil
}

var _ Acknowledger = &acknowledger{}

type acknowledger struct {
	inner     rmq.Delivery
	publisher Publisher
}

func (a *acknowledger) Ack(ctx context.Context) error {
	if !a.inner.Ack() {
		return errors.New("cannot ack the message")
	}
	return nil
}
func (a *acknowledger) Reject(ctx context.Context) error {
	if !a.inner.Reject() {
		return errors.New("cannot reject the message")
	}
	return nil
}
func (a *acknowledger) Push(ctx context.Context) error {
	if !a.inner.Push() {
		return errors.New("cannot push the message")
	}
	return nil
}

func (a *acknowledger) Delay(ctx context.Context, d Delivery, delayedAt time.Time) error {
	pub := deliveryToPublishing(d)
	pub.Attempts++
	pub.DelayedAt = delayedAt
	if err := a.publisher.Publish(ctx, pub); err != nil {
		return err
	}
	if !a.inner.Ack() {
		return errors.New("cannot ack after delayed message")
	}
	return nil
}

func deliveryToPublishing(d Delivery) Publishing {
	return Publishing{
		Queue:    d.Queue,
		Headers:  d.Headers,
		Type:     d.Type,
		Attempts: d.Attempts,
		Body:     json.RawMessage(d.Body),
	}
}
