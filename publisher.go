package redisq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/adjust/rmq"
	"github.com/go-redis/redis"
)

type (
	Publisher interface {
		Publish(ctx context.Context, pub Publishing) error
	}
	Publishing struct {
		Headers   map[string]interface{}
		Queue     string
		Type      string
		Attempts  uint
		DelayedAt time.Time
		Body      interface{}
	}
)

func NewPublisher(rc *redis.Client) (*publisher, error) {
	conn, err := NewRmqConnection("nms_publisher", rc)
	if err != nil {
		return nil, err
	}

	return &publisher{conn, rc}, nil
}

type publisher struct {
	conn rmq.Connection
	rc   *redis.Client
}

func (p publisher) Publish(ctx context.Context, pub Publishing) error {
	m := message{
		Headers:  pub.Headers,
		Type:     pub.Type,
		Attempts: pub.Attempts,
		Data:     pub.Body,
	}

	bytes, err := json.Marshal(m)
	if err != nil {
		return err
	}
	q := p.conn.OpenQueue(pub.Queue)

	if !pub.DelayedAt.IsZero() {
		return p.publishToDelayQueue(pub.Queue, string(bytes), pub.DelayedAt)
	} else {
		if ok := q.PublishBytes(bytes); !ok {
			return fmt.Errorf("could not publish a message to `%s` queue", pub.Queue)
		}
	}
	return nil
}

func (p publisher) publishToDelayQueue(queue string, payload string, delayedAt time.Time) error {
	z := redis.Z{
		Score:  float64(delayedAt.Unix()),
		Member: payload,
	}

	delayedQueue := fmt.Sprintf("rmq::queue::[%s]::delayed", queue)
	return p.rc.ZAdd(delayedQueue, z).Err()
}
