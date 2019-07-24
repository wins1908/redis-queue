package redisq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/adjust/rmq"
)

type QueueManager struct {
	connections map[string]Connection
	consumers   map[string]Consumer
	queues      []rmq.Queue
}

func NewQueueManager() *QueueManager {
	return &QueueManager{
		connections: make(map[string]Connection),
		consumers:   make(map[string]Consumer),
		queues:      make([]rmq.Queue, 0),
	}
}

func (m *QueueManager) Connections(conns ...Connection) {
	for _, conn := range conns {
		m.connections[conn.Name()] = conn
	}
}

func (m *QueueManager) Consumers(consumers ...Consumer) {
	for _, consumer := range consumers {
		m.consumers[consumer.Type()] = consumer
	}
}

func (m *QueueManager) StartConsuming(connName string, queueName string, replicas int, pollDuration time.Duration) {
	conn, exists := m.connections[connName]
	if !exists {
		LogErr(fmt.Errorf("no connection with name %s", connName))
	}
	queue := conn.OpenQueue(queueName)
	failedQ := conn.OpenQueue(fmt.Sprintf("%s_failed", queueName))
	queue.SetPushQueue(failedQ)

	prefetchLimit := replicas + 1
	queue.StartConsuming(prefetchLimit, pollDuration)

	consumerName := fmt.Sprintf("%s_consumer", queueName)

	ca := ConsumerAdapter(func(delivery rmq.Delivery) {
		d, err := NewDelivery(queueName, delivery, conn.Publisher())
		if err != nil {
			LogErr(err)
			delivery.Reject()
			return
		}

		ctx := context.Background()
		consumerType := d.Type
		consumer, exists := m.consumers[consumerType]
		if !exists {
			LogErr(fmt.Errorf("no consumer for type `%s` on queue `%s`", consumerType, queueName))
			LogErr(d.Reject(ctx))
			return
		}

		if err := consumer.Consume(ctx, *d); err != nil {
			LogErr(err)
			LogErr(d.Reject(ctx))
		}
	})

	for i := 0; i < replicas; i++ {
		queue.AddConsumer(fmt.Sprintf("%v_%d", consumerName, i), ca)
	}

	m.queues = append(m.queues, queue)
}

func (m *QueueManager) StopConsuming() {
	for _, q := range m.queues {
		q.StopConsuming()
		q.Close()
	}
}

func (m *QueueManager) ConsumeOnce(connName string, queueName string) {
	conn, exists := m.connections[connName]
	if !exists {
		LogErr(fmt.Errorf("no connection with name %s", connName))
	}
	queue := conn.OpenQueue(queueName)

	prefetchLimit := 1
	queue.StartConsuming(prefetchLimit, 0)

	consumerName := fmt.Sprintf("%s_consumer", queueName)

	var wg sync.WaitGroup
	wg.Add(1)
	consumeOnce := ConsumerAdapter(func(delivery rmq.Delivery) {
		d, err := NewDelivery(queueName, delivery, conn.Publisher())
		if err != nil {
			LogErr(err)
			delivery.Reject()
			wg.Done()
			return
		}

		ctx := context.Background()
		consumerType := d.Type
		consumer, exists := m.consumers[consumerType]
		if !exists {
			LogErr(fmt.Errorf("no consumer for type `%s` on queue `%s`", consumerType, queueName))
			LogErr(d.Reject(ctx))
			wg.Done()
			return
		}

		if err := consumer.Consume(ctx, *d); err != nil {
			LogErr(err)
			LogErr(d.Reject(ctx))
		}

		wg.Done()
	})

	queue.AddConsumer(fmt.Sprintf("%v_%d", consumerName, 1), consumeOnce)
	wg.Wait()
	queue.StopConsuming()
	queue.Close()
}
