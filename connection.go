package redisq

import (
	"github.com/adjust/rmq"
	"github.com/go-redis/redis"
)

type Connection interface {
	Name() string
	OpenQueue(name string) rmq.Queue
	Publisher() Publisher
}

func NewRmqConnection(tag string, client *redis.Client) (rmq.Connection, error) {
	conn := rmq.OpenConnectionWithRedisClient(tag, client)
	cleaner := rmq.NewCleaner(conn)
	if err := cleaner.Clean(); err != nil {
		return nil, err
	}
	return conn, nil
}
