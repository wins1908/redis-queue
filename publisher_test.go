package redisq

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis"
	redis2 "github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/suite"
)

func TestPublisherSuite(t *testing.T) {
	s := &publisherTestSuite{}
	suite.Run(t, s)
}

type publisherTestSuite struct {
	suite.Suite

	miniredis  *miniredis.Miniredis
	redigoConn redis2.Conn
	publisher  Publisher
}

func (s *publisherTestSuite) SetupTest() {
	m, err := miniredis.Run()
	s.NoError(err)

	s.NoError(err)
	c, err := redis2.Dial("tcp", m.Addr())
	s.NoError(err)

	s.miniredis = m
	s.redigoConn = c
	s.publisher, _ = NewPublisher(redis.NewClient(&redis.Options{Addr: m.Addr()}))
}

func (s *publisherTestSuite) TearDownTest() {
	s.miniredis.Close()
}

func (s *publisherTestSuite) Test_Publish_ToReadyQueue() {
	err := s.publisher.Publish(context.TODO(), Publishing{
		Headers:  map[string]interface{}{"key1": "value1"},
		Queue:    "test-queue",
		Type:     "test-type",
		Attempts: 10,
		Body:     json.RawMessage(`{"test":{"body":"value"}}`),
	})
	if s.NoError(err) {

		size1, err := redis2.Int(s.redigoConn.Do("LLEN", "rmq::queue::[test-queue]::ready"))
		if s.NoError(err) {
			s.Equal(1, size1)
		}
		size2, err := redis2.Int(s.redigoConn.Do("ZCOUNT", "rmq::queue::[test-queue]::delayed", "-inf", "+inf"))
		if s.NoError(err) {
			s.Equal(0, size2)
		}

		messages, err := redis2.Strings(s.redigoConn.Do("LRANGE", "rmq::queue::[test-queue]::ready", 0, -1))
		if s.NoError(err) {
			s.JSONEq(`{"headers":{"key1":"value1"},"type":"test-type","attempts":10,"data":{"test":{"body":"value"}}}`, messages[0])
		}
	}
}

func (s *publisherTestSuite) Test_Publish_ToDelayedQueue() {
	timeUnix := int64(1405544146)
	err := s.publisher.Publish(context.TODO(), Publishing{
		Headers:   map[string]interface{}{"key1": "value1"},
		Queue:     "test-queue",
		Type:      "test-type",
		Attempts:  10,
		DelayedAt: time.Unix(timeUnix, 0),
		Body:      json.RawMessage(`{"test":{"body":"value"}}`),
	})
	if s.NoError(err) {
		size1, err := redis2.Int(s.redigoConn.Do("ZCOUNT", "rmq::queue::[test-queue]::delayed", "-inf", "+inf"))
		if s.NoError(err) {
			s.Equal(1, size1)
		}

		size2, err := redis2.Int(s.redigoConn.Do("LLEN", "rmq::queue::[test-queue]::ready"))
		if s.NoError(err) {
			s.Equal(0, size2)
		}

		messages, err := redis2.Int64Map(s.redigoConn.Do("ZRANGE", "rmq::queue::[test-queue]::delayed", 0, -1, "WITHSCORES"))
		if s.NoError(err) {
			s.Equal(timeUnix, messages[`{"headers":{"key1":"value1"},"type":"test-type","attempts":10,"data":{"test":{"body":"value"}}}`])
		}
	}

}
