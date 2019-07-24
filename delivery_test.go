package redisq

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/adjust/rmq"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

func TestDeliverySuite(t *testing.T) {
	suite.Run(t, &deliveryTestSuite{})
}

type deliveryTestSuite struct {
	suite.Suite
	delivery      *Delivery
	mockPublisher *MockPublisher
	rmqDelivery   *rmq.TestDelivery
	ctx           context.Context
}

func (s *deliveryTestSuite) SetupTest() {
	s.rmqDelivery = rmq.NewTestDelivery(`{"headers":{"key1":"value1"},"type":"test-type","attempts":10,"data":{"test":{"body":"value"}}}`)
	s.mockPublisher = &MockPublisher{}
	d, err := NewDelivery("test-queue", s.rmqDelivery, s.mockPublisher)
	s.NoError(err)
	s.delivery = d
	s.ctx = context.TODO()
}

func (s *deliveryTestSuite) Test_NewDelivery() {
	s.Equal("test-queue", s.delivery.Queue)
	s.Equal(map[string]interface{}{"key1": "value1"}, s.delivery.Headers)
	s.Equal("test-type", s.delivery.Type)
	s.Equal(uint(10), s.delivery.Attempts)
	s.Equal([]byte(`{"test":{"body":"value"}}`), s.delivery.Body)
	s.Equal(&acknowledger{s.rmqDelivery, s.mockPublisher}, s.delivery.Acknowledger)
}

func (s *deliveryTestSuite) Test_Ack() {
	if s.NoError(s.delivery.Ack(s.ctx)) {
		s.Equal(rmq.Acked, s.rmqDelivery.State)
	}
}

func (s *deliveryTestSuite) Test_Ack_Error() {
	s.rmqDelivery.State = rmq.Rejected
	s.Error(s.delivery.Ack(s.ctx), "cannot ack the message")
}

func (s *deliveryTestSuite) Test_Reject() {
	if s.NoError(s.delivery.Reject(s.ctx)) {
		s.Equal(rmq.Rejected, s.rmqDelivery.State)
	}
}

func (s *deliveryTestSuite) Test_Reject_Error() {
	s.rmqDelivery.State = rmq.Acked
	s.Error(s.delivery.Reject(s.ctx), "cannot reject the message")
}

func (s *deliveryTestSuite) Test_Push() {
	if s.NoError(s.delivery.Push(s.ctx)) {
		s.Equal(rmq.Pushed, s.rmqDelivery.State)
	}
}

func (s *deliveryTestSuite) Test_Push_Error() {
	s.rmqDelivery.State = rmq.Acked
	s.Error(s.delivery.Push(s.ctx), "cannot push the message")
}

func (s *deliveryTestSuite) Test_Delay() {
	delay := time.Unix(int64(1405544146), 0)
	s.mockPublisher.
		On("Publish",
			s.ctx,
			mock.MatchedBy(func(pub Publishing) bool {
				expect := Publishing{
					Headers:   map[string]interface{}{"key1": "value1"},
					Queue:     "test-queue",
					Type:      "test-type",
					Attempts:  11,
					DelayedAt: delay,
					Body:      json.RawMessage(`{"test":{"body":"value"}}`),
				}
				return s.Equal(expect, pub)
			}),
		).Return(nil).Once()

	s.NoError(s.delivery.Delay(s.ctx, delay))
	s.Equal(rmq.Acked, s.rmqDelivery.State)

	s.mockPublisher.AssertExpectations(s.T())
}

func (s *deliveryTestSuite) Test_Delay_Error() {
	delay := time.Unix(int64(1405544146), 0)
	ctx := context.TODO()
	stubErr := errors.New("test delay error")
	s.mockPublisher.
		On("Publish",
			ctx,
			mock.MatchedBy(func(pub Publishing) bool {
				expect := Publishing{
					Headers:   map[string]interface{}{"key1": "value1"},
					Queue:     "test-queue",
					Type:      "test-type",
					Attempts:  11,
					DelayedAt: delay,
					Body:      json.RawMessage(`{"test":{"body":"value"}}`),
				}
				return s.Equal(expect, pub)
			}),
		).Return(stubErr).Once()

	s.Equal(stubErr, s.delivery.Delay(ctx, delay))
	s.Equal(rmq.Unacked, s.rmqDelivery.State)

	s.mockPublisher.AssertExpectations(s.T())
}

func (s *deliveryTestSuite) Test_Delay_AckError() {
	s.rmqDelivery.State = rmq.Rejected
	delay := time.Unix(int64(1405544146), 0)
	s.mockPublisher.
		On("Publish",
			s.ctx,
			mock.MatchedBy(func(pub Publishing) bool {
				expect := Publishing{
					Headers:   map[string]interface{}{"key1": "value1"},
					Queue:     "test-queue",
					Type:      "test-type",
					Attempts:  11,
					DelayedAt: delay,
					Body:      json.RawMessage(`{"test":{"body":"value"}}`),
				}
				return s.Equal(expect, pub)
			}),
		).Return(nil).Once()

	s.Error(s.delivery.Delay(s.ctx, delay), "cannot ack after delayed message")
	s.mockPublisher.AssertExpectations(s.T())
}
