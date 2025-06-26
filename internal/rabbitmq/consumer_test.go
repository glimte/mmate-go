package rabbitmq

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Mock channel for testing
type mockChannel struct {
	mock.Mock
	*amqp.Channel
}

func (m *mockChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	args := m.Called(prefetchCount, prefetchSize, global)
	return args.Error(0)
}

func (m *mockChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	mockArgs := m.Called(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
	if mockArgs.Get(0) == nil {
		return nil, mockArgs.Error(1)
	}
	return mockArgs.Get(0).(<-chan amqp.Delivery), mockArgs.Error(1)
}

func (m *mockChannel) IsClosed() bool {
	args := m.Called()
	return args.Bool(0)
}

func TestConsumer(t *testing.T) {
	t.Run("NewConsumer creates with defaults", func(t *testing.T) {
		pool := &ChannelPool{}
		consumer := NewConsumer(pool)
		
		assert.Equal(t, pool, consumer.pool)
		assert.Equal(t, 10, consumer.prefetchCount)
		assert.Equal(t, 0, consumer.prefetchSize)
		assert.False(t, consumer.autoAck)
		assert.False(t, consumer.exclusive)
		assert.Empty(t, consumer.consumerTag)
		assert.NotNil(t, consumer.logger)
	})
	
	t.Run("NewConsumer applies options", func(t *testing.T) {
		pool := &ChannelPool{}
		logger := slog.Default()
		
		consumer := NewConsumer(
			pool,
			WithPrefetchCount(20),
			WithAutoAck(true),
			WithExclusive(true),
			WithConsumerTag("test-consumer"),
			WithConsumerLogger(logger),
		)
		
		assert.Equal(t, 20, consumer.prefetchCount)
		assert.True(t, consumer.autoAck)
		assert.True(t, consumer.exclusive)
		assert.Equal(t, "test-consumer", consumer.consumerTag)
		assert.Equal(t, logger, consumer.logger)
	})
	
	t.Run("GetActiveConsumers returns empty list initially", func(t *testing.T) {
		consumer := NewConsumer(&ChannelPool{})
		assert.Empty(t, consumer.GetActiveConsumers())
	})
	
	t.Run("Unsubscribe returns error for non-existent queue", func(t *testing.T) {
		consumer := NewConsumer(&ChannelPool{})
		err := consumer.Unsubscribe("non-existent")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no active consumer")
	})
}

func TestConsumerGroup(t *testing.T) {
	t.Run("NewConsumerGroup creates group", func(t *testing.T) {
		consumer := NewConsumer(&ChannelPool{})
		group := NewConsumerGroup(consumer, AckOnSuccess)
		
		assert.Equal(t, consumer, group.consumer)
		assert.Equal(t, AckOnSuccess, group.strategy)
		assert.NotNil(t, group.logger)
	})
	
	t.Run("wrapHandler with AckOnSuccess", func(t *testing.T) {
		consumer := NewConsumer(&ChannelPool{})
		group := NewConsumerGroup(consumer, AckOnSuccess)
		
		called := false
		handler := func(ctx context.Context, delivery amqp.Delivery) error {
			called = true
			return nil
		}
		
		wrapped := group.wrapHandler(handler)
		
		// Create mock delivery
		delivery := amqp.Delivery{}
		mockAck := &mockDeliveryAcknowledger{}
		mockAck.On("Ack", uint64(0), false).Return(nil)
		delivery.Acknowledger = mockAck
		
		// Execute wrapped handler
		err := wrapped(context.Background(), delivery)
		
		assert.NoError(t, err)
		assert.True(t, called)
		mockAck.AssertExpectations(t)
	})
	
	t.Run("wrapHandler with AckOnSuccess and error", func(t *testing.T) {
		consumer := NewConsumer(&ChannelPool{})
		group := NewConsumerGroup(consumer, AckOnSuccess)
		
		handlerErr := errors.New("handler error")
		handler := func(ctx context.Context, delivery amqp.Delivery) error {
			return handlerErr
		}
		
		wrapped := group.wrapHandler(handler)
		
		// Create mock delivery
		delivery := amqp.Delivery{}
		mockAck := &mockDeliveryAcknowledger{}
		mockAck.On("Nack", uint64(0), false, true).Return(nil)
		delivery.Acknowledger = mockAck
		
		// Execute wrapped handler
		err := wrapped(context.Background(), delivery)
		
		assert.NoError(t, err) // Nack error is not returned
		mockAck.AssertExpectations(t)
	})
	
	t.Run("wrapHandler with AckAlways", func(t *testing.T) {
		consumer := NewConsumer(&ChannelPool{})
		group := NewConsumerGroup(consumer, AckAlways)
		
		handlerErr := errors.New("handler error")
		handler := func(ctx context.Context, delivery amqp.Delivery) error {
			return handlerErr
		}
		
		wrapped := group.wrapHandler(handler)
		
		// Create mock delivery
		delivery := amqp.Delivery{}
		mockAck := &mockDeliveryAcknowledger{}
		mockAck.On("Ack", uint64(0), false).Return(nil)
		delivery.Acknowledger = mockAck
		
		// Execute wrapped handler
		err := wrapped(context.Background(), delivery)
		
		assert.Equal(t, handlerErr, err) // Original error is returned
		mockAck.AssertExpectations(t)
	})
	
	t.Run("wrapHandler with AckManual", func(t *testing.T) {
		consumer := NewConsumer(&ChannelPool{})
		group := NewConsumerGroup(consumer, AckManual)
		
		handlerErr := errors.New("handler error")
		handler := func(ctx context.Context, delivery amqp.Delivery) error {
			return handlerErr
		}
		
		wrapped := group.wrapHandler(handler)
		
		// Create mock delivery (no ack expected)
		delivery := amqp.Delivery{}
		
		// Execute wrapped handler
		err := wrapped(context.Background(), delivery)
		
		assert.Equal(t, handlerErr, err)
	})
}

func TestAcknowledgmentStrategy(t *testing.T) {
	t.Run("strategy constants", func(t *testing.T) {
		assert.Equal(t, AcknowledgmentStrategy(0), AckOnSuccess)
		assert.Equal(t, AcknowledgmentStrategy(1), AckAlways)
		assert.Equal(t, AcknowledgmentStrategy(2), AckManual)
	})
}

// mockListener for testing
type mockListener struct {
	mock.Mock
}

func (m *mockListener) OnConnected() {
	m.Called()
}

func (m *mockListener) OnDisconnected(err error) {
	m.Called(err)
}

func (m *mockListener) OnReconnecting(attempt int) {
	m.Called(attempt)
}

func TestConnectionStateListener(t *testing.T) {
	t.Run("listener interface", func(t *testing.T) {
		// Verify it implements the interface
		var _ ConnectionStateListener = (*mockListener)(nil)
		
		// Test usage
		listener := &mockListener{}
		listener.On("OnConnected").Return()
		listener.On("OnDisconnected", mock.Anything).Return()
		listener.On("OnReconnecting", 1).Return()
		
		listener.OnConnected()
		listener.OnDisconnected(errors.New("test"))
		listener.OnReconnecting(1)
		
		listener.AssertExpectations(t)
	})
}

// mockDeliveryAcknowledger for testing - renamed to avoid conflict
type mockDeliveryAcknowledger struct {
	mock.Mock
}

func (m *mockDeliveryAcknowledger) Ack(tag uint64, multiple bool) error {
	args := m.Called(tag, multiple)
	return args.Error(0)
}

func (m *mockDeliveryAcknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
	args := m.Called(tag, multiple, requeue)
	return args.Error(0)
}

func (m *mockDeliveryAcknowledger) Reject(tag uint64, requeue bool) error {
	args := m.Called(tag, requeue)
	return args.Error(0)
}