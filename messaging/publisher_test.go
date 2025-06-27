package messaging

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/internal/reliability"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Mock TransportPublisher
type mockTransportPublisher struct {
	mock.Mock
}

func (m *mockTransportPublisher) Publish(ctx context.Context, exchange, routingKey string, envelope *contracts.Envelope) error {
	args := m.Called(ctx, exchange, routingKey, envelope)
	return args.Error(0)
}

func (m *mockTransportPublisher) PublishWithConfirm(ctx context.Context, exchange, routingKey string, envelope *contracts.Envelope) error {
	args := m.Called(ctx, exchange, routingKey, envelope)
	return args.Error(0)
}

// PublishBatch is not currently part of TransportPublisher interface
// Remove this method from the mock

func (m *mockTransportPublisher) PublishWithTransaction(ctx context.Context, exchange, routingKey string, envelope *contracts.Envelope) error {
	args := m.Called(ctx, exchange, routingKey, envelope)
	return args.Error(0)
}

func (m *mockTransportPublisher) Close() error {
	args := m.Called()
	return args.Error(0)
}

// Test Messages
type testEvent struct {
	contracts.BaseEvent
	Data string `json:"data"`
}

type testCommand struct {
	contracts.BaseCommand
	Data string `json:"data"`
}

type testQuery struct {
	contracts.BaseQuery
	Filter string `json:"filter"`
}

type testReply struct {
	contracts.BaseReply
	Result string `json:"result"`
}

func TestNewMessagePublisher(t *testing.T) {
	t.Run("creates publisher with defaults", func(t *testing.T) {
		transport := &mockTransportPublisher{}
		publisher := NewMessagePublisher(transport)
		
		assert.NotNil(t, publisher)
		assert.Equal(t, transport, publisher.transport)
		assert.NotNil(t, publisher.logger)
		assert.Equal(t, 5*time.Minute, publisher.defaultTTL)
		assert.NotNil(t, publisher.retryPolicy)
		assert.NotNil(t, publisher.factory)
		assert.Nil(t, publisher.circuitBreaker)
	})
	
	t.Run("applies options", func(t *testing.T) {
		transport := &mockTransportPublisher{}
		cb := reliability.NewCircuitBreaker(
			reliability.WithFailureThreshold(5),
			reliability.WithTimeout(30*time.Second),
		)
		ttl := 10 * time.Minute
		
		publisher := NewMessagePublisher(transport,
			WithDefaultTTL(ttl),
			WithCircuitBreaker(cb),
		)
		
		assert.Equal(t, ttl, publisher.defaultTTL)
		assert.Equal(t, cb, publisher.circuitBreaker)
	})
}

func TestPublish(t *testing.T) {
	t.Run("publishes message successfully", func(t *testing.T) {
		transport := &mockTransportPublisher{}
		publisher := NewMessagePublisher(transport)
		
		msg := &testEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
				AggregateID: "order-123",
			},
			Data: "test data",
		}
		
		transport.On("Publish", mock.Anything, "mmate.messages", mock.Anything, mock.Anything).Return(nil)
		
		err := publisher.Publish(context.Background(), msg)
		
		assert.NoError(t, err)
		transport.AssertExpectations(t)
		
		// Verify envelope was created correctly
		assert.Equal(t, 1, len(transport.Calls))
		envelope := transport.Calls[0].Arguments[3].(*contracts.Envelope)
		assert.Equal(t, msg.GetID(), envelope.ID)
		assert.Equal(t, "TestEvent", envelope.Type)
	})
	
	t.Run("fails with nil message", func(t *testing.T) {
		transport := &mockTransportPublisher{}
		publisher := NewMessagePublisher(transport)
		
		err := publisher.Publish(context.Background(), nil)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "message cannot be nil")
	})
	
	t.Run("applies publish options", func(t *testing.T) {
		transport := &mockTransportPublisher{}
		publisher := NewMessagePublisher(transport)
		
		msg := &testEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
			},
		}
		
		transport.On("Publish", mock.Anything, "custom.exchange", "custom.key", mock.Anything).Return(nil)
		
		err := publisher.Publish(context.Background(), msg,
			WithExchange("custom.exchange"),
			WithRoutingKey("custom.key"),
			WithPriority(5),
			WithTTL(60*time.Second),
		)
		
		assert.NoError(t, err)
		transport.AssertExpectations(t)
	})
	
	t.Run("retries on failure", func(t *testing.T) {
		transport := &mockTransportPublisher{}
		retryPolicy := reliability.NewExponentialBackoff(time.Millisecond, time.Second, 2.0, 3)
		publisher := NewMessagePublisher(transport, WithRetryPolicy(retryPolicy))
		
		msg := &testEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
			},
		}
		
		// Fail first attempt, succeed on retry
		transport.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(errors.New("temporary error")).Once()
		transport.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil).Once()
		
		err := publisher.Publish(context.Background(), msg)
		
		assert.NoError(t, err)
		assert.Equal(t, 2, len(transport.Calls))
	})
	
	t.Run("circuit breaker integration", func(t *testing.T) {
		transport := &mockTransportPublisher{}
		cb := reliability.NewCircuitBreaker(
			reliability.WithFailureThreshold(1),
			reliability.WithTimeout(time.Second),
		)
		// Create publisher without retry policy to isolate circuit breaker behavior
		publisher := NewMessagePublisher(transport, 
			WithCircuitBreaker(cb),
			WithRetryPolicy(reliability.NewExponentialBackoff(time.Millisecond, time.Second, 2.0, 0)), // 0 retries
		)
		
		msg := &testEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
			},
		}
		
		// First call fails
		transport.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(errors.New("error")).Once()
		
		err := publisher.Publish(context.Background(), msg)
		assert.Error(t, err)
		
		// Circuit should be open now - second call should fail immediately without calling transport
		err = publisher.Publish(context.Background(), msg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "circuit breaker open")
		
		// Verify transport was only called once
		transport.AssertExpectations(t)
	})
}

func TestPublishCommand(t *testing.T) {
	t.Run("publishes command with correct routing", func(t *testing.T) {
		transport := &mockTransportPublisher{}
		publisher := NewMessagePublisher(transport)
		
		cmd := &testCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage:    contracts.NewBaseMessage("CreateOrder"),
				TargetService: "order-service",
			},
			Data: "order data",
		}
		
		transport.On("Publish", mock.Anything, "mmate.commands", "cmd.order-service.CreateOrder", mock.Anything).Return(nil)
		
		err := publisher.PublishCommand(context.Background(), cmd)
		
		assert.NoError(t, err)
		transport.AssertExpectations(t)
	})
	
	t.Run("allows overriding command options", func(t *testing.T) {
		transport := &mockTransportPublisher{}
		publisher := NewMessagePublisher(transport)
		
		cmd := &testCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage:    contracts.NewBaseMessage("CreateOrder"),
				TargetService: "order-service",
			},
		}
		
		transport.On("Publish", mock.Anything, "custom.commands", "cmd.order-service.CreateOrder", mock.Anything).Return(nil)
		
		err := publisher.PublishCommand(context.Background(), cmd, WithExchange("custom.commands"))
		
		assert.NoError(t, err)
		transport.AssertExpectations(t)
	})
}

func TestPublishEvent(t *testing.T) {
	t.Run("publishes event with correct routing", func(t *testing.T) {
		transport := &mockTransportPublisher{}
		publisher := NewMessagePublisher(transport)
		
		evt := &testEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("OrderCreated"),
				AggregateID: "order-123",
			},
			Data: "event data",
		}
		
		transport.On("Publish", mock.Anything, "mmate.events", "evt.order-123.OrderCreated", mock.Anything).Return(nil)
		
		err := publisher.PublishEvent(context.Background(), evt)
		
		assert.NoError(t, err)
		transport.AssertExpectations(t)
	})
}

func TestPublishReply(t *testing.T) {
	t.Run("publishes reply to specific queue", func(t *testing.T) {
		transport := &mockTransportPublisher{}
		publisher := NewMessagePublisher(transport)
		
		reply := &testReply{
			BaseReply: contracts.BaseReply{
				BaseMessage: contracts.NewBaseMessage("OrderQueryResult"),
			},
			Result: "query result",
		}
		
		transport.On("Publish", mock.Anything, "", "reply.queue", mock.Anything).Return(nil)
		
		err := publisher.PublishReply(context.Background(), reply, "reply.queue")
		
		assert.NoError(t, err)
		transport.AssertExpectations(t)
	})
}

func TestGetRoutingKey(t *testing.T) {
	publisher := &MessagePublisher{}
	
	tests := []struct {
		name     string
		message  contracts.Message
		expected string
	}{
		{
			name: "command routing",
			message: &testCommand{
				BaseCommand: contracts.BaseCommand{
					BaseMessage:    contracts.NewBaseMessage("CreateOrder"),
					TargetService: "order-service",
				},
			},
			expected: "cmd.order-service.CreateOrder",
		},
		{
			name: "event routing",
			message: &testEvent{
				BaseEvent: contracts.BaseEvent{
					BaseMessage: contracts.NewBaseMessage("OrderCreated"),
					AggregateID: "order-123",
				},
			},
			expected: "evt.order-123.OrderCreated",
		},
		{
			name: "query routing",
			message: &testQuery{
				BaseQuery: contracts.BaseQuery{
					BaseMessage: contracts.NewBaseMessage("GetOrders"),
				},
			},
			expected: "qry.GetOrders",
		},
		{
			name: "reply routing",
			message: &testReply{
				BaseReply: contracts.BaseReply{
					BaseMessage: contracts.NewBaseMessage("OrdersResult"),
				},
			},
			expected: "rpl.OrdersResult",
		},
		{
			name: "default routing",
			message: &contracts.BaseMessage{
				Type: "CustomMessage",
			},
			expected: "msg.CustomMessage",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := publisher.getRoutingKey(tt.message)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAddStandardHeaders(t *testing.T) {
	publisher := &MessagePublisher{}
	
	t.Run("adds all standard headers", func(t *testing.T) {
		msg := &testEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.BaseMessage{
					ID:            "msg-123",
					Type:          "TestEvent",
					Timestamp:     time.Now(),
					CorrelationID: "corr-456",
				},
			},
		}
		
		opts := &PublishOptions{
			Headers:      make(map[string]interface{}),
			TTL:          30 * time.Second,
			Priority:     5,
			DeliveryMode: 2,
		}
		
		publisher.addStandardHeaders(opts, msg)
		
		assert.Equal(t, "msg-123", opts.Headers["message-id"])
		assert.Equal(t, "TestEvent", opts.Headers["message-type"])
		assert.NotEmpty(t, opts.Headers["timestamp"])
		assert.Equal(t, "mmate-go", opts.Headers["source"])
		assert.Equal(t, "corr-456", opts.Headers["correlation-id"])
		assert.Equal(t, "30000", opts.Headers["expiration"])
		assert.Equal(t, uint8(5), opts.Headers["priority"])
		assert.Equal(t, uint8(2), opts.Headers["delivery-mode"])
	})
	
	t.Run("skips optional headers when not set", func(t *testing.T) {
		msg := &testEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.BaseMessage{
					ID:        "msg-123",
					Type:      "TestEvent",
					Timestamp: time.Now(),
					// No correlation ID
				},
			},
		}
		
		opts := &PublishOptions{
			Headers: make(map[string]interface{}),
			// No TTL
		}
		
		publisher.addStandardHeaders(opts, msg)
		
		_, hasCorrelationID := opts.Headers["correlation-id"]
		_, hasExpiration := opts.Headers["expiration"]
		
		assert.False(t, hasCorrelationID)
		assert.False(t, hasExpiration)
	})
}

func TestPublisherClose(t *testing.T) {
	t.Run("closes transport", func(t *testing.T) {
		transport := &mockTransportPublisher{}
		publisher := NewMessagePublisher(transport)
		
		transport.On("Close").Return(nil)
		
		err := publisher.Close()
		
		assert.NoError(t, err)
		transport.AssertExpectations(t)
	})
	
	t.Run("returns transport close error", func(t *testing.T) {
		transport := &mockTransportPublisher{}
		publisher := NewMessagePublisher(transport)
		
		expectedErr := errors.New("close error")
		transport.On("Close").Return(expectedErr)
		
		err := publisher.Close()
		
		assert.Equal(t, expectedErr, err)
		transport.AssertExpectations(t)
	})
}