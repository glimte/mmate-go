package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Test messages for subscriber (named differently to avoid conflicts with publisher_test.go)
type subscriberTestEvent struct {
	contracts.BaseEvent
	Data string `json:"data"`
}

// Mock TransportSubscriber
type mockTransportSubscriber struct {
	mock.Mock
}

func (m *mockTransportSubscriber) Subscribe(ctx context.Context, queue string, handler func(TransportDelivery) error, opts SubscriptionOptions) error {
	args := m.Called(ctx, queue, handler, opts)
	return args.Error(0)
}

func (m *mockTransportSubscriber) Unsubscribe(queue string) error {
	args := m.Called(queue)
	return args.Error(0)
}

func (m *mockTransportSubscriber) GetConsumerCount(queue string) (int, error) {
	args := m.Called(queue)
	return args.Int(0), args.Error(1)
}

func (m *mockTransportSubscriber) SetQosSettings(prefetchCount int, prefetchSize int, global bool) error {
	args := m.Called(prefetchCount, prefetchSize, global)
	return args.Error(0)
}

func (m *mockTransportSubscriber) Close() error {
	args := m.Called()
	return args.Error(0)
}

// Mock TransportDelivery
type mockTransportDelivery struct {
	mock.Mock
}

func (m *mockTransportDelivery) Body() []byte {
	args := m.Called()
	return args.Get(0).([]byte)
}

func (m *mockTransportDelivery) Headers() map[string]interface{} {
	args := m.Called()
	return args.Get(0).(map[string]interface{})
}

func (m *mockTransportDelivery) Acknowledge() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockTransportDelivery) Reject(requeue bool) error {
	args := m.Called(requeue)
	return args.Error(0)
}

func (m *mockTransportDelivery) ConsumerTag() string {
	args := m.Called()
	return args.String(0)
}

func (m *mockTransportDelivery) MessageCount() uint32 {
	args := m.Called()
	return args.Get(0).(uint32)
}

// Mock MessageHandler
type mockMessageHandler struct {
	mock.Mock
}

func (m *mockMessageHandler) Handle(ctx context.Context, msg contracts.Message) error {
	args := m.Called(ctx, msg)
	return args.Error(0)
}

func TestNewMessageSubscriber(t *testing.T) {
	t.Run("creates subscriber with defaults", func(t *testing.T) {
		transport := &mockTransportSubscriber{}
		dispatcher := NewMessageDispatcher()
		subscriber := NewMessageSubscriber(transport, dispatcher)
		
		assert.NotNil(t, subscriber)
		assert.Equal(t, transport, subscriber.transport)
		assert.Equal(t, dispatcher, subscriber.dispatcher)
		assert.NotNil(t, subscriber.logger)
		assert.NotNil(t, subscriber.errorHandler)
		assert.Equal(t, "mmate.dlq", subscriber.deadLetterQueue)
		assert.NotNil(t, subscriber.subscriptions)
	})
	
	t.Run("applies options", func(t *testing.T) {
		transport := &mockTransportSubscriber{}
		dispatcher := NewMessageDispatcher()
		customDLQ := "custom.dlq"
		
		subscriber := NewMessageSubscriber(transport, dispatcher,
			WithDeadLetterQueue(customDLQ),
		)
		
		assert.Equal(t, customDLQ, subscriber.deadLetterQueue)
	})
}

func TestSubscribe(t *testing.T) {
	t.Run("subscribes successfully", func(t *testing.T) {
		transport := &mockTransportSubscriber{}
		dispatcher := NewMessageDispatcher()
		subscriber := NewMessageSubscriber(transport, dispatcher)
		handler := &mockMessageHandler{}
		
		transport.On("Subscribe", mock.Anything, "test.queue", mock.Anything, mock.Anything).Return(nil)
		
		err := subscriber.Subscribe(context.Background(), "test.queue", "TestMessage", handler)
		
		assert.NoError(t, err)
		transport.AssertExpectations(t)
		
		// Verify subscription is stored
		subs := subscriber.GetSubscriptions()
		assert.Len(t, subs, 1)
		assert.Equal(t, "test.queue", subs["test.queue"].Queue)
		assert.Equal(t, "TestMessage", subs["test.queue"].MessageType)
		assert.Equal(t, handler, subs["test.queue"].Handler)
	})
	
	t.Run("fails with empty queue", func(t *testing.T) {
		transport := &mockTransportSubscriber{}
		dispatcher := NewMessageDispatcher()
		subscriber := NewMessageSubscriber(transport, dispatcher)
		handler := &mockMessageHandler{}
		
		err := subscriber.Subscribe(context.Background(), "", "TestMessage", handler)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "queue name cannot be empty")
	})
	
	t.Run("fails with empty message type", func(t *testing.T) {
		transport := &mockTransportSubscriber{}
		dispatcher := NewMessageDispatcher()
		subscriber := NewMessageSubscriber(transport, dispatcher)
		handler := &mockMessageHandler{}
		
		err := subscriber.Subscribe(context.Background(), "test.queue", "", handler)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "message type cannot be empty")
	})
	
	t.Run("fails with nil handler", func(t *testing.T) {
		transport := &mockTransportSubscriber{}
		dispatcher := NewMessageDispatcher()
		subscriber := NewMessageSubscriber(transport, dispatcher)
		
		err := subscriber.Subscribe(context.Background(), "test.queue", "TestMessage", nil)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "handler cannot be nil")
	})
	
	t.Run("fails if already subscribed", func(t *testing.T) {
		transport := &mockTransportSubscriber{}
		dispatcher := NewMessageDispatcher()
		subscriber := NewMessageSubscriber(transport, dispatcher)
		handler := &mockMessageHandler{}
		
		transport.On("Subscribe", mock.Anything, "test.queue", mock.Anything, mock.Anything).Return(nil).Once()
		
		// First subscription succeeds
		err := subscriber.Subscribe(context.Background(), "test.queue", "TestMessage", handler)
		assert.NoError(t, err)
		
		// Second subscription to same queue fails
		err = subscriber.Subscribe(context.Background(), "test.queue", "TestMessage", handler)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already subscribed to queue")
	})
	
	t.Run("applies subscription options", func(t *testing.T) {
		transport := &mockTransportSubscriber{}
		dispatcher := NewMessageDispatcher()
		subscriber := NewMessageSubscriber(transport, dispatcher)
		handler := &mockMessageHandler{}
		
		expectedOpts := SubscriptionOptions{
			PrefetchCount:      20,
			AutoAck:            true,
			Exclusive:          true,
			Durable:            false,
			AutoDelete:         true,
			Arguments:          make(map[string]interface{}),
			MaxRetries:         5,
			DeadLetterExchange: "custom.dlx",
		}
		
		transport.On("Subscribe", mock.Anything, "test.queue", mock.Anything, mock.MatchedBy(func(opts SubscriptionOptions) bool {
			return opts.PrefetchCount == expectedOpts.PrefetchCount &&
				opts.AutoAck == expectedOpts.AutoAck &&
				opts.Exclusive == expectedOpts.Exclusive &&
				opts.Durable == expectedOpts.Durable &&
				opts.AutoDelete == expectedOpts.AutoDelete &&
				opts.MaxRetries == expectedOpts.MaxRetries &&
				opts.DeadLetterExchange == expectedOpts.DeadLetterExchange
		})).Return(nil)
		
		err := subscriber.Subscribe(context.Background(), "test.queue", "TestMessage", handler,
			WithPrefetchCount(20),
			WithAutoAck(true),
			WithSubscriberExclusive(true),
			WithSubscriberDurable(false),
			WithAutoDelete(true),
			WithMaxRetries(5),
			WithDeadLetterExchange("custom.dlx"),
		)
		
		assert.NoError(t, err)
		transport.AssertExpectations(t)
	})
}

func TestUnsubscribe(t *testing.T) {
	t.Run("unsubscribes successfully", func(t *testing.T) {
		transport := &mockTransportSubscriber{}
		dispatcher := NewMessageDispatcher()
		subscriber := NewMessageSubscriber(transport, dispatcher)
		handler := &mockMessageHandler{}
		
		// Subscribe first
		transport.On("Subscribe", mock.Anything, "test.queue", mock.Anything, mock.Anything).Return(nil)
		err := subscriber.Subscribe(context.Background(), "test.queue", "TestMessage", handler)
		assert.NoError(t, err)
		
		// Then unsubscribe
		transport.On("Unsubscribe", "test.queue").Return(nil)
		err = subscriber.Unsubscribe("test.queue")
		
		assert.NoError(t, err)
		transport.AssertExpectations(t)
		
		// Verify subscription is removed
		subs := subscriber.GetSubscriptions()
		assert.Len(t, subs, 0)
	})
	
	t.Run("fails if not subscribed", func(t *testing.T) {
		transport := &mockTransportSubscriber{}
		dispatcher := NewMessageDispatcher()
		subscriber := NewMessageSubscriber(transport, dispatcher)
		
		err := subscriber.Unsubscribe("test.queue")
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not subscribed to queue")
	})
	
	t.Run("continues even if transport unsubscribe fails", func(t *testing.T) {
		transport := &mockTransportSubscriber{}
		dispatcher := NewMessageDispatcher()
		subscriber := NewMessageSubscriber(transport, dispatcher)
		handler := &mockMessageHandler{}
		
		// Subscribe first
		transport.On("Subscribe", mock.Anything, "test.queue", mock.Anything, mock.Anything).Return(nil)
		err := subscriber.Subscribe(context.Background(), "test.queue", "TestMessage", handler)
		assert.NoError(t, err)
		
		// Transport unsubscribe fails
		transport.On("Unsubscribe", "test.queue").Return(errors.New("transport error"))
		err = subscriber.Unsubscribe("test.queue")
		
		// Should not return error
		assert.NoError(t, err)
		
		// Subscription should still be removed
		subs := subscriber.GetSubscriptions()
		assert.Len(t, subs, 0)
	})
}

func TestHandleDelivery(t *testing.T) {
	t.Run("processes valid message successfully", func(t *testing.T) {
		transport := &mockTransportSubscriber{}
		dispatcher := NewMessageDispatcher()
		subscriber := NewMessageSubscriber(transport, dispatcher)
		handler := &mockMessageHandler{}
		delivery := &mockTransportDelivery{}
		
		// Create test message
		msg := &subscriberTestEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
			},
			Data: "test data",
		}
		
		// Create envelope
		envelope := contracts.Envelope{
			ID:        msg.GetID(),
			Type:      msg.GetType(),
			Timestamp: msg.GetTimestamp().Format(time.RFC3339),
			Payload:   json.RawMessage(`{"data":"test data"}`),
		}
		envelopeData, _ := json.Marshal(envelope)
		
		delivery.On("Body").Return(envelopeData)
		delivery.On("Acknowledge").Return(nil)
		handler.On("Handle", mock.Anything, mock.Anything).Return(nil)
		
		subscription := &Subscription{
			Queue:       "test.queue",
			MessageType: "TestEvent",
			Handler:     handler,
			Options:     SubscriptionOptions{AutoAck: false},
		}
		
		subscriber.handleDelivery(context.Background(), delivery, subscription)
		
		delivery.AssertExpectations(t)
		handler.AssertExpectations(t)
	})
	
	t.Run("rejects invalid envelope", func(t *testing.T) {
		transport := &mockTransportSubscriber{}
		dispatcher := NewMessageDispatcher()
		subscriber := NewMessageSubscriber(transport, dispatcher)
		handler := &mockMessageHandler{}
		delivery := &mockTransportDelivery{}
		
		delivery.On("Body").Return([]byte("invalid json"))
		delivery.On("Reject", false).Return(nil) // Don't requeue invalid messages
		
		subscription := &Subscription{
			Queue:       "test.queue",
			MessageType: "TestEvent",
			Handler:     handler,
			Options:     SubscriptionOptions{AutoAck: false},
		}
		
		subscriber.handleDelivery(context.Background(), delivery, subscription)
		
		delivery.AssertExpectations(t)
		handler.AssertNotCalled(t, "Handle")
	})
	
	t.Run("handles handler error with reject action", func(t *testing.T) {
		transport := &mockTransportSubscriber{}
		dispatcher := NewMessageDispatcher()
		subscriber := NewMessageSubscriber(transport, dispatcher)
		handler := &mockMessageHandler{}
		delivery := &mockTransportDelivery{}
		
		// Create test message
		msg := &subscriberTestEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
			},
		}
		
		envelope := contracts.Envelope{
			ID:        msg.GetID(),
			Type:      msg.GetType(),
			Timestamp: msg.GetTimestamp().Format(time.RFC3339),
			Payload:   json.RawMessage(`{}`),
		}
		envelopeData, _ := json.Marshal(envelope)
		
		delivery.On("Body").Return(envelopeData)
		delivery.On("Reject", false).Return(nil) // Send to DLQ
		handler.On("Handle", mock.Anything, mock.Anything).Return(errors.New("handler error"))
		
		subscription := &Subscription{
			Queue:       "test.queue",
			MessageType: "TestEvent",
			Handler:     handler,
			Options:     SubscriptionOptions{AutoAck: false},
		}
		
		subscriber.handleDelivery(context.Background(), delivery, subscription)
		
		delivery.AssertExpectations(t)
		handler.AssertExpectations(t)
	})
	
	t.Run("auto acknowledges when AutoAck is true", func(t *testing.T) {
		transport := &mockTransportSubscriber{}
		dispatcher := NewMessageDispatcher()
		subscriber := NewMessageSubscriber(transport, dispatcher)
		handler := &mockMessageHandler{}
		delivery := &mockTransportDelivery{}
		
		// Create test message
		msg := &subscriberTestEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
			},
		}
		
		envelope := contracts.Envelope{
			ID:        msg.GetID(),
			Type:      msg.GetType(),
			Timestamp: msg.GetTimestamp().Format(time.RFC3339),
			Payload:   json.RawMessage(`{}`),
		}
		envelopeData, _ := json.Marshal(envelope)
		
		delivery.On("Body").Return(envelopeData)
		// Should not call Acknowledge when AutoAck is true
		handler.On("Handle", mock.Anything, mock.Anything).Return(nil)
		
		subscription := &Subscription{
			Queue:       "test.queue",
			MessageType: "TestEvent",
			Handler:     handler,
			Options:     SubscriptionOptions{AutoAck: true},
		}
		
		subscriber.handleDelivery(context.Background(), delivery, subscription)
		
		delivery.AssertExpectations(t)
		delivery.AssertNotCalled(t, "Acknowledge")
		handler.AssertExpectations(t)
	})
}

func TestExtractMessage(t *testing.T) {
	subscriber := &MessageSubscriber{}
	
	// Register test message type
	registry := GetTypeRegistry()
	registry.Register("TestEvent", &subscriberTestEvent{})
	
	t.Run("extracts registered message type", func(t *testing.T) {
		envelope := &contracts.Envelope{
			ID:        "msg-123",
			Type:      "TestEvent",
			Timestamp: time.Now().Format(time.RFC3339),
			Payload:   json.RawMessage(`{"aggregateId":"","sequence":0,"data":"test data"}`),
		}
		
		msg, err := subscriber.extractMessage(envelope)
		
		assert.NoError(t, err)
		assert.NotNil(t, msg)
		assert.Equal(t, "msg-123", msg.GetID())
		assert.Equal(t, "TestEvent", msg.GetType())
		
		// Check specific fields if casting succeeds
		if testMsg, ok := msg.(*subscriberTestEvent); ok {
			assert.Equal(t, "test data", testMsg.Data)
		} else {
			t.Errorf("Message is not of type *subscriberTestEvent: %T", msg)
		}
	})
	
	t.Run("handles unregistered message type", func(t *testing.T) {
		envelope := &contracts.Envelope{
			ID:            "msg-456",
			Type:          "UnknownMessage",
			Timestamp:     time.Now().Format(time.RFC3339),
			CorrelationID: "corr-789",
		}
		
		msg, err := subscriber.extractMessage(envelope)
		
		assert.NoError(t, err)
		assert.NotNil(t, msg)
		assert.Equal(t, "msg-456", msg.GetID())
		assert.Equal(t, "UnknownMessage", msg.GetType())
		assert.Equal(t, "corr-789", msg.GetCorrelationID())
	})
	
	t.Run("merges envelope and payload fields", func(t *testing.T) {
		envelope := &contracts.Envelope{
			ID:            "msg-789",
			Type:          "TestEvent",
			Timestamp:     time.Now().Format(time.RFC3339),
			CorrelationID: "corr-123",
			Payload:       json.RawMessage(`{"aggregateId":"","sequence":0,"data":"payload data","extra":"field"}`),
		}
		
		msg, err := subscriber.extractMessage(envelope)
		
		assert.NoError(t, err)
		assert.NotNil(t, msg)
		
		testMsg, ok := msg.(*subscriberTestEvent)
		assert.True(t, ok)
		assert.Equal(t, "msg-789", testMsg.GetID())
		assert.Equal(t, "corr-123", testMsg.GetCorrelationID())
		assert.Equal(t, "payload data", testMsg.Data)
	})
}

func TestClose(t *testing.T) {
	t.Run("closes all subscriptions", func(t *testing.T) {
		transport := &mockTransportSubscriber{}
		dispatcher := NewMessageDispatcher()
		subscriber := NewMessageSubscriber(transport, dispatcher)
		handler := &mockMessageHandler{}
		
		// Subscribe to multiple queues
		transport.On("Subscribe", mock.Anything, "queue1", mock.Anything, mock.Anything).Return(nil)
		transport.On("Subscribe", mock.Anything, "queue2", mock.Anything, mock.Anything).Return(nil)
		
		err := subscriber.Subscribe(context.Background(), "queue1", "TestMessage", handler)
		assert.NoError(t, err)
		err = subscriber.Subscribe(context.Background(), "queue2", "TestMessage", handler)
		assert.NoError(t, err)
		
		// Close should cancel all subscriptions
		err = subscriber.Close()
		assert.NoError(t, err)
		
		// Verify all subscriptions are removed
		subs := subscriber.GetSubscriptions()
		assert.Len(t, subs, 0)
	})
}

func TestDefaultErrorHandler(t *testing.T) {
	t.Run("returns reject action", func(t *testing.T) {
		handler := &DefaultErrorHandler{}
		msg := &subscriberTestEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
			},
		}
		
		action := handler.HandleError(context.Background(), msg, errors.New("test error"))
		
		assert.Equal(t, Reject, action)
	})
}