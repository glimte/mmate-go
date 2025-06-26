package bridge

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/internal/reliability"
	"github.com/glimte/mmate-go/messaging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Test message types
type TestCommand struct {
	contracts.BaseCommand
	Data string `json:"data"`
}

type TestQuery struct {
	contracts.BaseQuery
	QueryData string `json:"queryData"`
}

type TestReply struct {
	contracts.BaseReply
	Result string `json:"result"`
}

// Mock Publisher
type mockPublisher struct {
	mock.Mock
}

func (m *mockPublisher) PublishCommand(ctx context.Context, cmd contracts.Command, opts ...messaging.PublishOption) error {
	args := m.Called(ctx, cmd, opts)
	return args.Error(0)
}

func (m *mockPublisher) PublishQuery(ctx context.Context, query contracts.Query, opts ...messaging.PublishOption) error {
	args := m.Called(ctx, query, opts)
	return args.Error(0)
}

// Mock Subscriber
type mockSubscriber struct {
	mock.Mock
	handlers map[string]messaging.MessageHandler
}

func (m *mockSubscriber) Subscribe(ctx context.Context, queueName string, messageType string, handler messaging.MessageHandler, opts ...messaging.SubscriptionOption) error {
	if m.handlers == nil {
		m.handlers = make(map[string]messaging.MessageHandler)
	}
	m.handlers[queueName] = handler
	args := m.Called(ctx, queueName, messageType, handler, opts)
	return args.Error(0)
}

func (m *mockSubscriber) Unsubscribe(queueName string) error {
	if m.handlers != nil {
		delete(m.handlers, queueName)
	}
	args := m.Called(queueName)
	return args.Error(0)
}

func (m *mockSubscriber) simulateReply(queueName string, reply contracts.Reply) error {
	if handler, exists := m.handlers[queueName]; exists {
		return handler.Handle(context.Background(), reply)
	}
	return errors.New("no handler for queue")
}

func TestNewSyncAsyncBridge(t *testing.T) {
	t.Run("NewSyncAsyncBridge creates bridge with defaults", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := &mockSubscriber{}
		
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil)
		
		bridge, err := NewSyncAsyncBridge(publisher, subscriber, nil)
		
		assert.NoError(t, err)
		assert.NotNil(t, bridge)
		assert.NotEmpty(t, bridge.replyQueue)
		assert.Equal(t, 0, bridge.GetPendingRequestCount())
		
		subscriber.AssertExpectations(t)
		
		// Cleanup
		subscriber.On("Unsubscribe", mock.AnythingOfType("string")).Return(nil)
		bridge.Close()
	})
	
	t.Run("NewSyncAsyncBridge applies options", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := &mockSubscriber{}
		
		cb := reliability.NewCircuitBreaker()
		retryPolicy := reliability.NewExponentialBackoff(100*time.Millisecond, 5*time.Second, 2.0, 3)
		
		subscriber.On("Subscribe", mock.Anything, "custom.reply", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil)
		
		bridge, err := NewSyncAsyncBridge(
			publisher, 
			subscriber,
			nil,
			WithReplyQueue("custom.reply"),
			WithCleanupInterval(10*time.Second),
			WithBridgeCircuitBreaker(cb),
			WithBridgeRetryPolicy(retryPolicy),
			WithMaxPendingRequests(500),
		)
		
		assert.NoError(t, err)
		assert.NotNil(t, bridge)
		assert.Equal(t, "custom.reply", bridge.replyQueue)
		assert.Equal(t, cb, bridge.circuitBreaker)
		assert.Equal(t, retryPolicy, bridge.retryPolicy)
		
		subscriber.AssertExpectations(t)
		
		// Cleanup
		subscriber.On("Unsubscribe", "custom.reply").Return(nil)
		bridge.Close()
	})
	
	t.Run("NewSyncAsyncBridge fails with nil publisher", func(t *testing.T) {
		subscriber := &mockSubscriber{}
		
		bridge, err := NewSyncAsyncBridge(nil, subscriber, nil)
		
		assert.Error(t, err)
		assert.Nil(t, bridge)
		assert.Contains(t, err.Error(), "publisher cannot be nil")
	})
	
	t.Run("NewSyncAsyncBridge fails with nil subscriber", func(t *testing.T) {
		publisher := &mockPublisher{}
		
		bridge, err := NewSyncAsyncBridge(publisher, nil, nil)
		
		assert.Error(t, err)
		assert.Nil(t, bridge)
		assert.Contains(t, err.Error(), "subscriber cannot be nil")
	})
	
	t.Run("NewSyncAsyncBridge fails when subscription fails", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := &mockSubscriber{}
		
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(errors.New("subscription failed"))
		
		bridge, err := NewSyncAsyncBridge(publisher, subscriber, nil)
		
		assert.Error(t, err)
		assert.Nil(t, bridge)
		assert.Contains(t, err.Error(), "failed to subscribe to reply queue")
		
		subscriber.AssertExpectations(t)
	})
}

func TestRequestCommand(t *testing.T) {
	t.Run("RequestCommand succeeds with reply", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := &mockSubscriber{}
		
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil)
		
		bridge, err := NewSyncAsyncBridge(publisher, subscriber, nil)
		assert.NoError(t, err)
		
		// Setup command
		cmd := &TestCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
			Data: "test data",
		}
		
		// Setup expected publish call and capture correlation ID
		var capturedCorrelationID string
		var mu sync.Mutex
		publisher.On("PublishCommand", mock.Anything, mock.MatchedBy(func(c contracts.Command) bool {
			// Capture the correlation ID set by RequestCommand
			mu.Lock()
			capturedCorrelationID = c.GetCorrelationID()
			mu.Unlock()
			return c == cmd
		}), mock.Anything).Return(nil)
		
		// Start request in goroutine
		resultCh := make(chan struct {
			reply contracts.Reply
			err   error
		}, 1)
		
		go func() {
			reply, err := bridge.RequestCommand(context.Background(), cmd, 5*time.Second)
			resultCh <- struct {
				reply contracts.Reply
				err   error
			}{reply, err}
		}()
		
		// Wait a bit to ensure publish has been called
		time.Sleep(10 * time.Millisecond)
		
		reply := &TestReply{
			BaseReply: contracts.BaseReply{
				BaseMessage: contracts.NewBaseMessage("TestReply"),
				Success:     true,
			},
			Result: "success",
		}
		mu.Lock()
		reply.SetCorrelationID(capturedCorrelationID)
		mu.Unlock()
		
		err = subscriber.simulateReply(bridge.replyQueue, reply)
		assert.NoError(t, err)
		
		// Get result
		result := <-resultCh
		assert.NoError(t, result.err)
		assert.NotNil(t, result.reply)
		assert.Equal(t, "success", result.reply.(*TestReply).Result)
		
		publisher.AssertExpectations(t)
		
		// Cleanup
		subscriber.On("Unsubscribe", mock.AnythingOfType("string")).Return(nil)
		bridge.Close()
	})
	
	t.Run("RequestCommand fails with nil command", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := &mockSubscriber{}
		
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil)
		
		bridge, err := NewSyncAsyncBridge(publisher, subscriber, nil)
		assert.NoError(t, err)
		
		reply, err := bridge.RequestCommand(context.Background(), nil, 5*time.Second)
		
		assert.Error(t, err)
		assert.Nil(t, reply)
		assert.Contains(t, err.Error(), "command cannot be nil")
		
		// Cleanup
		subscriber.On("Unsubscribe", mock.AnythingOfType("string")).Return(nil)
		bridge.Close()
	})
	
	t.Run("RequestCommand times out", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := &mockSubscriber{}
		
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil)
		
		bridge, err := NewSyncAsyncBridge(publisher, subscriber, nil)
		assert.NoError(t, err)
		
		cmd := &TestCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
			Data: "test data",
		}
		
		publisher.On("PublishCommand", mock.Anything, cmd, mock.Anything).Return(nil)
		
		reply, err := bridge.RequestCommand(context.Background(), cmd, 10*time.Millisecond)
		
		assert.Error(t, err)
		assert.Nil(t, reply)
		assert.Contains(t, err.Error(), "request timeout or cancelled")
		
		publisher.AssertExpectations(t)
		
		// Cleanup
		subscriber.On("Unsubscribe", mock.AnythingOfType("string")).Return(nil)
		bridge.Close()
	})
}

func TestRequestQuery(t *testing.T) {
	t.Run("RequestQuery succeeds with reply", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := &mockSubscriber{}
		
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil)
		
		bridge, err := NewSyncAsyncBridge(publisher, subscriber, nil)
		assert.NoError(t, err)
		
		query := &TestQuery{
			BaseQuery: contracts.BaseQuery{
				BaseMessage: contracts.NewBaseMessage("TestQuery"),
			},
			QueryData: "query data",
		}
		
		// Setup expected publish call and capture correlation ID
		var capturedCorrelationID string
		var mu sync.Mutex
		publisher.On("PublishQuery", mock.Anything, mock.MatchedBy(func(q contracts.Query) bool {
			// Capture the correlation ID set by RequestQuery
			mu.Lock()
			capturedCorrelationID = q.GetCorrelationID()
			mu.Unlock()
			return q == query
		}), mock.Anything).Return(nil)
		
		// Start request in goroutine
		resultCh := make(chan struct {
			reply contracts.Reply
			err   error
		}, 1)
		
		go func() {
			reply, err := bridge.RequestQuery(context.Background(), query, 5*time.Second)
			resultCh <- struct {
				reply contracts.Reply
				err   error
			}{reply, err}
		}()
		
		// Wait a bit to ensure publish has been called
		time.Sleep(10 * time.Millisecond)
		
		reply := &TestReply{
			BaseReply: contracts.BaseReply{
				BaseMessage: contracts.NewBaseMessage("TestReply"),
				Success:     true,
			},
			Result: "query result",
		}
		mu.Lock()
		reply.SetCorrelationID(capturedCorrelationID)
		mu.Unlock()
		
		err = subscriber.simulateReply(bridge.replyQueue, reply)
		assert.NoError(t, err)
		
		// Get result
		result := <-resultCh
		assert.NoError(t, result.err)
		assert.NotNil(t, result.reply)
		assert.Equal(t, "query result", result.reply.(*TestReply).Result)
		
		publisher.AssertExpectations(t)
		
		// Cleanup
		subscriber.On("Unsubscribe", mock.AnythingOfType("string")).Return(nil)
		bridge.Close()
	})
}

func TestBridgeCleanup(t *testing.T) {
	t.Run("cleanupExpiredRequests removes expired requests", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := &mockSubscriber{}
		
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil)
		
		bridge, err := NewSyncAsyncBridge(publisher, subscriber, nil, WithCleanupInterval(5*time.Millisecond))
		assert.NoError(t, err)
		
		// Add a pending request manually with expired timeout
		correlationID := uuid.New().String()
		ctx, cancel := context.WithCancel(context.Background())
		pending := &PendingRequest{
			ID:         correlationID,
			ResponseCh: make(chan contracts.Reply, 1),
			Timeout:    time.Now().Add(-1 * time.Hour), // Already expired
			Context:    ctx,
			Cancel:     cancel,
		}
		
		bridge.mu.Lock()
		bridge.pendingRequests[correlationID] = pending
		bridge.mu.Unlock()
		
		assert.Equal(t, 1, bridge.GetPendingRequestCount())
		
		// Wait for cleanup
		time.Sleep(15 * time.Millisecond)
		
		assert.Equal(t, 0, bridge.GetPendingRequestCount())
		
		// Cleanup
		subscriber.On("Unsubscribe", mock.AnythingOfType("string")).Return(nil)
		bridge.Close()
	})
}

func TestBridgeOptions(t *testing.T) {
	t.Run("BridgeOptions apply correctly", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := &mockSubscriber{}
		
		subscriber.On("Subscribe", mock.Anything, "test.reply", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil)
		
		cb := reliability.NewCircuitBreaker()
		retryPolicy := reliability.NewExponentialBackoff(100*time.Millisecond, 5*time.Second, 2.0, 3)
		
		bridge, err := NewSyncAsyncBridge(
			publisher,
			subscriber,
			nil,
			WithReplyQueue("test.reply"),
			WithCleanupInterval(5*time.Second),
			WithBridgeCircuitBreaker(cb),
			WithBridgeRetryPolicy(retryPolicy),
			WithMaxPendingRequests(100),
		)
		
		assert.NoError(t, err)
		assert.Equal(t, "test.reply", bridge.replyQueue)
		assert.Equal(t, cb, bridge.circuitBreaker)
		assert.Equal(t, retryPolicy, bridge.retryPolicy)
		
		subscriber.AssertExpectations(t)
		
		// Cleanup
		subscriber.On("Unsubscribe", "test.reply").Return(nil)
		bridge.Close()
	})
}

func TestHandleReply(t *testing.T) {
	t.Run("handleReply processes valid reply", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := &mockSubscriber{}
		
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil)
		
		bridge, err := NewSyncAsyncBridge(publisher, subscriber, nil)
		assert.NoError(t, err)
		
		// Add pending request
		correlationID := uuid.New().String()
		ctx, cancel := context.WithCancel(context.Background())
		pending := &PendingRequest{
			ID:         correlationID,
			ResponseCh: make(chan contracts.Reply, 1),
			Timeout:    time.Now().Add(1 * time.Hour),
			Context:    ctx,
			Cancel:     cancel,
		}
		
		bridge.mu.Lock()
		bridge.pendingRequests[correlationID] = pending
		bridge.mu.Unlock()
		
		// Create reply
		reply := &TestReply{
			BaseReply: contracts.BaseReply{
				BaseMessage: contracts.NewBaseMessage("TestReply"),
				Success:     true,
			},
			Result: "test result",
		}
		reply.SetCorrelationID(correlationID)
		
		// Handle reply
		err = bridge.handleReply(context.Background(), reply)
		assert.NoError(t, err)
		
		// Verify reply was delivered
		select {
		case receivedReply := <-pending.ResponseCh:
			assert.Equal(t, "test result", receivedReply.(*TestReply).Result)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("reply not delivered")
		}
		
		cancel()
		
		// Cleanup
		subscriber.On("Unsubscribe", mock.AnythingOfType("string")).Return(nil)
		bridge.Close()
	})
	
	t.Run("handleReply fails with non-reply message", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := &mockSubscriber{}
		
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil)
		
		bridge, err := NewSyncAsyncBridge(publisher, subscriber, nil)
		assert.NoError(t, err)
		
		// Create non-reply message
		cmd := &TestCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
			Data: "test",
		}
		
		err = bridge.handleReply(context.Background(), cmd)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "received non-reply message")
		
		// Cleanup
		subscriber.On("Unsubscribe", mock.AnythingOfType("string")).Return(nil)
		bridge.Close()
	})
	
	t.Run("handleReply fails with missing correlation ID", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := &mockSubscriber{}
		
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil)
		
		bridge, err := NewSyncAsyncBridge(publisher, subscriber, nil)
		assert.NoError(t, err)
		
		// Create reply without correlation ID
		reply := &TestReply{
			BaseReply: contracts.BaseReply{
				BaseMessage: contracts.NewBaseMessage("TestReply"),
				Success:     true,
			},
			Result: "test result",
		}
		
		err = bridge.handleReply(context.Background(), reply)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "reply missing correlation ID")
		
		// Cleanup
		subscriber.On("Unsubscribe", mock.AnythingOfType("string")).Return(nil)
		bridge.Close()
	})
}