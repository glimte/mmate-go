package messaging

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/internal/reliability"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Mock message for testing
type TestGroupMessage struct {
	contracts.BaseMessage
	Data string `json:"data"`
}

// Mock subscriber for consumer group testing
type mockGroupSubscriber struct {
	mock.Mock
	subscriptions sync.Map
}

func (m *mockGroupSubscriber) Subscribe(ctx context.Context, queue string, messageType string, handler MessageHandler, opts ...SubscriptionOption) error {
	args := m.Called(ctx, queue, messageType, handler, opts)
	if args.Error(0) == nil {
		m.subscriptions.Store(queue, handler)
	}
	return args.Error(0)
}

func (m *mockGroupSubscriber) Unsubscribe(queue string) error {
	args := m.Called(queue)
	m.subscriptions.Delete(queue)
	return args.Error(0)
}

func (m *mockGroupSubscriber) Close() error {
	args := m.Called()
	return args.Error(0)
}

// Test handler that tracks calls
type testGroupHandler struct {
	callCount int32
	mu        sync.Mutex
	errors    []error
	messages  []contracts.Message
}

func (h *testGroupHandler) Handle(ctx context.Context, msg contracts.Message) error {
	atomic.AddInt32(&h.callCount, 1)
	
	h.mu.Lock()
	h.messages = append(h.messages, msg)
	h.mu.Unlock()
	
	if len(h.errors) > 0 {
		h.mu.Lock()
		err := h.errors[0]
		h.errors = h.errors[1:]
		h.mu.Unlock()
		return err
	}
	
	return nil
}

func (h *testGroupHandler) GetCallCount() int {
	return int(atomic.LoadInt32(&h.callCount))
}

// Test error handler
type testErrorHandler struct {
	action ErrorAction
}

func (h *testErrorHandler) HandleError(ctx context.Context, msg contracts.Message, err error) ErrorAction {
	return h.action
}

// Test retry policy for consumer group
type testGroupRetryPolicy struct {
	maxRetries int
	delay      time.Duration
}

func (p *testGroupRetryPolicy) ShouldRetry(attempt int, err error) (bool, time.Duration) {
	if attempt > p.maxRetries {
		return false, 0
	}
	return true, p.delay
}

func (p *testGroupRetryPolicy) MaxRetries() int {
	return p.maxRetries
}

func (p *testGroupRetryPolicy) NextDelay(attempt int) time.Duration {
	return p.delay
}

func TestNewConsumerGroup(t *testing.T) {
	t.Run("creates consumer group with defaults", func(t *testing.T) {
		subscriber := &mockGroupSubscriber{}
		handler := &testGroupHandler{}
		
		group, err := NewConsumerGroup(subscriber, "test.queue", "TestMessage", handler)
		
		assert.NoError(t, err)
		assert.NotNil(t, group)
		assert.Equal(t, "group-test.queue-TestMessage", group.config.GroupID)
		assert.Equal(t, 1, group.config.MinConsumers)
		assert.Equal(t, 10, group.config.MaxConsumers)
		assert.Equal(t, 10, group.config.PrefetchCount)
		assert.Equal(t, 30*time.Second, group.config.MetricsInterval)
		assert.Equal(t, 0.8, group.config.ScaleUpThreshold)
		assert.Equal(t, 0.2, group.config.ScaleDownThreshold)
	})

	t.Run("creates consumer group with options", func(t *testing.T) {
		subscriber := &mockGroupSubscriber{}
		handler := &testGroupHandler{}
		
		group, err := NewConsumerGroup(subscriber, "test.queue", "TestMessage", handler,
			WithGroupID("custom-group"),
			WithMinConsumers(2),
			WithMaxConsumers(20),
			WithGroupPrefetchCount(50),
			WithMetricsInterval(10*time.Second),
			WithScaleThresholds(0.9, 0.1),
		)
		
		assert.NoError(t, err)
		assert.NotNil(t, group)
		assert.Equal(t, "custom-group", group.config.GroupID)
		assert.Equal(t, 2, group.config.MinConsumers)
		assert.Equal(t, 20, group.config.MaxConsumers)
		assert.Equal(t, 50, group.config.PrefetchCount)
		assert.Equal(t, 10*time.Second, group.config.MetricsInterval)
		assert.Equal(t, 0.9, group.config.ScaleUpThreshold)
		assert.Equal(t, 0.1, group.config.ScaleDownThreshold)
	})

	t.Run("validates parameters", func(t *testing.T) {
		tests := []struct {
			name        string
			subscriber  Subscriber
			queue       string
			messageType string
			handler     MessageHandler
			expectError string
		}{
			{
				name:        "nil subscriber",
				subscriber:  nil,
				queue:       "test.queue",
				messageType: "TestMessage",
				handler:     &testGroupHandler{},
				expectError: "subscriber cannot be nil",
			},
			{
				name:        "empty queue",
				subscriber:  &mockGroupSubscriber{},
				queue:       "",
				messageType: "TestMessage",
				handler:     &testGroupHandler{},
				expectError: "queue cannot be empty",
			},
			{
				name:        "empty message type",
				subscriber:  &mockGroupSubscriber{},
				queue:       "test.queue",
				messageType: "",
				handler:     &testGroupHandler{},
				expectError: "message type cannot be empty",
			},
			{
				name:        "nil handler",
				subscriber:  &mockGroupSubscriber{},
				queue:       "test.queue",
				messageType: "TestMessage",
				handler:     nil,
				expectError: "handler cannot be nil",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				group, err := NewConsumerGroup(tt.subscriber, tt.queue, tt.messageType, tt.handler)
				
				assert.Error(t, err)
				assert.Nil(t, group)
				assert.Contains(t, err.Error(), tt.expectError)
			})
		}
	})

	t.Run("corrects invalid configuration", func(t *testing.T) {
		subscriber := &mockGroupSubscriber{}
		handler := &testGroupHandler{}
		
		group, err := NewConsumerGroup(subscriber, "test.queue", "TestMessage", handler,
			WithMinConsumers(0),    // Should be corrected to 1
			WithMaxConsumers(0),    // Should be corrected to MinConsumers
		)
		
		assert.NoError(t, err)
		assert.NotNil(t, group)
		assert.Equal(t, 1, group.config.MinConsumers)
		assert.Equal(t, 1, group.config.MaxConsumers)
	})
}

func TestConsumerGroupStart(t *testing.T) {
	t.Run("starts with minimum consumers", func(t *testing.T) {
		subscriber := &mockGroupSubscriber{}
		handler := &testGroupHandler{}
		
		group, _ := NewConsumerGroup(subscriber, "test.queue", "TestMessage", handler,
			WithMinConsumers(3),
		)
		
		// Expect 3 subscriptions
		subscriber.On("Subscribe", 
			mock.Anything, 
			"test.queue",
			"TestMessage",
			mock.Anything,
			mock.Anything,
		).Return(nil).Times(3)
		
		err := group.Start(context.Background())
		
		assert.NoError(t, err)
		assert.True(t, group.running)
		assert.Equal(t, 3, len(group.consumers))
		subscriber.AssertExpectations(t)
		
		// Cleanup
		subscriber.On("Unsubscribe", "test.queue").Return(nil).Times(3)
		group.Stop()
	})

	t.Run("fails if already running", func(t *testing.T) {
		subscriber := &mockGroupSubscriber{}
		handler := &testGroupHandler{}
		
		group, _ := NewConsumerGroup(subscriber, "test.queue", "TestMessage", handler)
		
		subscriber.On("Subscribe", 
			mock.Anything, 
			"test.queue",
			"TestMessage",
			mock.Anything,
			mock.Anything,
		).Return(nil).Once()
		
		err := group.Start(context.Background())
		assert.NoError(t, err)
		
		// Try to start again
		err = group.Start(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "consumer group already running")
		
		// Cleanup
		subscriber.On("Unsubscribe", "test.queue").Return(nil).Once()
		group.Stop()
	})

	t.Run("cleans up on subscription failure", func(t *testing.T) {
		subscriber := &mockGroupSubscriber{}
		handler := &testGroupHandler{}
		
		group, _ := NewConsumerGroup(subscriber, "test.queue", "TestMessage", handler,
			WithMinConsumers(3),
		)
		
		// First two succeed, third fails
		subscriber.On("Subscribe", 
			mock.Anything, 
			"test.queue",
			"TestMessage",
			mock.Anything,
			mock.Anything,
		).Return(nil).Twice()
		
		subscriber.On("Subscribe", 
			mock.Anything, 
			"test.queue",
			"TestMessage",
			mock.Anything,
			mock.Anything,
		).Return(errors.New("subscription failed")).Once()
		
		err := group.Start(context.Background())
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to start consumer")
		assert.False(t, group.running)
		assert.Equal(t, 0, len(group.consumers))
	})
}

func TestConsumerGroupStop(t *testing.T) {
	t.Run("stops all consumers", func(t *testing.T) {
		subscriber := &mockGroupSubscriber{}
		handler := &testGroupHandler{}
		
		group, _ := NewConsumerGroup(subscriber, "test.queue", "TestMessage", handler,
			WithMinConsumers(2),
		)
		
		// Start
		subscriber.On("Subscribe", 
			mock.Anything, 
			"test.queue",
			"TestMessage",
			mock.Anything,
			mock.Anything,
		).Return(nil).Times(2)
		
		group.Start(context.Background())
		
		// Stop
		subscriber.On("Unsubscribe", "test.queue").Return(nil).Times(2)
		
		err := group.Stop()
		
		assert.NoError(t, err)
		assert.False(t, group.running)
		assert.Equal(t, 0, len(group.consumers))
		subscriber.AssertExpectations(t)
	})

	t.Run("fails if not running", func(t *testing.T) {
		subscriber := &mockGroupSubscriber{}
		handler := &testGroupHandler{}
		
		group, _ := NewConsumerGroup(subscriber, "test.queue", "TestMessage", handler)
		
		err := group.Stop()
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "consumer group not running")
	})

	t.Run("reports unsubscribe errors", func(t *testing.T) {
		subscriber := &mockGroupSubscriber{}
		handler := &testGroupHandler{}
		
		group, _ := NewConsumerGroup(subscriber, "test.queue", "TestMessage", handler)
		
		// Start
		subscriber.On("Subscribe", 
			mock.Anything, 
			"test.queue",
			"TestMessage",
			mock.Anything,
			mock.Anything,
		).Return(nil).Once()
		
		group.Start(context.Background())
		
		// Unsubscribe fails
		subscriber.On("Unsubscribe", "test.queue").Return(errors.New("unsubscribe failed")).Once()
		
		err := group.Stop()
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "errors during shutdown")
	})
}

func TestConsumerGroupAddRemove(t *testing.T) {
	t.Run("adds consumer successfully", func(t *testing.T) {
		subscriber := &mockGroupSubscriber{}
		handler := &testGroupHandler{}
		
		group, _ := NewConsumerGroup(subscriber, "test.queue", "TestMessage", handler,
			WithMinConsumers(1),
			WithMaxConsumers(5),
		)
		
		// Start with 1 consumer
		subscriber.On("Subscribe", 
			mock.Anything, 
			"test.queue",
			"TestMessage",
			mock.Anything,
			mock.Anything,
		).Return(nil).Once()
		
		group.Start(context.Background())
		assert.Equal(t, 1, group.GetConsumerCount())
		
		// Add another consumer
		subscriber.On("Subscribe", 
			mock.Anything, 
			"test.queue",
			"TestMessage",
			mock.Anything,
			mock.Anything,
		).Return(nil).Once()
		
		err := group.AddConsumer()
		
		assert.NoError(t, err)
		assert.Equal(t, 2, group.GetConsumerCount())
		
		// Cleanup
		subscriber.On("Unsubscribe", "test.queue").Return(nil).Times(2)
		group.Stop()
	})

	t.Run("respects max consumers", func(t *testing.T) {
		subscriber := &mockGroupSubscriber{}
		handler := &testGroupHandler{}
		
		group, _ := NewConsumerGroup(subscriber, "test.queue", "TestMessage", handler,
			WithMinConsumers(2),
			WithMaxConsumers(2),
		)
		
		// Start with 2 consumers (max)
		subscriber.On("Subscribe", 
			mock.Anything, 
			"test.queue",
			"TestMessage",
			mock.Anything,
			mock.Anything,
		).Return(nil).Times(2)
		
		group.Start(context.Background())
		
		// Try to add another
		err := group.AddConsumer()
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "maximum consumers reached")
		assert.Equal(t, 2, group.GetConsumerCount())
		
		// Cleanup
		subscriber.On("Unsubscribe", "test.queue").Return(nil).Times(2)
		group.Stop()
	})

	t.Run("removes consumer successfully", func(t *testing.T) {
		subscriber := &mockGroupSubscriber{}
		handler := &testGroupHandler{}
		
		group, _ := NewConsumerGroup(subscriber, "test.queue", "TestMessage", handler,
			WithMinConsumers(1),
			WithMaxConsumers(3),
		)
		
		// Start with 2 consumers
		subscriber.On("Subscribe", 
			mock.Anything, 
			"test.queue",
			"TestMessage",
			mock.Anything,
			mock.Anything,
		).Return(nil).Times(2)
		
		group.Start(context.Background())
		group.addConsumerLocked() // Add second consumer
		assert.Equal(t, 2, group.GetConsumerCount())
		
		// Remove one
		subscriber.On("Unsubscribe", "test.queue").Return(nil).Once()
		
		err := group.RemoveConsumer()
		
		assert.NoError(t, err)
		assert.Equal(t, 1, group.GetConsumerCount())
		
		// Cleanup
		subscriber.On("Unsubscribe", "test.queue").Return(nil).Once()
		group.Stop()
	})

	t.Run("respects min consumers", func(t *testing.T) {
		subscriber := &mockGroupSubscriber{}
		handler := &testGroupHandler{}
		
		group, _ := NewConsumerGroup(subscriber, "test.queue", "TestMessage", handler,
			WithMinConsumers(2),
		)
		
		// Start with minimum (2)
		subscriber.On("Subscribe", 
			mock.Anything, 
			"test.queue",
			"TestMessage",
			mock.Anything,
			mock.Anything,
		).Return(nil).Times(2)
		
		group.Start(context.Background())
		
		// Try to remove one
		err := group.RemoveConsumer()
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "minimum consumers reached")
		assert.Equal(t, 2, group.GetConsumerCount())
		
		// Cleanup
		subscriber.On("Unsubscribe", "test.queue").Return(nil).Times(2)
		group.Stop()
	})
}

func TestConsumerGroupMessageHandling(t *testing.T) {
	t.Run("processes messages successfully", func(t *testing.T) {
		subscriber := &mockGroupSubscriber{}
		handler := &testGroupHandler{}
		
		group, _ := NewConsumerGroup(subscriber, "test.queue", "TestMessage", handler)
		
		// Start
		subscriber.On("Subscribe", 
			mock.Anything, 
			"test.queue",
			"TestMessage",
			mock.Anything,
			mock.Anything,
		).Return(nil).Once()
		
		group.Start(context.Background())
		
		// Get the registered handler
		val, _ := subscriber.subscriptions.Load("test.queue")
		registeredHandler := val.(MessageHandler)
		
		// Simulate messages
		for i := 0; i < 5; i++ {
			msg := &TestGroupMessage{
				BaseMessage: contracts.NewBaseMessage("TestMessage"),
				Data:        "test data",
			}
			err := registeredHandler.Handle(context.Background(), msg)
			assert.NoError(t, err)
		}
		
		// Wait for processing
		time.Sleep(100 * time.Millisecond)
		
		// Check metrics
		metrics := group.GetMetrics()
		assert.Equal(t, 5, int(metrics.MessagesProcessed))
		assert.Equal(t, 0, int(metrics.MessagesFailed))
		assert.Equal(t, 5, handler.GetCallCount())
		
		// Cleanup
		subscriber.On("Unsubscribe", "test.queue").Return(nil).Once()
		group.Stop()
	})

	t.Run("handles errors with retry", func(t *testing.T) {
		subscriber := &mockGroupSubscriber{}
		handler := &testGroupHandler{
			errors: []error{errors.New("first error"), nil}, // Fail once, then succeed
		}
		
		retryPolicy := reliability.NewExponentialBackoff(10*time.Millisecond, 100*time.Millisecond, 2.0, 3)
		
		group, _ := NewConsumerGroup(subscriber, "test.queue", "TestMessage", handler,
			WithGroupRetryPolicy(retryPolicy),
		)
		
		// Start
		subscriber.On("Subscribe", 
			mock.Anything, 
			"test.queue",
			"TestMessage",
			mock.Anything,
			mock.Anything,
		).Return(nil).Once()
		
		group.Start(context.Background())
		
		// Get the registered handler
		val, _ := subscriber.subscriptions.Load("test.queue")
		registeredHandler := val.(MessageHandler)
		
		// Process message
		msg := &TestGroupMessage{
			BaseMessage: contracts.NewBaseMessage("TestMessage"),
			Data:        "test data",
		}
		
		err := registeredHandler.Handle(context.Background(), msg)
		assert.NoError(t, err)
		
		// Wait for processing
		time.Sleep(200 * time.Millisecond)
		
		// Check that handler was called twice (initial + retry)
		assert.Equal(t, 2, handler.GetCallCount())
		
		// Check metrics
		metrics := group.GetMetrics()
		assert.Equal(t, 1, int(metrics.MessagesProcessed))
		assert.Equal(t, 0, int(metrics.MessagesFailed))
		
		// Cleanup
		subscriber.On("Unsubscribe", "test.queue").Return(nil).Once()
		group.Stop()
	})

	t.Run("handles permanent errors", func(t *testing.T) {
		subscriber := &mockGroupSubscriber{}
		// Provide enough errors to persist through all retries
		handler := &testGroupHandler{
			errors: []error{
				errors.New("permanent error"),
				errors.New("permanent error"),
				errors.New("permanent error"),
				errors.New("permanent error"),
			},
		}
		
		errorHandler := &DefaultErrorHandler{}
		
		group, _ := NewConsumerGroup(subscriber, "test.queue", "TestMessage", handler,
			WithGroupErrorHandler(errorHandler),
		)
		
		// Start
		subscriber.On("Subscribe", 
			mock.Anything, 
			"test.queue",
			"TestMessage",
			mock.Anything,
			mock.Anything,
		).Return(nil).Once()
		
		group.Start(context.Background())
		
		// Get the registered handler
		val, _ := subscriber.subscriptions.Load("test.queue")
		registeredHandler := val.(MessageHandler)
		
		// Process message
		msg := &TestGroupMessage{
			BaseMessage: contracts.NewBaseMessage("TestMessage"),
			Data:        "test data",
		}
		
		err := registeredHandler.Handle(context.Background(), msg)
		assert.Error(t, err) // Error is returned when handler rejects
		
		// Wait for processing (no need to wait since error is immediate)
		time.Sleep(50 * time.Millisecond)
		
		// Check metrics
		metrics := group.GetMetrics()
		assert.Equal(t, 0, int(metrics.MessagesProcessed))
		assert.Equal(t, 1, int(metrics.MessagesFailed))
		
		// Cleanup
		subscriber.On("Unsubscribe", "test.queue").Return(nil).Once()
		group.Stop()
	})
}

func TestConsumerGroupMetrics(t *testing.T) {
	t.Run("tracks metrics correctly", func(t *testing.T) {
		subscriber := &mockGroupSubscriber{}
		
		// Pre-populate errors for the handler - simple pattern
		errs := []error{
			errors.New("error1"), // Message 0 - fail
			nil,                  // Message 1 - success
			nil,                  // Message 2 - success
			errors.New("error2"), // Message 3 - fail
			nil,                  // Message 4 - success
			nil,                  // Message 5 - success
			errors.New("error3"), // Message 6 - fail
			nil,                  // Message 7 - success
			nil,                  // Message 8 - success
			errors.New("error4"), // Message 9 - fail
		}
		
		handler := &testGroupHandler{
			errors: errs,
		}
		
		// Custom error handler that rejects on error
		errorHandler := &testErrorHandler{action: Reject}
		
		// Use a retry policy with 0 retries to simplify the test
		retryPolicy := &testGroupRetryPolicy{maxRetries: 0, delay: 0}
		
		group, _ := NewConsumerGroup(subscriber, "test.queue", "TestMessage", handler,
			WithMetricsInterval(100*time.Millisecond),
			WithGroupErrorHandler(errorHandler),
			WithGroupRetryPolicy(retryPolicy),
		)
		
		// Start
		subscriber.On("Subscribe", 
			mock.Anything, 
			"test.queue",
			"TestMessage",
			mock.Anything,
			mock.Anything,
		).Return(nil).Once()
		
		group.Start(context.Background())
		
		// Get the registered handler
		val, _ := subscriber.subscriptions.Load("test.queue")
		registeredHandler := val.(MessageHandler)
		
		// Process some messages
		for i := 0; i < 10; i++ {
			msg := &TestGroupMessage{
				BaseMessage: contracts.NewBaseMessage("TestMessage"),
				Data:        "test data",
			}
			
			registeredHandler.Handle(context.Background(), msg)
		}
		
		// Wait for processing and metrics collection
		time.Sleep(300 * time.Millisecond)
		
		// Check metrics
		metrics := group.GetMetrics()
		assert.Equal(t, 1, metrics.ConsumerCount)
		assert.Equal(t, 6, int(metrics.MessagesProcessed)) // 10 - 4 failures
		assert.Equal(t, 4, int(metrics.MessagesFailed))    // 4 failures
		assert.Greater(t, metrics.AverageProcessTime, time.Duration(0))
		assert.NotZero(t, metrics.LastMessageTime)
		
		// Cleanup
		subscriber.On("Unsubscribe", "test.queue").Return(nil).Once()
		group.Stop()
	})
}

func TestConsumerGroupAutoScaling(t *testing.T) {
	t.Run("scales down when idle", func(t *testing.T) {
		subscriber := &mockGroupSubscriber{}
		handler := &testGroupHandler{}
		
		group, _ := NewConsumerGroup(subscriber, "test.queue", "TestMessage", handler,
			WithMinConsumers(1),
			WithMaxConsumers(3),
		)
		
		// Start with 2 consumers
		subscriber.On("Subscribe", 
			mock.Anything, 
			"test.queue",
			"TestMessage",
			mock.Anything,
			mock.Anything,
		).Return(nil).Times(2)
		
		group.Start(context.Background())
		group.addConsumerLocked()
		assert.Equal(t, 2, group.GetConsumerCount())
		
		// Set last message time to past to simulate idle
		group.lastMessageTime.Store(time.Now().Add(-45 * time.Second))
		
		// Trigger scaling check
		group.scalingTimer.Reset(1 * time.Millisecond)
		
		// Expect one consumer to be removed
		subscriber.On("Unsubscribe", "test.queue").Return(nil).Once()
		
		// Wait for scaling
		time.Sleep(100 * time.Millisecond)
		
		// Should have scaled down to 1
		assert.Equal(t, 1, group.GetConsumerCount())
		
		// Cleanup
		subscriber.On("Unsubscribe", "test.queue").Return(nil).Once()
		group.Stop()
	})
}

func TestConsumerGroupConcurrency(t *testing.T) {
	t.Run("handles concurrent operations safely", func(t *testing.T) {
		subscriber := &mockGroupSubscriber{}
		handler := &testGroupHandler{}
		
		group, _ := NewConsumerGroup(subscriber, "test.queue", "TestMessage", handler,
			WithMinConsumers(1),
			WithMaxConsumers(10),
		)
		
		// Set up subscriber expectations
		subscriber.On("Subscribe", 
			mock.Anything, 
			"test.queue",
			"TestMessage",
			mock.Anything,
			mock.Anything,
		).Return(nil).Maybe()
		
		subscriber.On("Unsubscribe", "test.queue").Return(nil).Maybe()
		
		// Start
		err := group.Start(context.Background())
		require.NoError(t, err)
		
		// Run concurrent operations
		var wg sync.WaitGroup
		
		// Add consumers
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				group.AddConsumer()
			}()
		}
		
		// Remove consumers
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(10 * time.Millisecond)
				group.RemoveConsumer()
			}()
		}
		
		// Get metrics
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				metrics := group.GetMetrics()
				assert.GreaterOrEqual(t, metrics.ConsumerCount, 1)
				assert.LessOrEqual(t, metrics.ConsumerCount, 10)
			}()
		}
		
		wg.Wait()
		
		// Final state should be consistent
		count := group.GetConsumerCount()
		assert.GreaterOrEqual(t, count, 1)
		assert.LessOrEqual(t, count, 10)
		
		// Stop
		err = group.Stop()
		assert.NoError(t, err)
	})
}