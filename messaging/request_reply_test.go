package messaging

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Mock implementations for testing
type mockPublisher struct {
	mock.Mock
}

func (m *mockPublisher) Publish(ctx context.Context, msg contracts.Message, options ...PublishOption) error {
	args := m.Called(ctx, msg, options)
	return args.Error(0)
}

func (m *mockPublisher) PublishCommand(ctx context.Context, command contracts.Command, options ...PublishOption) error {
	args := m.Called(ctx, command, options)
	return args.Error(0)
}

func (m *mockPublisher) PublishEvent(ctx context.Context, event contracts.Event, options ...PublishOption) error {
	args := m.Called(ctx, event, options)
	return args.Error(0)
}

func (m *mockPublisher) Close() error {
	args := m.Called()
	return args.Error(0)
}

type mockSubscriber struct {
	mock.Mock
	handlers map[string]MessageHandler
	mu       sync.RWMutex
}

func newMockSubscriber() *mockSubscriber {
	return &mockSubscriber{
		handlers: make(map[string]MessageHandler),
	}
}

func (m *mockSubscriber) Subscribe(ctx context.Context, queue string, messageType string, handler MessageHandler, options ...SubscriptionOption) error {
	args := m.Called(ctx, queue, messageType, handler, options)
	
	// Store handler for simulation
	m.mu.Lock()
	m.handlers[queue] = handler
	m.mu.Unlock()
	
	return args.Error(0)
}

func (m *mockSubscriber) Unsubscribe(queueName string) error {
	args := m.Called(queueName)
	
	m.mu.Lock()
	delete(m.handlers, queueName)
	m.mu.Unlock()
	
	return args.Error(0)
}

func (m *mockSubscriber) Close() error {
	args := m.Called()
	return args.Error(0)
}

// Helper to simulate reply message
func (m *mockSubscriber) simulateReply(ctx context.Context, queue string, reply contracts.Reply) error {
	m.mu.RLock()
	handler, exists := m.handlers[queue]
	m.mu.RUnlock()
	
	if exists && handler != nil {
		return handler.Handle(ctx, reply)
	}
	return fmt.Errorf("no handler for queue %s", queue)
}

// Test messages
type testRequestReplyQuery struct {
	contracts.BaseQuery
	Data string `json:"data"`
}

type testRequestReplyCommand struct {
	contracts.BaseCommand
	Data string `json:"data"`
}

type testRequestReplyReply struct {
	contracts.BaseReply
	Result string `json:"result"`
}

// Test InMemoryRequestTracker
func TestInMemoryRequestTracker(t *testing.T) {
	t.Run("TrackRequest", func(t *testing.T) {
		tracker := NewInMemoryRequestTracker()
		
		request := &TrackedRequest{
			ID:            "req-123",
			CorrelationID: "corr-456",
			Status:        RequestStatusPending,
			Request:       &testRequestReplyQuery{BaseQuery: contracts.BaseQuery{BaseMessage: contracts.NewBaseMessage("TestQuery")}},
			SentAt:        time.Now(),
			Timeout:       5 * time.Second,
		}
		
		err := tracker.TrackRequest(request)
		assert.NoError(t, err)
		
		// Verify request was tracked
		tracked, err := tracker.GetRequest("corr-456")
		assert.NoError(t, err)
		assert.Equal(t, request.ID, tracked.ID)
		assert.Equal(t, request.CorrelationID, tracked.CorrelationID)
	})
	
	t.Run("TrackRequest with nil request", func(t *testing.T) {
		tracker := NewInMemoryRequestTracker()
		
		err := tracker.TrackRequest(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "request cannot be nil")
	})
	
	t.Run("TrackRequest without correlation ID", func(t *testing.T) {
		tracker := NewInMemoryRequestTracker()
		
		request := &TrackedRequest{
			ID:     "req-123",
			Status: RequestStatusPending,
		}
		
		err := tracker.TrackRequest(request)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "correlation ID is required")
	})
	
	t.Run("UpdateStatus", func(t *testing.T) {
		tracker := NewInMemoryRequestTracker()
		
		request := &TrackedRequest{
			CorrelationID: "corr-123",
			Status:        RequestStatusPending,
		}
		
		tracker.TrackRequest(request)
		
		// Update status
		err := tracker.UpdateStatus("corr-123", RequestStatusSent)
		assert.NoError(t, err)
		
		// Verify status updated
		tracked, _ := tracker.GetRequest("corr-123")
		assert.Equal(t, RequestStatusSent, tracked.Status)
	})
	
	t.Run("UpdateStatus for non-existent request", func(t *testing.T) {
		tracker := NewInMemoryRequestTracker()
		
		err := tracker.UpdateStatus("non-existent", RequestStatusSent)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "request not found")
	})
	
	t.Run("GetActiveRequests", func(t *testing.T) {
		tracker := NewInMemoryRequestTracker()
		
		// Add various requests
		requests := []*TrackedRequest{
			{CorrelationID: "1", Status: RequestStatusPending},
			{CorrelationID: "2", Status: RequestStatusSent},
			{CorrelationID: "3", Status: RequestStatusCompleted},
			{CorrelationID: "4", Status: RequestStatusFailed},
			{CorrelationID: "5", Status: RequestStatusTimeout},
			{CorrelationID: "6", Status: RequestStatusPending},
		}
		
		for _, req := range requests {
			tracker.TrackRequest(req)
		}
		
		active := tracker.GetActiveRequests()
		assert.Len(t, active, 3) // Only pending and sent
		
		// Verify only active requests returned
		for _, req := range active {
			assert.True(t, req.Status == RequestStatusPending || req.Status == RequestStatusSent)
		}
	})
	
	t.Run("CompleteRequest", func(t *testing.T) {
		tracker := NewInMemoryRequestTracker()
		
		request := &TrackedRequest{
			CorrelationID: "corr-123",
			Status:        RequestStatusSent,
		}
		
		tracker.TrackRequest(request)
		
		reply := &testRequestReplyReply{
			BaseReply: contracts.BaseReply{
				BaseMessage: contracts.NewBaseMessage("TestReply"),
			},
			Result: "success",
		}
		
		err := tracker.CompleteRequest("corr-123", reply)
		assert.NoError(t, err)
		
		// Verify request completed
		tracked, _ := tracker.GetRequest("corr-123")
		assert.Equal(t, RequestStatusCompleted, tracked.Status)
		assert.Equal(t, reply, tracked.Response)
		assert.NotNil(t, tracked.ReceivedAt)
	})
	
	t.Run("FailRequest", func(t *testing.T) {
		tracker := NewInMemoryRequestTracker()
		
		request := &TrackedRequest{
			CorrelationID: "corr-123",
			Status:        RequestStatusSent,
		}
		
		tracker.TrackRequest(request)
		
		err := errors.New("request failed")
		tracker.FailRequest("corr-123", err)
		
		// Verify request failed
		tracked, _ := tracker.GetRequest("corr-123")
		assert.Equal(t, RequestStatusFailed, tracked.Status)
		assert.Equal(t, err, tracked.Error)
	})
	
	t.Run("CleanupExpired", func(t *testing.T) {
		tracker := NewInMemoryRequestTracker()
		
		// Add requests with different states and ages
		now := time.Now()
		
		requests := []*TrackedRequest{
			{
				CorrelationID: "1",
				Status:        RequestStatusPending,
				SentAt:        now.Add(-10 * time.Second),
				Timeout:       5 * time.Second, // Should timeout
			},
			{
				CorrelationID: "2",
				Status:        RequestStatusSent,
				SentAt:        now.Add(-2 * time.Second),
				Timeout:       5 * time.Second, // Still valid
			},
			{
				CorrelationID: "3",
				Status:        RequestStatusCompleted,
				SentAt:        now.Add(-10 * time.Minute), // Should be removed
			},
			{
				CorrelationID: "4",
				Status:        RequestStatusCompleted,
				SentAt:        now.Add(-1 * time.Minute), // Keep for now
			},
		}
		
		for _, req := range requests {
			tracker.TrackRequest(req)
		}
		
		// Clean up
		removed := tracker.CleanupExpired()
		assert.Equal(t, 1, removed) // Only request 1 should timeout
		
		// Verify request 1 is timeout
		req1, _ := tracker.GetRequest("1")
		assert.Equal(t, RequestStatusTimeout, req1.Status)
		
		// Verify request 3 is removed
		_, err := tracker.GetRequest("3")
		assert.Error(t, err)
		
		// Verify others still exist
		req2, _ := tracker.GetRequest("2")
		assert.Equal(t, RequestStatusSent, req2.Status)
		
		req4, _ := tracker.GetRequest("4")
		assert.Equal(t, RequestStatusCompleted, req4.Status)
	})
}

// Test DefaultRequestReplyClient
func TestDefaultRequestReplyClient(t *testing.T) {
	t.Run("NewRequestReplyClient", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := newMockSubscriber()
		
		subscriber.On("Subscribe", mock.Anything, mock.Anything, "reply", mock.Anything, mock.Anything).Return(nil)
		
		client, err := NewRequestReplyClient(publisher, subscriber)
		assert.NoError(t, err)
		assert.NotNil(t, client)
		assert.NotEmpty(t, client.replyQueue)
		assert.NotNil(t, client.tracker)
		
		// Clean up
		subscriber.On("Unsubscribe", client.replyQueue).Return(nil)
		client.Close()
		
		subscriber.AssertExpectations(t)
	})
	
	t.Run("NewRequestReplyClient with options", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := newMockSubscriber()
		tracker := NewInMemoryRequestTracker()
		
		subscriber.On("Subscribe", mock.Anything, "custom.reply.queue", "reply", mock.Anything, mock.Anything).Return(nil)
		
		client, err := NewRequestReplyClient(publisher, subscriber,
			WithReplyQueue("custom.reply.queue"),
			WithRequestTracker(tracker),
		)
		
		assert.NoError(t, err)
		assert.Equal(t, "custom.reply.queue", client.replyQueue)
		assert.Equal(t, tracker, client.tracker)
		
		// Clean up
		subscriber.On("Unsubscribe", "custom.reply.queue").Return(nil)
		client.Close()
	})
	
	t.Run("SendAndReceive success", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := newMockSubscriber()
		
		subscriber.On("Subscribe", mock.Anything, mock.Anything, "reply", mock.Anything, mock.Anything).Return(nil)
		
		client, err := NewRequestReplyClient(publisher, subscriber)
		assert.NoError(t, err)
		
		query := &testRequestReplyQuery{
			BaseQuery: contracts.BaseQuery{
				BaseMessage: contracts.NewBaseMessage("TestQuery"),
			},
			Data: "test data",
		}
		
		// Mock publish
		publisher.On("Publish", mock.Anything, query, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
			// Extract correlation ID from the message
			msg := args.Get(1).(contracts.Message)
			correlationID := msg.GetCorrelationID()
			
			// Simulate reply after a short delay
			go func() {
				time.Sleep(10 * time.Millisecond)
				reply := &testRequestReplyReply{
					BaseReply: contracts.BaseReply{
						BaseMessage: contracts.BaseMessage{
							ID:            "reply-123",
							Type:          "TestReply",
							Timestamp:     time.Now(),
							CorrelationID: correlationID,
						},
					},
					Result: "query result",
				}
				subscriber.simulateReply(context.Background(), client.replyQueue, reply)
			}()
		})
		
		// Send and receive
		ctx := context.Background()
		reply, err := client.SendAndReceive(ctx, query, 1*time.Second)
		
		assert.NoError(t, err)
		assert.NotNil(t, reply)
		
		testRequestReplyReply, ok := reply.(*testRequestReplyReply)
		assert.True(t, ok)
		assert.Equal(t, "query result", testRequestReplyReply.Result)
		
		// Verify request was tracked
		activeRequests := client.tracker.GetActiveRequests()
		assert.Len(t, activeRequests, 0) // Should be completed
		
		// Clean up
		subscriber.On("Unsubscribe", client.replyQueue).Return(nil)
		client.Close()
		
		publisher.AssertExpectations(t)
	})
	
	t.Run("SendAndReceive timeout", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := newMockSubscriber()
		
		subscriber.On("Subscribe", mock.Anything, mock.Anything, "reply", mock.Anything, mock.Anything).Return(nil)
		
		client, err := NewRequestReplyClient(publisher, subscriber)
		assert.NoError(t, err)
		
		query := &testRequestReplyQuery{
			BaseQuery: contracts.BaseQuery{
				BaseMessage: contracts.NewBaseMessage("TestQuery"),
			},
		}
		
		// Mock publish but don't send reply
		publisher.On("Publish", mock.Anything, query, mock.Anything).Return(nil)
		
		// Send with short timeout
		ctx := context.Background()
		reply, err := client.SendAndReceive(ctx, query, 50*time.Millisecond)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "timeout")
		assert.Nil(t, reply)
		
		// Clean up
		subscriber.On("Unsubscribe", client.replyQueue).Return(nil)
		client.Close()
	})
	
	t.Run("SendAndReceive context cancellation", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := newMockSubscriber()
		
		subscriber.On("Subscribe", mock.Anything, mock.Anything, "reply", mock.Anything, mock.Anything).Return(nil)
		
		client, err := NewRequestReplyClient(publisher, subscriber)
		assert.NoError(t, err)
		
		query := &testRequestReplyQuery{
			BaseQuery: contracts.BaseQuery{
				BaseMessage: contracts.NewBaseMessage("TestQuery"),
			},
		}
		
		// Mock publish
		publisher.On("Publish", mock.Anything, query, mock.Anything).Return(nil)
		
		// Create cancellable context
		ctx, cancel := context.WithCancel(context.Background())
		
		// Cancel after publish
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()
		
		reply, err := client.SendAndReceive(ctx, query, 1*time.Second)
		
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
		assert.Nil(t, reply)
		
		// Clean up
		subscriber.On("Unsubscribe", client.replyQueue).Return(nil)
		client.Close()
	})
	
	t.Run("SendAndReceive publish error", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := newMockSubscriber()
		
		subscriber.On("Subscribe", mock.Anything, mock.Anything, "reply", mock.Anything, mock.Anything).Return(nil)
		
		client, err := NewRequestReplyClient(publisher, subscriber)
		assert.NoError(t, err)
		
		query := &testRequestReplyQuery{
			BaseQuery: contracts.BaseQuery{
				BaseMessage: contracts.NewBaseMessage("TestQuery"),
			},
		}
		
		// Mock publish error
		publishErr := errors.New("publish failed")
		publisher.On("Publish", mock.Anything, query, mock.Anything).Return(publishErr)
		
		ctx := context.Background()
		reply, err := client.SendAndReceive(ctx, query, 1*time.Second)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to send request")
		assert.Nil(t, reply)
		
		// Clean up
		subscriber.On("Unsubscribe", client.replyQueue).Return(nil)
		client.Close()
	})
	
	t.Run("SendCommand", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := newMockSubscriber()
		
		subscriber.On("Subscribe", mock.Anything, mock.Anything, "reply", mock.Anything, mock.Anything).Return(nil)
		
		client, err := NewRequestReplyClient(publisher, subscriber)
		assert.NoError(t, err)
		
		command := &testRequestReplyCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage:    contracts.NewBaseMessage("TestCommand"),
				TargetService: "test-service",
			},
			Data: "command data",
		}
		
		// Mock publish and reply
		publisher.On("Publish", mock.Anything, command, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
			msg := args.Get(1).(contracts.Message)
			correlationID := msg.GetCorrelationID()
			
			go func() {
				time.Sleep(10 * time.Millisecond)
				reply := &testRequestReplyReply{
					BaseReply: contracts.BaseReply{
						BaseMessage: contracts.BaseMessage{
							CorrelationID: correlationID,
						},
					},
					Result: "command executed",
				}
				subscriber.simulateReply(context.Background(), client.replyQueue, reply)
			}()
		})
		
		ctx := context.Background()
		reply, err := client.SendCommand(ctx, command, 1*time.Second)
		
		assert.NoError(t, err)
		assert.NotNil(t, reply)
		
		// Clean up
		subscriber.On("Unsubscribe", client.replyQueue).Return(nil)
		client.Close()
	})
	
	t.Run("SendQuery", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := newMockSubscriber()
		
		subscriber.On("Subscribe", mock.Anything, mock.Anything, "reply", mock.Anything, mock.Anything).Return(nil)
		
		client, err := NewRequestReplyClient(publisher, subscriber)
		assert.NoError(t, err)
		
		query := &testRequestReplyQuery{
			BaseQuery: contracts.BaseQuery{
				BaseMessage: contracts.NewBaseMessage("TestQuery"),
			},
			Data: "query data",
		}
		
		// Mock publish and reply
		publisher.On("Publish", mock.Anything, query, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
			msg := args.Get(1).(contracts.Message)
			correlationID := msg.GetCorrelationID()
			
			go func() {
				time.Sleep(10 * time.Millisecond)
				reply := &testRequestReplyReply{
					BaseReply: contracts.BaseReply{
						BaseMessage: contracts.BaseMessage{
							CorrelationID: correlationID,
						},
					},
					Result: "query result",
				}
				subscriber.simulateReply(context.Background(), client.replyQueue, reply)
			}()
		})
		
		ctx := context.Background()
		reply, err := client.SendQuery(ctx, query, 1*time.Second)
		
		assert.NoError(t, err)
		assert.NotNil(t, reply)
		
		// Clean up
		subscriber.On("Unsubscribe", client.replyQueue).Return(nil)
		client.Close()
	})
	
	t.Run("handleReply with valid reply", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := newMockSubscriber()
		
		subscriber.On("Subscribe", mock.Anything, mock.Anything, "reply", mock.Anything, mock.Anything).Return(nil)
		
		client, err := NewRequestReplyClient(publisher, subscriber)
		assert.NoError(t, err)
		
		// Add pending request
		correlationID := "test-corr-123"
		responseChan := make(chan contracts.Reply, 1)
		
		client.mu.Lock()
		client.pendingChan[correlationID] = responseChan
		client.mu.Unlock()
		
		// Track request
		client.tracker.TrackRequest(&TrackedRequest{
			CorrelationID: correlationID,
			Status:        RequestStatusSent,
		})
		
		// Handle reply
		reply := &testRequestReplyReply{
			BaseReply: contracts.BaseReply{
				BaseMessage: contracts.BaseMessage{
					CorrelationID: correlationID,
				},
			},
			Result: "test result",
		}
		
		err = client.handleReply(context.Background(), reply)
		assert.NoError(t, err)
		
		// Verify reply received
		select {
		case received := <-responseChan:
			assert.Equal(t, reply, received)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("reply not received")
		}
		
		// Clean up
		subscriber.On("Unsubscribe", client.replyQueue).Return(nil)
		client.Close()
	})
	
	t.Run("handleReply with non-reply message", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := newMockSubscriber()
		
		subscriber.On("Subscribe", mock.Anything, mock.Anything, "reply", mock.Anything, mock.Anything).Return(nil)
		
		client, err := NewRequestReplyClient(publisher, subscriber)
		assert.NoError(t, err)
		
		// Handle non-reply message
		nonReply := &testRequestReplyQuery{
			BaseQuery: contracts.BaseQuery{
				BaseMessage: contracts.NewBaseMessage("TestQuery"),
			},
		}
		
		err = client.handleReply(context.Background(), nonReply)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "non-reply message")
		
		// Clean up
		subscriber.On("Unsubscribe", client.replyQueue).Return(nil)
		client.Close()
	})
	
	t.Run("handleReply without correlation ID", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := newMockSubscriber()
		
		subscriber.On("Subscribe", mock.Anything, mock.Anything, "reply", mock.Anything, mock.Anything).Return(nil)
		
		client, err := NewRequestReplyClient(publisher, subscriber)
		assert.NoError(t, err)
		
		// Handle reply without correlation ID
		reply := &testRequestReplyReply{
			BaseReply: contracts.BaseReply{
				BaseMessage: contracts.BaseMessage{
					// No correlation ID
				},
			},
		}
		
		err = client.handleReply(context.Background(), reply)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing correlation ID")
		
		// Clean up
		subscriber.On("Unsubscribe", client.replyQueue).Return(nil)
		client.Close()
	})
}

// Test concurrent request/reply operations
func TestConcurrentRequestReply(t *testing.T) {
	t.Run("multiple concurrent requests", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := newMockSubscriber()
		
		subscriber.On("Subscribe", mock.Anything, mock.Anything, "reply", mock.Anything, mock.Anything).Return(nil)
		
		client, err := NewRequestReplyClient(publisher, subscriber)
		assert.NoError(t, err)
		
		// Mock publish to always succeed and send reply
		publisher.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
			msg := args.Get(1).(contracts.Message)
			correlationID := msg.GetCorrelationID()
			
			// Simulate variable response times
			go func() {
				time.Sleep(time.Duration(10+len(correlationID)%20) * time.Millisecond)
				reply := &testRequestReplyReply{
					BaseReply: contracts.BaseReply{
						BaseMessage: contracts.BaseMessage{
							CorrelationID: correlationID,
						},
					},
					Result: fmt.Sprintf("result-%s", correlationID[:8]),
				}
				subscriber.simulateReply(context.Background(), client.replyQueue, reply)
			}()
		})
		
		// Send multiple concurrent requests
		numRequests := 20
		var wg sync.WaitGroup
		results := make(chan error, numRequests)
		
		for i := 0; i < numRequests; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				
				query := &testRequestReplyQuery{
					BaseQuery: contracts.BaseQuery{
						BaseMessage: contracts.NewBaseMessage("TestQuery"),
					},
					Data: fmt.Sprintf("query-%d", id),
				}
				
				ctx := context.Background()
				reply, err := client.SendAndReceive(ctx, query, 1*time.Second)
				
				if err != nil {
					results <- err
				} else {
					if reply == nil {
						results <- errors.New("reply is nil")
					} else {
						results <- nil
					}
				}
			}(i)
		}
		
		wg.Wait()
		close(results)
		
		// Verify all succeeded
		errorCount := 0
		for err := range results {
			if err != nil {
				errorCount++
				t.Errorf("Request failed: %v", err)
			}
		}
		assert.Equal(t, 0, errorCount)
		
		// Clean up
		subscriber.On("Unsubscribe", client.replyQueue).Return(nil)
		client.Close()
	})
}

// Test RequestHandlerFunc
func TestRequestHandlerFunc(t *testing.T) {
	t.Run("RequestHandlerFunc adapter", func(t *testing.T) {
		called := false
		var receivedRequest contracts.Message
		
		handler := RequestHandlerFunc(func(ctx context.Context, request contracts.Message) (contracts.Reply, error) {
			called = true
			receivedRequest = request
			
			return &testRequestReplyReply{
				BaseReply: contracts.BaseReply{
					BaseMessage: contracts.NewBaseMessage("TestReply"),
				},
				Result: "handled",
			}, nil
		})
		
		query := &testRequestReplyQuery{
			BaseQuery: contracts.BaseQuery{
				BaseMessage: contracts.NewBaseMessage("TestQuery"),
			},
		}
		
		reply, err := handler.HandleRequest(context.Background(), query)
		
		assert.NoError(t, err)
		assert.True(t, called)
		assert.Equal(t, query, receivedRequest)
		assert.NotNil(t, reply)
		
		testRequestReplyReply, ok := reply.(*testRequestReplyReply)
		assert.True(t, ok)
		assert.Equal(t, "handled", testRequestReplyReply.Result)
	})
}

// Test cleanup routine
func TestCleanupRoutine(t *testing.T) {
	t.Run("cleanup routine runs periodically", func(t *testing.T) {
		// Create custom tracker that counts cleanup calls
		cleanupCount := int32(0)
		tracker := &countingTracker{
			InMemoryRequestTracker: NewInMemoryRequestTracker(),
			cleanupCount:           &cleanupCount,
		}
		
		publisher := &mockPublisher{}
		subscriber := newMockSubscriber()
		
		subscriber.On("Subscribe", mock.Anything, mock.Anything, "reply", mock.Anything, mock.Anything).Return(nil)
		
		// Create client with custom tracker
		client, err := NewRequestReplyClient(publisher, subscriber,
			WithRequestTracker(tracker),
		)
		assert.NoError(t, err)
		
		// Override cleanup interval for faster testing
		client.done = make(chan struct{})
		go func() {
			ticker := time.NewTicker(50 * time.Millisecond)
			defer ticker.Stop()
			
			for {
				select {
				case <-ticker.C:
					client.tracker.CleanupExpired()
				case <-client.done:
					return
				}
			}
		}()
		
		// Wait for cleanup to run a few times
		time.Sleep(200 * time.Millisecond)
		
		// Verify cleanup was called
		count := atomic.LoadInt32(&cleanupCount)
		assert.Greater(t, count, int32(2))
		
		// Clean up
		subscriber.On("Unsubscribe", client.replyQueue).Return(nil)
		client.Close()
	})
}

// Helper tracker that counts cleanup calls
type countingTracker struct {
	*InMemoryRequestTracker
	cleanupCount *int32
}

func (t *countingTracker) CleanupExpired() int {
	atomic.AddInt32(t.cleanupCount, 1)
	return t.InMemoryRequestTracker.CleanupExpired()
}

// Benchmark request/reply operations
func BenchmarkRequestReply(b *testing.B) {
	publisher := &mockPublisher{}
	subscriber := newMockSubscriber()
	
	subscriber.On("Subscribe", mock.Anything, mock.Anything, "reply", mock.Anything, mock.Anything).Return(nil)
	
	client, _ := NewRequestReplyClient(publisher, subscriber)
	defer func() {
		subscriber.On("Unsubscribe", client.replyQueue).Return(nil)
		client.Close()
	}()
	
	// Mock instant replies
	publisher.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		msg := args.Get(1).(contracts.Message)
		correlationID := msg.GetCorrelationID()
		
		reply := &testRequestReplyReply{
			BaseReply: contracts.BaseReply{
				BaseMessage: contracts.BaseMessage{
					CorrelationID: correlationID,
				},
			},
			Result: "benchmark result",
		}
		// Immediate reply
		subscriber.simulateReply(context.Background(), client.replyQueue, reply)
	})
	
	query := &testRequestReplyQuery{
		BaseQuery: contracts.BaseQuery{
			BaseMessage: contracts.NewBaseMessage("BenchQuery"),
		},
		Data: "benchmark data",
	}
	
	ctx := context.Background()
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := client.SendAndReceive(ctx, query, 1*time.Second)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Benchmark tracker operations
func BenchmarkRequestTracker(b *testing.B) {
	b.Run("TrackRequest", func(b *testing.B) {
		tracker := NewInMemoryRequestTracker()
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			request := &TrackedRequest{
				ID:            fmt.Sprintf("req-%d", i),
				CorrelationID: fmt.Sprintf("corr-%d", i),
				Status:        RequestStatusPending,
				SentAt:        time.Now(),
				Timeout:       5 * time.Second,
			}
			tracker.TrackRequest(request)
		}
	})
	
	b.Run("GetRequest", func(b *testing.B) {
		tracker := NewInMemoryRequestTracker()
		
		// Pre-populate tracker
		for i := 0; i < 1000; i++ {
			request := &TrackedRequest{
				ID:            fmt.Sprintf("req-%d", i),
				CorrelationID: fmt.Sprintf("corr-%d", i),
				Status:        RequestStatusPending,
			}
			tracker.TrackRequest(request)
		}
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			correlationID := fmt.Sprintf("corr-%d", i%1000)
			tracker.GetRequest(correlationID)
		}
	})
	
	b.Run("UpdateStatus", func(b *testing.B) {
		tracker := NewInMemoryRequestTracker()
		
		// Pre-populate tracker
		for i := 0; i < 1000; i++ {
			request := &TrackedRequest{
				ID:            fmt.Sprintf("req-%d", i),
				CorrelationID: fmt.Sprintf("corr-%d", i),
				Status:        RequestStatusPending,
			}
			tracker.TrackRequest(request)
		}
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			correlationID := fmt.Sprintf("corr-%d", i%1000)
			status := RequestStatus([]RequestStatus{
				RequestStatusSent,
				RequestStatusReceived,
				RequestStatusCompleted,
			}[i%3])
			tracker.UpdateStatus(correlationID, status)
		}
	})
}