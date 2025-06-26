package messaging

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Mock subscriber for testing
type mockServerSubscriber struct {
	mock.Mock
	handlers map[string]MessageHandler
	mu       sync.Mutex
}

func (m *mockServerSubscriber) Subscribe(ctx context.Context, queue string, messageType string, handler MessageHandler, opts ...SubscriptionOption) error {
	m.mu.Lock()
	if m.handlers == nil {
		m.handlers = make(map[string]MessageHandler)
	}
	m.handlers[queue] = handler
	m.mu.Unlock()
	
	args := m.Called(ctx, queue, messageType, handler, opts)
	return args.Error(0)
}

func (m *mockServerSubscriber) Unsubscribe(queue string) error {
	m.mu.Lock()
	delete(m.handlers, queue)
	m.mu.Unlock()
	
	args := m.Called(queue)
	return args.Error(0)
}

func (m *mockServerSubscriber) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockServerSubscriber) simulateRequest(queue string, request contracts.Message) error {
	m.mu.Lock()
	handler, exists := m.handlers[queue]
	m.mu.Unlock()
	
	if !exists {
		return errors.New("no handler for queue")
	}
	return handler.Handle(context.Background(), request)
}

// Mock publisher for testing
type mockServerPublisher struct {
	mock.Mock
}

func (m *mockServerPublisher) Publish(ctx context.Context, msg contracts.Message, opts ...PublishOption) error {
	args := m.Called(ctx, msg, opts)
	return args.Error(0)
}

func (m *mockServerPublisher) PublishEvent(ctx context.Context, event contracts.Event, opts ...PublishOption) error {
	args := m.Called(ctx, event, opts)
	return args.Error(0)
}

func (m *mockServerPublisher) PublishCommand(ctx context.Context, cmd contracts.Command, opts ...PublishOption) error {
	args := m.Called(ctx, cmd, opts)
	return args.Error(0)
}

func (m *mockServerPublisher) Close() error {
	args := m.Called()
	return args.Error(0)
}

// Test command type
type TestServerCommand struct {
	contracts.BaseCommand
	Value string `json:"value"`
}

// Test query type
type TestServerQuery struct {
	contracts.BaseQuery
	Question string `json:"question"`
}

// Test reply type
type TestServerReply struct {
	contracts.BaseReply
	Answer string `json:"answer"`
}

// Test retry policy
type testRetryPolicy struct {
	maxRetries int
	delay      time.Duration
}

func (p *testRetryPolicy) ShouldRetry(attempt int, err error) (bool, time.Duration) {
	if attempt > p.maxRetries {
		return false, 0
	}
	return true, p.delay
}

func (p *testRetryPolicy) MaxRetries() int {
	return p.maxRetries
}

func (p *testRetryPolicy) NextDelay(attempt int) time.Duration {
	return p.delay
}

func TestNewRequestReplyServer(t *testing.T) {
	t.Run("creates server with valid dependencies", func(t *testing.T) {
		subscriber := &mockServerSubscriber{}
		publisher := &mockServerPublisher{}
		
		server, err := NewRequestReplyServer(subscriber, publisher)
		
		assert.NoError(t, err)
		assert.NotNil(t, server)
		assert.Equal(t, 0, server.GetHandlerCount())
	})

	t.Run("fails with nil subscriber", func(t *testing.T) {
		publisher := &mockServerPublisher{}
		
		server, err := NewRequestReplyServer(nil, publisher)
		
		assert.Error(t, err)
		assert.Nil(t, server)
		assert.Contains(t, err.Error(), "subscriber cannot be nil")
	})

	t.Run("fails with nil publisher", func(t *testing.T) {
		subscriber := &mockServerSubscriber{}
		
		server, err := NewRequestReplyServer(subscriber, nil)
		
		assert.Error(t, err)
		assert.Nil(t, server)
		assert.Contains(t, err.Error(), "publisher cannot be nil")
	})
}

func TestRegisterHandler(t *testing.T) {
	t.Run("registers handler successfully", func(t *testing.T) {
		subscriber := &mockServerSubscriber{}
		publisher := &mockServerPublisher{}
		server, _ := NewRequestReplyServer(subscriber, publisher)
		
		handler := ServerRequestHandlerFunc(func(ctx context.Context, req contracts.Message) (contracts.Reply, error) {
			return &TestServerReply{Answer: "test"}, nil
		})
		
		err := server.RegisterHandler("TestCommand", handler)
		
		assert.NoError(t, err)
		assert.Equal(t, 1, server.GetHandlerCount())
	})

	t.Run("fails with empty message type", func(t *testing.T) {
		subscriber := &mockServerSubscriber{}
		publisher := &mockServerPublisher{}
		server, _ := NewRequestReplyServer(subscriber, publisher)
		
		handler := ServerRequestHandlerFunc(func(ctx context.Context, req contracts.Message) (contracts.Reply, error) {
			return nil, nil
		})
		
		err := server.RegisterHandler("", handler)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "message type cannot be empty")
	})

	t.Run("fails with nil handler", func(t *testing.T) {
		subscriber := &mockServerSubscriber{}
		publisher := &mockServerPublisher{}
		server, _ := NewRequestReplyServer(subscriber, publisher)
		
		err := server.RegisterHandler("TestCommand", nil)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "handler cannot be nil")
	})

	t.Run("fails with duplicate handler", func(t *testing.T) {
		subscriber := &mockServerSubscriber{}
		publisher := &mockServerPublisher{}
		server, _ := NewRequestReplyServer(subscriber, publisher)
		
		handler := ServerRequestHandlerFunc(func(ctx context.Context, req contracts.Message) (contracts.Reply, error) {
			return nil, nil
		})
		
		err := server.RegisterHandler("TestCommand", handler)
		assert.NoError(t, err)
		
		err = server.RegisterHandler("TestCommand", handler)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "handler already registered")
	})
}

func TestRegisterCommandHandler(t *testing.T) {
	t.Run("registers command handler successfully", func(t *testing.T) {
		subscriber := &mockServerSubscriber{}
		publisher := &mockServerPublisher{}
		server, _ := NewRequestReplyServer(subscriber, publisher)
		
		handler := func(ctx context.Context, cmd contracts.Command) (contracts.Reply, error) {
			testCmd := cmd.(*TestServerCommand)
			return &TestServerReply{
				BaseReply: contracts.BaseReply{
					BaseMessage: contracts.NewBaseMessage("TestReply"),
					Success:     true,
				},
				Answer: "Received: " + testCmd.Value,
			}, nil
		}
		
		err := server.RegisterCommandHandler("TestCommand", handler)
		
		assert.NoError(t, err)
		assert.Equal(t, 1, server.GetHandlerCount())
	})
}

func TestRegisterQueryHandler(t *testing.T) {
	t.Run("registers query handler successfully", func(t *testing.T) {
		subscriber := &mockServerSubscriber{}
		publisher := &mockServerPublisher{}
		server, _ := NewRequestReplyServer(subscriber, publisher)
		
		handler := func(ctx context.Context, query contracts.Query) (contracts.Reply, error) {
			testQuery := query.(*TestServerQuery)
			return &TestServerReply{
				BaseReply: contracts.BaseReply{
					BaseMessage: contracts.NewBaseMessage("TestReply"),
					Success:     true,
				},
				Answer: "Answer to: " + testQuery.Question,
			}, nil
		}
		
		err := server.RegisterQueryHandler("TestQuery", handler)
		
		assert.NoError(t, err)
		assert.Equal(t, 1, server.GetHandlerCount())
	})
}

func TestServerStart(t *testing.T) {
	t.Run("starts server successfully", func(t *testing.T) {
		subscriber := &mockServerSubscriber{}
		publisher := &mockServerPublisher{}
		server, _ := NewRequestReplyServer(subscriber, publisher,
			WithServerQueuePrefix("test.rpc"),
		)
		
		// Register handler
		handler := ServerRequestHandlerFunc(func(ctx context.Context, req contracts.Message) (contracts.Reply, error) {
			return &TestServerReply{Answer: "test"}, nil
		})
		server.RegisterHandler("TestCommand", handler)
		
		// Setup mock expectations
		subscriber.On("Subscribe", 
			mock.Anything, 
			mock.AnythingOfType("string"), 
			"TestCommand",
			mock.Anything,
			mock.Anything,
		).Return(nil)
		
		err := server.Start(context.Background())
		
		assert.NoError(t, err)
		subscriber.AssertExpectations(t)
		
		// Cleanup
		subscriber.On("Unsubscribe", mock.AnythingOfType("string")).Return(nil)
		server.Stop()
	})

	t.Run("fails to start without handlers", func(t *testing.T) {
		subscriber := &mockServerSubscriber{}
		publisher := &mockServerPublisher{}
		server, _ := NewRequestReplyServer(subscriber, publisher)
		
		err := server.Start(context.Background())
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no handlers registered")
	})

	t.Run("fails to start when already running", func(t *testing.T) {
		subscriber := &mockServerSubscriber{}
		publisher := &mockServerPublisher{}
		server, _ := NewRequestReplyServer(subscriber, publisher)
		
		// Register handler
		handler := ServerRequestHandlerFunc(func(ctx context.Context, req contracts.Message) (contracts.Reply, error) {
			return &TestServerReply{Answer: "test"}, nil
		})
		server.RegisterHandler("TestCommand", handler)
		
		subscriber.On("Subscribe", 
			mock.Anything, 
			mock.AnythingOfType("string"), 
			"TestCommand",
			mock.Anything,
			mock.Anything,
		).Return(nil)
		
		err := server.Start(context.Background())
		assert.NoError(t, err)
		
		// Try to start again
		err = server.Start(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "server already running")
		
		// Cleanup
		subscriber.On("Unsubscribe", mock.AnythingOfType("string")).Return(nil)
		server.Stop()
	})

	t.Run("cannot register handler while running", func(t *testing.T) {
		subscriber := &mockServerSubscriber{}
		publisher := &mockServerPublisher{}
		server, _ := NewRequestReplyServer(subscriber, publisher)
		
		// Register initial handler
		handler := ServerRequestHandlerFunc(func(ctx context.Context, req contracts.Message) (contracts.Reply, error) {
			return &TestServerReply{Answer: "test"}, nil
		})
		server.RegisterHandler("TestCommand", handler)
		
		subscriber.On("Subscribe", 
			mock.Anything, 
			mock.AnythingOfType("string"), 
			"TestCommand",
			mock.Anything,
			mock.Anything,
		).Return(nil)
		
		err := server.Start(context.Background())
		assert.NoError(t, err)
		
		// Try to register another handler
		err = server.RegisterHandler("TestQuery", handler)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot register handler while server is running")
		
		// Cleanup
		subscriber.On("Unsubscribe", mock.AnythingOfType("string")).Return(nil)
		server.Stop()
	})
}

func TestServerStop(t *testing.T) {
	t.Run("stops server successfully", func(t *testing.T) {
		subscriber := &mockServerSubscriber{}
		publisher := &mockServerPublisher{}
		server, _ := NewRequestReplyServer(subscriber, publisher)
		
		// Register and start
		handler := ServerRequestHandlerFunc(func(ctx context.Context, req contracts.Message) (contracts.Reply, error) {
			return &TestServerReply{Answer: "test"}, nil
		})
		server.RegisterHandler("TestCommand", handler)
		
		subscriber.On("Subscribe", 
			mock.Anything, 
			mock.AnythingOfType("string"), 
			"TestCommand",
			mock.Anything,
			mock.Anything,
		).Return(nil)
		
		server.Start(context.Background())
		
		// Stop
		subscriber.On("Unsubscribe", mock.AnythingOfType("string")).Return(nil)
		
		err := server.Stop()
		
		assert.NoError(t, err)
		subscriber.AssertExpectations(t)
	})

	t.Run("fails to stop when not running", func(t *testing.T) {
		subscriber := &mockServerSubscriber{}
		publisher := &mockServerPublisher{}
		server, _ := NewRequestReplyServer(subscriber, publisher)
		
		err := server.Stop()
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "server not running")
	})
}

func TestHandleMessage(t *testing.T) {
	t.Run("handles request successfully", func(t *testing.T) {
		subscriber := &mockServerSubscriber{}
		publisher := &mockServerPublisher{}
		server, _ := NewRequestReplyServer(subscriber, publisher)
		
		// Register handler
		handler := ServerRequestHandlerFunc(func(ctx context.Context, req contracts.Message) (contracts.Reply, error) {
			cmd := req.(*TestServerCommand)
			return &TestServerReply{
				BaseReply: contracts.BaseReply{
					BaseMessage: contracts.NewBaseMessage("TestReply"),
					Success:     true,
				},
				Answer: "Processed: " + cmd.Value,
			}, nil
		})
		server.RegisterHandler("TestCommand", handler)
		
		// Start server
		subscriber.On("Subscribe", 
			mock.Anything, 
			mock.AnythingOfType("string"), 
			"TestCommand",
			mock.Anything,
			mock.Anything,
		).Return(nil)
		
		server.Start(context.Background())
		
		// Create test command with reply-to set
		baseMsg := contracts.NewBaseMessage("TestCommand")
		cmd := &TestServerCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage:   baseMsg,
				TargetService: "test",
				ReplyTo:       "test.reply",
			},
			Value: "test data",
		}
		cmd.SetCorrelationID("corr-123")
		
		// Expect reply to be published
		publisher.On("Publish", 
			mock.Anything,
			mock.MatchedBy(func(msg contracts.Message) bool {
				reply, ok := msg.(*TestServerReply)
				return ok && reply.Answer == "Processed: test data" && reply.GetCorrelationID() == "corr-123"
			}),
			mock.Anything,
		).Return(nil)
		
		// Simulate request - use the queue name format
		queue := "mmate.rpc.TestCommand"
		err := subscriber.simulateRequest(queue, cmd)
		
		assert.NoError(t, err)
		publisher.AssertExpectations(t)
		
		// Cleanup
		subscriber.On("Unsubscribe", mock.AnythingOfType("string")).Return(nil)
		server.Stop()
	})

	t.Run("sends error reply on handler failure", func(t *testing.T) {
		subscriber := &mockServerSubscriber{}
		publisher := &mockServerPublisher{}
		server, _ := NewRequestReplyServer(subscriber, publisher)
		
		// Register failing handler
		handler := ServerRequestHandlerFunc(func(ctx context.Context, req contracts.Message) (contracts.Reply, error) {
			return nil, errors.New("handler error")
		})
		server.RegisterHandler("TestCommand", handler)
		
		// Start server
		subscriber.On("Subscribe", 
			mock.Anything, 
			mock.AnythingOfType("string"), 
			"TestCommand",
			mock.Anything,
			mock.Anything,
		).Return(nil)
		
		server.Start(context.Background())
		
		// Create test command with reply-to set
		baseMsg := contracts.NewBaseMessage("TestCommand")
		cmd := &TestServerCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage:   baseMsg,
				TargetService: "test",
				ReplyTo:       "test.reply",
			},
			Value: "test data",
		}
		cmd.SetCorrelationID("corr-123")
		
		// Expect error reply to be published
		publisher.On("Publish", 
			mock.Anything,
			mock.MatchedBy(func(msg contracts.Message) bool {
				reply, ok := msg.(*ErrorReply)
				return ok && !reply.Success && reply.ErrorMessage == "handler error"
			}),
			mock.Anything,
		).Return(nil)
		
		// Simulate request - use the queue name format
		queue := "mmate.rpc.TestCommand"
		err := subscriber.simulateRequest(queue, cmd)
		
		// Error is handled internally, so no error returned
		assert.NoError(t, err)
		publisher.AssertExpectations(t)
		
		// Cleanup
		subscriber.On("Unsubscribe", mock.AnythingOfType("string")).Return(nil)
		server.Stop()
	})

	t.Run("handles request with retry", func(t *testing.T) {
		subscriber := &mockServerSubscriber{}
		publisher := &mockServerPublisher{}
		
		// Create custom retry policy
		retryPolicy := &testRetryPolicy{maxRetries: 2, delay: 10 * time.Millisecond}
		
		server, _ := NewRequestReplyServer(subscriber, publisher,
			WithServerRetryPolicy(retryPolicy),
		)
		
		// Register handler that fails once then succeeds
		callCount := 0
		handler := ServerRequestHandlerFunc(func(ctx context.Context, req contracts.Message) (contracts.Reply, error) {
			callCount++
			if callCount == 1 {
				return nil, errors.New("temporary error")
			}
			return &TestServerReply{
				BaseReply: contracts.BaseReply{
					BaseMessage: contracts.NewBaseMessage("TestReply"),
					Success:     true,
				},
				Answer: "Success after retry",
			}, nil
		})
		server.RegisterHandler("TestCommand", handler)
		
		// Start server
		subscriber.On("Subscribe", 
			mock.Anything, 
			mock.AnythingOfType("string"), 
			"TestCommand",
			mock.Anything,
			mock.Anything,
		).Return(nil)
		
		server.Start(context.Background())
		
		// Create test command with reply-to set
		baseMsg := contracts.NewBaseMessage("TestCommand")
		cmd := &TestServerCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage:   baseMsg,
				TargetService: "test",
				ReplyTo:       "test.reply",
			},
			Value: "test data",
		}
		cmd.SetCorrelationID("corr-456")
		
		// Expect successful reply after retry
		publisher.On("Publish", 
			mock.Anything,
			mock.MatchedBy(func(msg contracts.Message) bool {
				reply, ok := msg.(*TestServerReply)
				return ok && reply.Answer == "Success after retry"
			}),
			mock.Anything,
		).Return(nil)
		
		// Simulate request - use the queue name format
		queue := "mmate.rpc.TestCommand"
		err := subscriber.simulateRequest(queue, cmd)
		
		assert.NoError(t, err)
		assert.Equal(t, 2, callCount) // Should have been called twice
		publisher.AssertExpectations(t)
		
		// Cleanup
		subscriber.On("Unsubscribe", mock.AnythingOfType("string")).Return(nil)
		server.Stop()
	})
}

func TestErrorReply(t *testing.T) {
	t.Run("creates error reply correctly", func(t *testing.T) {
		reply := &ErrorReply{
			BaseReply: contracts.BaseReply{
				BaseMessage: contracts.NewBaseMessage("ErrorReply"),
				Success:     false,
			},
			ErrorMessage: "Something went wrong",
			ErrorCode:    "ERR_001",
			Timestamp:    time.Now().UTC(),
		}
		
		assert.False(t, reply.IsSuccess())
		assert.Equal(t, "ERR_001: Something went wrong", reply.GetError().Error())
	})

	t.Run("marshals to JSON correctly", func(t *testing.T) {
		timestamp := time.Now().UTC()
		reply := &ErrorReply{
			BaseReply: contracts.BaseReply{
				BaseMessage: contracts.NewBaseMessage("ErrorReply"),
				Success:     false,
			},
			ErrorMessage: "Test error",
			ErrorCode:    "TEST_ERR",
			Timestamp:    timestamp,
		}
		
		data, err := reply.MarshalJSON()
		require.NoError(t, err)
		
		// Verify JSON contains expected fields
		jsonStr := string(data)
		assert.Contains(t, jsonStr, `"errorMessage":"Test error"`)
		assert.Contains(t, jsonStr, `"errorCode":"TEST_ERR"`)
		assert.Contains(t, jsonStr, `"success":false`)
		assert.Contains(t, jsonStr, timestamp.Format(time.RFC3339))
	})
}