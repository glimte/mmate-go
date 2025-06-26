package messaging

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/glimte/mmate-go/contracts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Test message types
type TestCommand struct {
	contracts.BaseCommand
	Data string `json:"data"`
}

func (c *TestCommand) GetTargetService() string {
	return c.TargetService
}

type TestEvent struct {
	contracts.BaseEvent
	Data string `json:"data"`
}

func (e *TestEvent) GetAggregateID() string {
	return e.AggregateID
}

func (e *TestEvent) GetSequence() int64 {
	return e.Sequence
}

// Mock handler
type mockHandler struct {
	mock.Mock
}

func (m *mockHandler) Handle(ctx context.Context, msg contracts.Message) error {
	args := m.Called(ctx, msg)
	return args.Error(0)
}

func TestMessageDispatcher(t *testing.T) {
	t.Run("NewMessageDispatcher creates dispatcher with defaults", func(t *testing.T) {
		dispatcher := NewMessageDispatcher()
		
		assert.NotNil(t, dispatcher)
		assert.NotNil(t, dispatcher.handlers)
		assert.NotNil(t, dispatcher.logger)
		assert.Empty(t, dispatcher.middleware)
	})
	
	t.Run("NewMessageDispatcher applies options", func(t *testing.T) {
		logger := slog.Default()
		middleware := func(ctx context.Context, msg contracts.Message, next MessageHandler) error {
			return next.Handle(ctx, msg)
		}
		
		dispatcher := NewMessageDispatcher(
			WithDispatcherLogger(logger),
			WithMiddleware(middleware),
		)
		
		assert.Equal(t, logger, dispatcher.logger)
		assert.Len(t, dispatcher.middleware, 1)
	})
	
	t.Run("RegisterHandler succeeds with valid parameters", func(t *testing.T) {
		dispatcher := NewMessageDispatcher()
		handler := &mockHandler{}
		cmd := &TestCommand{}
		
		err := dispatcher.RegisterHandler(cmd, handler)
		
		assert.NoError(t, err)
		handlers := dispatcher.GetHandlers(cmd)
		assert.Len(t, handlers, 1)
		assert.Equal(t, handler, handlers[0].Handler)
	})
	
	t.Run("RegisterHandler fails with nil message type", func(t *testing.T) {
		dispatcher := NewMessageDispatcher()
		handler := &mockHandler{}
		
		err := dispatcher.RegisterHandler(nil, handler)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "messageType cannot be nil")
	})
	
	t.Run("RegisterHandler fails with nil handler", func(t *testing.T) {
		dispatcher := NewMessageDispatcher()
		cmd := &TestCommand{}
		
		err := dispatcher.RegisterHandler(cmd, nil)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "handler cannot be nil")
	})
	
	t.Run("RegisterHandlerFunc registers function handler", func(t *testing.T) {
		dispatcher := NewMessageDispatcher()
		cmd := &TestCommand{}
		called := false
		
		handlerFunc := MessageHandlerFunc(func(ctx context.Context, msg contracts.Message) error {
			called = true
			return nil
		})
		
		err := dispatcher.RegisterHandlerFunc(cmd, handlerFunc)
		
		assert.NoError(t, err)
		
		// Dispatch message to verify it was registered
		err = dispatcher.Dispatch(context.Background(), &TestCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
		})
		
		assert.NoError(t, err)
		assert.True(t, called)
	})
	
	t.Run("Dispatch succeeds with registered handler", func(t *testing.T) {
		dispatcher := NewMessageDispatcher()
		handler := &mockHandler{}
		cmd := &TestCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
		}
		
		handler.On("Handle", mock.Anything, mock.Anything).Return(nil)
		
		err := dispatcher.RegisterHandler(cmd, handler)
		assert.NoError(t, err)
		
		err = dispatcher.Dispatch(context.Background(), cmd)
		
		assert.NoError(t, err)
		handler.AssertExpectations(t)
	})
	
	t.Run("Dispatch fails with nil message", func(t *testing.T) {
		dispatcher := NewMessageDispatcher()
		
		err := dispatcher.Dispatch(context.Background(), nil)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "message cannot be nil")
	})
	
	t.Run("Dispatch fails with no registered handlers", func(t *testing.T) {
		dispatcher := NewMessageDispatcher()
		cmd := &TestCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
		}
		
		err := dispatcher.Dispatch(context.Background(), cmd)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no handlers registered")
	})
	
	t.Run("Dispatch fails when handler returns error", func(t *testing.T) {
		dispatcher := NewMessageDispatcher()
		handler := &mockHandler{}
		cmd := &TestCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
		}
		
		handlerError := errors.New("handler failed")
		handler.On("Handle", mock.Anything, mock.Anything).Return(handlerError)
		
		err := dispatcher.RegisterHandler(cmd, handler)
		assert.NoError(t, err)
		
		err = dispatcher.Dispatch(context.Background(), cmd)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "dispatch failed")
		handler.AssertExpectations(t)
	})
	
	t.Run("UnregisterHandler removes handler", func(t *testing.T) {
		dispatcher := NewMessageDispatcher()
		handler := &mockHandler{}
		cmd := &TestCommand{}
		
		err := dispatcher.RegisterHandler(cmd, handler)
		assert.NoError(t, err)
		
		err = dispatcher.UnregisterHandler(cmd, handler)
		assert.NoError(t, err)
		
		handlers := dispatcher.GetHandlers(cmd)
		assert.Empty(t, handlers)
	})
	
	t.Run("UnregisterHandler fails with non-existent handler", func(t *testing.T) {
		dispatcher := NewMessageDispatcher()
		handler := &mockHandler{}
		cmd := &TestCommand{}
		
		err := dispatcher.UnregisterHandler(cmd, handler)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no handlers registered")
	})
	
	t.Run("GetRegisteredTypes returns all registered types", func(t *testing.T) {
		dispatcher := NewMessageDispatcher()
		handler := &mockHandler{}
		cmd := &TestCommand{}
		event := &TestEvent{}
		
		err := dispatcher.RegisterHandler(cmd, handler)
		assert.NoError(t, err)
		err = dispatcher.RegisterHandler(event, handler)
		assert.NoError(t, err)
		
		types := dispatcher.GetRegisteredTypes()
		
		assert.Len(t, types, 2)
		assert.Contains(t, types, "TestCommand")
		assert.Contains(t, types, "TestEvent")
	})
	
	t.Run("Middleware chain executes in correct order", func(t *testing.T) {
		var order []string
		
		middleware1 := func(ctx context.Context, msg contracts.Message, next MessageHandler) error {
			order = append(order, "middleware1-start")
			err := next.Handle(ctx, msg)
			order = append(order, "middleware1-end")
			return err
		}
		
		middleware2 := func(ctx context.Context, msg contracts.Message, next MessageHandler) error {
			order = append(order, "middleware2-start")
			err := next.Handle(ctx, msg)
			order = append(order, "middleware2-end")
			return err
		}
		
		handler := MessageHandlerFunc(func(ctx context.Context, msg contracts.Message) error {
			order = append(order, "handler")
			return nil
		})
		
		dispatcher := NewMessageDispatcher(WithMiddleware(middleware1, middleware2))
		cmd := &TestCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
		}
		
		err := dispatcher.RegisterHandler(cmd, handler)
		assert.NoError(t, err)
		
		err = dispatcher.Dispatch(context.Background(), cmd)
		assert.NoError(t, err)
		
		expected := []string{
			"middleware1-start",
			"middleware2-start", 
			"handler",
			"middleware2-end",
			"middleware1-end",
		}
		assert.Equal(t, expected, order)
	})
}

func TestHandlerOptions(t *testing.T) {
	t.Run("WithConcurrency sets concurrency", func(t *testing.T) {
		opts := HandlerOptions{}
		WithConcurrency(5)(&opts)
		assert.Equal(t, 5, opts.Concurrency)
	})
	
	t.Run("WithQueue sets queue name", func(t *testing.T) {
		opts := HandlerOptions{}
		WithQueue("test.queue")(&opts)
		assert.Equal(t, "test.queue", opts.Queue)
	})
	
	t.Run("WithDurable sets durable flag", func(t *testing.T) {
		opts := HandlerOptions{}
		WithDurable(false)(&opts)
		assert.False(t, opts.Durable)
	})
	
	t.Run("WithExclusive sets exclusive flag", func(t *testing.T) {
		opts := HandlerOptions{}
		WithExclusive(true)(&opts)
		assert.True(t, opts.Exclusive)
	})
}