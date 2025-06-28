package messaging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/glimte/mmate-go/contracts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Mock acknowledger
type mockAcknowledger struct {
	mock.Mock
}

func (m *mockAcknowledger) Ack() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockAcknowledger) Nack(requeue bool) error {
	args := m.Called(requeue)
	return args.Error(0)
}

func (m *mockAcknowledger) Reject(requeue bool) error {
	args := m.Called(requeue)
	return args.Error(0)
}

// Mock message handler for auto ack tests
type mockAutoAckHandler struct {
	mock.Mock
}

func (m *mockAutoAckHandler) Handle(ctx context.Context, msg contracts.Message) error {
	args := m.Called(ctx, msg)
	return args.Error(0)
}

// Test message
type ackTestMessage struct {
	contracts.BaseMessage
	Data string `json:"data"`
}

// Custom logger that captures logs for testing
type testLogHandler struct {
	logs []map[string]interface{}
}

func (h *testLogHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *testLogHandler) Handle(ctx context.Context, r slog.Record) error {
	log := make(map[string]interface{})
	log["level"] = r.Level
	log["msg"] = r.Message
	
	r.Attrs(func(a slog.Attr) bool {
		log[a.Key] = a.Value.Any()
		return true
	})
	
	h.logs = append(h.logs, log)
	return nil
}

func (h *testLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *testLogHandler) WithGroup(name string) slog.Handler {
	return h
}

func TestAcknowledgmentStrategy(t *testing.T) {
	t.Run("strategy constants have correct values", func(t *testing.T) {
		assert.Equal(t, AcknowledgmentStrategy(0), AckOnSuccess)
		assert.Equal(t, AcknowledgmentStrategy(1), AckAlways)
		assert.Equal(t, AcknowledgmentStrategy(2), AckManual)
	})
}

func TestNewAutoAcknowledgingHandler(t *testing.T) {
	t.Run("creates handler with default options", func(t *testing.T) {
		innerHandler := &mockAutoAckHandler{}
		handler := NewAutoAcknowledgingHandler(innerHandler)
		
		assert.NotNil(t, handler)
		assert.Equal(t, innerHandler, handler.handler)
		assert.Equal(t, AckOnSuccess, handler.strategy)
		assert.NotNil(t, handler.logger)
		assert.NotNil(t, handler.onError)
		
		// Test default error handler returns Retry
		action := handler.onError(context.Background(), &ackTestMessage{}, errors.New("test"))
		assert.Equal(t, Retry, action)
	})
	
	t.Run("applies options correctly", func(t *testing.T) {
		innerHandler := &mockAutoAckHandler{}
		logHandler := &testLogHandler{}
		logger := slog.New(logHandler)
		
		customErrorHandler := func(ctx context.Context, msg contracts.Message, err error) ErrorAction {
			return Reject
		}
		
		handler := NewAutoAcknowledgingHandler(
			innerHandler,
			WithAckStrategy(AckAlways),
			WithAckLogger(logger),
			WithAckErrorHandler(customErrorHandler),
		)
		
		assert.Equal(t, AckAlways, handler.strategy)
		assert.Equal(t, logger, handler.logger)
		
		// Test custom error handler
		action := handler.onError(context.Background(), &ackTestMessage{}, errors.New("test"))
		assert.Equal(t, Reject, action)
	})
}

func TestAutoAcknowledgingHandler_Handle(t *testing.T) {
	t.Run("handles message without acknowledger in context", func(t *testing.T) {
		innerHandler := &mockAutoAckHandler{}
		handler := NewAutoAcknowledgingHandler(innerHandler)
		
		ctx := context.Background()
		msg := &ackTestMessage{
			BaseMessage: contracts.NewBaseMessage("TestMessage"),
			Data:        "test data",
		}
		
		innerHandler.On("Handle", ctx, msg).Return(nil)
		
		err := handler.Handle(ctx, msg)
		
		assert.NoError(t, err)
		innerHandler.AssertExpectations(t)
	})
	
	t.Run("AckAlways strategy acknowledges regardless of result", func(t *testing.T) {
		t.Run("acknowledges on success", func(t *testing.T) {
			innerHandler := &mockAutoAckHandler{}
			ack := &mockAcknowledger{}
			handler := NewAutoAcknowledgingHandler(innerHandler, WithAckStrategy(AckAlways))
			
			ctx := WithAcknowledger(context.Background(), ack)
			msg := &ackTestMessage{
				BaseMessage: contracts.NewBaseMessage("TestMessage"),
			}
			
			innerHandler.On("Handle", ctx, msg).Return(nil)
			ack.On("Ack").Return(nil)
			
			err := handler.Handle(ctx, msg)
			
			assert.NoError(t, err)
			innerHandler.AssertExpectations(t)
			ack.AssertExpectations(t)
		})
		
		t.Run("acknowledges on error", func(t *testing.T) {
			innerHandler := &mockAutoAckHandler{}
			ack := &mockAcknowledger{}
			handler := NewAutoAcknowledgingHandler(innerHandler, WithAckStrategy(AckAlways))
			
			ctx := WithAcknowledger(context.Background(), ack)
			msg := &ackTestMessage{
				BaseMessage: contracts.NewBaseMessage("TestMessage"),
			}
			
			handlerErr := errors.New("processing error")
			innerHandler.On("Handle", ctx, msg).Return(handlerErr)
			ack.On("Ack").Return(nil)
			
			err := handler.Handle(ctx, msg)
			
			assert.Equal(t, handlerErr, err)
			innerHandler.AssertExpectations(t)
			ack.AssertExpectations(t)
		})
		
		t.Run("logs acknowledgment error", func(t *testing.T) {
			innerHandler := &mockAutoAckHandler{}
			ack := &mockAcknowledger{}
			logHandler := &testLogHandler{}
			logger := slog.New(logHandler)
			handler := NewAutoAcknowledgingHandler(
				innerHandler,
				WithAckStrategy(AckAlways),
				WithAckLogger(logger),
			)
			
			ctx := WithAcknowledger(context.Background(), ack)
			msg := &ackTestMessage{
				BaseMessage: contracts.NewBaseMessage("TestMessage"),
			}
			msg.ID = "msg-123"
			
			innerHandler.On("Handle", ctx, msg).Return(nil)
			ack.On("Ack").Return(errors.New("ack error"))
			
			err := handler.Handle(ctx, msg)
			
			assert.NoError(t, err) // Original error is returned
			assert.Len(t, logHandler.logs, 1)
			assert.Equal(t, "failed to acknowledge message", logHandler.logs[0]["msg"])
			assert.Equal(t, "msg-123", logHandler.logs[0]["messageId"])
		})
	})
	
	t.Run("AckOnSuccess strategy", func(t *testing.T) {
		t.Run("acknowledges on success", func(t *testing.T) {
			innerHandler := &mockAutoAckHandler{}
			ack := &mockAcknowledger{}
			handler := NewAutoAcknowledgingHandler(innerHandler, WithAckStrategy(AckOnSuccess))
			
			ctx := WithAcknowledger(context.Background(), ack)
			msg := &ackTestMessage{
				BaseMessage: contracts.NewBaseMessage("TestMessage"),
			}
			
			innerHandler.On("Handle", ctx, msg).Return(nil)
			ack.On("Ack").Return(nil)
			
			err := handler.Handle(ctx, msg)
			
			assert.NoError(t, err)
			innerHandler.AssertExpectations(t)
			ack.AssertExpectations(t)
		})
		
		t.Run("returns error if acknowledgment fails", func(t *testing.T) {
			innerHandler := &mockAutoAckHandler{}
			ack := &mockAcknowledger{}
			handler := NewAutoAcknowledgingHandler(innerHandler, WithAckStrategy(AckOnSuccess))
			
			ctx := WithAcknowledger(context.Background(), ack)
			msg := &ackTestMessage{
				BaseMessage: contracts.NewBaseMessage("TestMessage"),
			}
			
			innerHandler.On("Handle", ctx, msg).Return(nil)
			ack.On("Ack").Return(errors.New("ack error"))
			
			err := handler.Handle(ctx, msg)
			
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "failed to acknowledge")
			innerHandler.AssertExpectations(t)
			ack.AssertExpectations(t)
		})
		
		t.Run("handles error with retry action", func(t *testing.T) {
			innerHandler := &mockAutoAckHandler{}
			ack := &mockAcknowledger{}
			handler := NewAutoAcknowledgingHandler(
				innerHandler,
				WithAckStrategy(AckOnSuccess),
				WithAckErrorHandler(func(ctx context.Context, msg contracts.Message, err error) ErrorAction {
					return Retry
				}),
			)
			
			ctx := WithAcknowledger(context.Background(), ack)
			msg := &ackTestMessage{
				BaseMessage: contracts.NewBaseMessage("TestMessage"),
			}
			
			handlerErr := errors.New("processing error")
			innerHandler.On("Handle", ctx, msg).Return(handlerErr)
			ack.On("Nack", true).Return(nil)
			
			err := handler.Handle(ctx, msg)
			
			assert.Equal(t, handlerErr, err)
			innerHandler.AssertExpectations(t)
			ack.AssertExpectations(t)
		})
		
		t.Run("handles error with reject action", func(t *testing.T) {
			innerHandler := &mockAutoAckHandler{}
			ack := &mockAcknowledger{}
			handler := NewAutoAcknowledgingHandler(
				innerHandler,
				WithAckStrategy(AckOnSuccess),
				WithAckErrorHandler(func(ctx context.Context, msg contracts.Message, err error) ErrorAction {
					return Reject
				}),
			)
			
			ctx := WithAcknowledger(context.Background(), ack)
			msg := &ackTestMessage{
				BaseMessage: contracts.NewBaseMessage("TestMessage"),
			}
			
			handlerErr := errors.New("processing error")
			innerHandler.On("Handle", ctx, msg).Return(handlerErr)
			ack.On("Reject", false).Return(nil)
			
			err := handler.Handle(ctx, msg)
			
			assert.Equal(t, handlerErr, err)
			innerHandler.AssertExpectations(t)
			ack.AssertExpectations(t)
		})
		
		t.Run("handles error with acknowledge action", func(t *testing.T) {
			innerHandler := &mockAutoAckHandler{}
			ack := &mockAcknowledger{}
			handler := NewAutoAcknowledgingHandler(
				innerHandler,
				WithAckStrategy(AckOnSuccess),
				WithAckErrorHandler(func(ctx context.Context, msg contracts.Message, err error) ErrorAction {
					return Acknowledge
				}),
			)
			
			ctx := WithAcknowledger(context.Background(), ack)
			msg := &ackTestMessage{
				BaseMessage: contracts.NewBaseMessage("TestMessage"),
			}
			
			handlerErr := errors.New("processing error")
			innerHandler.On("Handle", ctx, msg).Return(handlerErr)
			ack.On("Ack").Return(nil)
			
			err := handler.Handle(ctx, msg)
			
			assert.Equal(t, handlerErr, err)
			innerHandler.AssertExpectations(t)
			ack.AssertExpectations(t)
		})
		
		t.Run("logs nack error", func(t *testing.T) {
			innerHandler := &mockAutoAckHandler{}
			ack := &mockAcknowledger{}
			logHandler := &testLogHandler{}
			logger := slog.New(logHandler)
			handler := NewAutoAcknowledgingHandler(
				innerHandler,
				WithAckStrategy(AckOnSuccess),
				WithAckLogger(logger),
			)
			
			ctx := WithAcknowledger(context.Background(), ack)
			msg := &ackTestMessage{
				BaseMessage: contracts.NewBaseMessage("TestMessage"),
			}
			msg.ID = "msg-123"
			
			handlerErr := errors.New("processing error")
			innerHandler.On("Handle", ctx, msg).Return(handlerErr)
			ack.On("Nack", true).Return(errors.New("nack error"))
			
			err := handler.Handle(ctx, msg)
			
			assert.Equal(t, handlerErr, err)
			assert.Len(t, logHandler.logs, 1)
			assert.Equal(t, "failed to nack message", logHandler.logs[0]["msg"])
			assert.Equal(t, "msg-123", logHandler.logs[0]["messageId"])
		})
		
		t.Run("logs reject error", func(t *testing.T) {
			innerHandler := &mockAutoAckHandler{}
			ack := &mockAcknowledger{}
			logHandler := &testLogHandler{}
			logger := slog.New(logHandler)
			handler := NewAutoAcknowledgingHandler(
				innerHandler,
				WithAckStrategy(AckOnSuccess),
				WithAckLogger(logger),
				WithAckErrorHandler(func(ctx context.Context, msg contracts.Message, err error) ErrorAction {
					return Reject
				}),
			)
			
			ctx := WithAcknowledger(context.Background(), ack)
			msg := &ackTestMessage{
				BaseMessage: contracts.NewBaseMessage("TestMessage"),
			}
			msg.ID = "msg-123"
			
			handlerErr := errors.New("processing error")
			innerHandler.On("Handle", ctx, msg).Return(handlerErr)
			ack.On("Reject", false).Return(errors.New("reject error"))
			
			err := handler.Handle(ctx, msg)
			
			assert.Equal(t, handlerErr, err)
			assert.Len(t, logHandler.logs, 1)
			assert.Equal(t, "failed to reject message", logHandler.logs[0]["msg"])
			assert.Equal(t, "msg-123", logHandler.logs[0]["messageId"])
		})
	})
	
	t.Run("AckManual strategy", func(t *testing.T) {
		t.Run("does not acknowledge on success", func(t *testing.T) {
			innerHandler := &mockAutoAckHandler{}
			ack := &mockAcknowledger{}
			handler := NewAutoAcknowledgingHandler(innerHandler, WithAckStrategy(AckManual))
			
			ctx := WithAcknowledger(context.Background(), ack)
			msg := &ackTestMessage{
				BaseMessage: contracts.NewBaseMessage("TestMessage"),
			}
			
			innerHandler.On("Handle", ctx, msg).Return(nil)
			// No acknowledgment methods should be called
			
			err := handler.Handle(ctx, msg)
			
			assert.NoError(t, err)
			innerHandler.AssertExpectations(t)
			ack.AssertNotCalled(t, "Ack")
			ack.AssertNotCalled(t, "Nack", mock.Anything)
			ack.AssertNotCalled(t, "Reject", mock.Anything)
		})
		
		t.Run("does not acknowledge on error", func(t *testing.T) {
			innerHandler := &mockAutoAckHandler{}
			ack := &mockAcknowledger{}
			handler := NewAutoAcknowledgingHandler(innerHandler, WithAckStrategy(AckManual))
			
			ctx := WithAcknowledger(context.Background(), ack)
			msg := &ackTestMessage{
				BaseMessage: contracts.NewBaseMessage("TestMessage"),
			}
			
			handlerErr := errors.New("processing error")
			innerHandler.On("Handle", ctx, msg).Return(handlerErr)
			// No acknowledgment methods should be called
			
			err := handler.Handle(ctx, msg)
			
			assert.Equal(t, handlerErr, err)
			innerHandler.AssertExpectations(t)
			ack.AssertNotCalled(t, "Ack")
			ack.AssertNotCalled(t, "Nack", mock.Anything)
			ack.AssertNotCalled(t, "Reject", mock.Anything)
		})
	})
	
	t.Run("unknown strategy returns error", func(t *testing.T) {
		innerHandler := &mockAutoAckHandler{}
		ack := &mockAcknowledger{}
		handler := NewAutoAcknowledgingHandler(innerHandler)
		handler.strategy = AcknowledgmentStrategy(999) // Invalid strategy
		
		ctx := WithAcknowledger(context.Background(), ack)
		msg := &ackTestMessage{
			BaseMessage: contracts.NewBaseMessage("TestMessage"),
		}
		
		innerHandler.On("Handle", ctx, msg).Return(nil)
		
		err := handler.Handle(ctx, msg)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown acknowledgment strategy")
	})
}

func TestWithAcknowledger(t *testing.T) {
	t.Run("adds acknowledger to context", func(t *testing.T) {
		ctx := context.Background()
		ack := &mockAcknowledger{}
		
		newCtx := WithAcknowledger(ctx, ack)
		
		retrieved, ok := GetAcknowledger(newCtx)
		assert.True(t, ok)
		assert.Equal(t, ack, retrieved)
	})
	
	t.Run("returns false when no acknowledger in context", func(t *testing.T) {
		ctx := context.Background()
		
		retrieved, ok := GetAcknowledger(ctx)
		assert.False(t, ok)
		assert.Nil(t, retrieved)
	})
	
	t.Run("overwrites existing acknowledger", func(t *testing.T) {
		ctx := context.Background()
		ack1 := &mockAcknowledger{}
		ack2 := &mockAcknowledger{}
		
		ctx = WithAcknowledger(ctx, ack1)
		ctx = WithAcknowledger(ctx, ack2)
		
		retrieved, ok := GetAcknowledger(ctx)
		assert.True(t, ok)
		assert.Equal(t, ack2, retrieved)
	})
}

func TestMessageContext(t *testing.T) {
	t.Run("creates message context", func(t *testing.T) {
		ctx := context.Background()
		msg := &ackTestMessage{
			BaseMessage: contracts.NewBaseMessage("TestMessage"),
			Data:        "test data",
		}
		ack := &mockAcknowledger{}
		
		msgCtx := NewMessageContext(ctx, msg, ack)
		
		assert.NotNil(t, msgCtx)
		assert.Equal(t, ctx, msgCtx.Context)
		assert.Equal(t, msg, msgCtx.Message)
		assert.Equal(t, ack, msgCtx.Acknowledger)
	})
	
	t.Run("ack calls acknowledger", func(t *testing.T) {
		ctx := context.Background()
		msg := &ackTestMessage{}
		ack := &mockAcknowledger{}
		
		msgCtx := NewMessageContext(ctx, msg, ack)
		
		ack.On("Ack").Return(nil)
		
		err := msgCtx.Ack()
		
		assert.NoError(t, err)
		ack.AssertExpectations(t)
	})
	
	t.Run("ack returns error when acknowledger is nil", func(t *testing.T) {
		ctx := context.Background()
		msg := &ackTestMessage{}
		
		msgCtx := NewMessageContext(ctx, msg, nil)
		
		err := msgCtx.Ack()
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no acknowledger available")
	})
	
	t.Run("nack calls acknowledger", func(t *testing.T) {
		ctx := context.Background()
		msg := &ackTestMessage{}
		ack := &mockAcknowledger{}
		
		msgCtx := NewMessageContext(ctx, msg, ack)
		
		ack.On("Nack", true).Return(nil)
		
		err := msgCtx.Nack(true)
		
		assert.NoError(t, err)
		ack.AssertExpectations(t)
	})
	
	t.Run("nack returns error when acknowledger is nil", func(t *testing.T) {
		ctx := context.Background()
		msg := &ackTestMessage{}
		
		msgCtx := NewMessageContext(ctx, msg, nil)
		
		err := msgCtx.Nack(true)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no acknowledger available")
	})
	
	t.Run("reject calls acknowledger", func(t *testing.T) {
		ctx := context.Background()
		msg := &ackTestMessage{}
		ack := &mockAcknowledger{}
		
		msgCtx := NewMessageContext(ctx, msg, ack)
		
		ack.On("Reject", false).Return(nil)
		
		err := msgCtx.Reject(false)
		
		assert.NoError(t, err)
		ack.AssertExpectations(t)
	})
	
	t.Run("reject returns error when acknowledger is nil", func(t *testing.T) {
		ctx := context.Background()
		msg := &ackTestMessage{}
		
		msgCtx := NewMessageContext(ctx, msg, nil)
		
		err := msgCtx.Reject(false)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no acknowledger available")
	})
	
	t.Run("propagates acknowledger errors", func(t *testing.T) {
		ctx := context.Background()
		msg := &ackTestMessage{}
		ack := &mockAcknowledger{}
		
		msgCtx := NewMessageContext(ctx, msg, ack)
		
		expectedErr := errors.New("acknowledger error")
		ack.On("Ack").Return(expectedErr)
		ack.On("Nack", true).Return(expectedErr)
		ack.On("Reject", false).Return(expectedErr)
		
		err := msgCtx.Ack()
		assert.Equal(t, expectedErr, err)
		
		err = msgCtx.Nack(true)
		assert.Equal(t, expectedErr, err)
		
		err = msgCtx.Reject(false)
		assert.Equal(t, expectedErr, err)
		
		ack.AssertExpectations(t)
	})
}

func TestComplexScenarios(t *testing.T) {
	t.Run("handles custom error handler with message inspection", func(t *testing.T) {
		innerHandler := &mockAutoAckHandler{}
		ack := &mockAcknowledger{}
		
		// Error handler that decides action based on message content
		errorHandler := func(ctx context.Context, msg contracts.Message, err error) ErrorAction {
			if testMsg, ok := msg.(*ackTestMessage); ok {
				if testMsg.Data == "retry" {
					return Retry
				} else if testMsg.Data == "reject" {
					return Reject
				}
			}
			return Acknowledge
		}
		
		handler := NewAutoAcknowledgingHandler(
			innerHandler,
			WithAckStrategy(AckOnSuccess),
			WithAckErrorHandler(errorHandler),
		)
		
		ctx := WithAcknowledger(context.Background(), ack)
		
		// Test retry case
		retryMsg := &ackTestMessage{
			BaseMessage: contracts.NewBaseMessage("TestMessage"),
			Data:        "retry",
		}
		
		handlerErr := errors.New("processing error")
		innerHandler.On("Handle", ctx, retryMsg).Return(handlerErr)
		ack.On("Nack", true).Return(nil)
		
		err := handler.Handle(ctx, retryMsg)
		assert.Equal(t, handlerErr, err)
		
		// Test reject case
		rejectMsg := &ackTestMessage{
			BaseMessage: contracts.NewBaseMessage("TestMessage"),
			Data:        "reject",
		}
		
		innerHandler.On("Handle", ctx, rejectMsg).Return(handlerErr)
		ack.On("Reject", false).Return(nil)
		
		err = handler.Handle(ctx, rejectMsg)
		assert.Equal(t, handlerErr, err)
		
		// Test acknowledge case
		ackMsg := &ackTestMessage{
			BaseMessage: contracts.NewBaseMessage("TestMessage"),
			Data:        "acknowledge",
		}
		
		innerHandler.On("Handle", ctx, ackMsg).Return(handlerErr)
		ack.On("Ack").Return(nil)
		
		err = handler.Handle(ctx, ackMsg)
		assert.Equal(t, handlerErr, err)
		
		innerHandler.AssertExpectations(t)
		ack.AssertExpectations(t)
	})
	
	t.Run("handles chain of handlers", func(t *testing.T) {
		// Create a chain: AutoAck -> Inner -> Actual
		actualHandler := &mockAutoAckHandler{}
		innerAutoAck := NewAutoAcknowledgingHandler(actualHandler, WithAckStrategy(AckManual))
		outerAutoAck := NewAutoAcknowledgingHandler(innerAutoAck, WithAckStrategy(AckOnSuccess))
		
		ctx := context.Background()
		ack := &mockAcknowledger{}
		ctx = WithAcknowledger(ctx, ack)
		
		msg := &ackTestMessage{
			BaseMessage: contracts.NewBaseMessage("TestMessage"),
		}
		
		actualHandler.On("Handle", ctx, msg).Return(nil)
		ack.On("Ack").Return(nil)
		
		err := outerAutoAck.Handle(ctx, msg)
		
		assert.NoError(t, err)
		actualHandler.AssertExpectations(t)
		ack.AssertExpectations(t)
	})
}

func TestEdgeCases(t *testing.T) {
	t.Run("handles nil context value", func(t *testing.T) {
		innerHandler := &mockAutoAckHandler{}
		handler := NewAutoAcknowledgingHandler(innerHandler)
		
		// Create context with nil value
		ctx := context.WithValue(context.Background(), acknowledgerKey{}, nil)
		msg := &ackTestMessage{
			BaseMessage: contracts.NewBaseMessage("TestMessage"),
		}
		
		innerHandler.On("Handle", ctx, msg).Return(nil)
		
		err := handler.Handle(ctx, msg)
		
		assert.NoError(t, err)
		innerHandler.AssertExpectations(t)
	})
	
	t.Run("handles wrong type in context", func(t *testing.T) {
		innerHandler := &mockAutoAckHandler{}
		handler := NewAutoAcknowledgingHandler(innerHandler)
		
		// Create context with wrong type
		ctx := context.WithValue(context.Background(), acknowledgerKey{}, "not an acknowledger")
		msg := &ackTestMessage{
			BaseMessage: contracts.NewBaseMessage("TestMessage"),
		}
		
		innerHandler.On("Handle", ctx, msg).Return(nil)
		
		err := handler.Handle(ctx, msg)
		
		assert.NoError(t, err)
		innerHandler.AssertExpectations(t)
	})
	
	t.Run("handles panic in inner handler", func(t *testing.T) {
		innerHandler := &mockAutoAckHandler{}
		ack := &mockAcknowledger{}
		handler := NewAutoAcknowledgingHandler(innerHandler, WithAckStrategy(AckOnSuccess))
		
		ctx := WithAcknowledger(context.Background(), ack)
		msg := &ackTestMessage{
			BaseMessage: contracts.NewBaseMessage("TestMessage"),
		}
		
		innerHandler.On("Handle", ctx, msg).Run(func(args mock.Arguments) {
			panic("handler panic")
		}).Maybe()
		
		assert.Panics(t, func() {
			handler.Handle(ctx, msg)
		})
	})
	
	t.Run("handles panic in error handler", func(t *testing.T) {
		innerHandler := &mockAutoAckHandler{}
		ack := &mockAcknowledger{}
		
		panicErrorHandler := func(ctx context.Context, msg contracts.Message, err error) ErrorAction {
			panic("error handler panic")
		}
		
		handler := NewAutoAcknowledgingHandler(
			innerHandler,
			WithAckStrategy(AckOnSuccess),
			WithAckErrorHandler(panicErrorHandler),
		)
		
		ctx := WithAcknowledger(context.Background(), ack)
		msg := &ackTestMessage{
			BaseMessage: contracts.NewBaseMessage("TestMessage"),
		}
		
		innerHandler.On("Handle", ctx, msg).Return(errors.New("test error"))
		
		assert.Panics(t, func() {
			handler.Handle(ctx, msg)
		})
	})
}

func TestConcurrentUsage(t *testing.T) {
	t.Run("handles concurrent message processing", func(t *testing.T) {
		innerHandler := &mockAutoAckHandler{}
		handler := NewAutoAcknowledgingHandler(innerHandler, WithAckStrategy(AckOnSuccess))
		
		// Set up expectations for concurrent calls
		innerHandler.On("Handle", mock.Anything, mock.Anything).Return(nil).Times(100)
		
		done := make(chan bool, 100)
		
		for i := 0; i < 100; i++ {
			go func(idx int) {
				ctx := context.Background()
				ack := &mockAcknowledger{}
				ctx = WithAcknowledger(ctx, ack)
				
				msg := &ackTestMessage{
					BaseMessage: contracts.NewBaseMessage("TestMessage"),
					Data:        fmt.Sprintf("msg-%d", idx),
				}
				
				ack.On("Ack").Return(nil).Once()
				
				err := handler.Handle(ctx, msg)
				assert.NoError(t, err)
				
				done <- true
			}(i)
		}
		
		// Wait for all goroutines
		for i := 0; i < 100; i++ {
			<-done
		}
		
		innerHandler.AssertExpectations(t)
	})
}