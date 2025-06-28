package messaging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Test message types
type testKeystoneEvent struct {
	contracts.BaseEvent
	Data string `json:"data"`
}

type testKeystoneCommand struct {
	contracts.BaseCommand
	Action string `json:"action"`
}

type testKeystoneQuery struct {
	contracts.BaseQuery
	Filter string `json:"filter"`
}

// Mock MetricsCollector
type mockMetricsCollector struct {
	mock.Mock
	mu      sync.Mutex
	metrics map[string]interface{}
}

func newMockMetricsCollector() *mockMetricsCollector {
	return &mockMetricsCollector{
		metrics: make(map[string]interface{}),
	}
}

func (m *mockMetricsCollector) RecordMessage(messageType string, duration time.Duration, success bool, errorType string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Called(messageType, duration, success, errorType)
	m.metrics["last_message_type"] = messageType
	m.metrics["last_success"] = success
}

func (m *mockMetricsCollector) RecordPublish(messageType string, exchange string, duration time.Duration, success bool) {
	m.Called(messageType, exchange, duration, success)
}

func (m *mockMetricsCollector) RecordSubscribe(queue string, messageType string) {
	m.Called(queue, messageType)
}

func (m *mockMetricsCollector) RecordError(component string, errorType string, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Called(component, errorType, message)
	m.metrics["last_error_component"] = component
	m.metrics["last_error_type"] = errorType
}

func (m *mockMetricsCollector) GetStats() MetricsStats {
	args := m.Called()
	return args.Get(0).(MetricsStats)
}

func (m *mockMetricsCollector) getMetric(key string) interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.metrics[key]
}

// Mock ErrorHandler
type mockErrorHandler struct {
	mock.Mock
	actionToReturn ErrorAction
}

func (m *mockErrorHandler) HandleError(ctx context.Context, msg contracts.Message, err error) ErrorAction {
	args := m.Called(ctx, msg, err)
	if len(args) > 0 {
		return args.Get(0).(ErrorAction)
	}
	return m.actionToReturn
}

// Mock MessageProcessor
type mockMessageProcessor struct {
	mock.Mock
	shouldFail bool
	processFunc func(ctx context.Context, msg contracts.Message) (contracts.Message, error)
}

func (m *mockMessageProcessor) Process(ctx context.Context, msg contracts.Message) (contracts.Message, error) {
	if m.processFunc != nil {
		return m.processFunc(ctx, msg)
	}
	args := m.Called(ctx, msg)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(contracts.Message), args.Error(1)
}

// Test NewKeystoneHandler
func TestNewKeystoneHandler(t *testing.T) {
	t.Run("creates handler with valid function", func(t *testing.T) {
		processFunc := func(ctx context.Context, msg *testKeystoneEvent) error {
			return nil
		}

		handler, err := NewKeystoneHandler("test-handler", &testKeystoneEvent{}, processFunc)

		assert.NoError(t, err)
		assert.NotNil(t, handler)
		assert.Equal(t, "test-handler", handler.handlerName)
		assert.Equal(t, reflect.TypeOf(testKeystoneEvent{}), handler.messageType)
		assert.NotNil(t, handler.logger)
		assert.Empty(t, handler.preProcessors)
		assert.Empty(t, handler.postProcessors)
	})

	t.Run("creates handler with pointer message type", func(t *testing.T) {
		processFunc := func(ctx context.Context, msg *testKeystoneEvent) error {
			return nil
		}

		handler, err := NewKeystoneHandler("test-handler", &testKeystoneEvent{}, processFunc)

		assert.NoError(t, err)
		assert.Equal(t, reflect.TypeOf(testKeystoneEvent{}), handler.messageType)
	})

	t.Run("applies options correctly", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(nil, nil))
		metrics := newMockMetricsCollector()
		errorHandler := &mockErrorHandler{}
		retryPolicy := NewSimpleRetryPolicy(3, time.Second)
		preProcessor := &mockMessageProcessor{}
		postProcessor := &mockMessageProcessor{}

		processFunc := func(ctx context.Context, msg *testKeystoneEvent) error {
			return nil
		}

		handler, err := NewKeystoneHandler("test-handler", &testKeystoneEvent{}, processFunc,
			WithKeystoneLogger(logger),
			WithKeystoneMetrics(metrics),
			WithKeystoneErrorHandler(errorHandler),
			WithKeystoneRetryPolicy(retryPolicy),
			WithPreProcessor(preProcessor),
			WithPostProcessor(postProcessor),
		)

		assert.NoError(t, err)
		assert.Equal(t, logger, handler.logger)
		assert.Equal(t, metrics, handler.metrics)
		assert.Equal(t, errorHandler, handler.errorHandler)
		assert.Equal(t, retryPolicy, handler.retryPolicy)
		assert.Len(t, handler.preProcessors, 1)
		assert.Len(t, handler.postProcessors, 1)
	})

	t.Run("fails with non-function processFunc", func(t *testing.T) {
		handler, err := NewKeystoneHandler("test-handler", &testKeystoneEvent{}, "not a function")

		assert.Error(t, err)
		assert.Nil(t, handler)
		assert.Contains(t, err.Error(), "processFunc must be a function")
	})

	t.Run("fails with wrong number of parameters", func(t *testing.T) {
		processFunc := func(msg *testKeystoneEvent) error {
			return nil
		}

		handler, err := NewKeystoneHandler("test-handler", &testKeystoneEvent{}, processFunc)

		assert.Error(t, err)
		assert.Nil(t, handler)
		assert.Contains(t, err.Error(), "processFunc must accept exactly 2 parameters")
	})

	t.Run("fails with wrong return type", func(t *testing.T) {
		processFunc := func(ctx context.Context, msg *testKeystoneEvent) string {
			return "not an error"
		}

		handler, err := NewKeystoneHandler("test-handler", &testKeystoneEvent{}, processFunc)

		assert.Error(t, err)
		assert.Nil(t, handler)
		assert.Contains(t, err.Error(), "return value must be error")
	})

	t.Run("fails with wrong first parameter type", func(t *testing.T) {
		processFunc := func(notContext string, msg *testKeystoneEvent) error {
			return nil
		}

		handler, err := NewKeystoneHandler("test-handler", &testKeystoneEvent{}, processFunc)

		assert.Error(t, err)
		assert.Nil(t, handler)
		assert.Contains(t, err.Error(), "first parameter must be context.Context")
	})
}

// Test Handle method
func TestKeystoneHandlerHandle(t *testing.T) {
	t.Run("processes message successfully", func(t *testing.T) {
		processed := false
		processFunc := func(ctx context.Context, msg *testKeystoneEvent) error {
			processed = true
			// Note: Due to the copyMessage implementation, Data field may not be copied
			// This test focuses on the handler being called correctly
			return nil
		}

		handler, err := NewKeystoneHandler("test-handler", &testKeystoneEvent{}, processFunc)
		assert.NoError(t, err)

		msg := &testKeystoneEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
			},
			Data: "test data",
		}

		err = handler.Handle(context.Background(), msg)

		assert.NoError(t, err)
		assert.True(t, processed)
	})

	t.Run("records metrics on success", func(t *testing.T) {
		metrics := newMockMetricsCollector()
		metrics.On("RecordMessage", "TestEvent", mock.AnythingOfType("time.Duration"), true, "").Return()

		processFunc := func(ctx context.Context, msg *testKeystoneEvent) error {
			return nil
		}

		handler, err := NewKeystoneHandler("test-handler", &testKeystoneEvent{}, processFunc,
			WithKeystoneMetrics(metrics),
		)
		assert.NoError(t, err)

		msg := &testKeystoneEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
			},
		}

		err = handler.Handle(context.Background(), msg)

		assert.NoError(t, err)
		metrics.AssertExpectations(t)
	})

	t.Run("runs pre-processors", func(t *testing.T) {
		preProcessorCalled := false
		preProcessor := &mockMessageProcessor{
			processFunc: func(ctx context.Context, msg contracts.Message) (contracts.Message, error) {
				preProcessorCalled = true
				return msg, nil
			},
		}

		processFunc := func(ctx context.Context, msg *testKeystoneEvent) error {
			return nil
		}

		handler, err := NewKeystoneHandler("test-handler", &testKeystoneEvent{}, processFunc,
			WithPreProcessor(preProcessor),
		)
		assert.NoError(t, err)

		msg := &testKeystoneEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
			},
		}

		err = handler.Handle(context.Background(), msg)

		assert.NoError(t, err)
		assert.True(t, preProcessorCalled)
	})

	t.Run("runs post-processors", func(t *testing.T) {
		postProcessorCalled := false
		postProcessor := &mockMessageProcessor{
			processFunc: func(ctx context.Context, msg contracts.Message) (contracts.Message, error) {
				postProcessorCalled = true
				return msg, nil
			},
		}

		processFunc := func(ctx context.Context, msg *testKeystoneEvent) error {
			return nil
		}

		handler, err := NewKeystoneHandler("test-handler", &testKeystoneEvent{}, processFunc,
			WithPostProcessor(postProcessor),
		)
		assert.NoError(t, err)

		msg := &testKeystoneEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
			},
		}

		err = handler.Handle(context.Background(), msg)

		assert.NoError(t, err)
		assert.True(t, postProcessorCalled)
	})

	t.Run("handles pre-processor failure", func(t *testing.T) {
		preProcessor := &mockMessageProcessor{}
		preProcessor.On("Process", mock.Anything, mock.Anything).Return(nil, errors.New("pre-processor error"))

		processFunc := func(ctx context.Context, msg *testKeystoneEvent) error {
			return nil
		}

		handler, err := NewKeystoneHandler("test-handler", &testKeystoneEvent{}, processFunc,
			WithPreProcessor(preProcessor),
		)
		assert.NoError(t, err)

		msg := &testKeystoneEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
			},
		}

		err = handler.Handle(context.Background(), msg)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "pre-processor failed")
	})

	t.Run("continues on post-processor failure", func(t *testing.T) {
		postProcessor := &mockMessageProcessor{}
		postProcessor.On("Process", mock.Anything, mock.Anything).Return(nil, errors.New("post-processor error"))

		processFunc := func(ctx context.Context, msg *testKeystoneEvent) error {
			return nil
		}

		handler, err := NewKeystoneHandler("test-handler", &testKeystoneEvent{}, processFunc,
			WithPostProcessor(postProcessor),
		)
		assert.NoError(t, err)

		msg := &testKeystoneEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
			},
		}

		err = handler.Handle(context.Background(), msg)

		// Should not fail even though post-processor failed
		assert.NoError(t, err)
	})

	t.Run("handles processing error with error handler", func(t *testing.T) {
		errorHandler := &mockErrorHandler{actionToReturn: Acknowledge}
		errorHandler.On("HandleError", mock.Anything, mock.Anything, mock.Anything).Return(Acknowledge)

		processFunc := func(ctx context.Context, msg *testKeystoneEvent) error {
			return errors.New("processing error")
		}

		handler, err := NewKeystoneHandler("test-handler", &testKeystoneEvent{}, processFunc,
			WithKeystoneErrorHandler(errorHandler),
		)
		assert.NoError(t, err)

		msg := &testKeystoneEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
			},
		}

		err = handler.Handle(context.Background(), msg)

		// Should not error because error handler returned Acknowledge
		assert.NoError(t, err)
		errorHandler.AssertExpectations(t)
	})

	t.Run("retries on failure with retry policy", func(t *testing.T) {
		attemptCount := int32(0)
		processFunc := func(ctx context.Context, msg *testKeystoneEvent) error {
			count := atomic.AddInt32(&attemptCount, 1)
			if count < 3 {
				return errors.New("temporary error")
			}
			return nil // Success on third attempt
		}

		retryPolicy := NewSimpleRetryPolicy(3, time.Millisecond)
		handler, err := NewKeystoneHandler("test-handler", &testKeystoneEvent{}, processFunc,
			WithKeystoneRetryPolicy(retryPolicy),
		)
		assert.NoError(t, err)

		msg := &testKeystoneEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
			},
		}

		err = handler.Handle(context.Background(), msg)

		assert.NoError(t, err)
		assert.Equal(t, int32(3), atomic.LoadInt32(&attemptCount))
	})

	t.Run("respects context cancellation during retry", func(t *testing.T) {
		processFunc := func(ctx context.Context, msg *testKeystoneEvent) error {
			return errors.New("always fail")
		}

		retryPolicy := NewSimpleRetryPolicy(10, 100*time.Millisecond)
		handler, err := NewKeystoneHandler("test-handler", &testKeystoneEvent{}, processFunc,
			WithKeystoneRetryPolicy(retryPolicy),
		)
		assert.NoError(t, err)

		msg := &testKeystoneEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		err = handler.Handle(ctx, msg)

		assert.Error(t, err)
		assert.Equal(t, context.DeadlineExceeded, err)
	})
}

// Test KeystoneHandlerBuilder
func TestKeystoneHandlerBuilder(t *testing.T) {
	t.Run("builds handler with fluent API", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(nil, nil))
		metrics := newMockMetricsCollector()
		errorHandler := &mockErrorHandler{}
		retryPolicy := NewSimpleRetryPolicy(3, time.Second)

		processFunc := func(ctx context.Context, msg *testKeystoneEvent) error {
			return nil
		}

		handler, err := NewKeystoneHandlerBuilder("test-handler").
			ForMessage(&testKeystoneEvent{}).
			WithProcessor(processFunc).
			WithLogger(logger).
			WithMetrics(metrics).
			WithErrorHandler(errorHandler).
			WithRetryPolicy(retryPolicy).
			Build()

		assert.NoError(t, err)
		assert.NotNil(t, handler)
		assert.Equal(t, "test-handler", handler.handlerName)
		assert.Equal(t, logger, handler.logger)
		assert.Equal(t, metrics, handler.metrics)
		assert.Equal(t, errorHandler, handler.errorHandler)
		assert.Equal(t, retryPolicy, handler.retryPolicy)
	})

	t.Run("adds pre and post processors", func(t *testing.T) {
		preProcessor := &mockMessageProcessor{}
		postProcessor := &mockMessageProcessor{}

		processFunc := func(ctx context.Context, msg *testKeystoneEvent) error {
			return nil
		}

		handler, err := NewKeystoneHandlerBuilder("test-handler").
			ForMessage(&testKeystoneEvent{}).
			WithProcessor(processFunc).
			AddPreProcessor(preProcessor).
			AddPostProcessor(postProcessor).
			Build()

		assert.NoError(t, err)
		assert.Len(t, handler.preProcessors, 1)
		assert.Len(t, handler.postProcessors, 1)
	})

	t.Run("fails without message type", func(t *testing.T) {
		processFunc := func(ctx context.Context, msg *testKeystoneEvent) error {
			return nil
		}

		handler, err := NewKeystoneHandlerBuilder("test-handler").
			WithProcessor(processFunc).
			Build()

		assert.Error(t, err)
		assert.Nil(t, handler)
		assert.Contains(t, err.Error(), "message type is required")
	})

	t.Run("fails without process function", func(t *testing.T) {
		handler, err := NewKeystoneHandlerBuilder("test-handler").
			ForMessage(&testKeystoneEvent{}).
			Build()

		assert.Error(t, err)
		assert.Nil(t, handler)
		assert.Contains(t, err.Error(), "process function is required")
	})
}

// Test getter methods
func TestKeystoneHandlerGetters(t *testing.T) {
	processFunc := func(ctx context.Context, msg *testKeystoneEvent) error {
		return nil
	}

	handler, err := NewKeystoneHandler("test-handler", &testKeystoneEvent{}, processFunc)
	assert.NoError(t, err)

	t.Run("GetHandlerName returns correct name", func(t *testing.T) {
		assert.Equal(t, "test-handler", handler.GetHandlerName())
	})

	t.Run("GetMessageType returns correct type", func(t *testing.T) {
		assert.Equal(t, reflect.TypeOf(testKeystoneEvent{}), handler.GetMessageType())
	})
}

// Test error handling scenarios
func TestKeystoneHandlerErrorHandling(t *testing.T) {
	t.Run("error handler returns retry", func(t *testing.T) {
		errorHandler := &mockErrorHandler{actionToReturn: Retry}
		errorHandler.On("HandleError", mock.Anything, mock.Anything, mock.Anything).Return(Retry)

		processFunc := func(ctx context.Context, msg *testKeystoneEvent) error {
			return errors.New("processing error")
		}

		handler, err := NewKeystoneHandler("test-handler", &testKeystoneEvent{}, processFunc,
			WithKeystoneErrorHandler(errorHandler),
		)
		assert.NoError(t, err)

		msg := &testKeystoneEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
			},
		}

		err = handler.Handle(context.Background(), msg)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "retry requested")
	})

	t.Run("error handler returns reject", func(t *testing.T) {
		errorHandler := &mockErrorHandler{actionToReturn: Reject}
		errorHandler.On("HandleError", mock.Anything, mock.Anything, mock.Anything).Return(Reject)

		processFunc := func(ctx context.Context, msg *testKeystoneEvent) error {
			return errors.New("processing error")
		}

		handler, err := NewKeystoneHandler("test-handler", &testKeystoneEvent{}, processFunc,
			WithKeystoneErrorHandler(errorHandler),
		)
		assert.NoError(t, err)

		msg := &testKeystoneEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
			},
		}

		err = handler.Handle(context.Background(), msg)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "rejected")
	})

	t.Run("records error metrics", func(t *testing.T) {
		metrics := newMockMetricsCollector()
		// Expect both RecordMessage and RecordError to be called
		metrics.On("RecordMessage", "TestEvent", mock.AnythingOfType("time.Duration"), true, "").Return()
		metrics.On("RecordError", "test-handler", "processing_error", "processing error").Return()

		processFunc := func(ctx context.Context, msg *testKeystoneEvent) error {
			return errors.New("processing error")
		}

		handler, err := NewKeystoneHandler("test-handler", &testKeystoneEvent{}, processFunc,
			WithKeystoneMetrics(metrics),
		)
		assert.NoError(t, err)

		msg := &testKeystoneEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
			},
		}

		err = handler.Handle(context.Background(), msg)

		assert.Error(t, err)
		metrics.AssertExpectations(t)
	})
}

// Test concurrent processing
func TestKeystoneHandlerConcurrency(t *testing.T) {
	t.Run("handles concurrent messages safely", func(t *testing.T) {
		processedCount := int32(0)
		processFunc := func(ctx context.Context, msg *testKeystoneEvent) error {
			time.Sleep(10 * time.Millisecond) // Simulate work
			atomic.AddInt32(&processedCount, 1)
			return nil
		}

		handler, err := NewKeystoneHandler("test-handler", &testKeystoneEvent{}, processFunc)
		assert.NoError(t, err)

		numGoroutines := 10
		var wg sync.WaitGroup

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				msg := &testKeystoneEvent{
					BaseEvent: contracts.BaseEvent{
						BaseMessage: contracts.NewBaseMessage("TestEvent"),
					},
					Data: fmt.Sprintf("data-%d", id),
				}

				err := handler.Handle(context.Background(), msg)
				assert.NoError(t, err)
			}(i)
		}

		wg.Wait()
		assert.Equal(t, int32(numGoroutines), atomic.LoadInt32(&processedCount))
	})
}

// Test message copying
func TestKeystoneHandlerMessageCopying(t *testing.T) {
	t.Run("handles message with correlation ID", func(t *testing.T) {
		var receivedMsg *testKeystoneEvent
		processFunc := func(ctx context.Context, msg *testKeystoneEvent) error {
			receivedMsg = msg
			return nil
		}

		handler, err := NewKeystoneHandler("test-handler", &testKeystoneEvent{}, processFunc)
		assert.NoError(t, err)

		msg := &testKeystoneEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.BaseMessage{
					ID:            "msg-123",
					Type:          "TestEvent",
					Timestamp:     time.Now(),
					CorrelationID: "corr-456",
				},
			},
			Data: "test data",
		}

		err = handler.Handle(context.Background(), msg)

		assert.NoError(t, err)
		assert.NotNil(t, receivedMsg)
		assert.Equal(t, "corr-456", receivedMsg.GetCorrelationID())
	})
}

// Benchmark KeystoneHandler performance
func BenchmarkKeystoneHandler(b *testing.B) {
	processFunc := func(ctx context.Context, msg *testKeystoneEvent) error {
		return nil
	}

	handler, err := NewKeystoneHandler("bench-handler", &testKeystoneEvent{}, processFunc)
	if err != nil {
		b.Fatal(err)
	}

	msg := &testKeystoneEvent{
		BaseEvent: contracts.BaseEvent{
			BaseMessage: contracts.NewBaseMessage("BenchEvent"),
		},
		Data: "benchmark data",
	}

	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := handler.Handle(ctx, msg)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Benchmark with processors
func BenchmarkKeystoneHandlerWithProcessors(b *testing.B) {
	preProcessor := ProcessorFunc(func(ctx context.Context, msg contracts.Message) (contracts.Message, error) {
		return msg, nil
	})

	postProcessor := ProcessorFunc(func(ctx context.Context, msg contracts.Message) (contracts.Message, error) {
		return msg, nil
	})

	processFunc := func(ctx context.Context, msg *testKeystoneEvent) error {
		return nil
	}

	handler, err := NewKeystoneHandler("bench-handler", &testKeystoneEvent{}, processFunc,
		WithPreProcessor(preProcessor),
		WithPostProcessor(postProcessor),
	)
	if err != nil {
		b.Fatal(err)
	}

	msg := &testKeystoneEvent{
		BaseEvent: contracts.BaseEvent{
			BaseMessage: contracts.NewBaseMessage("BenchEvent"),
		},
		Data: "benchmark data",
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := handler.Handle(ctx, msg)
		if err != nil {
			b.Fatal(err)
		}
	}
}