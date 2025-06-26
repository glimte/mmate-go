package interceptors

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Test message
type testMessage struct {
	contracts.BaseMessage
	Data string `json:"data"`
}

// Mock handler
type mockHandler struct {
	mock.Mock
}

func (m *mockHandler) Handle(ctx context.Context, msg contracts.Message) error {
	args := m.Called(ctx, msg)
	return args.Error(0)
}

// Mock interfaces for testing
type mockMetricsCollector struct {
	mock.Mock
}

func (m *mockMetricsCollector) IncrementMessageCount(messageType string) {
	m.Called(messageType)
}

func (m *mockMetricsCollector) RecordProcessingTime(messageType string, duration time.Duration) {
	m.Called(messageType, duration)
}

func (m *mockMetricsCollector) IncrementErrorCount(messageType string, errorType string) {
	m.Called(messageType, errorType)
}

type mockTracer struct {
	mock.Mock
}

func (m *mockTracer) StartSpan(ctx context.Context, operationName string, msg contracts.Message) (context.Context, Span) {
	args := m.Called(ctx, operationName, msg)
	return args.Get(0).(context.Context), args.Get(1).(Span)
}

type mockSpan struct {
	mock.Mock
}

func (m *mockSpan) SetTag(key string, value interface{}) {
	m.Called(key, value)
}

func (m *mockSpan) SetError(err error) {
	m.Called(err)
}

func (m *mockSpan) Finish() {
	m.Called()
}

func TestInterceptorChain(t *testing.T) {
	t.Run("NewInterceptorChain creates empty chain", func(t *testing.T) {
		logger := slog.Default()
		chain := NewInterceptorChain(logger)
		
		assert.NotNil(t, chain)
		assert.Equal(t, logger, chain.logger)
		assert.Empty(t, chain.interceptors)
	})
	
	t.Run("Add adds interceptor to chain", func(t *testing.T) {
		chain := NewInterceptorChain(nil)
		interceptor := NewLoggingInterceptor(nil)
		
		result := chain.Add(interceptor)
		
		assert.Equal(t, chain, result) // Fluent interface
		assert.Len(t, chain.interceptors, 1)
	})
	
	t.Run("Execute calls final handler when no interceptors", func(t *testing.T) {
		chain := NewInterceptorChain(nil)
		handler := &mockHandler{}
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		
		handler.On("Handle", mock.Anything, msg).Return(nil)
		
		err := chain.Execute(context.Background(), msg, handler)
		
		assert.NoError(t, err)
		handler.AssertExpectations(t)
	})
	
	t.Run("Execute runs interceptors in correct order", func(t *testing.T) {
		var order []string
		
		interceptor1 := NewInterceptorFunc("first", func(ctx context.Context, msg contracts.Message, next MessageHandler) error {
			order = append(order, "first-start")
			err := next.Handle(ctx, msg)
			order = append(order, "first-end")
			return err
		})
		
		interceptor2 := NewInterceptorFunc("second", func(ctx context.Context, msg contracts.Message, next MessageHandler) error {
			order = append(order, "second-start")
			err := next.Handle(ctx, msg)
			order = append(order, "second-end")
			return err
		})
		
		handler := MessageHandlerFunc(func(ctx context.Context, msg contracts.Message) error {
			order = append(order, "handler")
			return nil
		})
		
		chain := NewInterceptorChain(nil).
			Add(interceptor1).
			Add(interceptor2)
		
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		
		err := chain.Execute(context.Background(), msg, handler)
		
		assert.NoError(t, err)
		expected := []string{
			"first-start",
			"second-start",
			"handler",
			"second-end",
			"first-end",
		}
		assert.Equal(t, expected, order)
	})
}

func TestLoggingInterceptor(t *testing.T) {
	t.Run("NewLoggingInterceptor creates interceptor", func(t *testing.T) {
		logger := slog.Default()
		interceptor := NewLoggingInterceptor(logger)
		
		assert.NotNil(t, interceptor)
		assert.Equal(t, logger, interceptor.logger)
		assert.Equal(t, "LoggingInterceptor", interceptor.Name())
	})
	
	t.Run("Intercept logs successful execution", func(t *testing.T) {
		interceptor := NewLoggingInterceptor(slog.Default())
		handler := &mockHandler{}
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		
		handler.On("Handle", mock.Anything, msg).Return(nil)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.NoError(t, err)
		handler.AssertExpectations(t)
	})
	
	t.Run("Intercept logs failed execution", func(t *testing.T) {
		interceptor := NewLoggingInterceptor(slog.Default())
		handler := &mockHandler{}
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		handlerError := errors.New("handler failed")
		
		handler.On("Handle", mock.Anything, msg).Return(handlerError)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.Equal(t, handlerError, err)
		handler.AssertExpectations(t)
	})
}

func TestMetricsInterceptor(t *testing.T) {
	t.Run("NewMetricsInterceptor creates interceptor", func(t *testing.T) {
		collector := &mockMetricsCollector{}
		interceptor := NewMetricsInterceptor(collector)
		
		assert.NotNil(t, interceptor)
		assert.Equal(t, collector, interceptor.collector)
		assert.Equal(t, "MetricsInterceptor", interceptor.Name())
	})
	
	t.Run("Intercept collects metrics on success", func(t *testing.T) {
		collector := &mockMetricsCollector{}
		interceptor := NewMetricsInterceptor(collector)
		handler := &mockHandler{}
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		
		collector.On("IncrementMessageCount", "test").Return()
		collector.On("RecordProcessingTime", "test", mock.AnythingOfType("time.Duration")).Return()
		handler.On("Handle", mock.Anything, msg).Return(nil)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.NoError(t, err)
		collector.AssertExpectations(t)
		handler.AssertExpectations(t)
	})
	
	t.Run("Intercept collects error metrics on failure", func(t *testing.T) {
		collector := &mockMetricsCollector{}
		interceptor := NewMetricsInterceptor(collector)
		handler := &mockHandler{}
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		handlerError := errors.New("handler failed")
		
		collector.On("IncrementMessageCount", "test").Return()
		collector.On("RecordProcessingTime", "test", mock.AnythingOfType("time.Duration")).Return()
		collector.On("IncrementErrorCount", "test", "processing_error").Return()
		handler.On("Handle", mock.Anything, msg).Return(handlerError)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.Equal(t, handlerError, err)
		collector.AssertExpectations(t)
		handler.AssertExpectations(t)
	})
}

func TestTracingInterceptor(t *testing.T) {
	t.Run("NewTracingInterceptor creates interceptor", func(t *testing.T) {
		tracer := &mockTracer{}
		interceptor := NewTracingInterceptor(tracer)
		
		assert.NotNil(t, interceptor)
		assert.Equal(t, tracer, interceptor.tracer)
		assert.Equal(t, "TracingInterceptor", interceptor.Name())
	})
	
	t.Run("Intercept creates span and sets tags", func(t *testing.T) {
		tracer := &mockTracer{}
		span := &mockSpan{}
		interceptor := NewTracingInterceptor(tracer)
		handler := &mockHandler{}
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		
		tracer.On("StartSpan", mock.Anything, "message.process", msg).Return(context.Background(), span)
		span.On("SetTag", "message.id", msg.GetID()).Return()
		span.On("SetTag", "message.type", "test").Return()
		span.On("SetTag", "message.correlationId", "").Return()
		span.On("Finish").Return()
		handler.On("Handle", mock.Anything, msg).Return(nil)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.NoError(t, err)
		tracer.AssertExpectations(t)
		span.AssertExpectations(t)
		handler.AssertExpectations(t)
	})
	
	t.Run("Intercept sets error on span when handler fails", func(t *testing.T) {
		tracer := &mockTracer{}
		span := &mockSpan{}
		interceptor := NewTracingInterceptor(tracer)
		handler := &mockHandler{}
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		handlerError := errors.New("handler failed")
		
		tracer.On("StartSpan", mock.Anything, "message.process", msg).Return(context.Background(), span)
		span.On("SetTag", mock.Anything, mock.Anything).Return()
		span.On("SetError", handlerError).Return()
		span.On("Finish").Return()
		handler.On("Handle", mock.Anything, msg).Return(handlerError)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.Equal(t, handlerError, err)
		tracer.AssertExpectations(t)
		span.AssertExpectations(t)
		handler.AssertExpectations(t)
	})
}

func TestTimeoutInterceptor(t *testing.T) {
	t.Run("NewTimeoutInterceptor creates interceptor", func(t *testing.T) {
		timeout := 30 * time.Second
		interceptor := NewTimeoutInterceptor(timeout)
		
		assert.NotNil(t, interceptor)
		assert.Equal(t, timeout, interceptor.timeout)
		assert.Equal(t, "TimeoutInterceptor", interceptor.Name())
	})
	
	t.Run("Intercept succeeds when handler completes in time", func(t *testing.T) {
		timeout := 100 * time.Millisecond
		interceptor := NewTimeoutInterceptor(timeout)
		handler := &mockHandler{}
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		
		handler.On("Handle", mock.Anything, msg).Return(nil)
		
		start := time.Now()
		err := interceptor.Intercept(context.Background(), msg, handler)
		duration := time.Since(start)
		
		assert.NoError(t, err)
		assert.Less(t, duration, timeout)
		handler.AssertExpectations(t)
	})
	
	t.Run("Intercept times out when handler takes too long", func(t *testing.T) {
		timeout := 10 * time.Millisecond
		interceptor := NewTimeoutInterceptor(timeout)
		
		slowHandler := MessageHandlerFunc(func(ctx context.Context, msg contracts.Message) error {
			time.Sleep(50 * time.Millisecond)
			return nil
		})
		
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		
		err := interceptor.Intercept(context.Background(), msg, slowHandler)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "timeout")
	})
}

func TestDefaultInterceptorChainBuilder(t *testing.T) {
	t.Run("NewDefaultInterceptorChainBuilder creates builder", func(t *testing.T) {
		logger := slog.Default()
		builder := NewDefaultInterceptorChainBuilder(logger)
		
		assert.NotNil(t, builder)
		assert.Equal(t, logger, builder.logger)
		assert.NotNil(t, builder.chain)
	})
	
	t.Run("Builder methods add interceptors", func(t *testing.T) {
		collector := &mockMetricsCollector{}
		builder := NewDefaultInterceptorChainBuilder(nil).
			WithLogging().
			WithMetrics(collector).
			WithTimeout(30 * time.Second)
		
		chain := builder.Build()
		
		assert.Len(t, chain.interceptors, 3)
		assert.Equal(t, "LoggingInterceptor", chain.interceptors[0].Name())
		assert.Equal(t, "MetricsInterceptor", chain.interceptors[1].Name())
		assert.Equal(t, "TimeoutInterceptor", chain.interceptors[2].Name())
	})
	
	t.Run("WithCustom adds custom interceptor", func(t *testing.T) {
		customInterceptor := NewInterceptorFunc("custom", func(ctx context.Context, msg contracts.Message, next MessageHandler) error {
			return next.Handle(ctx, msg)
		})
		
		builder := NewDefaultInterceptorChainBuilder(nil).
			WithCustom(customInterceptor)
		
		chain := builder.Build()
		
		assert.Len(t, chain.interceptors, 1)
		assert.Equal(t, "custom", chain.interceptors[0].Name())
	})
}