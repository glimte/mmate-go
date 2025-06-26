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

// Additional tests to improve coverage

func TestInterceptorFunc(t *testing.T) {
	t.Run("NewInterceptorFunc creates interceptor", func(t *testing.T) {
		interceptor := NewInterceptorFunc("test", func(ctx context.Context, msg contracts.Message, next MessageHandler) error {
			return next.Handle(ctx, msg)
		})
		
		assert.NotNil(t, interceptor)
		assert.Equal(t, "test", interceptor.Name())
	})
	
	t.Run("InterceptorFunc executes function", func(t *testing.T) {
		called := false
		interceptor := NewInterceptorFunc("test", func(ctx context.Context, msg contracts.Message, next MessageHandler) error {
			called = true
			return next.Handle(ctx, msg)
		})
		
		handler := &mockHandler{}
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		
		handler.On("Handle", mock.Anything, msg).Return(nil)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.NoError(t, err)
		assert.True(t, called)
		handler.AssertExpectations(t)
	})
}

func TestValidationInterceptor(t *testing.T) {
	t.Run("NewValidationInterceptor creates interceptor", func(t *testing.T) {
		validator := &mockValidator{}
		interceptor := NewValidationInterceptor(validator)
		
		assert.NotNil(t, interceptor)
		assert.Equal(t, validator, interceptor.validator)
		assert.Equal(t, "ValidationInterceptor", interceptor.Name())
	})
	
	t.Run("Intercept succeeds when validation passes", func(t *testing.T) {
		validator := &mockValidator{}
		interceptor := NewValidationInterceptor(validator)
		handler := &mockHandler{}
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		
		validator.On("Validate", mock.Anything, msg).Return(nil)
		handler.On("Handle", mock.Anything, msg).Return(nil)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.NoError(t, err)
		validator.AssertExpectations(t)
		handler.AssertExpectations(t)
	})
	
	t.Run("Intercept fails when validation fails", func(t *testing.T) {
		validator := &mockValidator{}
		interceptor := NewValidationInterceptor(validator)
		handler := &mockHandler{}
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		
		validationError := errors.New("validation failed")
		validator.On("Validate", mock.Anything, msg).Return(validationError)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "message validation failed")
		validator.AssertExpectations(t)
		handler.AssertNotCalled(t, "Handle")
	})
}

func TestAuthenticationInterceptor(t *testing.T) {
	t.Run("NewAuthenticationInterceptor creates interceptor", func(t *testing.T) {
		authenticator := &mockAuthenticator{}
		interceptor := NewAuthenticationInterceptor(authenticator)
		
		assert.NotNil(t, interceptor)
		assert.Equal(t, authenticator, interceptor.authenticator)
		assert.Equal(t, "AuthenticationInterceptor", interceptor.Name())
	})
	
	t.Run("Intercept succeeds when authentication passes", func(t *testing.T) {
		authenticator := &mockAuthenticator{}
		interceptor := NewAuthenticationInterceptor(authenticator)
		handler := &mockHandler{}
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		
		authenticator.On("Authenticate", mock.Anything, msg).Return(nil)
		handler.On("Handle", mock.Anything, msg).Return(nil)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.NoError(t, err)
		authenticator.AssertExpectations(t)
		handler.AssertExpectations(t)
	})
	
	t.Run("Intercept fails when authentication fails", func(t *testing.T) {
		authenticator := &mockAuthenticator{}
		interceptor := NewAuthenticationInterceptor(authenticator)
		handler := &mockHandler{}
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		
		authError := errors.New("authentication failed")
		authenticator.On("Authenticate", mock.Anything, msg).Return(authError)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "message authentication failed")
		authenticator.AssertExpectations(t)
		handler.AssertNotCalled(t, "Handle")
	})
}

func TestRateLimitingInterceptor(t *testing.T) {
	t.Run("NewRateLimitingInterceptor creates interceptor", func(t *testing.T) {
		limiter := &mockRateLimiter{}
		interceptor := NewRateLimitingInterceptor(limiter)
		
		assert.NotNil(t, interceptor)
		assert.Equal(t, limiter, interceptor.limiter)
		assert.Equal(t, "RateLimitingInterceptor", interceptor.Name())
	})
	
	t.Run("Intercept succeeds when rate limit allows", func(t *testing.T) {
		limiter := &mockRateLimiter{}
		interceptor := NewRateLimitingInterceptor(limiter)
		handler := &mockHandler{}
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		
		limiter.On("Allow", mock.Anything, "test").Return(nil)
		handler.On("Handle", mock.Anything, msg).Return(nil)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.NoError(t, err)
		limiter.AssertExpectations(t)
		handler.AssertExpectations(t)
	})
	
	t.Run("Intercept fails when rate limit exceeded", func(t *testing.T) {
		limiter := &mockRateLimiter{}
		interceptor := NewRateLimitingInterceptor(limiter)
		handler := &mockHandler{}
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		
		rateLimitError := errors.New("rate limit exceeded")
		limiter.On("Allow", mock.Anything, "test").Return(rateLimitError)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "rate limit exceeded")
		limiter.AssertExpectations(t)
		handler.AssertNotCalled(t, "Handle")
	})
}

func TestErrorHandlingInterceptor(t *testing.T) {
	t.Run("NewErrorHandlingInterceptor creates interceptor", func(t *testing.T) {
		errorHandler := &mockErrorHandler{}
		logger := slog.Default()
		interceptor := NewErrorHandlingInterceptor(errorHandler, logger)
		
		assert.NotNil(t, interceptor)
		assert.Equal(t, errorHandler, interceptor.errorHandler)
		assert.Equal(t, logger, interceptor.logger)
		assert.Equal(t, "ErrorHandlingInterceptor", interceptor.Name())
	})
	
	t.Run("NewErrorHandlingInterceptor with nil logger uses default", func(t *testing.T) {
		errorHandler := &mockErrorHandler{}
		interceptor := NewErrorHandlingInterceptor(errorHandler, nil)
		
		assert.NotNil(t, interceptor)
		assert.NotNil(t, interceptor.logger)
	})
	
	t.Run("Intercept succeeds when handler succeeds", func(t *testing.T) {
		errorHandler := &mockErrorHandler{}
		interceptor := NewErrorHandlingInterceptor(errorHandler, slog.Default())
		handler := &mockHandler{}
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		
		handler.On("Handle", mock.Anything, msg).Return(nil)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.NoError(t, err)
		handler.AssertExpectations(t)
		errorHandler.AssertNotCalled(t, "HandleError")
	})
	
	t.Run("Intercept handles error when handler fails", func(t *testing.T) {
		errorHandler := &mockErrorHandler{}
		interceptor := NewErrorHandlingInterceptor(errorHandler, slog.Default())
		handler := &mockHandler{}
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		
		handlerError := errors.New("handler failed")
		recoveredError := errors.New("recovered")
		
		handler.On("Handle", mock.Anything, msg).Return(handlerError)
		errorHandler.On("HandleError", mock.Anything, msg, handlerError).Return(recoveredError)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.Equal(t, recoveredError, err)
		handler.AssertExpectations(t)
		errorHandler.AssertExpectations(t)
	})
}

func TestCircuitBreakerInterceptor(t *testing.T) {
	t.Run("NewCircuitBreakerInterceptor creates interceptor", func(t *testing.T) {
		cb := &mockCircuitBreaker{}
		interceptor := NewCircuitBreakerInterceptor(cb)
		
		assert.NotNil(t, interceptor)
		assert.Equal(t, cb, interceptor.circuitBreaker)
		assert.Equal(t, "CircuitBreakerInterceptor", interceptor.Name())
	})
	
	t.Run("Intercept executes through circuit breaker", func(t *testing.T) {
		cb := &mockCircuitBreaker{}
		interceptor := NewCircuitBreakerInterceptor(cb)
		handler := &mockHandler{}
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		
		cb.On("Execute", mock.Anything, mock.AnythingOfType("func() error")).Return(nil)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.NoError(t, err)
		cb.AssertExpectations(t)
	})
}

func TestBuilderMethods(t *testing.T) {
	t.Run("Builder methods return self for chaining", func(t *testing.T) {
		builder := NewDefaultInterceptorChainBuilder(slog.Default())
		
		result1 := builder.WithLogging()
		assert.Equal(t, builder, result1)
		
		collector := &mockMetricsCollector{}
		result2 := builder.WithMetrics(collector)
		assert.Equal(t, builder, result2)
		
		tracer := &mockTracer{}
		result3 := builder.WithTracing(tracer)
		assert.Equal(t, builder, result3)
		
		validator := &mockValidator{}
		result4 := builder.WithValidation(validator)
		assert.Equal(t, builder, result4)
		
		authenticator := &mockAuthenticator{}
		result5 := builder.WithAuthentication(authenticator)
		assert.Equal(t, builder, result5)
		
		limiter := &mockRateLimiter{}
		result6 := builder.WithRateLimit(limiter)
		assert.Equal(t, builder, result6)
		
		result7 := builder.WithTimeout(30 * time.Second)
		assert.Equal(t, builder, result7)
		
		errorHandler := &mockErrorHandler{}
		result8 := builder.WithErrorHandling(errorHandler)
		assert.Equal(t, builder, result8)
		
		cb := &mockCircuitBreaker{}
		result9 := builder.WithCircuitBreaker(cb)
		assert.Equal(t, builder, result9)
		
		custom := NewInterceptorFunc("custom", func(ctx context.Context, msg contracts.Message, next MessageHandler) error {
			return next.Handle(ctx, msg)
		})
		result10 := builder.WithCustom(custom)
		assert.Equal(t, builder, result10)
	})
	
	t.Run("Builder creates chain with all interceptors", func(t *testing.T) {
		collector := &mockMetricsCollector{}
		tracer := &mockTracer{}
		validator := &mockValidator{}
		authenticator := &mockAuthenticator{}
		limiter := &mockRateLimiter{}
		errorHandler := &mockErrorHandler{}
		cb := &mockCircuitBreaker{}
		custom := NewInterceptorFunc("custom", func(ctx context.Context, msg contracts.Message, next MessageHandler) error {
			return next.Handle(ctx, msg)
		})
		
		chain := NewDefaultInterceptorChainBuilder(slog.Default()).
			WithLogging().
			WithMetrics(collector).
			WithTracing(tracer).
			WithValidation(validator).
			WithAuthentication(authenticator).
			WithRateLimit(limiter).
			WithTimeout(30 * time.Second).
			WithErrorHandling(errorHandler).
			WithCircuitBreaker(cb).
			WithCustom(custom).
			Build()
		
		assert.Len(t, chain.interceptors, 10)
		assert.Equal(t, "LoggingInterceptor", chain.interceptors[0].Name())
		assert.Equal(t, "MetricsInterceptor", chain.interceptors[1].Name())
		assert.Equal(t, "TracingInterceptor", chain.interceptors[2].Name())
		assert.Equal(t, "ValidationInterceptor", chain.interceptors[3].Name())
		assert.Equal(t, "AuthenticationInterceptor", chain.interceptors[4].Name())
		assert.Equal(t, "RateLimitingInterceptor", chain.interceptors[5].Name())
		assert.Equal(t, "TimeoutInterceptor", chain.interceptors[6].Name())
		assert.Equal(t, "ErrorHandlingInterceptor", chain.interceptors[7].Name())
		assert.Equal(t, "CircuitBreakerInterceptor", chain.interceptors[8].Name())
		assert.Equal(t, "custom", chain.interceptors[9].Name())
	})
}

// Additional mock interfaces

type mockValidator struct {
	mock.Mock
}

func (m *mockValidator) Validate(ctx context.Context, msg contracts.Message) error {
	args := m.Called(ctx, msg)
	return args.Error(0)
}

type mockAuthenticator struct {
	mock.Mock
}

func (m *mockAuthenticator) Authenticate(ctx context.Context, msg contracts.Message) error {
	args := m.Called(ctx, msg)
	return args.Error(0)
}

type mockRateLimiter struct {
	mock.Mock
}

func (m *mockRateLimiter) Allow(ctx context.Context, key string) error {
	args := m.Called(ctx, key)
	return args.Error(0)
}

type mockErrorHandler struct {
	mock.Mock
}

func (m *mockErrorHandler) HandleError(ctx context.Context, msg contracts.Message, err error) error {
	args := m.Called(ctx, msg, err)
	return args.Error(0)
}

type mockCircuitBreaker struct {
	mock.Mock
}

func (m *mockCircuitBreaker) Execute(ctx context.Context, fn func() error) error {
	args := m.Called(ctx, fn)
	return args.Error(0)
}