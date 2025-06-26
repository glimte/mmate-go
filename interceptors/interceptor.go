package interceptors

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/glimte/mmate-go/contracts"
)

// MessageHandler represents a message handler in the interceptor chain
type MessageHandler interface {
	Handle(ctx context.Context, msg contracts.Message) error
}

// MessageHandlerFunc is a function adapter for MessageHandler
type MessageHandlerFunc func(ctx context.Context, msg contracts.Message) error

// Handle implements MessageHandler
func (f MessageHandlerFunc) Handle(ctx context.Context, msg contracts.Message) error {
	return f(ctx, msg)
}

// Interceptor processes messages before they reach the final handler
type Interceptor interface {
	// Intercept processes a message and calls the next handler in the chain
	Intercept(ctx context.Context, msg contracts.Message, next MessageHandler) error

	// Name returns the interceptor name for logging and debugging
	Name() string
}

// InterceptorFunc is a function adapter for Interceptor
type InterceptorFunc struct {
	name string
	fn   func(ctx context.Context, msg contracts.Message, next MessageHandler) error
}

// NewInterceptorFunc creates a new function-based interceptor
func NewInterceptorFunc(name string, fn func(ctx context.Context, msg contracts.Message, next MessageHandler) error) *InterceptorFunc {
	return &InterceptorFunc{name: name, fn: fn}
}

// Intercept implements Interceptor
func (i *InterceptorFunc) Intercept(ctx context.Context, msg contracts.Message, next MessageHandler) error {
	return i.fn(ctx, msg, next)
}

// Name implements Interceptor
func (i *InterceptorFunc) Name() string {
	return i.name
}

// InterceptorChain manages a chain of interceptors
type InterceptorChain struct {
	interceptors []Interceptor
	logger       *slog.Logger
}

// NewInterceptorChain creates a new interceptor chain
func NewInterceptorChain(logger *slog.Logger) *InterceptorChain {
	if logger == nil {
		logger = slog.Default()
	}

	return &InterceptorChain{
		interceptors: make([]Interceptor, 0),
		logger:       logger,
	}
}

// Add adds an interceptor to the chain
func (c *InterceptorChain) Add(interceptor Interceptor) *InterceptorChain {
	c.interceptors = append(c.interceptors, interceptor)
	return c
}

// Execute executes the interceptor chain
func (c *InterceptorChain) Execute(ctx context.Context, msg contracts.Message, finalHandler MessageHandler) error {
	if len(c.interceptors) == 0 {
		return finalHandler.Handle(ctx, msg)
	}

	// Build the chain in reverse order
	handler := finalHandler
	for i := len(c.interceptors) - 1; i >= 0; i-- {
		interceptor := c.interceptors[i]
		currentHandler := handler
		handler = MessageHandlerFunc(func(ctx context.Context, msg contracts.Message) error {
			return interceptor.Intercept(ctx, msg, currentHandler)
		})
	}

	return handler.Handle(ctx, msg)
}

// Built-in interceptors

// LoggingInterceptor logs message processing
type LoggingInterceptor struct {
	logger *slog.Logger
}

// NewLoggingInterceptor creates a new logging interceptor
func NewLoggingInterceptor(logger *slog.Logger) *LoggingInterceptor {
	if logger == nil {
		logger = slog.Default()
	}

	return &LoggingInterceptor{logger: logger}
}

// Intercept implements Interceptor
func (i *LoggingInterceptor) Intercept(ctx context.Context, msg contracts.Message, next MessageHandler) error {
	start := time.Now()

	i.logger.Info("processing message",
		"messageId", msg.GetID(),
		"messageType", msg.GetType(),
		"correlationId", msg.GetCorrelationID(),
	)

	err := next.Handle(ctx, msg)
	duration := time.Since(start)

	if err != nil {
		i.logger.Error("message processing failed",
			"messageId", msg.GetID(),
			"messageType", msg.GetType(),
			"duration", duration,
			"error", err,
		)
	} else {
		i.logger.Info("message processed successfully",
			"messageId", msg.GetID(),
			"messageType", msg.GetType(),
			"duration", duration,
		)
	}

	return err
}

// Name implements Interceptor
func (i *LoggingInterceptor) Name() string {
	return "LoggingInterceptor"
}

// MetricsInterceptor collects metrics about message processing
type MetricsInterceptor struct {
	collector MetricsCollector
}

// MetricsCollector defines the interface for collecting metrics
type MetricsCollector interface {
	IncrementMessageCount(messageType string)
	RecordProcessingTime(messageType string, duration time.Duration)
	IncrementErrorCount(messageType string, errorType string)
}

// NewMetricsInterceptor creates a new metrics interceptor
func NewMetricsInterceptor(collector MetricsCollector) *MetricsInterceptor {
	return &MetricsInterceptor{collector: collector}
}

// Intercept implements Interceptor
func (i *MetricsInterceptor) Intercept(ctx context.Context, msg contracts.Message, next MessageHandler) error {
	start := time.Now()
	messageType := msg.GetType()

	i.collector.IncrementMessageCount(messageType)

	err := next.Handle(ctx, msg)
	duration := time.Since(start)

	i.collector.RecordProcessingTime(messageType, duration)

	if err != nil {
		i.collector.IncrementErrorCount(messageType, "processing_error")
	}

	return err
}

// Name implements Interceptor
func (i *MetricsInterceptor) Name() string {
	return "MetricsInterceptor"
}

// TracingInterceptor adds distributed tracing support
type TracingInterceptor struct {
	tracer Tracer
}

// Tracer defines the interface for distributed tracing
type Tracer interface {
	StartSpan(ctx context.Context, operationName string, msg contracts.Message) (context.Context, Span)
}

// Span represents a tracing span
type Span interface {
	SetTag(key string, value interface{})
	SetError(err error)
	Finish()
}

// NewTracingInterceptor creates a new tracing interceptor
func NewTracingInterceptor(tracer Tracer) *TracingInterceptor {
	return &TracingInterceptor{tracer: tracer}
}

// Intercept implements Interceptor
func (i *TracingInterceptor) Intercept(ctx context.Context, msg contracts.Message, next MessageHandler) error {
	spanCtx, span := i.tracer.StartSpan(ctx, "message.process", msg)
	defer span.Finish()

	span.SetTag("message.id", msg.GetID())
	span.SetTag("message.type", msg.GetType())
	span.SetTag("message.correlationId", msg.GetCorrelationID())

	err := next.Handle(spanCtx, msg)
	if err != nil {
		span.SetError(err)
	}

	return err
}

// Name implements Interceptor
func (i *TracingInterceptor) Name() string {
	return "TracingInterceptor"
}

// ValidationInterceptor validates messages before processing
type ValidationInterceptor struct {
	validator MessageValidator
}

// MessageValidator defines the interface for message validation
type MessageValidator interface {
	Validate(ctx context.Context, msg contracts.Message) error
}

// NewValidationInterceptor creates a new validation interceptor
func NewValidationInterceptor(validator MessageValidator) *ValidationInterceptor {
	return &ValidationInterceptor{validator: validator}
}

// Intercept implements Interceptor
func (i *ValidationInterceptor) Intercept(ctx context.Context, msg contracts.Message, next MessageHandler) error {
	if err := i.validator.Validate(ctx, msg); err != nil {
		return fmt.Errorf("message validation failed: %w", err)
	}

	return next.Handle(ctx, msg)
}

// Name implements Interceptor
func (i *ValidationInterceptor) Name() string {
	return "ValidationInterceptor"
}

// AuthenticationInterceptor validates message authentication
type AuthenticationInterceptor struct {
	authenticator MessageAuthenticator
}

// MessageAuthenticator defines the interface for message authentication
type MessageAuthenticator interface {
	Authenticate(ctx context.Context, msg contracts.Message) error
}

// NewAuthenticationInterceptor creates a new authentication interceptor
func NewAuthenticationInterceptor(authenticator MessageAuthenticator) *AuthenticationInterceptor {
	return &AuthenticationInterceptor{authenticator: authenticator}
}

// Intercept implements Interceptor
func (i *AuthenticationInterceptor) Intercept(ctx context.Context, msg contracts.Message, next MessageHandler) error {
	if err := i.authenticator.Authenticate(ctx, msg); err != nil {
		return fmt.Errorf("message authentication failed: %w", err)
	}

	return next.Handle(ctx, msg)
}

// Name implements Interceptor
func (i *AuthenticationInterceptor) Name() string {
	return "AuthenticationInterceptor"
}

// RateLimitingInterceptor implements rate limiting
type RateLimitingInterceptor struct {
	limiter RateLimiter
}

// RateLimiter defines the interface for rate limiting
type RateLimiter interface {
	Allow(ctx context.Context, key string) error
}

// NewRateLimitingInterceptor creates a new rate limiting interceptor
func NewRateLimitingInterceptor(limiter RateLimiter) *RateLimitingInterceptor {
	return &RateLimitingInterceptor{limiter: limiter}
}

// Intercept implements Interceptor
func (i *RateLimitingInterceptor) Intercept(ctx context.Context, msg contracts.Message, next MessageHandler) error {
	// Use message type as rate limiting key
	key := msg.GetType()

	if err := i.limiter.Allow(ctx, key); err != nil {
		return fmt.Errorf("rate limit exceeded for message type %s: %w", key, err)
	}

	return next.Handle(ctx, msg)
}

// Name implements Interceptor
func (i *RateLimitingInterceptor) Name() string {
	return "RateLimitingInterceptor"
}

// TimeoutInterceptor adds timeout handling
type TimeoutInterceptor struct {
	timeout time.Duration
}

// NewTimeoutInterceptor creates a new timeout interceptor
func NewTimeoutInterceptor(timeout time.Duration) *TimeoutInterceptor {
	return &TimeoutInterceptor{timeout: timeout}
}

// Intercept implements Interceptor
func (i *TimeoutInterceptor) Intercept(ctx context.Context, msg contracts.Message, next MessageHandler) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, i.timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- next.Handle(timeoutCtx, msg)
	}()

	select {
	case err := <-done:
		return err
	case <-timeoutCtx.Done():
		return fmt.Errorf("message processing timeout after %v for message %s", i.timeout, msg.GetID())
	}
}

// Name implements Interceptor
func (i *TimeoutInterceptor) Name() string {
	return "TimeoutInterceptor"
}

// ErrorHandlingInterceptor handles errors and provides recovery
type ErrorHandlingInterceptor struct {
	errorHandler ErrorHandler
	logger       *slog.Logger
}

// ErrorHandler defines the interface for error handling
type ErrorHandler interface {
	HandleError(ctx context.Context, msg contracts.Message, err error) error
}

// NewErrorHandlingInterceptor creates a new error handling interceptor
func NewErrorHandlingInterceptor(errorHandler ErrorHandler, logger *slog.Logger) *ErrorHandlingInterceptor {
	if logger == nil {
		logger = slog.Default()
	}

	return &ErrorHandlingInterceptor{
		errorHandler: errorHandler,
		logger:       logger,
	}
}

// Intercept implements Interceptor
func (i *ErrorHandlingInterceptor) Intercept(ctx context.Context, msg contracts.Message, next MessageHandler) error {
	err := next.Handle(ctx, msg)
	if err != nil {
		i.logger.Error("message processing error",
			"messageId", msg.GetID(),
			"messageType", msg.GetType(),
			"error", err,
		)

		// Let error handler decide how to handle the error
		return i.errorHandler.HandleError(ctx, msg, err)
	}

	return nil
}

// Name implements Interceptor
func (i *ErrorHandlingInterceptor) Name() string {
	return "ErrorHandlingInterceptor"
}

// CircuitBreakerInterceptor provides circuit breaker functionality
type CircuitBreakerInterceptor struct {
	circuitBreaker CircuitBreaker
}

// CircuitBreaker defines the interface for circuit breaker functionality
type CircuitBreaker interface {
	Execute(ctx context.Context, fn func() error) error
}

// NewCircuitBreakerInterceptor creates a new circuit breaker interceptor
func NewCircuitBreakerInterceptor(circuitBreaker CircuitBreaker) *CircuitBreakerInterceptor {
	return &CircuitBreakerInterceptor{circuitBreaker: circuitBreaker}
}

// Intercept implements Interceptor
func (i *CircuitBreakerInterceptor) Intercept(ctx context.Context, msg contracts.Message, next MessageHandler) error {
	return i.circuitBreaker.Execute(ctx, func() error {
		return next.Handle(ctx, msg)
	})
}

// Name implements Interceptor
func (i *CircuitBreakerInterceptor) Name() string {
	return "CircuitBreakerInterceptor"
}

// Default interceptor chain builder

// DefaultInterceptorChainBuilder builds a common interceptor chain
type DefaultInterceptorChainBuilder struct {
	chain  *InterceptorChain
	logger *slog.Logger
}

// NewDefaultInterceptorChainBuilder creates a new builder
func NewDefaultInterceptorChainBuilder(logger *slog.Logger) *DefaultInterceptorChainBuilder {
	if logger == nil {
		logger = slog.Default()
	}

	return &DefaultInterceptorChainBuilder{
		chain:  NewInterceptorChain(logger),
		logger: logger,
	}
}

// WithLogging adds logging interceptor
func (b *DefaultInterceptorChainBuilder) WithLogging() *DefaultInterceptorChainBuilder {
	b.chain.Add(NewLoggingInterceptor(b.logger))
	return b
}

// WithMetrics adds metrics interceptor
func (b *DefaultInterceptorChainBuilder) WithMetrics(collector MetricsCollector) *DefaultInterceptorChainBuilder {
	b.chain.Add(NewMetricsInterceptor(collector))
	return b
}

// WithTracing adds tracing interceptor
func (b *DefaultInterceptorChainBuilder) WithTracing(tracer Tracer) *DefaultInterceptorChainBuilder {
	b.chain.Add(NewTracingInterceptor(tracer))
	return b
}

// WithValidation adds validation interceptor
func (b *DefaultInterceptorChainBuilder) WithValidation(validator MessageValidator) *DefaultInterceptorChainBuilder {
	b.chain.Add(NewValidationInterceptor(validator))
	return b
}

// WithAuthentication adds authentication interceptor
func (b *DefaultInterceptorChainBuilder) WithAuthentication(authenticator MessageAuthenticator) *DefaultInterceptorChainBuilder {
	b.chain.Add(NewAuthenticationInterceptor(authenticator))
	return b
}

// WithRateLimit adds rate limiting interceptor
func (b *DefaultInterceptorChainBuilder) WithRateLimit(limiter RateLimiter) *DefaultInterceptorChainBuilder {
	b.chain.Add(NewRateLimitingInterceptor(limiter))
	return b
}

// WithTimeout adds timeout interceptor
func (b *DefaultInterceptorChainBuilder) WithTimeout(timeout time.Duration) *DefaultInterceptorChainBuilder {
	b.chain.Add(NewTimeoutInterceptor(timeout))
	return b
}

// WithErrorHandling adds error handling interceptor
func (b *DefaultInterceptorChainBuilder) WithErrorHandling(errorHandler ErrorHandler) *DefaultInterceptorChainBuilder {
	b.chain.Add(NewErrorHandlingInterceptor(errorHandler, b.logger))
	return b
}

// WithCircuitBreaker adds circuit breaker interceptor
func (b *DefaultInterceptorChainBuilder) WithCircuitBreaker(circuitBreaker CircuitBreaker) *DefaultInterceptorChainBuilder {
	b.chain.Add(NewCircuitBreakerInterceptor(circuitBreaker))
	return b
}

// WithCustom adds a custom interceptor
func (b *DefaultInterceptorChainBuilder) WithCustom(interceptor Interceptor) *DefaultInterceptorChainBuilder {
	b.chain.Add(interceptor)
	return b
}

// Build returns the built interceptor chain
func (b *DefaultInterceptorChainBuilder) Build() *InterceptorChain {
	return b.chain
}
