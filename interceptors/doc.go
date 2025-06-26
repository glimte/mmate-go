// Package interceptors provides a flexible interceptor system for message processing.
//
// The interceptor pattern allows you to add cross-cutting concerns to message processing
// without modifying the core business logic. This package provides:
//   - Interceptor interface and chain management
//   - Built-in interceptors for common concerns
//   - Builder pattern for easy chain construction
//   - Integration with external systems (metrics, tracing, etc.)
//
// Built-in interceptors:
//   - LoggingInterceptor: Logs message processing with timing information
//   - MetricsInterceptor: Collects metrics about message processing
//   - TracingInterceptor: Adds distributed tracing support
//   - ValidationInterceptor: Validates messages before processing
//   - AuthenticationInterceptor: Validates message authentication
//   - RateLimitingInterceptor: Implements rate limiting per message type
//   - TimeoutInterceptor: Adds timeout handling to message processing
//   - ErrorHandlingInterceptor: Provides error recovery and handling
//   - CircuitBreakerInterceptor: Implements circuit breaker pattern
//
// Example usage:
//
//	// Build an interceptor chain
//	chain := interceptors.NewDefaultInterceptorChainBuilder(logger).
//		WithLogging().
//		WithMetrics(metricsCollector).
//		WithValidation(validator).
//		WithTimeout(30 * time.Second).
//		WithCircuitBreaker(circuitBreaker).
//		Build()
//	
//	// Use with a message handler
//	err := chain.Execute(ctx, message, finalHandler)
//
// Custom interceptors can be created by implementing the Interceptor interface:
//
//	type CustomInterceptor struct {}
//	
//	func (i *CustomInterceptor) Intercept(ctx context.Context, msg contracts.Message, next MessageHandler) error {
//		// Pre-processing logic
//		err := next.Handle(ctx, msg)
//		// Post-processing logic
//		return err
//	}
//	
//	func (i *CustomInterceptor) Name() string {
//		return "CustomInterceptor"
//	}
//
// Interceptors are executed in the order they are added to the chain, with the
// final handler being called last. This allows for proper nesting of concerns.
package interceptors