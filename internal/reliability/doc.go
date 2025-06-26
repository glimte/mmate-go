// Package reliability provides patterns for building reliable messaging systems.
//
// This package implements common reliability patterns:
//   - Circuit Breaker: Prevents cascading failures by monitoring error rates
//   - Retry Policies: Configurable retry strategies (exponential backoff, linear, fixed)
//   - Dead Letter Queue Handler: Manages failed messages with retry and error storage
//
// Key features:
//   - Thread-safe implementations suitable for concurrent use
//   - Configurable thresholds and timeouts
//   - Support for custom error classification (retryable vs non-retryable)
//   - Metrics collection interfaces for observability
//   - Error storage for permanent failure tracking
//
// Example usage:
//
//	// Create a circuit breaker
//	cb := NewCircuitBreaker(
//	    WithFailureThreshold(5),
//	    WithSuccessThreshold(3),
//	    WithTimeout(30 * time.Second),
//	)
//
//	// Use it to protect a function
//	err := cb.Execute(ctx, func() error {
//	    return riskyOperation()
//	})
package reliability