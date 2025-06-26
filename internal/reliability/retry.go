package reliability

import (
	"context"
	"math"
	"math/rand"
	"time"
)

// RetryPolicy defines the interface for retry policies
type RetryPolicy interface {
	// ShouldRetry determines if a retry should be attempted
	ShouldRetry(attempt int, err error) (bool, time.Duration)
	// MaxRetries returns the maximum number of retries
	MaxRetries() int
	// NextDelay calculates the next retry delay
	NextDelay(attempt int) time.Duration
}

// ExponentialBackoff implements exponential backoff retry policy
type ExponentialBackoff struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
	MaxAttempts     int
	Jitter          bool
}

// NewExponentialBackoff creates a new exponential backoff policy
func NewExponentialBackoff(initial, max time.Duration, multiplier float64, maxRetries int) *ExponentialBackoff {
	return &ExponentialBackoff{
		InitialInterval: initial,
		MaxInterval:     max,
		Multiplier:      multiplier,
		MaxAttempts:     maxRetries,
		Jitter:          true,
	}
}

// ShouldRetry implements RetryPolicy
func (e *ExponentialBackoff) ShouldRetry(attempt int, err error) (bool, time.Duration) {
	if attempt >= e.MaxAttempts {
		return false, 0
	}

	// Check if error is retryable
	if !isRetryableError(err) {
		return false, 0
	}

	delay := e.NextDelay(attempt)
	return true, delay
}

// MaxRetries implements RetryPolicy
func (e *ExponentialBackoff) MaxRetries() int {
	return e.MaxAttempts
}

// NextDelay implements RetryPolicy
func (e *ExponentialBackoff) NextDelay(attempt int) time.Duration {
	delay := float64(e.InitialInterval) * math.Pow(e.Multiplier, float64(attempt))
	
	// Cap at max interval
	if delay > float64(e.MaxInterval) {
		delay = float64(e.MaxInterval)
	}

	// Add jitter if enabled
	if e.Jitter {
		jitter := rand.Float64() * 0.3 * delay // ±15% jitter
		delay = delay + jitter - (0.15 * delay)
	}

	return time.Duration(delay)
}

// LinearBackoff implements linear backoff retry policy
type LinearBackoff struct {
	Interval    time.Duration
	MaxAttempts int
	Jitter      bool
}

// NewLinearBackoff creates a new linear backoff policy
func NewLinearBackoff(interval time.Duration, maxRetries int) *LinearBackoff {
	return &LinearBackoff{
		Interval:   interval,
		MaxAttempts: maxRetries,
		Jitter:      true,
	}
}

// ShouldRetry implements RetryPolicy
func (l *LinearBackoff) ShouldRetry(attempt int, err error) (bool, time.Duration) {
	if attempt >= l.MaxAttempts {
		return false, 0
	}

	if !isRetryableError(err) {
		return false, 0
	}

	delay := l.NextDelay(attempt)
	return true, delay
}

// MaxRetries implements RetryPolicy
func (l *LinearBackoff) MaxRetries() int {
	return l.MaxAttempts
}

// NextDelay implements RetryPolicy
func (l *LinearBackoff) NextDelay(attempt int) time.Duration {
	delay := l.Interval

	// Add jitter if enabled
	if l.Jitter {
		jitter := time.Duration(rand.Float64() * float64(delay) * 0.3)
		delay = delay + jitter - (delay * 15 / 100)
	}

	return delay
}

// FixedDelay implements a fixed delay retry policy
type FixedDelay struct {
	Delay       time.Duration
	MaxAttempts int
}

// NewFixedDelay creates a new fixed delay policy
func NewFixedDelay(delay time.Duration, maxRetries int) *FixedDelay {
	return &FixedDelay{
		Delay:       delay,
		MaxAttempts: maxRetries,
	}
}

// ShouldRetry implements RetryPolicy
func (f *FixedDelay) ShouldRetry(attempt int, err error) (bool, time.Duration) {
	if attempt >= f.MaxAttempts {
		return false, 0
	}

	if !isRetryableError(err) {
		return false, 0
	}

	return true, f.Delay
}

// MaxRetries implements RetryPolicy
func (f *FixedDelay) MaxRetries() int {
	return f.MaxAttempts
}

// NextDelay implements RetryPolicy
func (f *FixedDelay) NextDelay(attempt int) time.Duration {
	return f.Delay
}

// Retry executes a function with retry logic
func Retry(ctx context.Context, policy RetryPolicy, fn func() error) error {
	var lastErr error
	
	for attempt := 0; ; attempt++ {
		// Check context
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Execute function
		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err

		// Check if we should retry
		shouldRetry, delay := policy.ShouldRetry(attempt, err)
		if !shouldRetry {
			return lastErr
		}

		// Wait before retry
		select {
		case <-time.After(delay):
			// Continue with retry
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// RetryWithBackoff is a convenience function for exponential backoff retry
func RetryWithBackoff(ctx context.Context, fn func() error) error {
	policy := NewExponentialBackoff(
		100*time.Millisecond,
		10*time.Second,
		2.0,
		5,
	)
	return Retry(ctx, policy, fn)
}

// isRetryableError determines if an error is retryable
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Define retryable error interface
	type retryable interface {
		IsRetryable() bool
	}

	// Check if error implements retryable
	if r, ok := err.(retryable); ok {
		return r.IsRetryable()
	}

	// Default to retryable for unknown errors
	return true
}

// RetryableError wraps an error to indicate it's retryable
type RetryableError struct {
	Err       error
	Retryable bool
}

// Error implements error interface
func (r RetryableError) Error() string {
	return r.Err.Error()
}

// IsRetryable indicates if the error is retryable
func (r RetryableError) IsRetryable() bool {
	return r.Retryable
}

// Unwrap returns the wrapped error
func (r RetryableError) Unwrap() error {
	return r.Err
}