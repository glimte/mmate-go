package reliability

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExponentialBackoff(t *testing.T) {
	t.Run("creates with correct defaults", func(t *testing.T) {
		eb := NewExponentialBackoff(
			100*time.Millisecond,
			5*time.Second,
			2.0,
			3,
		)
		
		assert.Equal(t, 100*time.Millisecond, eb.InitialInterval)
		assert.Equal(t, 5*time.Second, eb.MaxInterval)
		assert.Equal(t, 2.0, eb.Multiplier)
		assert.Equal(t, 3, eb.MaxAttempts)
		assert.True(t, eb.Jitter)
	})

	t.Run("ShouldRetry respects max retries", func(t *testing.T) {
		eb := NewExponentialBackoff(100*time.Millisecond, 1*time.Second, 2.0, 3)
		
		// Should retry for attempts 0, 1, 2
		for i := 0; i < 3; i++ {
			shouldRetry, delay := eb.ShouldRetry(i, errors.New("test"))
			assert.True(t, shouldRetry)
			assert.Greater(t, delay, time.Duration(0))
		}
		
		// Should not retry for attempt 3
		shouldRetry, delay := eb.ShouldRetry(3, errors.New("test"))
		assert.False(t, shouldRetry)
		assert.Equal(t, time.Duration(0), delay)
	})

	t.Run("NextDelay calculates exponential backoff", func(t *testing.T) {
		eb := NewExponentialBackoff(100*time.Millisecond, 10*time.Second, 2.0, 5)
		eb.Jitter = false // Disable jitter for predictable results
		
		tests := []struct {
			attempt  int
			expected time.Duration
		}{
			{0, 100 * time.Millisecond},
			{1, 200 * time.Millisecond},
			{2, 400 * time.Millisecond},
			{3, 800 * time.Millisecond},
			{4, 1600 * time.Millisecond},
			{10, 10 * time.Second}, // Should cap at max
		}
		
		for _, tt := range tests {
			t.Run(time.Duration(tt.attempt).String(), func(t *testing.T) {
				delay := eb.NextDelay(tt.attempt)
				assert.Equal(t, tt.expected, delay)
			})
		}
	})

	t.Run("NextDelay with jitter", func(t *testing.T) {
		eb := NewExponentialBackoff(1*time.Second, 10*time.Second, 2.0, 5)
		eb.Jitter = true
		
		// Run multiple times to ensure jitter is applied
		delays := make([]time.Duration, 10)
		for i := 0; i < 10; i++ {
			delays[i] = eb.NextDelay(0)
		}
		
		// Check that not all delays are the same (jitter is working)
		allSame := true
		for i := 1; i < len(delays); i++ {
			if delays[i] != delays[0] {
				allSame = false
				break
			}
		}
		assert.False(t, allSame, "Jitter should produce different delays")
		
		// All delays should be within expected range (±15%)
		for _, delay := range delays {
			assert.GreaterOrEqual(t, delay, 850*time.Millisecond)
			assert.LessOrEqual(t, delay, 1150*time.Millisecond)
		}
	})

	t.Run("respects non-retryable errors", func(t *testing.T) {
		eb := NewExponentialBackoff(100*time.Millisecond, 1*time.Second, 2.0, 3)
		
		nonRetryableErr := RetryableError{
			Err:       errors.New("non-retryable"),
			Retryable: false,
		}
		
		shouldRetry, _ := eb.ShouldRetry(0, nonRetryableErr)
		assert.False(t, shouldRetry)
	})
}

func TestLinearBackoff(t *testing.T) {
	t.Run("creates with correct defaults", func(t *testing.T) {
		lb := NewLinearBackoff(500*time.Millisecond, 5)
		
		assert.Equal(t, 500*time.Millisecond, lb.Interval)
		assert.Equal(t, 5, lb.MaxAttempts)
		assert.True(t, lb.Jitter)
	})

	t.Run("NextDelay returns constant interval", func(t *testing.T) {
		lb := NewLinearBackoff(1*time.Second, 5)
		lb.Jitter = false
		
		for i := 0; i < 5; i++ {
			delay := lb.NextDelay(i)
			assert.Equal(t, 1*time.Second, delay)
		}
	})

	t.Run("ShouldRetry respects max retries", func(t *testing.T) {
		lb := NewLinearBackoff(100*time.Millisecond, 2)
		
		shouldRetry, _ := lb.ShouldRetry(0, errors.New("test"))
		assert.True(t, shouldRetry)
		
		shouldRetry, _ = lb.ShouldRetry(1, errors.New("test"))
		assert.True(t, shouldRetry)
		
		shouldRetry, _ = lb.ShouldRetry(2, errors.New("test"))
		assert.False(t, shouldRetry)
	})
}

func TestFixedDelay(t *testing.T) {
	t.Run("creates with correct values", func(t *testing.T) {
		fd := NewFixedDelay(2*time.Second, 3)
		
		assert.Equal(t, 2*time.Second, fd.Delay)
		assert.Equal(t, 3, fd.MaxAttempts)
	})

	t.Run("NextDelay always returns same delay", func(t *testing.T) {
		fd := NewFixedDelay(750*time.Millisecond, 10)
		
		for i := 0; i < 10; i++ {
			delay := fd.NextDelay(i)
			assert.Equal(t, 750*time.Millisecond, delay)
		}
	})
}

func TestRetry(t *testing.T) {
	t.Run("succeeds on first attempt", func(t *testing.T) {
		policy := NewFixedDelay(100*time.Millisecond, 3)
		attempts := 0
		
		err := Retry(context.Background(), policy, func() error {
			attempts++
			return nil
		})
		
		assert.NoError(t, err)
		assert.Equal(t, 1, attempts)
	})

	t.Run("retries on failure", func(t *testing.T) {
		policy := NewFixedDelay(10*time.Millisecond, 3)
		attempts := 0
		
		err := Retry(context.Background(), policy, func() error {
			attempts++
			if attempts < 3 {
				return errors.New("temporary error")
			}
			return nil
		})
		
		assert.NoError(t, err)
		assert.Equal(t, 3, attempts)
	})

	t.Run("returns last error after max retries", func(t *testing.T) {
		policy := NewFixedDelay(10*time.Millisecond, 2)
		attempts := 0
		
		err := Retry(context.Background(), policy, func() error {
			attempts++
			return errors.New("persistent error")
		})
		
		assert.Error(t, err)
		assert.Equal(t, "persistent error", err.Error())
		assert.Equal(t, 3, attempts) // Initial + 2 retries
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		policy := NewFixedDelay(1*time.Second, 5)
		ctx, cancel := context.WithCancel(context.Background())
		
		attempts := int32(0)
		
		// Cancel after a short delay
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()
		
		err := Retry(ctx, policy, func() error {
			atomic.AddInt32(&attempts, 1)
			return errors.New("error")
		})
		
		assert.Equal(t, context.Canceled, err)
		assert.LessOrEqual(t, atomic.LoadInt32(&attempts), int32(2))
	})

	t.Run("respects context deadline", func(t *testing.T) {
		policy := NewFixedDelay(100*time.Millisecond, 10)
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		
		attempts := 0
		start := time.Now()
		
		err := Retry(ctx, policy, func() error {
			attempts++
			return errors.New("error")
		})
		
		duration := time.Since(start)
		assert.Equal(t, context.DeadlineExceeded, err)
		assert.Less(t, attempts, 10) // Should not reach max retries
		assert.Less(t, duration, 500*time.Millisecond)
	})

	t.Run("stops on non-retryable error", func(t *testing.T) {
		policy := NewExponentialBackoff(100*time.Millisecond, 1*time.Second, 2.0, 5)
		attempts := 0
		
		err := Retry(context.Background(), policy, func() error {
			attempts++
			if attempts == 2 {
				return RetryableError{
					Err:       errors.New("fatal error"),
					Retryable: false,
				}
			}
			return errors.New("retryable error")
		})
		
		assert.Error(t, err)
		assert.Equal(t, "fatal error", err.Error())
		assert.Equal(t, 2, attempts)
	})
}

func TestRetryWithBackoff(t *testing.T) {
	t.Run("uses default exponential backoff", func(t *testing.T) {
		attempts := 0
		
		err := RetryWithBackoff(context.Background(), func() error {
			attempts++
			if attempts < 3 {
				return errors.New("error")
			}
			return nil
		})
		
		assert.NoError(t, err)
		assert.Equal(t, 3, attempts)
	})
}

func TestRetryableError(t *testing.T) {
	t.Run("implements error interface", func(t *testing.T) {
		baseErr := errors.New("base error")
		err := RetryableError{
			Err:       baseErr,
			Retryable: true,
		}
		
		assert.Equal(t, "base error", err.Error())
	})

	t.Run("IsRetryable returns correct value", func(t *testing.T) {
		err1 := RetryableError{
			Err:       errors.New("error"),
			Retryable: true,
		}
		assert.True(t, err1.IsRetryable())
		
		err2 := RetryableError{
			Err:       errors.New("error"),
			Retryable: false,
		}
		assert.False(t, err2.IsRetryable())
	})

	t.Run("Unwrap returns wrapped error", func(t *testing.T) {
		baseErr := errors.New("wrapped error")
		err := RetryableError{
			Err:       baseErr,
			Retryable: true,
		}
		
		assert.Equal(t, baseErr, err.Unwrap())
	})
}

func TestIsRetryableError(t *testing.T) {
	t.Run("nil error is not retryable", func(t *testing.T) {
		assert.False(t, isRetryableError(nil))
	})

	t.Run("RetryableError respects Retryable field", func(t *testing.T) {
		err1 := RetryableError{Err: errors.New("test"), Retryable: true}
		assert.True(t, isRetryableError(err1))
		
		err2 := RetryableError{Err: errors.New("test"), Retryable: false}
		assert.False(t, isRetryableError(err2))
	})

	t.Run("unknown errors are retryable by default", func(t *testing.T) {
		err := errors.New("unknown error")
		assert.True(t, isRetryableError(err))
	})
}

func BenchmarkRetry(b *testing.B) {
	policy := NewExponentialBackoff(1*time.Microsecond, 10*time.Microsecond, 2.0, 3)
	ctx := context.Background()
	
	b.Run("successful operation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			Retry(ctx, policy, func() error {
				return nil
			})
		}
	})
	
	b.Run("operation with retries", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			attempts := 0
			Retry(ctx, policy, func() error {
				attempts++
				if attempts < 2 {
					return errors.New("error")
				}
				return nil
			})
		}
	})
}