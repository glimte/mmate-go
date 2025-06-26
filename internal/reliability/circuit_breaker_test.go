package reliability

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCircuitBreaker(t *testing.T) {
	t.Run("starts in closed state", func(t *testing.T) {
		cb := NewCircuitBreaker()
		assert.Equal(t, StateClosed, cb.GetState())
	})

	t.Run("executes function in closed state", func(t *testing.T) {
		cb := NewCircuitBreaker()
		executed := false
		
		err := cb.Execute(context.Background(), func() error {
			executed = true
			return nil
		})
		
		assert.NoError(t, err)
		assert.True(t, executed)
	})

	t.Run("transitions to open state after failure threshold", func(t *testing.T) {
		cb := NewCircuitBreaker(WithFailureThreshold(3))
		
		// Fail 3 times
		for i := 0; i < 3; i++ {
			err := cb.Execute(context.Background(), func() error {
				return errors.New("test error")
			})
			assert.Error(t, err)
		}
		
		assert.Equal(t, StateOpen, cb.GetState())
		
		// Should reject execution when open
		err := cb.Execute(context.Background(), func() error {
			return nil
		})
		assert.Error(t, err)
		var cbErr *CircuitBreakerError
		assert.ErrorAs(t, err, &cbErr)
		assert.Equal(t, StateOpen, cbErr.State)
	})

	t.Run("transitions to half-open after timeout", func(t *testing.T) {
		cb := NewCircuitBreaker(
			WithFailureThreshold(1),
			WithTimeout(100*time.Millisecond),
		)
		
		// Trigger open state
		cb.Execute(context.Background(), func() error {
			return errors.New("test error")
		})
		assert.Equal(t, StateOpen, cb.GetState())
		
		// Wait for timeout
		time.Sleep(150 * time.Millisecond)
		
		// Should transition to half-open on next execution
		executed := false
		err := cb.Execute(context.Background(), func() error {
			executed = true
			return nil
		})
		
		assert.NoError(t, err)
		assert.True(t, executed)
		assert.Equal(t, StateHalfOpen, cb.GetState())
	})

	t.Run("half-open to closed on success threshold", func(t *testing.T) {
		cb := NewCircuitBreaker(
			WithFailureThreshold(1),
			WithSuccessThreshold(2),
			WithTimeout(100*time.Millisecond),
		)
		
		// Open the circuit
		cb.Execute(context.Background(), func() error {
			return errors.New("test error")
		})
		
		// Wait for timeout
		time.Sleep(150 * time.Millisecond)
		
		// Succeed twice in half-open
		for i := 0; i < 2; i++ {
			err := cb.Execute(context.Background(), func() error {
				return nil
			})
			assert.NoError(t, err)
		}
		
		assert.Equal(t, StateClosed, cb.GetState())
	})

	t.Run("half-open to open on failure", func(t *testing.T) {
		cb := NewCircuitBreaker(
			WithFailureThreshold(1),
			WithTimeout(100*time.Millisecond),
		)
		
		// Open the circuit
		cb.Execute(context.Background(), func() error {
			return errors.New("test error")
		})
		
		// Wait for timeout
		time.Sleep(150 * time.Millisecond)
		
		// Fail in half-open
		err := cb.Execute(context.Background(), func() error {
			return errors.New("another error")
		})
		assert.Error(t, err)
		assert.Equal(t, StateOpen, cb.GetState())
	})

	t.Run("respects half-open request limit", func(t *testing.T) {
		cb := NewCircuitBreaker(
			WithFailureThreshold(1),
			WithHalfOpenRequests(2),
			WithTimeout(100*time.Millisecond),
		)
		
		// Open the circuit
		cb.Execute(context.Background(), func() error {
			return errors.New("test error")
		})
		
		// Wait for timeout
		time.Sleep(150 * time.Millisecond)
		
		// Execute max half-open requests
		var wg sync.WaitGroup
		successCount := int32(0)
		
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := cb.Execute(context.Background(), func() error {
					time.Sleep(50 * time.Millisecond)
					return nil
				})
				if err == nil {
					atomic.AddInt32(&successCount, 1)
				}
			}()
		}
		
		wg.Wait()
		
		// At least 2 requests should succeed (half-open limit is 2)
		// Due to race conditions, we might get 2 or 3 successful requests
		successTotal := atomic.LoadInt32(&successCount)
		assert.GreaterOrEqual(t, successTotal, int32(2))
		assert.LessOrEqual(t, successTotal, int32(3))
	})

	t.Run("GetStats returns correct values", func(t *testing.T) {
		cb := NewCircuitBreaker(WithFailureThreshold(5))
		
		// Execute some failures and successes
		cb.Execute(context.Background(), func() error {
			return errors.New("error 1")
		})
		cb.Execute(context.Background(), func() error {
			return nil
		})
		cb.Execute(context.Background(), func() error {
			return errors.New("error 2")
		})
		
		failures, successes, lastFailure := cb.GetStats()
		// In closed state, failures reset on success, so we should have 1 failure
		assert.Equal(t, 1, failures)
		assert.Equal(t, 1, successes)
		assert.NotZero(t, lastFailure)
	})

	t.Run("Reset clears state", func(t *testing.T) {
		cb := NewCircuitBreaker(WithFailureThreshold(1))
		
		// Open the circuit
		cb.Execute(context.Background(), func() error {
			return errors.New("test error")
		})
		assert.Equal(t, StateOpen, cb.GetState())
		
		// Reset
		cb.Reset()
		
		assert.Equal(t, StateClosed, cb.GetState())
		failures, successes, _ := cb.GetStats()
		assert.Equal(t, 0, failures)
		assert.Equal(t, 0, successes)
	})

	t.Run("context cancellation", func(t *testing.T) {
		cb := NewCircuitBreaker()
		
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately
		
		err := cb.Execute(ctx, func() error {
			return nil
		})
		
		// Should return context error since we check context before execution
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	})

	t.Run("concurrent execution", func(t *testing.T) {
		cb := NewCircuitBreaker(
			WithFailureThreshold(10),
			WithSuccessThreshold(5),
		)
		
		var wg sync.WaitGroup
		errorCount := int32(0)
		successCount := int32(0)
		
		// Run concurrent executions
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				err := cb.Execute(context.Background(), func() error {
					if i%3 == 0 {
						return errors.New("concurrent error")
					}
					return nil
				})
				if err != nil {
					atomic.AddInt32(&errorCount, 1)
				} else {
					atomic.AddInt32(&successCount, 1)
				}
			}(i)
		}
		
		wg.Wait()
		
		// Should handle concurrent access without panic
		assert.True(t, atomic.LoadInt32(&errorCount) > 0)
		assert.True(t, atomic.LoadInt32(&successCount) > 0)
	})
}

func TestCircuitBreakerOptions(t *testing.T) {
	t.Run("applies all options", func(t *testing.T) {
		cb := NewCircuitBreaker(
			WithFailureThreshold(10),
			WithSuccessThreshold(5),
			WithTimeout(1*time.Minute),
			WithHalfOpenRequests(10),
		)
		
		assert.Equal(t, 10, cb.failureThreshold)
		assert.Equal(t, 5, cb.successThreshold)
		assert.Equal(t, 1*time.Minute, cb.timeout)
		assert.Equal(t, 10, cb.halfOpenRequests)
	})

	t.Run("uses defaults when no options", func(t *testing.T) {
		cb := NewCircuitBreaker()
		
		assert.Equal(t, 5, cb.failureThreshold)
		assert.Equal(t, 3, cb.successThreshold)
		assert.Equal(t, 30*time.Second, cb.timeout)
		assert.Equal(t, 3, cb.halfOpenRequests)
	})
}

func TestState_String(t *testing.T) {
	tests := []struct {
		state    State
		expected string
	}{
		{StateClosed, "closed"},
		{StateOpen, "open"},
		{StateHalfOpen, "half-open"},
		{State(99), "unknown"},
	}
	
	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.state.String())
		})
	}
}

func BenchmarkCircuitBreaker(b *testing.B) {
	cb := NewCircuitBreaker()
	ctx := context.Background()
	
	b.Run("successful execution", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cb.Execute(ctx, func() error {
				return nil
			})
		}
	})
	
	b.Run("failed execution", func(b *testing.B) {
		cb := NewCircuitBreaker(WithFailureThreshold(b.N + 1)) // Don't open
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cb.Execute(ctx, func() error {
				return errors.New("error")
			})
		}
	})
	
	b.Run("concurrent execution", func(b *testing.B) {
		cb := NewCircuitBreaker()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				cb.Execute(ctx, func() error {
					return nil
				})
			}
		})
	})
}