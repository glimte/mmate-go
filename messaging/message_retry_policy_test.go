package messaging

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/stretchr/testify/assert"
)

// MockRetryPolicy is a configurable retry policy for testing
type MockRetryPolicy struct {
	shouldRetryFunc func(attempts int, err error) bool
	nextDelayFunc   func(attempts int) time.Duration
	maxAttempts     int
}

func (m *MockRetryPolicy) ShouldRetry(attempts int, err error) bool {
	if m.shouldRetryFunc != nil {
		return m.shouldRetryFunc(attempts, err)
	}
	return attempts < m.maxAttempts
}

func (m *MockRetryPolicy) NextDelay(attempts int) time.Duration {
	if m.nextDelayFunc != nil {
		return m.nextDelayFunc(attempts)
	}
	return 10 * time.Millisecond
}

func (m *MockRetryPolicy) MaxAttempts() int {
	return m.maxAttempts
}

func TestSimpleRetryPolicy(t *testing.T) {
	t.Run("Basic functionality", func(t *testing.T) {
		policy := NewSimpleRetryPolicy(3, 100*time.Millisecond)
		
		assert.Equal(t, 3, policy.MaxAttempts())
		assert.Equal(t, 100*time.Millisecond, policy.NextDelay(0))
		assert.Equal(t, 100*time.Millisecond, policy.NextDelay(1))
		assert.Equal(t, 100*time.Millisecond, policy.NextDelay(2))
		
		// Should retry for attempts 0, 1, 2
		assert.True(t, policy.ShouldRetry(0, errors.New("error")))
		assert.True(t, policy.ShouldRetry(1, errors.New("error")))
		assert.True(t, policy.ShouldRetry(2, errors.New("error")))
		
		// Should not retry for attempt 3
		assert.False(t, policy.ShouldRetry(3, errors.New("error")))
	})
	
	t.Run("Zero attempts", func(t *testing.T) {
		policy := NewSimpleRetryPolicy(0, 50*time.Millisecond)
		
		assert.Equal(t, 0, policy.MaxAttempts())
		assert.False(t, policy.ShouldRetry(0, errors.New("error")))
	})
}

// TestMessageRetryWithHandler tests retry policy with message handlers
func TestMessageRetryWithHandler(t *testing.T) {
	t.Run("Retry on handler error", func(t *testing.T) {
		attemptCount := 0
		retryPolicy := NewSimpleRetryPolicy(3, 10*time.Millisecond)
		
		// Create a handler that fails first 2 times
		handler := func(ctx context.Context, env *contracts.Envelope) error {
			attemptCount++
			if attemptCount < 3 {
				return errors.New("temporary error")
			}
			return nil
		}
		
		// Simulate message processing with retry
		err := processWithRetry(context.Background(), &contracts.Envelope{
			ID:   "test-msg",
			Type: "test.message",
		}, handler, retryPolicy)
		
		assert.NoError(t, err)
		assert.Equal(t, 3, attemptCount)
	})
	
	t.Run("Max retries exhausted", func(t *testing.T) {
		attemptCount := 0
		retryPolicy := NewSimpleRetryPolicy(2, 10*time.Millisecond)
		
		// Handler that always fails
		handler := func(ctx context.Context, env *contracts.Envelope) error {
			attemptCount++
			return errors.New("persistent error")
		}
		
		err := processWithRetry(context.Background(), &contracts.Envelope{
			ID:   "test-msg",
			Type: "test.message",
		}, handler, retryPolicy)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "persistent error")
		assert.Equal(t, 3, attemptCount) // Initial + 2 retries
	})
}

// TestRetryPolicyWithExponentialBackoff tests exponential backoff-like behavior
func TestRetryPolicyWithExponentialBackoff(t *testing.T) {
	t.Run("Exponential delay pattern", func(t *testing.T) {
		baseDelay := 10 * time.Millisecond
		policy := &MockRetryPolicy{
			maxAttempts: 5,
			nextDelayFunc: func(attempts int) time.Duration {
				// Exponential backoff: 10ms, 20ms, 40ms, 80ms, 160ms
				return baseDelay * time.Duration(1<<attempts)
			},
		}
		
		delays := []time.Duration{
			policy.NextDelay(0),
			policy.NextDelay(1),
			policy.NextDelay(2),
			policy.NextDelay(3),
			policy.NextDelay(4),
		}
		
		assert.Equal(t, 10*time.Millisecond, delays[0])
		assert.Equal(t, 20*time.Millisecond, delays[1])
		assert.Equal(t, 40*time.Millisecond, delays[2])
		assert.Equal(t, 80*time.Millisecond, delays[3])
		assert.Equal(t, 160*time.Millisecond, delays[4])
	})
}

// NonRetryableError is a test error type that should not be retried
type NonRetryableError struct {
	msg string
}

func (e NonRetryableError) Error() string {
	return e.msg
}

// TestRetryPolicyErrorTypes tests retry behavior with different error types
func TestRetryPolicyErrorTypes(t *testing.T) {
	t.Run("Skip retry on non-retryable errors", func(t *testing.T) {
		policy := &MockRetryPolicy{
			maxAttempts: 5,
			shouldRetryFunc: func(attempts int, err error) bool {
				// Don't retry NonRetryableError
				if _, ok := err.(NonRetryableError); ok {
					return false
				}
				return attempts < 5
			},
		}
		
		// Retryable error
		assert.True(t, policy.ShouldRetry(0, errors.New("temporary")))
		
		// Non-retryable error
		assert.False(t, policy.ShouldRetry(0, NonRetryableError{"fatal error"}))
	})
}

// TestConcurrentRetries tests retry policy under concurrent access
func TestConcurrentRetries(t *testing.T) {
	t.Run("Thread-safe retry counting", func(t *testing.T) {
		policy := NewSimpleRetryPolicy(3, 5*time.Millisecond)
		
		var totalAttempts int64
		var successCount int64
		var wg sync.WaitGroup
		
		// Simulate 10 concurrent message processors
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				
				attempts := 0
				handler := func(ctx context.Context, env *contracts.Envelope) error {
					atomic.AddInt64(&totalAttempts, 1)
					attempts++
					if attempts < 2 {
						return errors.New("retry needed")
					}
					atomic.AddInt64(&successCount, 1)
					return nil
				}
				
				processWithRetry(context.Background(), &contracts.Envelope{
					ID: "msg-" + string(rune('0'+id)),
				}, handler, policy)
			}(i)
		}
		
		wg.Wait()
		
		// Each processor should attempt twice (1 fail + 1 success)
		assert.Equal(t, int64(20), atomic.LoadInt64(&totalAttempts))
		assert.Equal(t, int64(10), atomic.LoadInt64(&successCount))
	})
}

// TestRetryPolicyWithContext tests retry behavior with context cancellation
func TestRetryPolicyWithContext(t *testing.T) {
	t.Run("Context cancellation stops retries", func(t *testing.T) {
		policy := NewSimpleRetryPolicy(10, 50*time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		
		attemptCount := int32(0)
		
		// Cancel context after 75ms
		go func() {
			time.Sleep(75 * time.Millisecond)
			cancel()
		}()
		
		handler := func(ctx context.Context, env *contracts.Envelope) error {
			atomic.AddInt32(&attemptCount, 1)
			return errors.New("error")
		}
		
		err := processWithRetry(ctx, &contracts.Envelope{
			ID: "test-msg",
		}, handler, policy)
		
		assert.Equal(t, context.Canceled, err)
		// Should have attempted 1-2 times before cancellation
		attempts := atomic.LoadInt32(&attemptCount)
		assert.GreaterOrEqual(t, attempts, int32(1))
		assert.LessOrEqual(t, attempts, int32(3))
	})
	
	t.Run("Context deadline respected", func(t *testing.T) {
		policy := NewSimpleRetryPolicy(10, 30*time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		
		attemptCount := 0
		start := time.Now()
		
		handler := func(ctx context.Context, env *contracts.Envelope) error {
			attemptCount++
			return errors.New("error")
		}
		
		err := processWithRetry(ctx, &contracts.Envelope{
			ID: "test-msg",
		}, handler, policy)
		
		duration := time.Since(start)
		assert.Equal(t, context.DeadlineExceeded, err)
		assert.Less(t, attemptCount, 10) // Should not reach max retries
		assert.Less(t, duration, 150*time.Millisecond)
	})
}

// TestRetryPolicyMetrics tests retry metrics collection
func TestRetryPolicyMetrics(t *testing.T) {
	t.Run("Track retry metrics", func(t *testing.T) {
		policy := NewSimpleRetryPolicy(3, 10*time.Millisecond)
		
		metrics := &retryMetrics{
			attempts:    make(map[string]int),
			successes:   make(map[string]int),
			failures:    make(map[string]int),
			totalDelay:  make(map[string]time.Duration),
		}
		
		// Process messages with different outcomes
		messages := []struct {
			id           string
			failureCount int
		}{
			{"msg-1", 0}, // Succeeds immediately
			{"msg-2", 2}, // Fails twice, then succeeds
			{"msg-3", 5}, // Exceeds retry limit
		}
		
		for _, msg := range messages {
			attempts := 0
			handler := func(ctx context.Context, env *contracts.Envelope) error {
				attempts++
				if attempts <= msg.failureCount {
					return errors.New("error")
				}
				return nil
			}
			
			err := processWithRetryAndMetrics(
				context.Background(),
				&contracts.Envelope{ID: msg.id},
				handler,
				policy,
				metrics,
			)
			
			if msg.failureCount >= policy.MaxAttempts() {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		}
		
		// Verify metrics
		assert.Equal(t, 1, metrics.attempts["msg-1"])
		assert.Equal(t, 3, metrics.attempts["msg-2"])
		assert.Equal(t, 4, metrics.attempts["msg-3"]) // Initial + 3 retries
		
		assert.Equal(t, 1, metrics.successes["msg-1"])
		assert.Equal(t, 1, metrics.successes["msg-2"])
		assert.Equal(t, 0, metrics.successes["msg-3"])
		
		assert.Equal(t, 0, metrics.failures["msg-1"])
		assert.Equal(t, 0, metrics.failures["msg-2"])
		assert.Equal(t, 1, metrics.failures["msg-3"])
	})
}

// TestRetryPolicyIntegrationWithDeadLetterQueue tests DLQ integration
func TestRetryPolicyIntegrationWithDeadLetterQueue(t *testing.T) {
	t.Run("Send to DLQ after max retries", func(t *testing.T) {
		policy := NewSimpleRetryPolicy(2, 10*time.Millisecond)
		dlq := &mockDLQ{
			messages: make([]*contracts.Envelope, 0),
		}
		
		// Handler that always fails
		handler := func(ctx context.Context, env *contracts.Envelope) error {
			return errors.New("persistent failure")
		}
		
		// Process with retry and DLQ
		envelope := &contracts.Envelope{
			ID:   "failed-msg",
			Type: "test.message",
		}
		
		err := processWithRetryAndDLQ(
			context.Background(),
			envelope,
			handler,
			policy,
			dlq,
		)
		
		assert.Error(t, err)
		assert.Len(t, dlq.messages, 1)
		assert.Equal(t, "failed-msg", dlq.messages[0].ID)
		
		// Check retry count was added to headers
		retryCount, ok := dlq.messages[0].Headers["x-retry-count"]
		assert.True(t, ok)
		assert.Equal(t, 3, retryCount) // Initial + 2 retries
	})
}

// Helper functions for testing

func processWithRetry(ctx context.Context, env *contracts.Envelope, handler func(context.Context, *contracts.Envelope) error, policy RetryPolicy) error {
	attempts := 0
	for {
		err := handler(ctx, env)
		if err == nil {
			return nil
		}
		
		if !policy.ShouldRetry(attempts, err) {
			return err
		}
		
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(policy.NextDelay(attempts)):
			attempts++
		}
	}
}

type retryMetrics struct {
	mu         sync.Mutex
	attempts   map[string]int
	successes  map[string]int
	failures   map[string]int
	totalDelay map[string]time.Duration
}

func processWithRetryAndMetrics(ctx context.Context, env *contracts.Envelope, handler func(context.Context, *contracts.Envelope) error, policy RetryPolicy, metrics *retryMetrics) error {
	metrics.mu.Lock()
	metrics.attempts[env.ID] = 0
	metrics.mu.Unlock()
	
	attempts := 0
	for {
		metrics.mu.Lock()
		metrics.attempts[env.ID]++
		metrics.mu.Unlock()
		
		err := handler(ctx, env)
		if err == nil {
			metrics.mu.Lock()
			metrics.successes[env.ID]++
			metrics.mu.Unlock()
			return nil
		}
		
		if !policy.ShouldRetry(attempts, err) {
			metrics.mu.Lock()
			metrics.failures[env.ID]++
			metrics.mu.Unlock()
			return err
		}
		
		delay := policy.NextDelay(attempts)
		metrics.mu.Lock()
		metrics.totalDelay[env.ID] += delay
		metrics.mu.Unlock()
		
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			attempts++
		}
	}
}

type mockDLQ struct {
	mu       sync.Mutex
	messages []*contracts.Envelope
}

func (d *mockDLQ) Send(env *contracts.Envelope) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.messages = append(d.messages, env)
	return nil
}

func processWithRetryAndDLQ(ctx context.Context, env *contracts.Envelope, handler func(context.Context, *contracts.Envelope) error, policy RetryPolicy, dlq *mockDLQ) error {
	attempts := 0
	for {
		err := handler(ctx, env)
		if err == nil {
			return nil
		}
		
		if !policy.ShouldRetry(attempts, err) {
			// Add retry count to headers
			if env.Headers == nil {
				env.Headers = make(map[string]interface{})
			}
			env.Headers["x-retry-count"] = attempts + 1
			env.Headers["x-last-error"] = err.Error()
			
			// Send to DLQ
			dlqErr := dlq.Send(env)
			if dlqErr != nil {
				return errors.New("failed to send to DLQ: " + dlqErr.Error())
			}
			return err
		}
		
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(policy.NextDelay(attempts)):
			attempts++
		}
	}
}

// Benchmark tests
func BenchmarkSimpleRetryPolicy(b *testing.B) {
	policy := NewSimpleRetryPolicy(3, 1*time.Microsecond)
	ctx := context.Background()
	
	b.Run("Successful operation", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			processWithRetry(ctx, &contracts.Envelope{ID: "bench"}, func(ctx context.Context, env *contracts.Envelope) error {
				return nil
			}, policy)
		}
	})
	
	b.Run("Operation with retries", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			attempts := 0
			processWithRetry(ctx, &contracts.Envelope{ID: "bench"}, func(ctx context.Context, env *contracts.Envelope) error {
				attempts++
				if attempts < 2 {
					return errors.New("error")
				}
				return nil
			}, policy)
		}
	})
}