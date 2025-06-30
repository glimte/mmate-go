package interceptors

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/internal/reliability"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Mock retry policy for testing
type mockRetryPolicy struct {
	mock.Mock
}

func (m *mockRetryPolicy) ShouldRetry(attempt int, err error) (bool, time.Duration) {
	args := m.Called(attempt, err)
	return args.Bool(0), args.Get(1).(time.Duration)
}

func (m *mockRetryPolicy) MaxRetries() int {
	args := m.Called()
	return args.Int(0)
}

func (m *mockRetryPolicy) NextDelay(attempt int) time.Duration {
	args := m.Called(attempt)
	return args.Get(0).(time.Duration)
}

func TestRetryInterceptor(t *testing.T) {
	t.Run("NewRetryInterceptor creates interceptor", func(t *testing.T) {
		policy := &mockRetryPolicy{}
		interceptor := NewRetryInterceptor(policy)
		
		assert.NotNil(t, interceptor)
		assert.Equal(t, policy, interceptor.retryPolicy)
		assert.NotNil(t, interceptor.logger)
		assert.Equal(t, "RetryInterceptor", interceptor.Name())
	})
	
	t.Run("WithLogger sets the logger", func(t *testing.T) {
		policy := &mockRetryPolicy{}
		logger := slog.Default()
		interceptor := NewRetryInterceptor(policy).WithLogger(logger)
		
		assert.Equal(t, logger, interceptor.logger)
	})
	
	t.Run("Intercept succeeds on first attempt", func(t *testing.T) {
		policy := &mockRetryPolicy{}
		interceptor := NewRetryInterceptor(policy)
		handler := &mockHandler{}
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		
		handler.On("Handle", mock.Anything, msg).Return(nil)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.NoError(t, err)
		handler.AssertExpectations(t)
		// Retry policy should not be called on success
		policy.AssertNotCalled(t, "ShouldRetry")
	})
	
	t.Run("Intercept retries on failure with exponential backoff", func(t *testing.T) {
		// Use real exponential backoff policy for this test
		policy := reliability.NewExponentialBackoff(5*time.Millisecond, 100*time.Millisecond, 2.0, 2)
		interceptor := NewRetryInterceptor(policy)
		
		callCount := 0
		handler := MessageHandlerFunc(func(ctx context.Context, msg contracts.Message) error {
			callCount++
			if callCount < 2 {
				return errors.New("temporary error")
			}
			return nil // Success on second try
		})
		
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.NoError(t, err)
		assert.Equal(t, 2, callCount)
		// The retry happened - we know because callCount is 2
		// Don't assert on timing as it can be flaky on different systems
	})
	
	t.Run("Intercept fails after max retries", func(t *testing.T) {
		policy := reliability.NewExponentialBackoff(1*time.Millisecond, 10*time.Millisecond, 2.0, 2)
		interceptor := NewRetryInterceptor(policy)
		
		callCount := 0
		persistentError := errors.New("persistent error")
		handler := MessageHandlerFunc(func(ctx context.Context, msg contracts.Message) error {
			callCount++
			return persistentError
		})
		
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.Error(t, err)
		assert.Equal(t, persistentError, err)
		assert.Equal(t, 3, callCount) // Initial attempt + 2 retries
	})
	
	t.Run("Intercept respects context cancellation", func(t *testing.T) {
		policy := reliability.NewExponentialBackoff(100*time.Millisecond, 1*time.Second, 2.0, 5)
		interceptor := NewRetryInterceptor(policy)
		
		handler := MessageHandlerFunc(func(ctx context.Context, msg contracts.Message) error {
			return errors.New("always fails")
		})
		
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		
		// Create context that cancels quickly
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		
		start := time.Now()
		err := interceptor.Intercept(ctx, msg, handler)
		duration := time.Since(start)
		
		assert.Error(t, err)
		// Should return quickly due to context cancellation
		assert.Less(t, duration, 200*time.Millisecond)
		assert.Contains(t, err.Error(), "context")
	})
	
	t.Run("Intercept with non-retryable error", func(t *testing.T) {
		policy := reliability.NewExponentialBackoff(1*time.Millisecond, 10*time.Millisecond, 2.0, 3)
		interceptor := NewRetryInterceptor(policy)
		
		callCount := 0
		// Create a non-retryable error
		nonRetryableError := reliability.RetryableError{
			Err:       errors.New("non-retryable error"),
			Retryable: false,
		}
		
		handler := MessageHandlerFunc(func(ctx context.Context, msg contracts.Message) error {
			callCount++
			return nonRetryableError
		})
		
		msg := &testMessage{BaseMessage: contracts.NewBaseMessage("test")}
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.Error(t, err)
		assert.Equal(t, 1, callCount) // Should not retry
		assert.Contains(t, err.Error(), "non-retryable error")
	})
}