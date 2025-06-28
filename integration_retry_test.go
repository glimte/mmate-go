package mmate

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/interceptors"
	"github.com/glimte/mmate-go/internal/reliability"
	"github.com/stretchr/testify/assert"
)

// TestMessage for integration testing
type IntegrationTestMessage struct {
	contracts.BaseMessage
	Content string `json:"content"`
}

func NewIntegrationTestMessage(content string) *IntegrationTestMessage {
	return &IntegrationTestMessage{
		BaseMessage: contracts.NewBaseMessage("IntegrationTestMessage"),
		Content:     content,
	}
}

func TestRetryIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Run("Client with default retry configuration", func(t *testing.T) {
		// Test that the client can be created with default retry enabled
		client, err := NewClientWithOptions(
			"amqp://guest:guest@localhost:5672/",
			WithServiceName("test-retry-service"),
			WithDefaultRetry(),
			WithLogger(slog.Default()),
		)
		
		// Note: This will fail if RabbitMQ is not running, which is expected
		// The test verifies the configuration is wired correctly
		if err != nil {
			t.Logf("Expected error when RabbitMQ not available: %v", err)
			return
		}
		
		defer client.Close()
		
		// Verify the service queue was created
		assert.NotEmpty(t, client.ServiceQueue())
		assert.Equal(t, "test-retry-service-queue", client.ServiceQueue())
	})

	t.Run("Client with custom retry policy", func(t *testing.T) {
		// Create custom retry policy
		retryPolicy := reliability.NewExponentialBackoff(
			50*time.Millisecond,  // initial delay
			5*time.Second,        // max delay
			2.0,                  // multiplier
			5,                    // max retries
		)
		
		client, err := NewClientWithOptions(
			"amqp://guest:guest@localhost:5672/",
			WithServiceName("test-custom-retry-service"),
			WithRetryPolicy(retryPolicy),
			WithLogger(slog.Default()),
		)
		
		if err != nil {
			t.Logf("Expected error when RabbitMQ not available: %v", err)
			return
		}
		
		defer client.Close()
		
		// Verify service queue
		assert.Equal(t, "test-custom-retry-service-queue", client.ServiceQueue())
	})

	t.Run("Client with DLQ handler", func(t *testing.T) {
		// Create DLQ handler with in-memory error store
		errorStore := reliability.NewInMemoryErrorStore()
		dlqHandler := reliability.NewDLQHandler(
			reliability.WithDLQLogger(slog.Default()),
			reliability.WithDLQMaxRetries(3),
			reliability.WithDLQRetryDelay(100*time.Millisecond),
			reliability.WithErrorStore(errorStore),
		)
		
		client, err := NewClientWithOptions(
			"amqp://guest:guest@localhost:5672/",
			WithServiceName("test-dlq-service"),
			WithDLQHandler(dlqHandler),
			WithDefaultRetry(),
			WithLogger(slog.Default()),
		)
		
		if err != nil {
			t.Logf("Expected error when RabbitMQ not available: %v", err)
			return
		}
		
		defer client.Close()
		
		// Verify service queue
		assert.Equal(t, "test-dlq-service-queue", client.ServiceQueue())
	})

	t.Run("Client with custom pipeline including retry", func(t *testing.T) {
		// Create custom pipeline with retry and logging
		retryPolicy := reliability.NewLinearBackoff(100*time.Millisecond, 2)
		pipeline := interceptors.NewPipeline()
		
		// Add logging interceptor
		pipeline.Use(interceptors.NewLoggingInterceptor(slog.Default()))
		
		// Add retry interceptor
		retryInterceptor := interceptors.NewRetryInterceptor(retryPolicy).WithLogger(slog.Default())
		pipeline.Use(retryInterceptor)
		
		client, err := NewClientWithOptions(
			"amqp://guest:guest@localhost:5672/",
			WithServiceName("test-pipeline-service"),
			WithSubscribeInterceptors(pipeline),
			WithLogger(slog.Default()),
		)
		
		if err != nil {
			t.Logf("Expected error when RabbitMQ not available: %v", err)
			return
		}
		
		defer client.Close()
		
		// Verify service queue
		assert.Equal(t, "test-pipeline-service-queue", client.ServiceQueue())
	})
}

func TestRetryLogic(t *testing.T) {
	t.Run("RetryInterceptor with exponential backoff", func(t *testing.T) {
		policy := reliability.NewExponentialBackoff(1*time.Millisecond, 10*time.Millisecond, 2.0, 3)
		interceptor := interceptors.NewRetryInterceptor(policy)
		
		callCount := 0
		handler := func(ctx context.Context, msg contracts.Message) error {
			callCount++
			if callCount < 3 {
				return errors.New("transient error")
			}
			return nil
		}
		
		handlerFunc := interceptors.MessageHandlerFunc(handler)
		msg := NewIntegrationTestMessage("test")
		
		start := time.Now()
		err := interceptor.Intercept(context.Background(), msg, handlerFunc)
		duration := time.Since(start)
		
		assert.NoError(t, err)
		assert.Equal(t, 3, callCount)
		assert.Greater(t, duration, 1*time.Millisecond) // Should have some delay
	})

	t.Run("RetryInterceptor with linear backoff", func(t *testing.T) {
		policy := reliability.NewLinearBackoff(2*time.Millisecond, 2)
		interceptor := interceptors.NewRetryInterceptor(policy)
		
		callCount := 0
		handler := func(ctx context.Context, msg contracts.Message) error {
			callCount++
			if callCount < 2 {
				return errors.New("transient error")
			}
			return nil
		}
		
		handlerFunc := interceptors.MessageHandlerFunc(handler)
		msg := NewIntegrationTestMessage("test")
		
		start := time.Now()
		err := interceptor.Intercept(context.Background(), msg, handlerFunc)
		duration := time.Since(start)
		
		assert.NoError(t, err)
		assert.Equal(t, 2, callCount)
		assert.Greater(t, duration, 2*time.Millisecond)
	})

	t.Run("RetryInterceptor with fixed delay", func(t *testing.T) {
		policy := reliability.NewFixedDelay(1*time.Millisecond, 2)
		interceptor := interceptors.NewRetryInterceptor(policy)
		
		callCount := 0
		persistentErr := errors.New("persistent error")
		handler := func(ctx context.Context, msg contracts.Message) error {
			callCount++
			return persistentErr
		}
		
		handlerFunc := interceptors.MessageHandlerFunc(handler)
		msg := NewIntegrationTestMessage("test")
		
		err := interceptor.Intercept(context.Background(), msg, handlerFunc)
		
		assert.Equal(t, persistentErr, err)
		assert.Equal(t, 3, callCount) // Initial + 2 retries
	})
}

func TestErrorClassification(t *testing.T) {
	t.Run("Non-retryable errors stop retry", func(t *testing.T) {
		policy := reliability.NewExponentialBackoff(1*time.Millisecond, 10*time.Millisecond, 2.0, 5)
		interceptor := interceptors.NewRetryInterceptor(policy)
		
		callCount := 0
		nonRetryableErr := reliability.RetryableError{
			Err:       errors.New("non-retryable error"),
			Retryable: false,
		}
		
		handler := func(ctx context.Context, msg contracts.Message) error {
			callCount++
			return nonRetryableErr
		}
		
		handlerFunc := interceptors.MessageHandlerFunc(handler)
		msg := NewIntegrationTestMessage("test")
		
		err := interceptor.Intercept(context.Background(), msg, handlerFunc)
		
		assert.Equal(t, nonRetryableErr, err)
		assert.Equal(t, 1, callCount) // Should not retry
	})
	
	t.Run("Retryable errors trigger retry", func(t *testing.T) {
		policy := reliability.NewExponentialBackoff(1*time.Millisecond, 10*time.Millisecond, 2.0, 2)
		interceptor := interceptors.NewRetryInterceptor(policy)
		
		callCount := 0
		retryableErr := reliability.RetryableError{
			Err:       errors.New("retryable error"),
			Retryable: true,
		}
		
		handler := func(ctx context.Context, msg contracts.Message) error {
			callCount++
			return retryableErr
		}
		
		handlerFunc := interceptors.MessageHandlerFunc(handler)
		msg := NewIntegrationTestMessage("test")
		
		err := interceptor.Intercept(context.Background(), msg, handlerFunc)
		
		assert.Equal(t, retryableErr, err)
		assert.Equal(t, 3, callCount) // Initial + 2 retries
	})
}