package messaging

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/stretchr/testify/assert"
)

// Simple unit tests that don't require complex mocking

func TestMessageHandlerFunc(t *testing.T) {
	t.Run("MessageHandlerFunc implements MessageHandler", func(t *testing.T) {
		called := false
		var receivedMsg contracts.Message
		var receivedCtx context.Context
		
		handlerFunc := MessageHandlerFunc(func(ctx context.Context, msg contracts.Message) error {
			called = true
			receivedMsg = msg
			receivedCtx = ctx
			return nil
		})
		
		msg := &TestCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
		}
		ctx := context.Background()
		
		err := handlerFunc.Handle(ctx, msg)
		
		assert.NoError(t, err)
		assert.True(t, called)
		assert.Equal(t, msg, receivedMsg)
		assert.Equal(t, ctx, receivedCtx)
	})
	
	t.Run("MessageHandlerFunc can return error", func(t *testing.T) {
		expectedError := errors.New("handler error")
		handlerFunc := MessageHandlerFunc(func(ctx context.Context, msg contracts.Message) error {
			return expectedError
		})
		
		msg := &TestCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
		}
		
		err := handlerFunc.Handle(context.Background(), msg)
		
		assert.Equal(t, expectedError, err)
	})
}

func TestHandlerOptionsSimple(t *testing.T) {
	t.Run("WithConcurrency option", func(t *testing.T) {
		opts := HandlerOptions{}
		option := WithConcurrency(10)
		option(&opts)
		
		assert.Equal(t, 10, opts.Concurrency)
	})
	
	t.Run("WithQueue option", func(t *testing.T) {
		opts := HandlerOptions{}
		option := WithQueue("test.queue")
		option(&opts)
		
		assert.Equal(t, "test.queue", opts.Queue)
	})
	
	t.Run("WithDurable option", func(t *testing.T) {
		opts := HandlerOptions{}
		option := WithDurable(false)
		option(&opts)
		
		assert.False(t, opts.Durable)
	})
	
	t.Run("WithExclusive option", func(t *testing.T) {
		opts := HandlerOptions{}
		option := WithExclusive(true)
		option(&opts)
		
		assert.True(t, opts.Exclusive)
	})
}

func TestErrorActions(t *testing.T) {
	t.Run("ErrorAction constants have correct values", func(t *testing.T) {
		assert.Equal(t, ErrorAction(0), Acknowledge)
		assert.Equal(t, ErrorAction(1), Retry)
		assert.Equal(t, ErrorAction(2), Reject)
	})
}

func TestMiddlewareFunc(t *testing.T) {
	t.Run("Middleware can modify message flow", func(t *testing.T) {
		var executionOrder []string
		
		// Create middleware that logs execution
		middleware := func(ctx context.Context, msg contracts.Message, next MessageHandler) error {
			executionOrder = append(executionOrder, "middleware-start")
			err := next.Handle(ctx, msg)
			executionOrder = append(executionOrder, "middleware-end")
			return err
		}
		
		// Create final handler
		finalHandler := MessageHandlerFunc(func(ctx context.Context, msg contracts.Message) error {
			executionOrder = append(executionOrder, "handler")
			return nil
		})
		
		// Create message
		msg := &TestCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
		}
		
		// Execute middleware
		err := middleware(context.Background(), msg, finalHandler)
		
		assert.NoError(t, err)
		assert.Equal(t, []string{"middleware-start", "handler", "middleware-end"}, executionOrder)
	})
	
	t.Run("Middleware can handle errors", func(t *testing.T) {
		expectedError := errors.New("handler error")
		
		middleware := func(ctx context.Context, msg contracts.Message, next MessageHandler) error {
			err := next.Handle(ctx, msg)
			if err != nil {
				// Transform the error
				return errors.New("middleware: " + err.Error())
			}
			return nil
		}
		
		failingHandler := MessageHandlerFunc(func(ctx context.Context, msg contracts.Message) error {
			return expectedError
		})
		
		msg := &TestCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
		}
		
		err := middleware(context.Background(), msg, failingHandler)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "middleware:")
		assert.Contains(t, err.Error(), "handler error")
	})
}

func TestSubscriptionOptions(t *testing.T) {
	t.Run("WithPrefetchCount sets prefetch count", func(t *testing.T) {
		opts := SubscriptionOptions{}
		WithPrefetchCount(20)(&opts)
		assert.Equal(t, 20, opts.PrefetchCount)
	})
	
	t.Run("WithAutoAck sets auto ack", func(t *testing.T) {
		opts := SubscriptionOptions{}
		WithAutoAck(true)(&opts)
		assert.True(t, opts.AutoAck)
	})
	
	t.Run("WithSubscriberExclusive sets exclusive", func(t *testing.T) {
		opts := SubscriptionOptions{}
		WithSubscriberExclusive(true)(&opts)
		assert.True(t, opts.Exclusive)
	})
	
	t.Run("WithSubscriberDurable sets durable", func(t *testing.T) {
		opts := SubscriptionOptions{}
		WithSubscriberDurable(false)(&opts)
		assert.False(t, opts.Durable)
	})
	
	t.Run("WithAutoDelete sets auto delete", func(t *testing.T) {
		opts := SubscriptionOptions{}
		WithAutoDelete(true)(&opts)
		assert.True(t, opts.AutoDelete)
	})
	
	t.Run("WithMaxRetries sets max retries", func(t *testing.T) {
		opts := SubscriptionOptions{}
		WithMaxRetries(5)(&opts)
		assert.Equal(t, 5, opts.MaxRetries)
	})
	
	t.Run("WithDeadLetterExchange sets DLX", func(t *testing.T) {
		opts := SubscriptionOptions{}
		WithDeadLetterExchange("custom.dlx")(&opts)
		assert.Equal(t, "custom.dlx", opts.DeadLetterExchange)
	})
}

func TestPublishOptions(t *testing.T) {
	t.Run("WithExchange sets exchange", func(t *testing.T) {
		opts := PublishOptions{}
		WithExchange("test.exchange")(&opts)
		assert.Equal(t, "test.exchange", opts.Exchange)
	})
	
	t.Run("WithRoutingKey sets routing key", func(t *testing.T) {
		opts := PublishOptions{}
		WithRoutingKey("test.key")(&opts)
		assert.Equal(t, "test.key", opts.RoutingKey)
	})
	
	t.Run("WithTTL sets TTL", func(t *testing.T) {
		opts := PublishOptions{}
		WithTTL(30 * time.Second)(&opts)
		assert.Equal(t, 30*time.Second, opts.TTL)
	})
	
	t.Run("WithPriority sets priority", func(t *testing.T) {
		opts := PublishOptions{}
		WithPriority(5)(&opts)
		assert.Equal(t, uint8(5), opts.Priority)
	})
	
	t.Run("WithPersistent sets delivery mode", func(t *testing.T) {
		opts := PublishOptions{}
		WithPersistent(true)(&opts)
		assert.Equal(t, uint8(2), opts.DeliveryMode)
		
		WithPersistent(false)(&opts)
		assert.Equal(t, uint8(1), opts.DeliveryMode)
	})
	
	t.Run("WithHeaders sets headers", func(t *testing.T) {
		opts := PublishOptions{}
		headers := map[string]interface{}{"key": "value"}
		WithHeaders(headers)(&opts)
		assert.Equal(t, "value", opts.Headers["key"])
	})
	
	t.Run("WithConfirmDelivery sets confirm flag", func(t *testing.T) {
		opts := PublishOptions{}
		WithConfirmDelivery(true)(&opts)
		assert.True(t, opts.ConfirmDelivery)
	})
}

func TestSagaStatus(t *testing.T) {
	t.Run("SagaStatus structure", func(t *testing.T) {
		now := time.Now()
		step := SagaStep{
			StepID:      "step-1",
			Status:      "completed",
			ExecutedAt:  &now,
			Error:       "",
		}
		
		status := SagaStatus{
			CorrelationID: "saga-123",
			Status:        "running",
			StartedAt:     now,
			LastEvent:     "OrderCreated",
			Steps:         []SagaStep{step},
		}
		
		assert.Equal(t, "saga-123", status.CorrelationID)
		assert.Equal(t, "running", status.Status)
		assert.Equal(t, "OrderCreated", status.LastEvent)
		assert.Len(t, status.Steps, 1)
		assert.Equal(t, "step-1", status.Steps[0].StepID)
		assert.Equal(t, "completed", status.Steps[0].Status)
	})
}