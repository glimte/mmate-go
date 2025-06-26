package messaging

import (
	"context"
	"time"

	"github.com/glimte/mmate-go/contracts"
)

// Publisher publishes messages
type Publisher interface {
	// Publish publishes a message with routing
	Publish(ctx context.Context, msg contracts.Message, options ...PublishOption) error

	// PublishEvent publishes an event
	PublishEvent(ctx context.Context, event contracts.Event, options ...PublishOption) error

	// PublishCommand publishes a command
	PublishCommand(ctx context.Context, command contracts.Command, options ...PublishOption) error

	// Close closes the publisher
	Close() error
}

// Subscriber subscribes to messages
type Subscriber interface {
	// Subscribe subscribes to messages on a queue
	Subscribe(ctx context.Context, queue string, messageType string, handler MessageHandler, options ...SubscriptionOption) error

	// Unsubscribe unsubscribes from a queue
	Unsubscribe(queue string) error

	// Close closes the subscriber
	Close() error
}

// MetricsCollector collects messaging metrics
type MetricsCollector interface {
	// RecordMessage records a message processing metric
	RecordMessage(messageType string, duration time.Duration, success bool, errorType string)

	// RecordPublish records a publish metric
	RecordPublish(messageType string, exchange string, duration time.Duration, success bool)

	// RecordSubscribe records a subscription metric
	RecordSubscribe(queue string, messageType string)

	// RecordError records an error metric
	RecordError(component string, errorType string, message string)

	// GetStats returns current stats
	GetStats() MetricsStats
}

// MetricsStats contains messaging statistics
type MetricsStats struct {
	MessagesProcessed   int64
	MessagesPublished   int64
	MessagesFailed      int64
	AverageProcessTime  time.Duration
	ActiveSubscriptions int
	ErrorCount          int64
}

// NoOpMetricsCollector is a no-op implementation of MetricsCollector
type NoOpMetricsCollector struct{}

// RecordMessage does nothing
func (n *NoOpMetricsCollector) RecordMessage(messageType string, duration time.Duration, success bool, errorType string) {
}

// RecordPublish does nothing
func (n *NoOpMetricsCollector) RecordPublish(messageType string, exchange string, duration time.Duration, success bool) {
}

// RecordSubscribe does nothing
func (n *NoOpMetricsCollector) RecordSubscribe(queue string, messageType string) {}

// RecordError does nothing
func (n *NoOpMetricsCollector) RecordError(component string, errorType string, message string) {}

// GetStats returns empty stats
func (n *NoOpMetricsCollector) GetStats() MetricsStats {
	return MetricsStats{}
}

// RetryPolicy defines message retry behavior
type RetryPolicy interface {
	// ShouldRetry determines if a message should be retried
	ShouldRetry(attempts int, err error) bool

	// NextDelay returns the delay before next retry
	NextDelay(attempts int) time.Duration

	// MaxAttempts returns the maximum number of attempts
	MaxAttempts() int
}

// SimpleRetryPolicy implements a simple retry policy
type SimpleRetryPolicy struct {
	maxAttempts int
	delay       time.Duration
}

// NewSimpleRetryPolicy creates a new simple retry policy
func NewSimpleRetryPolicy(maxAttempts int, delay time.Duration) *SimpleRetryPolicy {
	return &SimpleRetryPolicy{
		maxAttempts: maxAttempts,
		delay:       delay,
	}
}

// ShouldRetry determines if a message should be retried
func (p *SimpleRetryPolicy) ShouldRetry(attempts int, err error) bool {
	return attempts < p.maxAttempts
}

// NextDelay returns the delay before next retry
func (p *SimpleRetryPolicy) NextDelay(attempts int) time.Duration {
	return p.delay
}

// MaxAttempts returns the maximum number of attempts
func (p *SimpleRetryPolicy) MaxAttempts() int {
	return p.maxAttempts
}

// MessageProcessor processes messages in a pipeline
type MessageProcessor interface {
	// Process processes a message
	Process(ctx context.Context, msg contracts.Message) (contracts.Message, error)
}

// ProcessorFunc is a function that implements MessageProcessor
type ProcessorFunc func(ctx context.Context, msg contracts.Message) (contracts.Message, error)

// Process implements MessageProcessor
func (f ProcessorFunc) Process(ctx context.Context, msg contracts.Message) (contracts.Message, error) {
	return f(ctx, msg)
}
