package reliability

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/internal/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// TTLRetryScheduler implements retry scheduling using RabbitMQ TTL and DLX
type TTLRetryScheduler struct {
	channelPool      *rabbitmq.ChannelPool
	topologyManager  *rabbitmq.TopologyManager
	logger          *slog.Logger
	retryExchange   string
	delayExchange   string
	mu              sync.RWMutex
	delayQueues     map[string]bool // Track created delay queues
}

// RetryMessage represents a message scheduled for retry
type RetryMessage struct {
	MessageID       string                 `json:"messageId"`
	MessageType     string                 `json:"messageType"`
	CorrelationID   string                 `json:"correlationId"`
	OriginalPayload json.RawMessage        `json:"originalPayload"`
	OriginalQueue   string                 `json:"originalQueue"`
	AttemptNumber   int                    `json:"attemptNumber"`
	MaxAttempts     int                    `json:"maxAttempts"`
	RetryPolicy     string                 `json:"retryPolicy"`
	ScheduledAt     time.Time              `json:"scheduledAt"`
	RetryAt         time.Time              `json:"retryAt"`
	LastError       string                 `json:"lastError,omitempty"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
}

// TTLRetrySchedulerOptions configures the TTL retry scheduler
type TTLRetrySchedulerOptions struct {
	RetryExchange string
	DelayExchange string
	Logger        *slog.Logger
}

// NewTTLRetryScheduler creates a new TTL-based retry scheduler
func NewTTLRetryScheduler(pool *rabbitmq.ChannelPool, opts *TTLRetrySchedulerOptions) *TTLRetryScheduler {
	if opts == nil {
		opts = &TTLRetrySchedulerOptions{}
	}

	if opts.RetryExchange == "" {
		opts.RetryExchange = "mmate.retry"
	}
	if opts.DelayExchange == "" {
		opts.DelayExchange = "mmate.retry.delay"
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	scheduler := &TTLRetryScheduler{
		channelPool:     pool,
		topologyManager: rabbitmq.NewTopologyManager(pool),
		logger:          opts.Logger,
		retryExchange:   opts.RetryExchange,
		delayExchange:   opts.DelayExchange,
		delayQueues:     make(map[string]bool),
	}

	return scheduler
}

// Initialize sets up the required RabbitMQ topology for TTL-based retries
func (s *TTLRetryScheduler) Initialize(ctx context.Context) error {
	s.logger.Info("Initializing TTL retry scheduler topology")

	// Create retry exchanges
	exchanges := []rabbitmq.ExchangeDeclaration{
		{
			Name:    s.retryExchange,
			Type:    "direct",
			Durable: true,
		},
		{
			Name:    s.delayExchange,
			Type:    "direct",
			Durable: true,
		},
	}

	for _, exchange := range exchanges {
		if err := s.topologyManager.DeclareExchange(ctx, exchange); err != nil {
			return fmt.Errorf("failed to declare exchange %s: %w", exchange.Name, err)
		}
	}

	s.logger.Info("TTL retry scheduler topology initialized")
	return nil
}

// ScheduleRetry schedules a message for retry using TTL and DLX
func (s *TTLRetryScheduler) ScheduleRetry(ctx context.Context, msg contracts.Message, originalQueue string, attempt int, policy RetryPolicy, delay time.Duration, lastErr error) error {
	// Check if we should retry
	shouldRetry, _ := policy.ShouldRetry(attempt, lastErr)
	if !shouldRetry {
		return fmt.Errorf("retry policy indicates no more retries for attempt %d", attempt)
	}

	// Serialize original message
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to serialize message for retry: %w", err)
	}

	retryMessage := &RetryMessage{
		MessageID:       msg.GetID(),
		MessageType:     msg.GetType(),
		CorrelationID:   msg.GetCorrelationID(),
		OriginalPayload: json.RawMessage(payload),
		OriginalQueue:   originalQueue,
		AttemptNumber:   attempt + 1,
		MaxAttempts:     policy.MaxRetries(),
		RetryPolicy:     fmt.Sprintf("%T", policy),
		ScheduledAt:     time.Now(),
		RetryAt:         time.Now().Add(delay),
	}

	if lastErr != nil {
		retryMessage.LastError = lastErr.Error()
	}

	// Create delay queue for this specific delay if it doesn't exist
	delayQueueName := s.getDelayQueueName(delay)
	if err := s.ensureDelayQueue(ctx, delayQueueName, delay, originalQueue); err != nil {
		return fmt.Errorf("failed to ensure delay queue: %w", err)
	}

	// Publish to delay queue
	if err := s.publishToDelayQueue(ctx, delayQueueName, retryMessage); err != nil {
		return fmt.Errorf("failed to publish to delay queue: %w", err)
	}

	s.logger.Info("Scheduled message for retry",
		"messageId", msg.GetID(),
		"attempt", retryMessage.AttemptNumber,
		"delay", delay,
		"retryAt", retryMessage.RetryAt)

	return nil
}

// getDelayQueueName generates a queue name for a specific delay
func (s *TTLRetryScheduler) getDelayQueueName(delay time.Duration) string {
	// Round to nearest second to avoid too many queues
	seconds := int(delay.Seconds())
	return fmt.Sprintf("mmate.retry.delay.%ds", seconds)
}

// ensureDelayQueue creates a delay queue if it doesn't exist
func (s *TTLRetryScheduler) ensureDelayQueue(ctx context.Context, queueName string, delay time.Duration, targetQueue string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if already created
	if s.delayQueues[queueName] {
		return nil
	}

	// Create delay queue with TTL and DLX pointing to target queue
	queue := rabbitmq.QueueDeclaration{
		Name:    queueName,
		Durable: true,
		Arguments: amqp.Table{
			"x-message-ttl":            int(delay.Milliseconds()),
			"x-dead-letter-exchange":   s.retryExchange,
			"x-dead-letter-routing-key": targetQueue,
			"x-expires":                int(delay.Milliseconds()) + 300000, // Queue expires 5 min after TTL
		},
	}

	if _, err := s.topologyManager.DeclareQueue(ctx, queue); err != nil {
		return fmt.Errorf("failed to declare delay queue %s: %w", queueName, err)
	}

	// Bind delay queue to delay exchange
	binding := rabbitmq.Binding{
		Queue:      queueName,
		Exchange:   s.delayExchange,
		RoutingKey: queueName,
	}

	if err := s.topologyManager.BindQueue(ctx, binding); err != nil {
		return fmt.Errorf("failed to bind delay queue %s: %w", queueName, err)
	}

	s.delayQueues[queueName] = true
	s.logger.Debug("Created delay queue",
		"queue", queueName,
		"delay", delay,
		"targetQueue", targetQueue)

	return nil
}

// publishToDelayQueue publishes a retry message to the delay queue
func (s *TTLRetryScheduler) publishToDelayQueue(ctx context.Context, queueName string, retryMsg *RetryMessage) error {
	return s.channelPool.Execute(ctx, func(ch *amqp.Channel) error {
		body, err := json.Marshal(retryMsg)
		if err != nil {
			return fmt.Errorf("failed to marshal retry message: %w", err)
		}

		return ch.PublishWithContext(
			ctx,
			s.delayExchange, // exchange
			queueName,       // routing key
			false,           // mandatory
			false,           // immediate
			amqp.Publishing{
				ContentType:  "application/json",
				Body:         body,
				DeliveryMode: amqp.Persistent,
				MessageId:    retryMsg.MessageID,
				CorrelationId: retryMsg.CorrelationID,
				Timestamp:    retryMsg.ScheduledAt,
				Headers: amqp.Table{
					"x-retry-attempt":      retryMsg.AttemptNumber,
					"x-retry-max-attempts": retryMsg.MaxAttempts,
					"x-retry-original-queue": retryMsg.OriginalQueue,
					"x-retry-scheduled-at": retryMsg.ScheduledAt.Unix(),
					"x-retry-at":          retryMsg.RetryAt.Unix(),
				},
			},
		)
	})
}

// SetupRetryConsumer sets up a consumer for retry messages on the target queue
func (s *TTLRetryScheduler) SetupRetryConsumer(ctx context.Context, targetQueue string, handler func(ctx context.Context, retryMsg *RetryMessage) error) error {
	// Ensure target queue is bound to retry exchange
	binding := rabbitmq.Binding{
		Queue:      targetQueue,
		Exchange:   s.retryExchange,
		RoutingKey: targetQueue,
	}

	if err := s.topologyManager.BindQueue(ctx, binding); err != nil {
		return fmt.Errorf("failed to bind target queue to retry exchange: %w", err)
	}

	s.logger.Info("Set up retry consumer", "queue", targetQueue)
	return nil
}

// TTLRetryPolicyAdapter adapts existing retry policies to work with TTL scheduler
type TTLRetryPolicyAdapter struct {
	policy    RetryPolicy
	scheduler *TTLRetryScheduler
	queue     string
}

// NewTTLRetryPolicyAdapter creates an adapter for existing retry policies
func NewTTLRetryPolicyAdapter(policy RetryPolicy, scheduler *TTLRetryScheduler, queue string) *TTLRetryPolicyAdapter {
	return &TTLRetryPolicyAdapter{
		policy:    policy,
		scheduler: scheduler,
		queue:     queue,
	}
}

// ExecuteWithRetry executes a function with TTL-based retry
func (a *TTLRetryPolicyAdapter) ExecuteWithRetry(ctx context.Context, msg contracts.Message, fn func() error) error {
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
		shouldRetry, delay := a.policy.ShouldRetry(attempt, err)
		if !shouldRetry {
			return lastErr
		}

		// Use TTL-based retry instead of time.Sleep
		if err := a.scheduler.ScheduleRetry(ctx, msg, a.queue, attempt, a.policy, delay, lastErr); err != nil {
			a.scheduler.logger.Error("Failed to schedule TTL retry", "error", err)
			// Fall back to in-memory retry
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Message scheduled for retry, return special error to indicate this
		return &ScheduledRetryError{
			OriginalError: lastErr,
			AttemptNumber: attempt + 1,
			RetryAt:       time.Now().Add(delay),
		}
	}
}

// ScheduledRetryError indicates a message was scheduled for retry
type ScheduledRetryError struct {
	OriginalError error
	AttemptNumber int
	RetryAt       time.Time
}

func (e *ScheduledRetryError) Error() string {
	return fmt.Sprintf("message scheduled for retry (attempt %d at %v): %v", 
		e.AttemptNumber, e.RetryAt, e.OriginalError)
}

func (e *ScheduledRetryError) Unwrap() error {
	return e.OriginalError
}

func (e *ScheduledRetryError) IsScheduledRetry() bool {
	return true
}

// Close cleans up resources
func (s *TTLRetryScheduler) Close() error {
	// Channel pool will be closed by the transport
	return nil
}