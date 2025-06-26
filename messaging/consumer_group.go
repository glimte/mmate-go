package messaging

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/internal/reliability"
)

// ConsumerGroup manages a group of consumers for load balancing
type ConsumerGroup interface {
	// Start starts the consumer group
	Start(ctx context.Context) error

	// Stop stops the consumer group
	Stop() error

	// AddConsumer adds a consumer to the group
	AddConsumer() error

	// RemoveConsumer removes a consumer from the group
	RemoveConsumer() error

	// GetConsumerCount returns the current number of consumers
	GetConsumerCount() int

	// GetMetrics returns consumer group metrics
	GetMetrics() ConsumerGroupMetrics
}

// ConsumerGroupMetrics contains metrics for the consumer group
type ConsumerGroupMetrics struct {
	ConsumerCount      int
	MessagesProcessed  int64
	MessagesFailed     int64
	AverageProcessTime time.Duration
	LastMessageTime    time.Time
}

// ConsumerGroupConfig configures a consumer group
type ConsumerGroupConfig struct {
	GroupID            string
	Queue              string
	MessageType        string
	MinConsumers       int
	MaxConsumers       int
	PrefetchCount      int
	Handler            MessageHandler
	ErrorHandler       ErrorHandler
	RetryPolicy        reliability.RetryPolicy
	Logger             *slog.Logger
	MetricsInterval    time.Duration
	ScaleUpThreshold   float64 // Queue depth percentage to trigger scale up
	ScaleDownThreshold float64 // Queue depth percentage to trigger scale down
}

// ConsumerGroupOption configures the consumer group
type ConsumerGroupOption func(*ConsumerGroupConfig)

// WithGroupID sets the consumer group ID
func WithGroupID(id string) ConsumerGroupOption {
	return func(c *ConsumerGroupConfig) {
		c.GroupID = id
	}
}

// WithMinConsumers sets the minimum number of consumers
func WithMinConsumers(min int) ConsumerGroupOption {
	return func(c *ConsumerGroupConfig) {
		c.MinConsumers = min
	}
}

// WithMaxConsumers sets the maximum number of consumers
func WithMaxConsumers(max int) ConsumerGroupOption {
	return func(c *ConsumerGroupConfig) {
		c.MaxConsumers = max
	}
}

// WithGroupPrefetchCount sets the prefetch count per consumer
func WithGroupPrefetchCount(count int) ConsumerGroupOption {
	return func(c *ConsumerGroupConfig) {
		c.PrefetchCount = count
	}
}

// WithGroupLogger sets the logger
func WithGroupLogger(logger *slog.Logger) ConsumerGroupOption {
	return func(c *ConsumerGroupConfig) {
		c.Logger = logger
	}
}

// WithGroupErrorHandler sets the error handler
func WithGroupErrorHandler(handler ErrorHandler) ConsumerGroupOption {
	return func(c *ConsumerGroupConfig) {
		c.ErrorHandler = handler
	}
}

// WithGroupRetryPolicy sets the retry policy
func WithGroupRetryPolicy(policy reliability.RetryPolicy) ConsumerGroupOption {
	return func(c *ConsumerGroupConfig) {
		c.RetryPolicy = policy
	}
}

// WithMetricsInterval sets the metrics collection interval
func WithMetricsInterval(interval time.Duration) ConsumerGroupOption {
	return func(c *ConsumerGroupConfig) {
		c.MetricsInterval = interval
	}
}

// WithScaleThresholds sets the scale up/down thresholds
func WithScaleThresholds(scaleUp, scaleDown float64) ConsumerGroupOption {
	return func(c *ConsumerGroupConfig) {
		c.ScaleUpThreshold = scaleUp
		c.ScaleDownThreshold = scaleDown
	}
}

// DefaultConsumerGroup is the default implementation of ConsumerGroup
type DefaultConsumerGroup struct {
	config            *ConsumerGroupConfig
	subscriber        Subscriber
	consumers         []*groupConsumer
	mu                sync.RWMutex
	running           bool
	ctx               context.Context
	cancel            context.CancelFunc
	messagesProcessed atomic.Int64
	messagesFailed    atomic.Int64
	totalProcessTime  atomic.Int64
	lastMessageTime   atomic.Value // time.Time
	metricsTimer      *time.Ticker
	scalingTimer      *time.Ticker
}

// groupConsumer represents a single consumer in the group
type groupConsumer struct {
	id           string
	subscription string
	ctx          context.Context
	cancel       context.CancelFunc
}

// NewConsumerGroup creates a new consumer group
func NewConsumerGroup(subscriber Subscriber, queue string, messageType string, handler MessageHandler, opts ...ConsumerGroupOption) (*DefaultConsumerGroup, error) {
	if subscriber == nil {
		return nil, fmt.Errorf("subscriber cannot be nil")
	}
	if queue == "" {
		return nil, fmt.Errorf("queue cannot be empty")
	}
	if messageType == "" {
		return nil, fmt.Errorf("message type cannot be empty")
	}
	if handler == nil {
		return nil, fmt.Errorf("handler cannot be nil")
	}

	config := &ConsumerGroupConfig{
		GroupID:            fmt.Sprintf("group-%s-%s", queue, messageType),
		Queue:              queue,
		MessageType:        messageType,
		MinConsumers:       1,
		MaxConsumers:       10,
		PrefetchCount:      10,
		Handler:            handler,
		ErrorHandler:       &DefaultErrorHandler{Logger: slog.Default()},
		RetryPolicy:        reliability.NewExponentialBackoff(time.Second, 30*time.Second, 2.0, 3),
		Logger:             slog.Default(),
		MetricsInterval:    30 * time.Second,
		ScaleUpThreshold:   0.8, // Scale up when queue is 80% full
		ScaleDownThreshold: 0.2, // Scale down when queue is 20% full
	}

	for _, opt := range opts {
		opt(config)
	}

	// Validate configuration
	if config.MinConsumers < 1 {
		config.MinConsumers = 1
	}
	if config.MaxConsumers < config.MinConsumers {
		config.MaxConsumers = config.MinConsumers
	}

	group := &DefaultConsumerGroup{
		config:     config,
		subscriber: subscriber,
		consumers:  make([]*groupConsumer, 0, config.MaxConsumers),
	}

	group.lastMessageTime.Store(time.Now())

	return group, nil
}

// Start starts the consumer group
func (g *DefaultConsumerGroup) Start(ctx context.Context) error {
	g.mu.Lock()

	if g.running {
		g.mu.Unlock()
		return fmt.Errorf("consumer group already running")
	}

	g.ctx, g.cancel = context.WithCancel(ctx)

	// Start minimum consumers
	for i := 0; i < g.config.MinConsumers; i++ {
		if err := g.addConsumerLocked(); err != nil {
			// Clean up any started consumers
			for _, c := range g.consumers {
				c.cancel()
			}
			g.consumers = nil
			g.mu.Unlock()
			return fmt.Errorf("failed to start consumer %d: %w", i, err)
		}
	}

	// Start metrics collection
	g.metricsTimer = time.NewTicker(g.config.MetricsInterval)
	go g.collectMetrics()

	// Start auto-scaling
	g.scalingTimer = time.NewTicker(10 * time.Second)
	go g.autoScale()

	g.running = true
	g.mu.Unlock()

	g.config.Logger.Info("consumer group started",
		"groupId", g.config.GroupID,
		"queue", g.config.Queue,
		"initialConsumers", g.config.MinConsumers,
	)

	return nil
}

// Stop stops the consumer group
func (g *DefaultConsumerGroup) Stop() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.running {
		return fmt.Errorf("consumer group not running")
	}

	// Cancel context
	if g.cancel != nil {
		g.cancel()
	}

	// Stop timers
	if g.metricsTimer != nil {
		g.metricsTimer.Stop()
	}
	if g.scalingTimer != nil {
		g.scalingTimer.Stop()
	}

	// Stop all consumers
	var errs []error
	for _, consumer := range g.consumers {
		consumer.cancel()
		if err := g.subscriber.Unsubscribe(consumer.subscription); err != nil {
			errs = append(errs, fmt.Errorf("failed to unsubscribe consumer %s: %w", consumer.id, err))
		}
	}

	g.consumers = nil
	g.running = false

	g.config.Logger.Info("consumer group stopped",
		"groupId", g.config.GroupID,
		"messagesProcessed", g.messagesProcessed.Load(),
		"messagesFailed", g.messagesFailed.Load(),
	)

	if len(errs) > 0 {
		return fmt.Errorf("errors during shutdown: %v", errs)
	}

	return nil
}

// AddConsumer adds a consumer to the group
func (g *DefaultConsumerGroup) AddConsumer() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.running {
		return fmt.Errorf("consumer group not running")
	}

	if len(g.consumers) >= g.config.MaxConsumers {
		return fmt.Errorf("maximum consumers reached (%d)", g.config.MaxConsumers)
	}

	return g.addConsumerLocked()
}

// addConsumerLocked adds a consumer (must be called with lock held)
func (g *DefaultConsumerGroup) addConsumerLocked() error {
	consumerID := fmt.Sprintf("%s-%d-%d", g.config.GroupID, len(g.consumers), time.Now().Unix())

	consumer := &groupConsumer{
		id:           consumerID,
		subscription: g.config.Queue, // All consumers subscribe to the same queue
	}

	// Create consumer context
	consumer.ctx, consumer.cancel = context.WithCancel(g.ctx)

	// Create wrapped handler
	handler := g.createConsumerHandler(consumerID)

	// Subscribe
	err := g.subscriber.Subscribe(
		consumer.ctx,
		g.config.Queue,
		g.config.MessageType,
		handler,
		WithPrefetchCount(g.config.PrefetchCount),
		WithAutoAck(false), // Manual ack for reliability
	)
	if err != nil {
		consumer.cancel()
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	g.consumers = append(g.consumers, consumer)

	g.config.Logger.Info("added consumer to group",
		"groupId", g.config.GroupID,
		"consumerId", consumerID,
		"totalConsumers", len(g.consumers),
	)

	return nil
}

// RemoveConsumer removes a consumer from the group
func (g *DefaultConsumerGroup) RemoveConsumer() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.running {
		return fmt.Errorf("consumer group not running")
	}

	if len(g.consumers) <= g.config.MinConsumers {
		return fmt.Errorf("minimum consumers reached (%d)", g.config.MinConsumers)
	}

	// Remove the last consumer
	consumer := g.consumers[len(g.consumers)-1]
	g.consumers = g.consumers[:len(g.consumers)-1]

	// Cancel and unsubscribe
	consumer.cancel()
	if err := g.subscriber.Unsubscribe(consumer.subscription); err != nil {
		g.config.Logger.Warn("failed to unsubscribe consumer",
			"consumerId", consumer.id,
			"error", err,
		)
	}

	g.config.Logger.Info("removed consumer from group",
		"groupId", g.config.GroupID,
		"consumerId", consumer.id,
		"totalConsumers", len(g.consumers),
	)

	return nil
}

// GetConsumerCount returns the current number of consumers
func (g *DefaultConsumerGroup) GetConsumerCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.consumers)
}

// GetMetrics returns consumer group metrics
func (g *DefaultConsumerGroup) GetMetrics() ConsumerGroupMetrics {
	g.mu.RLock()
	consumerCount := len(g.consumers)
	g.mu.RUnlock()

	processed := g.messagesProcessed.Load()
	failed := g.messagesFailed.Load()
	totalTime := g.totalProcessTime.Load()

	var avgTime time.Duration
	if processed > 0 {
		avgTime = time.Duration(totalTime / processed)
	}

	lastTime := g.lastMessageTime.Load().(time.Time)

	return ConsumerGroupMetrics{
		ConsumerCount:      consumerCount,
		MessagesProcessed:  processed,
		MessagesFailed:     failed,
		AverageProcessTime: avgTime,
		LastMessageTime:    lastTime,
	}
}

// createConsumerHandler creates a message handler for a consumer
func (g *DefaultConsumerGroup) createConsumerHandler(consumerID string) MessageHandler {
	return MessageHandlerFunc(func(ctx context.Context, msg contracts.Message) error {
		startTime := time.Now()

		g.config.Logger.Debug("consumer processing message",
			"consumerId", consumerID,
			"messageId", msg.GetID(),
			"messageType", msg.GetType(),
		)

		// Execute handler with retry
		var err error
		for attempt := 0; attempt <= g.config.RetryPolicy.MaxRetries(); attempt++ {
			err = g.config.Handler.Handle(ctx, msg)
			if err == nil {
				break
			}

			shouldRetry, delay := g.config.RetryPolicy.ShouldRetry(attempt+1, err)
			if !shouldRetry {
				break
			}

			g.config.Logger.Warn("retrying message handler",
				"consumerId", consumerID,
				"messageId", msg.GetID(),
				"attempt", attempt+1,
				"delay", delay,
				"error", err,
			)

			select {
			case <-time.After(delay):
				// Continue retry
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Update metrics
		processingTime := time.Since(startTime)
		g.totalProcessTime.Add(processingTime.Nanoseconds())
		g.lastMessageTime.Store(time.Now())

		if err != nil {
			g.messagesFailed.Add(1)
			g.config.Logger.Error("consumer failed to process message",
				"consumerId", consumerID,
				"messageId", msg.GetID(),
				"error", err,
				"duration", processingTime,
			)

			// Handle error
			action := g.config.ErrorHandler.HandleError(ctx, msg, err)
			switch action {
			case Acknowledge:
				return nil // Suppress error, message will be acked
			case Retry:
				return fmt.Errorf("retry requested: %w", err)
			case Reject:
				return err // Return error, message will be rejected
			}
		}

		g.messagesProcessed.Add(1)
		g.config.Logger.Debug("consumer processed message successfully",
			"consumerId", consumerID,
			"messageId", msg.GetID(),
			"duration", processingTime,
		)

		return nil
	})
}

// collectMetrics periodically collects and logs metrics
func (g *DefaultConsumerGroup) collectMetrics() {
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-g.metricsTimer.C:
			metrics := g.GetMetrics()
			g.config.Logger.Info("consumer group metrics",
				"groupId", g.config.GroupID,
				"consumers", metrics.ConsumerCount,
				"processed", metrics.MessagesProcessed,
				"failed", metrics.MessagesFailed,
				"avgProcessTime", metrics.AverageProcessTime,
				"lastMessage", metrics.LastMessageTime.Format(time.RFC3339),
			)
		}
	}
}

// autoScale automatically scales consumers based on queue depth
func (g *DefaultConsumerGroup) autoScale() {
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-g.scalingTimer.C:
			// In a real implementation, you would check queue depth here
			// For now, we'll use time since last message as a proxy

			timeSinceLastMessage := time.Since(g.lastMessageTime.Load().(time.Time))
			currentCount := g.GetConsumerCount()

			// Scale down if idle for too long
			if timeSinceLastMessage > 30*time.Second && currentCount > g.config.MinConsumers {
				if err := g.RemoveConsumer(); err != nil {
					g.config.Logger.Warn("failed to scale down",
						"groupId", g.config.GroupID,
						"error", err,
					)
				} else {
					g.config.Logger.Info("scaled down consumer group",
						"groupId", g.config.GroupID,
						"consumers", g.GetConsumerCount(),
					)
				}
			}

			// Note: Scale up would require queue depth monitoring
			// which would need integration with RabbitMQ management API
		}
	}
}
