package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// MessageHandler processes incoming messages
type MessageHandler func(ctx context.Context, delivery amqp.Delivery) error

// Consumer manages message consumption from RabbitMQ
type Consumer struct {
	pool            *ChannelPool
	prefetchCount   int
	prefetchSize    int
	autoAck         bool
	exclusive       bool
	noLocal         bool
	noWait          bool
	consumerTag     string
	logger          *slog.Logger
	activeConsumers sync.Map
}

// ConsumerOption configures the consumer
type ConsumerOption func(*Consumer)

// WithPrefetchCount sets the prefetch count
func WithPrefetchCount(count int) ConsumerOption {
	return func(c *Consumer) {
		c.prefetchCount = count
	}
}

// WithAutoAck enables automatic acknowledgment
func WithAutoAck(autoAck bool) ConsumerOption {
	return func(c *Consumer) {
		c.autoAck = autoAck
	}
}

// WithExclusive sets exclusive consumer mode
func WithExclusive(exclusive bool) ConsumerOption {
	return func(c *Consumer) {
		c.exclusive = exclusive
	}
}

// WithConsumerTag sets the consumer tag
func WithConsumerTag(tag string) ConsumerOption {
	return func(c *Consumer) {
		c.consumerTag = tag
	}
}

// WithConsumerLogger sets the logger
func WithConsumerLogger(logger *slog.Logger) ConsumerOption {
	return func(c *Consumer) {
		c.logger = logger
	}
}

// NewConsumer creates a new consumer
func NewConsumer(pool *ChannelPool, options ...ConsumerOption) *Consumer {
	c := &Consumer{
		pool:          pool,
		prefetchCount: 10,
		prefetchSize:  0,
		autoAck:       false,
		exclusive:     false,
		noLocal:       false,
		noWait:        false,
		consumerTag:   "",
		logger:        slog.Default(),
	}

	for _, opt := range options {
		opt(c)
	}

	return c
}

// ConsumerInfo tracks active consumer information
type ConsumerInfo struct {
	Queue       string
	ConsumerTag string
	Channel     *PooledChannel
	Cancel      context.CancelFunc
	Done        chan struct{}
}

// Subscribe starts consuming messages from a queue
func (c *Consumer) Subscribe(ctx context.Context, queue string, handler MessageHandler) error {
	ch, err := c.pool.Get(ctx)
	if err != nil {
		return &ConsumerError{
			Queue:       queue,
			ConsumerTag: c.consumerTag,
			Op:          "subscribe",
			Err:         err,
			Timestamp:   time.Now(),
		}
	}

	// Set QoS
	if err := ch.Qos(c.prefetchCount, c.prefetchSize, false); err != nil {
		c.pool.Put(ch)
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	// Start consuming
	deliveries, err := ch.Consume(
		queue,
		c.consumerTag,
		c.autoAck,
		c.exclusive,
		c.noLocal,
		c.noWait,
		nil,
	)
	if err != nil {
		c.pool.Put(ch)
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	// Create consumer context
	consumerCtx, cancel := context.WithCancel(ctx)
	
	info := &ConsumerInfo{
		Queue:       queue,
		ConsumerTag: ch.id, // Use channel ID as consumer tag if not specified
		Channel:     ch,
		Cancel:      cancel,
		Done:        make(chan struct{}),
	}

	// Store consumer info
	c.activeConsumers.Store(queue, info)

	// Start processing messages
	go c.processMessages(consumerCtx, info, deliveries, handler)

	c.logger.Info("subscribed to queue",
		"queue", queue,
		"consumerTag", info.ConsumerTag,
		"prefetchCount", c.prefetchCount,
	)

	return nil
}

// processMessages handles incoming messages
func (c *Consumer) processMessages(ctx context.Context, info *ConsumerInfo, deliveries <-chan amqp.Delivery, handler MessageHandler) {
	defer func() {
		close(info.Done)
		c.pool.Put(info.Channel)
		c.activeConsumers.Delete(info.Queue)
		c.logger.Info("consumer stopped", "queue", info.Queue)
	}()

	for {
		select {
		case <-ctx.Done():
			return

		case delivery, ok := <-deliveries:
			if !ok {
				c.logger.Warn("delivery channel closed", "queue", info.Queue)
				return
			}

			// Process message
			if err := c.handleMessage(ctx, delivery, handler); err != nil {
				c.logger.Error("failed to handle message",
					"error", err,
					"queue", info.Queue,
					"messageId", delivery.MessageId,
				)
			}
		}
	}
}

// handleMessage processes a single message
func (c *Consumer) handleMessage(ctx context.Context, delivery amqp.Delivery, handler MessageHandler) error {
	// Create message context with timeout
	msgCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Execute handler
	err := handler(msgCtx, delivery)
	
	// Handle acknowledgment if not auto-ack
	if !c.autoAck {
		if err != nil {
			// Reject and requeue on error
			if nackErr := delivery.Nack(false, true); nackErr != nil {
				c.logger.Error("failed to nack message",
					"error", nackErr,
					"originalError", err,
				)
			}
		} else {
			// Acknowledge successful processing
			if ackErr := delivery.Ack(false); ackErr != nil {
				c.logger.Error("failed to ack message", "error", ackErr)
			}
		}
	}

	return err
}

// Unsubscribe stops consuming from a queue
func (c *Consumer) Unsubscribe(queue string) error {
	value, ok := c.activeConsumers.Load(queue)
	if !ok {
		return fmt.Errorf("no active consumer for queue: %s", queue)
	}

	info := value.(*ConsumerInfo)
	
	// Cancel consumer context
	info.Cancel()
	
	// Wait for consumer to stop
	<-info.Done

	return nil
}

// UnsubscribeAll stops all active consumers
func (c *Consumer) UnsubscribeAll() error {
	var wg sync.WaitGroup
	
	c.activeConsumers.Range(func(key, value interface{}) bool {
		wg.Add(1)
		go func(queue string) {
			defer wg.Done()
			if err := c.Unsubscribe(queue); err != nil {
				c.logger.Error("failed to unsubscribe", "queue", queue, "error", err)
			}
		}(key.(string))
		return true
	})

	wg.Wait()
	return nil
}

// GetActiveConsumers returns a list of active consumer queues
func (c *Consumer) GetActiveConsumers() []string {
	var queues []string
	c.activeConsumers.Range(func(key, value interface{}) bool {
		queues = append(queues, key.(string))
		return true
	})
	return queues
}

// AcknowledgmentStrategy defines how messages are acknowledged
type AcknowledgmentStrategy int

const (
	// AckOnSuccess acknowledges only on successful processing
	AckOnSuccess AcknowledgmentStrategy = iota
	// AckAlways acknowledges regardless of processing result
	AckAlways
	// AckManual requires manual acknowledgment in handler
	AckManual
)

// ConsumerGroup manages multiple consumers with shared configuration
type ConsumerGroup struct {
	consumer  *Consumer
	strategy  AcknowledgmentStrategy
	consumers sync.Map
	logger    *slog.Logger
}

// NewConsumerGroup creates a new consumer group
func NewConsumerGroup(consumer *Consumer, strategy AcknowledgmentStrategy) *ConsumerGroup {
	return &ConsumerGroup{
		consumer: consumer,
		strategy: strategy,
		logger:   consumer.logger,
	}
}

// AddConsumer adds a consumer to the group
func (cg *ConsumerGroup) AddConsumer(ctx context.Context, queue string, handler MessageHandler) error {
	// Wrap handler with acknowledgment strategy
	wrappedHandler := cg.wrapHandler(handler)
	
	if err := cg.consumer.Subscribe(ctx, queue, wrappedHandler); err != nil {
		return err
	}

	cg.consumers.Store(queue, true)
	return nil
}

// wrapHandler wraps the handler with acknowledgment strategy
func (cg *ConsumerGroup) wrapHandler(handler MessageHandler) MessageHandler {
	return func(ctx context.Context, delivery amqp.Delivery) error {
		err := handler(ctx, delivery)

		switch cg.strategy {
		case AckOnSuccess:
			if err == nil {
				return delivery.Ack(false)
			}
			return delivery.Nack(false, true)

		case AckAlways:
			ackErr := delivery.Ack(false)
			if err != nil {
				return err
			}
			return ackErr

		case AckManual:
			// Handler is responsible for acknowledgment
			return err
		}

		return err
	}
}

// StopAll stops all consumers in the group
func (cg *ConsumerGroup) StopAll() error {
	return cg.consumer.UnsubscribeAll()
}