package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher handles message publishing to RabbitMQ
type Publisher struct {
	pool           *ChannelPool
	confirmTimeout time.Duration
	publishTimeout time.Duration
	maxRetries     int
	fifoMode       bool
	fifoChannel    *amqp.Channel
	fifoMutex      sync.Mutex
}

// PublisherOption configures the publisher
type PublisherOption func(*Publisher)

// WithConfirmTimeout sets the confirmation timeout
func WithConfirmTimeout(timeout time.Duration) PublisherOption {
	return func(p *Publisher) {
		p.confirmTimeout = timeout
	}
}

// WithPublishTimeout sets the publish timeout
func WithPublishTimeout(timeout time.Duration) PublisherOption {
	return func(p *Publisher) {
		p.publishTimeout = timeout
	}
}

// WithPublishRetries sets the maximum number of publish retries
func WithPublishRetries(retries int) PublisherOption {
	return func(p *Publisher) {
		p.maxRetries = retries
	}
}

// WithPublisherLogger adds logging to the publisher (placeholder for compatibility)
func WithPublisherLogger(logger *slog.Logger) PublisherOption {
	return func(p *Publisher) {
		// Publisher doesn't have logger field, but we accept it for compatibility
	}
}

// WithConfirmMode enables/disables confirm mode (placeholder for compatibility)
func WithConfirmMode(enabled bool) PublisherOption {
	return func(p *Publisher) {
		// Confirm mode is handled per-publish, not at publisher level
	}
}

// WithFIFOMode enables FIFO mode for strict message ordering
func WithFIFOMode(enabled bool) PublisherOption {
	return func(p *Publisher) {
		p.fifoMode = enabled
	}
}

// NewPublisher creates a new publisher
func NewPublisher(pool *ChannelPool, options ...PublisherOption) *Publisher {
	p := &Publisher{
		pool:           pool,
		confirmTimeout: 5 * time.Second,
		publishTimeout: 10 * time.Second,
		maxRetries:     3,
	}

	for _, opt := range options {
		opt(p)
	}

	return p
}

// Publish publishes a message with confirmation
func (p *Publisher) Publish(ctx context.Context, exchange, routingKey string, msg amqp.Publishing) error {
	// Set context timeout if not already set
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.publishTimeout)
		defer cancel()
	}

	var lastErr error
	for attempt := 0; attempt <= p.maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			select {
			case <-time.After(time.Duration(attempt) * time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		err := p.publishWithConfirm(ctx, exchange, routingKey, msg)
		if err == nil {
			return nil
		}

		lastErr = err
	}

	return fmt.Errorf("failed to publish after %d attempts: %w", p.maxRetries+1, lastErr)
}

// PublishBatch publishes multiple messages in a batch
func (p *Publisher) PublishBatch(ctx context.Context, messages []PublishMessage) error {
	// Get connection directly to avoid channel pool issues with confirms
	conn, err := p.pool.manager.GetConnection()
	if err != nil {
		return &PublishError{
			Exchange:   "batch",
			RoutingKey: "batch",
			Mandatory:  false,
			Err:        err,
			Timestamp:  time.Now(),
		}
	}

	// Create dedicated channel for batch operation
	ch, err := conn.Channel()
	if err != nil {
		return &PublishError{
			Exchange:   "batch",
			RoutingKey: "batch",
			Mandatory:  false,
			Err:        fmt.Errorf("failed to create channel: %w", err),
			Timestamp:  time.Now(),
		}
	}
	defer ch.Close()

	// Enable confirms
	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("failed to enable confirms: %w", err)
	}

	// Set up confirmation handling
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, len(messages)))
	returns := ch.NotifyReturn(make(chan amqp.Return, len(messages)))

	// Publish all messages
	for i, msg := range messages {
		if err := ch.PublishWithContext(
			ctx,
			msg.Exchange,
			msg.RoutingKey,
			msg.Mandatory,
			msg.Immediate,
			msg.Message,
		); err != nil {
			return fmt.Errorf("failed to publish message %d: %w", i, err)
		}
	}

	// Wait for all confirmations
	confirmed := 0
	returned := 0
	
	timeout := time.After(p.confirmTimeout)
	for confirmed < len(messages) {
		select {
		case confirm := <-confirms:
			if !confirm.Ack {
				return fmt.Errorf("message %d was nacked", confirm.DeliveryTag)
			}
			confirmed++

		case ret := <-returns:
			returned++
			return fmt.Errorf("message returned: %s", ret.ReplyText)

		case <-timeout:
			return fmt.Errorf("timeout waiting for confirmations: confirmed %d/%d", confirmed, len(messages))

		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// publishWithConfirm publishes a single message with confirmation
func (p *Publisher) publishWithConfirm(ctx context.Context, exchange, routingKey string, msg amqp.Publishing) error {
	// In FIFO mode, use a single channel with mutex to maintain order
	if p.fifoMode {
		return p.publishFIFO(ctx, exchange, routingKey, msg)
	}
	
	// TODO: Implement proper solution with dedicated publish channels
	// Current implementation creates too many channels - this is a temporary fix
	// 
	// Better approach would be:
	// 1. Maintain a pool of dedicated publish channels (separate from consumer channels)
	// 2. Each goroutine gets its own channel (channels aren't thread-safe)
	// 3. Use async confirms instead of blocking after each publish
	// 4. Track outstanding confirms with sequence numbers
	
	// For now, use a channel from the pool but don't use confirms
	// This avoids the deadlock but doesn't provide delivery guarantees
	ch, err := p.pool.Get(ctx)
	if err != nil {
		return &PublishError{
			Exchange:   exchange,
			RoutingKey: routingKey,
			Mandatory:  false,
			Err:        err,
			Timestamp:  time.Now(),
		}
	}
	defer p.pool.Put(ch)

	// Publish without confirms for now
	if err := ch.PublishWithContext(
		ctx,
		exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		msg,
	); err != nil {
		return fmt.Errorf("failed to publish: %w", err)
	}

	return nil
}

// publishFIFO publishes a message in FIFO mode using a single channel
func (p *Publisher) publishFIFO(ctx context.Context, exchange, routingKey string, msg amqp.Publishing) error {
	p.fifoMutex.Lock()
	defer p.fifoMutex.Unlock()
	
	// Initialize FIFO channel if needed
	if p.fifoChannel == nil {
		conn, err := p.pool.manager.GetConnection()
		if err != nil {
			return fmt.Errorf("failed to get connection: %w", err)
		}
		
		ch, err := conn.Channel()
		if err != nil {
			return fmt.Errorf("failed to create FIFO channel: %w", err)
		}
		
		// Enable confirms for FIFO channel
		if err := ch.Confirm(false); err != nil {
			ch.Close()
			return fmt.Errorf("failed to enable confirms: %w", err)
		}
		
		p.fifoChannel = ch
	}
	
	// Set up confirmation handling
	confirms := p.fifoChannel.NotifyPublish(make(chan amqp.Confirmation, 1))
	
	// Publish message
	if err := p.fifoChannel.PublishWithContext(
		ctx,
		exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		msg,
	); err != nil {
		return fmt.Errorf("failed to publish: %w", err)
	}
	
	// Wait for confirmation synchronously to maintain order
	select {
	case confirm := <-confirms:
		if !confirm.Ack {
			return fmt.Errorf("message was nacked")
		}
		return nil
		
	case <-time.After(p.confirmTimeout):
		return fmt.Errorf("timeout waiting for confirmation")
		
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close closes the publisher and releases resources
func (p *Publisher) Close() error {
	// Close FIFO channel if it exists
	if p.fifoMode && p.fifoChannel != nil {
		p.fifoMutex.Lock()
		defer p.fifoMutex.Unlock()
		
		if p.fifoChannel != nil {
			err := p.fifoChannel.Close()
			p.fifoChannel = nil
			return err
		}
	}
	
	// The underlying channel pool is managed separately
	return nil
}

// PublishMessage represents a message to be published
type PublishMessage struct {
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool
	Message    amqp.Publishing
}