package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// DirectPublisher publishes messages directly to exchanges
type DirectPublisher struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	exchangeName string
	mandatory    bool
	immediate    bool
	mu           sync.RWMutex
	confirms     chan amqp.Confirmation
	isReliable   bool
}

// DirectPublisherOption configures the direct publisher
type DirectPublisherOption func(*DirectPublisherConfig)

// DirectPublisherConfig holds configuration for direct publisher
type DirectPublisherConfig struct {
	ExchangeName string
	Mandatory    bool
	Immediate    bool
	Reliable     bool
}

// WithDirectExchange sets the exchange name
func WithDirectExchange(exchange string) DirectPublisherOption {
	return func(c *DirectPublisherConfig) {
		c.ExchangeName = exchange
	}
}

// WithMandatory sets the mandatory flag
func WithMandatory(mandatory bool) DirectPublisherOption {
	return func(c *DirectPublisherConfig) {
		c.Mandatory = mandatory
	}
}

// WithImmediate sets the immediate flag
func WithImmediate(immediate bool) DirectPublisherOption {
	return func(c *DirectPublisherConfig) {
		c.Immediate = immediate
	}
}

// WithReliablePublishing enables reliable publishing with confirms
func WithReliablePublishing(reliable bool) DirectPublisherOption {
	return func(c *DirectPublisherConfig) {
		c.Reliable = reliable
	}
}

// NewDirectPublisher creates a new direct publisher
func NewDirectPublisher(conn *amqp.Connection, opts ...DirectPublisherOption) (*DirectPublisher, error) {
	if conn == nil {
		return nil, fmt.Errorf("connection cannot be nil")
	}

	config := &DirectPublisherConfig{
		ExchangeName: "",
		Mandatory:    false,
		Immediate:    false,
		Reliable:     true,
	}

	for _, opt := range opts {
		opt(config)
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to create channel: %w", err)
	}

	publisher := &DirectPublisher{
		conn:         conn,
		channel:      channel,
		exchangeName: config.ExchangeName,
		mandatory:    config.Mandatory,
		immediate:    config.Immediate,
		isReliable:   config.Reliable,
	}

	if config.Reliable {
		err = channel.Confirm(false)
		if err != nil {
			channel.Close()
			return nil, fmt.Errorf("failed to enable publisher confirms: %w", err)
		}

		publisher.confirms = channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	}

	return publisher, nil
}

// PublishDirect publishes a message directly to an exchange with routing key
func (p *DirectPublisher) PublishDirect(ctx context.Context, exchange, routingKey string, message []byte, headers amqp.Table) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.channel == nil {
		return fmt.Errorf("publisher is closed")
	}

	publishing := amqp.Publishing{
		Headers:      headers,
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		Body:         message,
	}

	err := p.channel.PublishWithContext(
		ctx,
		exchange,
		routingKey,
		p.mandatory,
		p.immediate,
		publishing,
	)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	// Wait for confirmation if reliable publishing is enabled
	if p.isReliable && p.confirms != nil {
		select {
		case confirm := <-p.confirms:
			if !confirm.Ack {
				return fmt.Errorf("message was not acknowledged by broker")
			}
		case <-time.After(5 * time.Second):
			return fmt.Errorf("timeout waiting for publish confirmation")
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// Publish publishes a message using the configured exchange
func (p *DirectPublisher) Publish(ctx context.Context, routingKey string, message []byte, headers amqp.Table) error {
	return p.PublishDirect(ctx, p.exchangeName, routingKey, message, headers)
}

// PublishBatch publishes multiple messages in a batch
func (p *DirectPublisher) PublishBatch(ctx context.Context, messages []DirectMessage) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.channel == nil {
		return fmt.Errorf("publisher is closed")
	}

	// For non-reliable publishing, just publish all messages
	if !p.isReliable {
		for _, msg := range messages {
			err := p.PublishDirect(ctx, msg.Exchange, msg.RoutingKey, msg.Body, msg.Headers)
			if err != nil {
				return fmt.Errorf("failed to publish message: %w", err)
			}
		}
		return nil
	}

	// For reliable publishing, track confirmations
	outstanding := make(map[uint64]bool)
	confirmed := make(chan uint64, len(messages))
	
	// Set up confirmation listener
	go func() {
		for confirm := range p.confirms {
			if confirm.Ack {
				confirmed <- confirm.DeliveryTag
			}
		}
	}()

	// Publish all messages
	for i, msg := range messages {
		deliveryTag := uint64(i + 1)
		outstanding[deliveryTag] = true

		publishing := amqp.Publishing{
			Headers:      msg.Headers,
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Body:         msg.Body,
		}

		err := p.channel.PublishWithContext(
			ctx,
			msg.Exchange,
			msg.RoutingKey,
			p.mandatory,
			p.immediate,
			publishing,
		)
		if err != nil {
			return fmt.Errorf("failed to publish message %d: %w", i, err)
		}
	}

	// Wait for all confirmations
	timeout := time.After(10 * time.Second)
	for len(outstanding) > 0 {
		select {
		case tag := <-confirmed:
			delete(outstanding, tag)
		case <-timeout:
			return fmt.Errorf("timeout waiting for confirmations, %d messages unconfirmed", len(outstanding))
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// Close closes the publisher
func (p *DirectPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.channel != nil {
		err := p.channel.Close()
		p.channel = nil
		return err
	}

	return nil
}

// DirectMessage represents a message to be published directly
type DirectMessage struct {
	Exchange   string
	RoutingKey string
	Headers    amqp.Table
	Body       []byte
}

// DirectExchangeManager manages direct exchange declarations
type DirectExchangeManager struct {
	channel *amqp.Channel
	mu      sync.RWMutex
}

// NewDirectExchangeManager creates a new direct exchange manager
func NewDirectExchangeManager(channel *amqp.Channel) *DirectExchangeManager {
	return &DirectExchangeManager{
		channel: channel,
	}
}

// DeclareDirectExchange declares a direct exchange
func (m *DirectExchangeManager) DeclareDirectExchange(name string, durable, autoDelete bool, args amqp.Table) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	err := m.channel.ExchangeDeclare(
		name,       // name
		"direct",   // type
		durable,    // durable
		autoDelete, // auto-deleted
		false,      // internal
		false,      // no-wait
		args,       // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare direct exchange %s: %w", name, err)
	}

	return nil
}

// BindQueue binds a queue to a direct exchange
func (m *DirectExchangeManager) BindQueue(queueName, exchangeName, routingKey string, args amqp.Table) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	err := m.channel.QueueBind(
		queueName,    // queue name
		routingKey,   // routing key
		exchangeName, // exchange
		false,        // no-wait
		args,         // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to bind queue %s to exchange %s: %w", queueName, exchangeName, err)
	}

	return nil
}