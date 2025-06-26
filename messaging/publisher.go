package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/internal/rabbitmq"
	"github.com/glimte/mmate-go/internal/reliability"
	amqp "github.com/rabbitmq/amqp091-go"
)

// MessagePublisher provides high-level message publishing capabilities
type MessagePublisher struct {
	publisher      *rabbitmq.Publisher
	circuitBreaker *reliability.CircuitBreaker
	retryPolicy    reliability.RetryPolicy
	logger         *slog.Logger
	defaultTTL     time.Duration
}

// PublisherOption configures the MessagePublisher
type PublisherOption func(*MessagePublisher)

// WithPublisherLogger sets the logger
func WithPublisherLogger(logger *slog.Logger) PublisherOption {
	return func(p *MessagePublisher) {
		p.logger = logger
	}
}

// WithCircuitBreaker sets the circuit breaker
func WithCircuitBreaker(cb *reliability.CircuitBreaker) PublisherOption {
	return func(p *MessagePublisher) {
		p.circuitBreaker = cb
	}
}

// WithRetryPolicy sets the retry policy
func WithRetryPolicy(policy reliability.RetryPolicy) PublisherOption {
	return func(p *MessagePublisher) {
		p.retryPolicy = policy
	}
}

// WithDefaultTTL sets the default message time-to-live
func WithDefaultTTL(ttl time.Duration) PublisherOption {
	return func(p *MessagePublisher) {
		p.defaultTTL = ttl
	}
}

// NewMessagePublisher creates a new message publisher
func NewMessagePublisher(publisher *rabbitmq.Publisher, options ...PublisherOption) *MessagePublisher {
	p := &MessagePublisher{
		publisher:   publisher,
		logger:      slog.Default(),
		defaultTTL:  5 * time.Minute,
		retryPolicy: reliability.NewExponentialBackoff(time.Second, 30*time.Second, 2.0, 3),
	}

	for _, opt := range options {
		opt(p)
	}

	return p
}

// PublishOptions configures message publishing
type PublishOptions struct {
	Exchange        string
	RoutingKey      string
	TTL             time.Duration
	Priority        uint8
	DeliveryMode    uint8 // 1 = non-persistent, 2 = persistent
	Headers         map[string]interface{}
	ConfirmDelivery bool
}

// PublishOption configures publish behavior
type PublishOption func(*PublishOptions)

// WithExchange sets the exchange name
func WithExchange(exchange string) PublishOption {
	return func(opts *PublishOptions) {
		opts.Exchange = exchange
	}
}

// WithRoutingKey sets the routing key
func WithRoutingKey(routingKey string) PublishOption {
	return func(opts *PublishOptions) {
		opts.RoutingKey = routingKey
	}
}

// WithTTL sets the message time-to-live
func WithTTL(ttl time.Duration) PublishOption {
	return func(opts *PublishOptions) {
		opts.TTL = ttl
	}
}

// WithPriority sets the message priority
func WithPriority(priority uint8) PublishOption {
	return func(opts *PublishOptions) {
		opts.Priority = priority
	}
}

// WithPersistent sets the message as persistent
func WithPersistent(persistent bool) PublishOption {
	return func(opts *PublishOptions) {
		if persistent {
			opts.DeliveryMode = 2
		} else {
			opts.DeliveryMode = 1
		}
	}
}

// WithHeaders sets custom headers
func WithHeaders(headers map[string]interface{}) PublishOption {
	return func(opts *PublishOptions) {
		if opts.Headers == nil {
			opts.Headers = make(map[string]interface{})
		}
		for k, v := range headers {
			opts.Headers[k] = v
		}
	}
}

// WithConfirmDelivery enables publisher confirms
func WithConfirmDelivery(confirm bool) PublishOption {
	return func(opts *PublishOptions) {
		opts.ConfirmDelivery = confirm
	}
}

// WithReplyTo sets the reply-to queue
func WithReplyTo(replyTo string) PublishOption {
	return func(opts *PublishOptions) {
		if opts.Headers == nil {
			opts.Headers = make(map[string]interface{})
		}
		opts.Headers["x-reply-to"] = replyTo
	}
}

// WithCorrelationID sets the correlation ID
func WithCorrelationID(correlationID string) PublishOption {
	return func(opts *PublishOptions) {
		if opts.Headers == nil {
			opts.Headers = make(map[string]interface{})
		}
		opts.Headers["x-correlation-id"] = correlationID
	}
}

// Publish publishes a message
func (p *MessagePublisher) Publish(ctx context.Context, msg contracts.Message, options ...PublishOption) error {
	if msg == nil {
		return fmt.Errorf("message cannot be nil")
	}

	// Apply default options
	opts := PublishOptions{
		Exchange:        "mmate.messages",
		RoutingKey:      p.getRoutingKey(msg),
		TTL:             p.defaultTTL,
		Priority:        0,
		DeliveryMode:    2, // Persistent by default
		Headers:         make(map[string]interface{}),
		ConfirmDelivery: true,
	}

	for _, opt := range options {
		opt(&opts)
	}

	// Create envelope
	envelope := p.createEnvelope(msg, &opts)

	// Serialize envelope
	body, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	// Add standard headers
	p.addStandardHeaders(&opts, msg)

	// Create AMQP publishing message
	publishing := amqp.Publishing{
		Body:         body,
		Headers:      opts.Headers,
		ContentType:  "application/json",
		DeliveryMode: opts.DeliveryMode,
		Priority:     opts.Priority,
	}
	if opts.TTL > 0 {
		publishing.Expiration = fmt.Sprintf("%d", opts.TTL.Milliseconds())
	}

	// Publish with reliability patterns
	publishFunc := func() error {
		return p.publisher.Publish(ctx, opts.Exchange, opts.RoutingKey, publishing)
	}

	// Apply circuit breaker if configured
	if p.circuitBreaker != nil {
		publishFunc = func() error {
			return p.circuitBreaker.Execute(ctx, func() error {
				return p.publisher.Publish(ctx, opts.Exchange, opts.RoutingKey, publishing)
			})
		}
	}

	// Apply retry policy
	err = reliability.Retry(ctx, p.retryPolicy, publishFunc)
	if err != nil {
		p.logger.Error("failed to publish message",
			"messageId", msg.GetID(),
			"messageType", msg.GetType(),
			"exchange", opts.Exchange,
			"routingKey", opts.RoutingKey,
			"error", err,
		)
		return fmt.Errorf("failed to publish message %s: %w", msg.GetID(), err)
	}

	p.logger.Debug("message published successfully",
		"messageId", msg.GetID(),
		"messageType", msg.GetType(),
		"exchange", opts.Exchange,
		"routingKey", opts.RoutingKey,
	)

	return nil
}

// PublishCommand publishes a command message
func (p *MessagePublisher) PublishCommand(ctx context.Context, cmd contracts.Command, options ...PublishOption) error {
	defaultOptions := []PublishOption{
		WithExchange("mmate.commands"),
		WithRoutingKey(fmt.Sprintf("cmd.%s.%s", cmd.GetTargetService(), cmd.GetType())),
	}

	allOptions := append(defaultOptions, options...)
	return p.Publish(ctx, cmd, allOptions...)
}

// PublishEvent publishes an event message
func (p *MessagePublisher) PublishEvent(ctx context.Context, event contracts.Event, options ...PublishOption) error {
	defaultOptions := []PublishOption{
		WithExchange("mmate.events"),
		WithRoutingKey(fmt.Sprintf("evt.%s.%s", event.GetAggregateID(), event.GetType())),
	}

	allOptions := append(defaultOptions, options...)
	return p.Publish(ctx, event, allOptions...)
}

// PublishQuery publishes a query message
func (p *MessagePublisher) PublishQuery(ctx context.Context, query contracts.Query, options ...PublishOption) error {
	defaultOptions := []PublishOption{
		WithExchange("mmate.queries"),
		WithRoutingKey(fmt.Sprintf("qry.%s", query.GetType())),
	}

	allOptions := append(defaultOptions, options...)
	return p.Publish(ctx, query, allOptions...)
}

// PublishReply publishes a reply message
func (p *MessagePublisher) PublishReply(ctx context.Context, reply contracts.Reply, replyTo string, options ...PublishOption) error {
	defaultOptions := []PublishOption{
		WithExchange("mmate.replies"),
		WithRoutingKey(replyTo),
	}

	allOptions := append(defaultOptions, options...)
	return p.Publish(ctx, reply, allOptions...)
}

// PublishBatch publishes multiple messages in a batch
func (p *MessagePublisher) PublishBatch(ctx context.Context, messages []contracts.Message, options ...PublishOption) error {
	if len(messages) == 0 {
		return nil
	}

	// Prepare all messages
	var publishMessages []rabbitmq.PublishMessage

	for _, msg := range messages {
		if msg == nil {
			continue
		}

		// Apply default options
		opts := PublishOptions{
			Exchange:        "mmate.messages",
			RoutingKey:      p.getRoutingKey(msg),
			TTL:             p.defaultTTL,
			Priority:        0,
			DeliveryMode:    2,
			Headers:         make(map[string]interface{}),
			ConfirmDelivery: true,
		}

		for _, opt := range options {
			opt(&opts)
		}

		// Create envelope and serialize
		envelope := p.createEnvelope(msg, &opts)
		body, err := json.Marshal(envelope)
		if err != nil {
			return fmt.Errorf("failed to serialize message %s: %w", msg.GetID(), err)
		}

		// Add standard headers
		p.addStandardHeaders(&opts, msg)

		publishing := amqp.Publishing{
			Body:         body,
			Headers:      opts.Headers,
			ContentType:  "application/json",
			DeliveryMode: opts.DeliveryMode,
			Priority:     opts.Priority,
		}
		if opts.TTL > 0 {
			publishing.Expiration = fmt.Sprintf("%d", opts.TTL.Milliseconds())
		}

		publishMessages = append(publishMessages, rabbitmq.PublishMessage{
			Exchange:   opts.Exchange,
			RoutingKey: opts.RoutingKey,
			Message:    publishing,
		})
	}

	// Publish batch with reliability
	publishFunc := func() error {
		return p.publisher.PublishBatch(ctx, publishMessages)
	}

	// Apply circuit breaker if configured
	if p.circuitBreaker != nil {
		publishFunc = func() error {
			return p.circuitBreaker.Execute(ctx, publishFunc)
		}
	}

	// Apply retry policy
	err := reliability.Retry(ctx, p.retryPolicy, publishFunc)
	if err != nil {
		p.logger.Error("failed to publish batch",
			"messageCount", len(messages),
			"error", err,
		)
		return fmt.Errorf("failed to publish batch of %d messages: %w", len(messages), err)
	}

	p.logger.Debug("batch published successfully",
		"messageCount", len(publishMessages),
	)

	return nil
}

// createEnvelope creates a message envelope
func (p *MessagePublisher) createEnvelope(msg contracts.Message, opts *PublishOptions) contracts.Envelope {
	// Serialize the message to JSON
	msgBytes, _ := json.Marshal(msg)

	return contracts.Envelope{
		ID:            msg.GetID(),
		Type:          msg.GetType(),
		Timestamp:     msg.GetTimestamp().UTC().Format(time.RFC3339),
		CorrelationID: msg.GetCorrelationID(),
		Headers:       opts.Headers,
		Body:          msgBytes,
	}
}

// addStandardHeaders adds standard messaging headers
func (p *MessagePublisher) addStandardHeaders(opts *PublishOptions, msg contracts.Message) {
	if opts.Headers == nil {
		opts.Headers = make(map[string]interface{})
	}

	opts.Headers["message-id"] = msg.GetID()
	opts.Headers["message-type"] = msg.GetType()
	opts.Headers["timestamp"] = msg.GetTimestamp().UTC().Format(time.RFC3339)

	if correlationID := msg.GetCorrelationID(); correlationID != "" {
		opts.Headers["correlation-id"] = correlationID
	}

	if opts.TTL > 0 {
		opts.Headers["expiration"] = fmt.Sprintf("%d", opts.TTL.Milliseconds())
	}

	opts.Headers["priority"] = opts.Priority
	opts.Headers["delivery-mode"] = opts.DeliveryMode
}

// getRoutingKey determines the routing key for a message
func (p *MessagePublisher) getRoutingKey(msg contracts.Message) string {
	switch m := msg.(type) {
	case contracts.Command:
		return fmt.Sprintf("cmd.%s.%s", m.GetTargetService(), m.GetType())
	case contracts.Event:
		return fmt.Sprintf("evt.%s.%s", m.GetAggregateID(), m.GetType())
	case contracts.Query:
		return fmt.Sprintf("qry.%s", m.GetType())
	case contracts.Reply:
		return fmt.Sprintf("rpl.%s", m.GetType())
	default:
		return fmt.Sprintf("msg.%s", m.GetType())
	}
}

// Close closes the publisher and releases resources
func (p *MessagePublisher) Close() error {
	// MessagePublisher doesn't hold direct resources to close
	// The underlying rabbitmq.Publisher is managed by the channel pool
	return nil
}
