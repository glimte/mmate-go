package messaging

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/internal/reliability"
	"github.com/glimte/mmate-go/interceptors"
)

// MessagePublisher provides high-level message publishing capabilities
type MessagePublisher struct {
	transport      TransportPublisher
	circuitBreaker *reliability.CircuitBreaker
	retryPolicy    reliability.RetryPolicy
	logger         *slog.Logger
	defaultTTL     time.Duration
	factory        EnvelopeFactory
	pipeline       *interceptors.Pipeline
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

// WithPublisherInterceptors sets the interceptor pipeline for publishing
func WithPublisherInterceptors(pipeline *interceptors.Pipeline) PublisherOption {
	return func(p *MessagePublisher) {
		p.pipeline = pipeline
	}
}

// NewMessagePublisher creates a new message publisher
func NewMessagePublisher(transport TransportPublisher, options ...PublisherOption) *MessagePublisher {
	p := &MessagePublisher{
		transport:   transport,
		logger:      slog.Default(),
		defaultTTL:  5 * time.Minute,
		retryPolicy: reliability.NewExponentialBackoff(time.Second, 30*time.Second, 2.0, 3),
		factory:     NewEnvelopeFactory(),
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

// PublishOption configures publish options
type PublishOption func(*PublishOptions)

// WithExchange sets the exchange
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

// WithDirectQueue publishes directly to a queue (for point-to-point messaging)
func WithDirectQueue(queue string) PublishOption {
	return func(opts *PublishOptions) {
		opts.Exchange = ""
		opts.RoutingKey = queue
	}
}

// WithTTL sets the message TTL
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

// WithPersistent sets persistent delivery mode
func WithPersistent(persistent bool) PublishOption {
	return func(opts *PublishOptions) {
		if persistent {
			opts.DeliveryMode = 2
		} else {
			opts.DeliveryMode = 1
		}
	}
}

// WithHeaders sets additional headers
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

// WithPublishReplyTo sets the reply-to queue for publishing
func WithPublishReplyTo(replyTo string) PublishOption {
	return func(opts *PublishOptions) {
		if opts.Headers == nil {
			opts.Headers = make(map[string]interface{})
		}
		opts.Headers["replyTo"] = replyTo
	}
}

// WithConfirmDelivery sets whether to confirm message delivery
func WithConfirmDelivery(confirm bool) PublishOption {
	return func(opts *PublishOptions) {
		opts.ConfirmDelivery = confirm
	}
}

// Publish publishes a message with options
func (p *MessagePublisher) Publish(ctx context.Context, msg contracts.Message, options ...PublishOption) error {
	if msg == nil {
		return fmt.Errorf("message cannot be nil")
	}

	// Execute through interceptor pipeline if configured
	if p.pipeline != nil {
		return p.pipeline.Execute(ctx, msg, interceptors.MessageHandlerFunc(func(ctx context.Context, msg contracts.Message) error {
			return p.publishInternal(ctx, msg, options...)
		}))
	}

	// Direct execution without interceptors
	return p.publishInternal(ctx, msg, options...)
}

// publishInternal handles the actual publishing logic
func (p *MessagePublisher) publishInternal(ctx context.Context, msg contracts.Message, options ...PublishOption) error {
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

	// Add standard headers
	p.addStandardHeaders(&opts, msg)
	
	// Create envelope with headers
	envelope, err := p.factory.CreateEnvelopeWithOptions(msg, WithEnvelopeHeaders(opts.Headers))
	if err != nil {
		return fmt.Errorf("failed to create envelope: %w", err)
	}

	// Publish with reliability patterns
	publishFunc := func() error {
		p.logger.Debug("Publishing message",
			"exchange", opts.Exchange,
			"routingKey", opts.RoutingKey,
			"messageId", msg.GetID(),
			"messageType", msg.GetType(),
		)
		return p.transport.Publish(ctx, opts.Exchange, opts.RoutingKey, envelope)
	}

	// Apply circuit breaker if configured
	if p.circuitBreaker != nil {
		publishFunc = func() error {
			return p.circuitBreaker.Execute(ctx, func() error {
				return p.transport.Publish(ctx, opts.Exchange, opts.RoutingKey, envelope)
			})
		}
	}

	// Apply retry policy
	err = reliability.Retry(ctx, p.retryPolicy, publishFunc)
	if err != nil {
		p.logger.Error("Failed to publish message",
			"messageId", msg.GetID(),
			"messageType", msg.GetType(),
			"error", err,
		)
		return fmt.Errorf("failed to publish message after retries: %w", err)
	}

	p.logger.Info("Message published successfully",
		"messageId", msg.GetID(),
		"messageType", msg.GetType(),
		"exchange", opts.Exchange,
		"routingKey", opts.RoutingKey,
	)

	return nil
}

// PublishCommand publishes a command message
func (p *MessagePublisher) PublishCommand(ctx context.Context, cmd contracts.Command, options ...PublishOption) error {
	defaultOpts := []PublishOption{
		WithExchange("mmate.commands"),
		WithRoutingKey(fmt.Sprintf("cmd.%s.%s", cmd.GetTargetService(), cmd.GetType())),
	}
	return p.Publish(ctx, cmd, append(defaultOpts, options...)...)
}

// PublishQuery publishes a query message
func (p *MessagePublisher) PublishQuery(ctx context.Context, query contracts.Query, options ...PublishOption) error {
	defaultOpts := []PublishOption{
		WithExchange("mmate.queries"),
		WithRoutingKey(fmt.Sprintf("qry.%s", query.GetType())),
	}
	return p.Publish(ctx, query, append(defaultOpts, options...)...)
}

// PublishEvent publishes an event message
func (p *MessagePublisher) PublishEvent(ctx context.Context, evt contracts.Event, options ...PublishOption) error {
	defaultOpts := []PublishOption{
		WithExchange("mmate.events"),
		WithRoutingKey(fmt.Sprintf("evt.%s.%s", evt.GetAggregateID(), evt.GetType())),
	}
	return p.Publish(ctx, evt, append(defaultOpts, options...)...)
}

// addStandardHeaders adds standard message headers
func (p *MessagePublisher) addStandardHeaders(opts *PublishOptions, msg contracts.Message) {
	opts.Headers["message-id"] = msg.GetID()
	opts.Headers["message-type"] = msg.GetType()
	opts.Headers["timestamp"] = msg.GetTimestamp().UTC().Format(time.RFC3339)
	opts.Headers["source"] = "mmate-go"

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

// PublishReply publishes a reply message
func (p *MessagePublisher) PublishReply(ctx context.Context, reply contracts.Reply, replyTo string, options ...PublishOption) error {
	defaultOpts := []PublishOption{
		WithExchange(""),  // Default exchange for direct replies
		WithRoutingKey(replyTo),
	}
	return p.Publish(ctx, reply, append(defaultOpts, options...)...)
}

// WithCorrelationID sets the correlation ID header
func WithCorrelationID(correlationID string) PublishOption {
	return func(opts *PublishOptions) {
		opts.Headers["correlation-id"] = correlationID
	}
}

// WithReplyTo sets the reply-to header
func WithReplyTo(replyTo string) PublishOption {
	return func(opts *PublishOptions) {
		opts.Headers["reply-to"] = replyTo
	}
}

// Close closes the publisher and releases resources
func (p *MessagePublisher) Close() error {
	return p.transport.Close()
}