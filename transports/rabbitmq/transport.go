package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/internal/rabbitmq"
	"github.com/glimte/mmate-go/messaging"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Transport implements messaging.Transport for RabbitMQ
type Transport struct {
	manager    *rabbitmq.ConnectionManager
	publisher  *rabbitmq.Publisher
	consumer   *rabbitmq.Consumer
	enableFIFO bool
}

// TransportConfig holds configuration for the transport
type TransportConfig struct {
	ConnectionOptions []rabbitmq.ConnectionOption
	PublisherOptions  []rabbitmq.PublisherOption
	ConsumerOptions   []rabbitmq.ConsumerOption
	EnableFIFO        bool
}

// TransportOption configures the transport
type TransportOption func(*TransportConfig)

// WithFIFOMode enables FIFO mode for strict message ordering
func WithFIFOMode(enabled bool) TransportOption {
	return func(cfg *TransportConfig) {
		cfg.EnableFIFO = enabled
	}
}

// WithConnectionOptions sets connection options
func WithConnectionOptions(opts ...rabbitmq.ConnectionOption) TransportOption {
	return func(cfg *TransportConfig) {
		cfg.ConnectionOptions = append(cfg.ConnectionOptions, opts...)
	}
}

// WithPublisherOptions sets publisher options
func WithPublisherOptions(opts ...rabbitmq.PublisherOption) TransportOption {
	return func(cfg *TransportConfig) {
		cfg.PublisherOptions = append(cfg.PublisherOptions, opts...)
	}
}

// NewTransport creates a new RabbitMQ transport
func NewTransport(connectionString string, options ...TransportOption) (*Transport, error) {
	// Apply default config
	cfg := &TransportConfig{
		EnableFIFO: false, // Default to non-FIFO mode
	}
	
	// Apply options
	for _, opt := range options {
		opt(cfg)
	}
	
	// Create connection manager
	manager := rabbitmq.NewConnectionManager(connectionString, cfg.ConnectionOptions...)
	
	// Connect
	if err := manager.Connect(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	// Create channel pool using the manager
	pool, err := rabbitmq.NewChannelPool(manager)
	if err != nil {
		manager.Close()
		return nil, fmt.Errorf("failed to create channel pool: %w", err)
	}

	// Create publisher with configured options
	// NOTE: FIFO mode only affects queue declaration, not publishing behavior
	publisher := rabbitmq.NewPublisher(pool, cfg.PublisherOptions...)

	// Create consumer
	consumer := rabbitmq.NewConsumer(pool, cfg.ConsumerOptions...)

	transport := &Transport{
		manager:    manager,
		publisher:  publisher,
		consumer:   consumer,
		enableFIFO: cfg.EnableFIFO,
	}

	// Declare standard exchanges
	if err := transport.declareExchanges(context.Background()); err != nil {
		manager.Close()
		return nil, fmt.Errorf("failed to declare exchanges: %w", err)
	}

	return transport, nil
}

// Publisher returns a transport publisher
func (t *Transport) Publisher() messaging.TransportPublisher {
	return &publisherAdapter{publisher: t.publisher}
}

// Subscriber returns a transport subscriber
func (t *Transport) Subscriber() messaging.TransportSubscriber {
	return &subscriberAdapter{
		consumer: t.consumer,
		manager:  t.manager,
	}
}

// CreateQueue creates a queue if it doesn't exist
func (t *Transport) CreateQueue(ctx context.Context, name string, options messaging.QueueOptions) error {
	conn, err := t.manager.GetConnection()
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to create channel: %w", err)
	}
	defer channel.Close()

	args := make(amqp.Table)
	for k, v := range options.Args {
		args[k] = v
	}
	
	// If FIFO mode is enabled, add single-active-consumer argument
	if t.enableFIFO {
		args["x-single-active-consumer"] = true
	}

	_, err = channel.QueueDeclare(
		name,
		options.Durable,
		options.AutoDelete,
		options.Exclusive,
		false, // no-wait
		args,
	)
	return err
}

// DeleteQueue deletes a queue
func (t *Transport) DeleteQueue(ctx context.Context, name string) error {
	conn, err := t.manager.GetConnection()
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to create channel: %w", err)
	}
	defer channel.Close()

	_, err = channel.QueueDelete(name, false, false, false)
	return err
}

// BindQueue creates a binding between queue and exchange
func (t *Transport) BindQueue(ctx context.Context, queue, exchange, routingKey string) error {
	conn, err := t.manager.GetConnection()
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to create channel: %w", err)
	}
	defer channel.Close()

	return channel.QueueBind(
		queue,
		routingKey,
		exchange,
		false, // no-wait
		nil,   // args
	)
}

// DeclareQueueWithBindings creates a queue and its bindings in one operation
func (t *Transport) DeclareQueueWithBindings(ctx context.Context, name string, options messaging.QueueOptions, bindings []messaging.QueueBinding) error {
	// First create the queue
	if err := t.CreateQueue(ctx, name, options); err != nil {
		return fmt.Errorf("failed to create queue: %w", err)
	}
	
	// Then create all bindings
	for _, binding := range bindings {
		if err := t.BindQueue(ctx, name, binding.Exchange, binding.RoutingKey); err != nil {
			return fmt.Errorf("failed to bind queue %s to exchange %s with routing key %s: %w", 
				name, binding.Exchange, binding.RoutingKey, err)
		}
	}
	
	return nil
}

// Connect establishes connection to the broker
func (t *Transport) Connect(ctx context.Context) error {
	return t.manager.Connect(ctx)
}

// Close closes all resources
func (t *Transport) Close() error {
	if t.publisher != nil {
		t.publisher.Close()
	}
	return t.manager.Close()
}

// IsConnected returns connection status
func (t *Transport) IsConnected() bool {
	return t.manager.IsConnected()
}

// declareExchanges declares the standard mmate exchanges
func (t *Transport) declareExchanges(ctx context.Context) error {
	conn, err := t.manager.GetConnection()
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to create channel: %w", err)
	}
	defer channel.Close()

	// Declare standard exchanges
	exchanges := []struct {
		name string
		kind string
	}{
		{"mmate.commands", "topic"},
		{"mmate.events", "topic"},
		{"mmate.queries", "topic"},
		{"mmate.messages", "topic"},  // General messages exchange
		{"mmate.contracts", "topic"}, // Contract publishing exchange
		{"mmate.dlx", "topic"},       // Dead letter exchange
	}

	for _, ex := range exchanges {
		err = channel.ExchangeDeclare(
			ex.name,
			ex.kind,
			true,  // durable
			false, // auto-delete
			false, // internal
			false, // no-wait
			nil,   // arguments
		)
		if err != nil {
			return fmt.Errorf("failed to declare exchange %s: %w", ex.name, err)
		}
	}

	return nil
}

// publisherAdapter adapts RabbitMQ publisher to TransportPublisher
type publisherAdapter struct {
	publisher *rabbitmq.Publisher
}

// Publish implements TransportPublisher
func (p *publisherAdapter) Publish(ctx context.Context, exchange, routingKey string, envelope *contracts.Envelope) error {
	// Serialize envelope to JSON
	body, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("failed to marshal envelope: %w", err)
	}

	// Create AMQP publishing
	msg := amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
	}

	// Set headers from envelope
	if envelope.Headers != nil {
		msg.Headers = make(amqp.Table)
		for k, v := range envelope.Headers {
			msg.Headers[k] = v
		}
	}

	// Publish using RabbitMQ publisher
	return p.publisher.Publish(ctx, exchange, routingKey, msg)
}

// Close implements TransportPublisher
func (p *publisherAdapter) Close() error {
	return p.publisher.Close()
}

// subscriberAdapter adapts RabbitMQ consumer to TransportSubscriber
type subscriberAdapter struct {
	consumer *rabbitmq.Consumer
	manager  *rabbitmq.ConnectionManager
	channels map[string]*channelSubscription
}

type channelSubscription struct {
	channel *amqp.Channel
	cancel  context.CancelFunc
}

// Subscribe implements TransportSubscriber
func (s *subscriberAdapter) Subscribe(ctx context.Context, queue string, handler func(messaging.TransportDelivery) error, options messaging.SubscriptionOptions) error {
	// Get connection from manager
	conn, err := s.manager.GetConnection()
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	// Create a dedicated channel for this subscription
	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to create channel: %w", err)
	}

	// For temporary reply queues (AutoDelete=true, Exclusive=true), create the queue
	// This enables the Bridge pattern to work without exposing queue creation in interfaces
	if options.AutoDelete && options.Exclusive {
		args := make(amqp.Table)
		for k, v := range options.Arguments {
			args[k] = v
		}
		
		_, err = channel.QueueDeclare(
			queue,
			false,              // durable=false for temporary queues
			options.AutoDelete, // auto-delete=true
			options.Exclusive,  // exclusive=true
			false,              // no-wait
			args,
		)
		if err != nil {
			channel.Close()
			return fmt.Errorf("failed to declare temporary queue %s: %w", queue, err)
		}
	}

	// Set QoS
	if err := channel.Qos(options.PrefetchCount, 0, false); err != nil {
		channel.Close()
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	// Start consuming
	deliveries, err := channel.Consume(
		queue,
		"",                    // consumer tag
		options.AutoAck,       // auto-ack
		options.Exclusive,     // exclusive
		false,                 // no-local
		false,                 // no-wait
		nil,                   // args
	)
	if err != nil {
		channel.Close()
		return fmt.Errorf("failed to consume: %w", err)
	}

	// Create cancellable context
	subCtx, cancel := context.WithCancel(ctx)

	// Store channel subscription
	if s.channels == nil {
		s.channels = make(map[string]*channelSubscription)
	}
	s.channels[queue] = &channelSubscription{
		channel: channel,
		cancel:  cancel,
	}

	// Start processing deliveries
	go func() {
		for {
			select {
			case <-subCtx.Done():
				channel.Close()
				return
			case d, ok := <-deliveries:
				if !ok {
					return
				}
				// Wrap delivery and call handler
				delivery := &deliveryAdapter{delivery: d}
				// Handler errors are handled at a higher level (in MessageSubscriber)
				// The transport handler always returns nil
				_ = handler(delivery)
			}
		}
	}()

	return nil
}

// Unsubscribe implements TransportSubscriber
func (s *subscriberAdapter) Unsubscribe(queue string) error {
	if sub, ok := s.channels[queue]; ok {
		sub.cancel()
		delete(s.channels, queue)
	}
	return nil
}

// Close implements TransportSubscriber
func (s *subscriberAdapter) Close() error {
	for queue, sub := range s.channels {
		sub.cancel()
		delete(s.channels, queue)
	}
	return nil
}

// deliveryAdapter adapts amqp.Delivery to TransportDelivery
type deliveryAdapter struct {
	delivery amqp.Delivery
}

// Body implements TransportDelivery
func (d *deliveryAdapter) Body() []byte {
	return d.delivery.Body
}

// Acknowledge implements TransportDelivery
func (d *deliveryAdapter) Acknowledge() error {
	return d.delivery.Ack(false)
}

// Reject implements TransportDelivery
func (d *deliveryAdapter) Reject(requeue bool) error {
	return d.delivery.Nack(false, requeue)
}

// Headers implements TransportDelivery
func (d *deliveryAdapter) Headers() map[string]interface{} {
	headers := make(map[string]interface{})
	for k, v := range d.delivery.Headers {
		headers[k] = v
	}
	return headers
}