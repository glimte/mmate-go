package messaging

import (
	"context"
	"time"

	"github.com/glimte/mmate-go/contracts"
)

// TransportPublisher defines the interface for publishing messages through a transport
type TransportPublisher interface {
	// Publish sends an envelope through the transport
	Publish(ctx context.Context, exchange, routingKey string, envelope *contracts.Envelope) error
	
	// Close closes the publisher
	Close() error
}

// TransactionalTransportPublisher extends TransportPublisher with transaction support
type TransactionalTransportPublisher interface {
	TransportPublisher
	
	// BeginTx begins a new transaction
	BeginTx(ctx context.Context) (TransportTransaction, error)
}

// TransportTransaction represents a transport-level transaction
type TransportTransaction interface {
	// Publish publishes a message within the transaction
	Publish(ctx context.Context, exchange, routingKey string, envelope *contracts.Envelope) error
	
	// Commit commits the transaction
	Commit() error
	
	// Rollback rolls back the transaction
	Rollback() error
}

// TransportSubscriber defines the interface for subscribing to messages through a transport
type TransportSubscriber interface {
	// Subscribe registers a handler for messages on a specific queue
	Subscribe(ctx context.Context, queue string, handler func(delivery TransportDelivery) error, options SubscriptionOptions) error
	
	// Unsubscribe removes a subscription
	Unsubscribe(queue string) error
	
	// Close closes the subscriber
	Close() error
}

// TransportDelivery represents a message delivery from the transport
type TransportDelivery interface {
	// Body returns the message body
	Body() []byte
	
	// Acknowledge marks the message as successfully processed
	Acknowledge() error
	
	// Reject rejects the message with optional requeue
	Reject(requeue bool) error
	
	// Headers returns message headers
	Headers() map[string]interface{}
}

// Transport provides both publisher and subscriber functionality
type Transport interface {
	// Publisher returns a transport publisher
	Publisher() TransportPublisher
	
	// Subscriber returns a transport subscriber
	Subscriber() TransportSubscriber
	
	// CreateQueue creates a queue if it doesn't exist
	CreateQueue(ctx context.Context, name string, options QueueOptions) error
	
	// DeleteQueue deletes a queue
	DeleteQueue(ctx context.Context, name string) error
	
	// BindQueue creates a binding between queue and exchange
	BindQueue(ctx context.Context, queue, exchange, routingKey string) error
	
	// DeclareQueueWithBindings creates a queue and its bindings in one operation
	DeclareQueueWithBindings(ctx context.Context, name string, options QueueOptions, bindings []QueueBinding) error
	
	// Connect establishes connection to the broker
	Connect(ctx context.Context) error
	
	// Close closes all resources
	Close() error
	
	// IsConnected returns connection status
	IsConnected() bool
}

// QueueBinding represents a queue to exchange binding
type QueueBinding struct {
	Exchange   string
	RoutingKey string
	Arguments  map[string]interface{}
}

// QueueOptions defines options for queue creation
type QueueOptions struct {
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	Args       map[string]interface{}
}

// MessageMetadata contains metadata for messages
type MessageMetadata struct {
	CorrelationID string
	ReplyTo       string
	Source        string
	Timestamp     time.Time
	Headers       map[string]interface{}
}