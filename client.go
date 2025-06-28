// Copyright 2024 Mmate Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mmate

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/glimte/mmate-go/bridge"
	"github.com/glimte/mmate-go/messaging"
	rabbitmqTransport "github.com/glimte/mmate-go/transports/rabbitmq"
	"github.com/glimte/mmate-go/internal/rabbitmq"
	"github.com/glimte/mmate-go/interceptors"
	"github.com/glimte/mmate-go/internal/reliability"
)

// Client provides the main entry point for mmate-go
type Client struct {
	transport    messaging.Transport
	publisher    *messaging.MessagePublisher
	subscriber   *messaging.MessageSubscriber
	dispatcher   *messaging.MessageDispatcher
	bridge       *bridge.SyncAsyncBridge
	serviceName  string
	receiveQueue string
}

// NewClient creates a new mmate client with default RabbitMQ transport
func NewClient(connectionString string) (*Client, error) {
	return NewClientWithOptions(connectionString, WithDefaultLogger())
}

// NewClientWithOptions creates a new mmate client with options
func NewClientWithOptions(connectionString string, options ...ClientOption) (*Client, error) {
	cfg := &clientConfig{
		logger: slog.Default(),
		serviceName: "service", // Default service name
	}

	for _, opt := range options {
		opt(cfg)
	}

	// Create RabbitMQ transport with connection options
	transportOpts := []rabbitmqTransport.TransportOption{
		rabbitmqTransport.WithConnectionOptions(
			rabbitmq.WithLogger(cfg.logger),
		),
	}
	
	// Add FIFO mode if requested
	if cfg.enableFIFO {
		transportOpts = append(transportOpts, rabbitmqTransport.WithFIFOMode(true))
	}
	
	transport, err := rabbitmqTransport.NewTransport(connectionString, transportOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	// Create dispatcher
	dispatcher := messaging.NewMessageDispatcher()

	// Create publisher with interceptors if configured
	publisherOpts := []messaging.PublisherOption{
		messaging.WithPublisherLogger(cfg.logger),
	}
	if cfg.publishPipeline != nil {
		publisherOpts = append(publisherOpts, messaging.WithPublisherInterceptors(cfg.publishPipeline))
	}
	
	publisher := messaging.NewMessagePublisher(
		transport.Publisher(),
		publisherOpts...,
	)

	// Create subscriber with interceptors and DLQ handler if configured
	subscriberOpts := []messaging.SubscriberOption{
		messaging.WithSubscriberLogger(cfg.logger),
	}
	
	// Add DLQ handler if configured
	if cfg.dlqHandler != nil {
		subscriberOpts = append(subscriberOpts, messaging.WithDLQHandler(cfg.dlqHandler))
	}
	
	// Create default pipeline with retry if enabled
	if cfg.subscribePipeline != nil {
		subscriberOpts = append(subscriberOpts, messaging.WithSubscriberInterceptors(cfg.subscribePipeline))
	} else if cfg.enableDefaultRetry || cfg.retryPolicy != nil {
		// Create default pipeline with retry interceptor
		pipeline := interceptors.NewPipeline()
		
		// Add retry interceptor if configured
		if cfg.retryPolicy != nil {
			retryInterceptor := interceptors.NewRetryInterceptor(cfg.retryPolicy).WithLogger(cfg.logger)
			pipeline.Use(retryInterceptor)
		} else if cfg.enableDefaultRetry {
			// Use default retry policy
			defaultPolicy := reliability.NewExponentialBackoff(100*time.Millisecond, 30*time.Second, 2.0, 3)
			retryInterceptor := interceptors.NewRetryInterceptor(defaultPolicy).WithLogger(cfg.logger)
			pipeline.Use(retryInterceptor)
		}
		
		subscriberOpts = append(subscriberOpts, messaging.WithSubscriberInterceptors(pipeline))
	}
	
	subscriber := messaging.NewMessageSubscriber(
		transport.Subscriber(),
		dispatcher,
		subscriberOpts...,
	)
	
	// Create the service's receive queue automatically
	queueName := fmt.Sprintf("%s-queue", cfg.serviceName)
	queueOpts := messaging.QueueOptions{
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
	}
	
	// Create queue with bindings if provided
	if len(cfg.queueBindings) > 0 {
		err = transport.DeclareQueueWithBindings(context.Background(), queueName, queueOpts, cfg.queueBindings)
		if err != nil {
			return nil, fmt.Errorf("failed to declare service queue with bindings: %w", err)
		}
		cfg.logger.Info("Service queue created with bindings", "queue", queueName, "bindings", len(cfg.queueBindings))
	} else {
		// Just create the queue without bindings
		err = transport.CreateQueue(context.Background(), queueName, queueOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to create service queue: %w", err)
		}
		cfg.logger.Info("Service queue created", "queue", queueName)
	}

	return &Client{
		transport:    transport,
		publisher:    publisher,
		subscriber:   subscriber,
		dispatcher:   dispatcher,
		serviceName:  cfg.serviceName,
		receiveQueue: queueName,
	}, nil
}

// Publisher returns the message publisher
func (c *Client) Publisher() *messaging.MessagePublisher {
	return c.publisher
}

// Subscriber returns the message subscriber
func (c *Client) Subscriber() *messaging.MessageSubscriber {
	return c.subscriber
}

// Dispatcher returns the message dispatcher
func (c *Client) Dispatcher() *messaging.MessageDispatcher {
	return c.dispatcher
}

// Transport returns the underlying transport
func (c *Client) Transport() messaging.Transport {
	return c.transport
}

// ServiceQueue returns the service's receive queue name
func (c *Client) ServiceQueue() string {
	return c.receiveQueue
}

// Bridge returns the sync-async bridge for request-response patterns
func (c *Client) Bridge() *bridge.SyncAsyncBridge {
	if c.bridge == nil {
		// Check if publisher and subscriber are available
		if c.publisher == nil || c.subscriber == nil {
			return nil
		}
		
		var err error
		c.bridge, err = bridge.NewSyncAsyncBridge(
			c.publisher,
			c.subscriber,
			nil, // logger - bridge accepts nil
		)
		if err != nil {
			// Bridge creation failed - this shouldn't happen with valid components
			// Log error but don't panic, return nil to indicate failure
			return nil
		}
	}
	return c.bridge
}

// Close closes all resources
func (c *Client) Close() error {
	if c.bridge != nil {
		c.bridge.Close()
	}
	if c.publisher != nil {
		c.publisher.Close()
	}
	if c.subscriber != nil {
		c.subscriber.Close()
	}
	if c.transport != nil {
		return c.transport.Close()
	}
	return nil
}

// clientConfig holds client configuration
type clientConfig struct {
	logger             *slog.Logger
	enableFIFO         bool
	serviceName        string
	queueBindings      []messaging.QueueBinding
	publishPipeline    *interceptors.Pipeline
	subscribePipeline  *interceptors.Pipeline
	retryPolicy        reliability.RetryPolicy
	dlqHandler         *reliability.DLQHandler
	enableDefaultRetry bool
}

// ClientOption configures the client
type ClientOption func(*clientConfig)

// WithLogger sets the logger for all components
func WithLogger(logger *slog.Logger) ClientOption {
	return func(cfg *clientConfig) {
		cfg.logger = logger
	}
}

// WithDefaultLogger uses the default logger
func WithDefaultLogger() ClientOption {
	return func(cfg *clientConfig) {
		cfg.logger = slog.Default()
	}
}

// WithFIFOMode enables FIFO mode for strict message ordering
func WithFIFOMode(enabled bool) ClientOption {
	return func(cfg *clientConfig) {
		cfg.enableFIFO = enabled
	}
}

// WithServiceName sets the service name (used for queue naming)
func WithServiceName(name string) ClientOption {
	return func(cfg *clientConfig) {
		cfg.serviceName = name
	}
}

// WithQueueBindings sets the queue bindings for the service's receive queue
func WithQueueBindings(bindings ...messaging.QueueBinding) ClientOption {
	return func(cfg *clientConfig) {
		cfg.queueBindings = bindings
	}
}

// WithInterceptors sets both publish and subscribe interceptor pipelines
func WithInterceptors(pipeline *interceptors.Pipeline) ClientOption {
	return func(cfg *clientConfig) {
		cfg.publishPipeline = pipeline
		cfg.subscribePipeline = pipeline
	}
}

// WithPublishInterceptors sets the publish interceptor pipeline
func WithPublishInterceptors(pipeline *interceptors.Pipeline) ClientOption {
	return func(cfg *clientConfig) {
		cfg.publishPipeline = pipeline
	}
}

// WithSubscribeInterceptors sets the subscribe interceptor pipeline  
func WithSubscribeInterceptors(pipeline *interceptors.Pipeline) ClientOption {
	return func(cfg *clientConfig) {
		cfg.subscribePipeline = pipeline
	}
}

// WithRetryPolicy sets the retry policy for message processing
func WithRetryPolicy(policy reliability.RetryPolicy) ClientOption {
	return func(cfg *clientConfig) {
		cfg.retryPolicy = policy
	}
}

// WithDLQHandler sets the DLQ handler for failed messages
func WithDLQHandler(handler *reliability.DLQHandler) ClientOption {
	return func(cfg *clientConfig) {
		cfg.dlqHandler = handler
	}
}

// WithDefaultRetry enables default retry interceptor
func WithDefaultRetry() ClientOption {
	return func(cfg *clientConfig) {
		cfg.enableDefaultRetry = true
	}
}