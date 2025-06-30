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
	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/messaging"
	rabbitmqTransport "github.com/glimte/mmate-go/transports/rabbitmq"
	"github.com/glimte/mmate-go/internal/rabbitmq"
	"github.com/glimte/mmate-go/interceptors"
	"github.com/glimte/mmate-go/internal/reliability"
	"github.com/glimte/mmate-go/monitor"
	"github.com/glimte/mmate-go/schema"
)

// Client provides the main entry point for mmate-go
type Client struct {
	transport         messaging.Transport
	publisher         *messaging.MessagePublisher
	subscriber        *messaging.MessageSubscriber
	dispatcher        *messaging.MessageDispatcher
	bridge            *bridge.SyncAsyncBridge
	contractDiscovery *messaging.ContractDiscovery
	serviceName       string
	receiveQueue      string
	metricsCollector  interceptors.MetricsCollector
	connectionString  string // Store for monitoring access
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

	// Determine which metrics collector to use (shared across all pipelines)
	var sharedMetricsCollector interceptors.MetricsCollector
	if cfg.metricsCollector != nil {
		sharedMetricsCollector = cfg.metricsCollector
	} else if cfg.enableDefaultMetrics {
		sharedMetricsCollector = monitor.NewSimpleMetricsCollector()
	}

	// Create publisher with interceptors if configured
	publisherOpts := []messaging.PublisherOption{
		messaging.WithPublisherLogger(cfg.logger),
	}
	
	// Add circuit breaker if configured
	if cfg.circuitBreaker != nil {
		publisherOpts = append(publisherOpts, messaging.WithCircuitBreaker(cfg.circuitBreaker))
	}
	if cfg.publishPipeline != nil {
		publisherOpts = append(publisherOpts, messaging.WithPublisherInterceptors(cfg.publishPipeline))
	} else if sharedMetricsCollector != nil {
		// Create default pipeline with metrics interceptor for publisher
		pipeline := interceptors.NewPipeline()
		metricsInterceptor := interceptors.NewMetricsInterceptor(sharedMetricsCollector)
		pipeline.Use(metricsInterceptor)
		publisherOpts = append(publisherOpts, messaging.WithPublisherInterceptors(pipeline))
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
	
	// Create default pipeline with interceptors if enabled
	if cfg.subscribePipeline != nil {
		subscriberOpts = append(subscriberOpts, messaging.WithSubscriberInterceptors(cfg.subscribePipeline))
	} else if cfg.enableDefaultRetry || cfg.retryPolicy != nil || sharedMetricsCollector != nil {
		// Create default pipeline with interceptors
		pipeline := interceptors.NewPipeline()
		
		// Add metrics interceptor first (to measure everything)
		if sharedMetricsCollector != nil {
			metricsInterceptor := interceptors.NewMetricsInterceptor(sharedMetricsCollector)
			pipeline.Use(metricsInterceptor)
		}
		
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

	// Create contract discovery if enabled
	var contractDiscovery *messaging.ContractDiscovery
	if cfg.enableContractDiscovery {
		// Create validator
		validator := schema.NewMessageValidator()
		contractValidator := schema.NewContractValidator(validator)
		
		// Create contract discovery
		contractDiscovery = messaging.NewContractDiscovery(
			subscriber,
			publisher,
			messaging.WithServiceName(cfg.serviceName),
			messaging.WithContractValidator(contractValidator),
		)
		
		// Start contract discovery
		if err := contractDiscovery.Start(context.Background()); err != nil {
			return nil, fmt.Errorf("failed to start contract discovery: %w", err)
		}
		
		cfg.logger.Info("Contract discovery enabled", "service", cfg.serviceName)
	}

	return &Client{
		transport:         transport,
		publisher:         publisher,
		subscriber:        subscriber,
		dispatcher:        dispatcher,
		contractDiscovery: contractDiscovery,
		serviceName:       cfg.serviceName,
		receiveQueue:      queueName,
		metricsCollector:  sharedMetricsCollector,
		connectionString:  connectionString,
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

// MetricsCollector returns the metrics collector if metrics are enabled
func (c *Client) MetricsCollector() interceptors.MetricsCollector {
	return c.metricsCollector
}

// GetMetricsSummary returns a summary of collected metrics (if using SimpleMetricsCollector)
func (c *Client) GetMetricsSummary() *monitor.MetricsSummary {
	if simpleCollector, ok := c.metricsCollector.(*monitor.SimpleMetricsCollector); ok {
		summary := simpleCollector.GetMetricsSummary()
		return &summary
	}
	return nil
}

// NewServiceMonitor creates a service-scoped monitor for this client
// This provides safe, service-scoped access to RabbitMQ monitoring without mastodon behavior
func (c *Client) NewServiceMonitor() (*monitor.ServiceMonitor, error) {
	// Extract channel pool from the transport
	rabbitmqTransport, ok := c.transport.(*rabbitmqTransport.Transport)
	if !ok {
		return nil, fmt.Errorf("service monitoring requires RabbitMQ transport")
	}
	
	// Get the channel pool from the transport
	channelPool, err := c.getChannelPoolFromTransport(rabbitmqTransport)
	if err != nil {
		return nil, fmt.Errorf("failed to get channel pool: %w", err)
	}
	
	return monitor.NewServiceMonitorFromChannelPool(c.serviceName, c.receiveQueue, channelPool), nil
}

// GetServiceMetrics returns combined local and queue metrics for this service only
func (c *Client) GetServiceMetrics(ctx context.Context) (*monitor.ServiceMetrics, error) {
	serviceMonitor, err := c.NewServiceMonitor()
	if err != nil {
		return nil, fmt.Errorf("failed to create service monitor: %w", err)
	}

	localMetrics := c.GetMetricsSummary()
	return serviceMonitor.ServiceMetrics(ctx, localMetrics)
}

// GetServiceHealth returns health status for this service's queue only
func (c *Client) GetServiceHealth(ctx context.Context) (*monitor.ServiceHealth, error) {
	serviceMonitor, err := c.NewServiceMonitor()
	if err != nil {
		return nil, fmt.Errorf("failed to create service monitor: %w", err)
	}

	return serviceMonitor.ServiceQueueHealth(ctx)
}

// GetMyConsumerStats returns consumer statistics for this service's queues only
func (c *Client) GetMyConsumerStats(ctx context.Context) (*monitor.ConsumerStats, error) {
	serviceMonitor, err := c.NewServiceMonitor()
	if err != nil {
		return nil, fmt.Errorf("failed to create service monitor: %w", err)
	}

	return serviceMonitor.GetMyConsumerStats(ctx)
}

// GetAdvancedMetrics returns advanced metrics analysis if available
func (c *Client) GetAdvancedMetrics() *monitor.AdvancedMetricsReport {
	if advancedCollector, ok := c.metricsCollector.(monitor.AdvancedMetricsCollector); ok {
		return &monitor.AdvancedMetricsReport{
			LatencyStats:   advancedCollector.GetLatencyPercentiles(),
			ThroughputStats: advancedCollector.GetThroughputStats(),
			ErrorAnalysis:  advancedCollector.GetErrorAnalysis(),
			CollectedAt:    time.Now(),
		}
	}
	return nil
}

// getChannelPoolFromTransport creates a channel pool for monitoring purposes
func (c *Client) getChannelPoolFromTransport(transport *rabbitmqTransport.Transport) (*rabbitmq.ChannelPool, error) {
	// Create a new connection manager for monitoring
	// This ensures monitoring doesn't interfere with the main transport
	connManager := rabbitmq.NewConnectionManager(c.connectionString)
	if err := connManager.Connect(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to connect for monitoring: %w", err)
	}
	
	channelPool, err := rabbitmq.NewChannelPool(connManager)
	if err != nil {
		connManager.Close()
		return nil, fmt.Errorf("failed to create channel pool: %w", err)
	}
	
	return channelPool, nil
}

// ContractDiscovery returns the contract discovery service if enabled
func (c *Client) ContractDiscovery() *messaging.ContractDiscovery {
	return c.contractDiscovery
}

// RegisterEndpoint registers a service endpoint for discovery
func (c *Client) RegisterEndpoint(ctx context.Context, contract *contracts.EndpointContract) error {
	if c.contractDiscovery == nil {
		return fmt.Errorf("contract discovery not enabled")
	}
	return c.contractDiscovery.RegisterEndpoint(ctx, contract)
}

// DiscoverEndpoint discovers a specific endpoint by ID
func (c *Client) DiscoverEndpoint(ctx context.Context, endpointID string) (*contracts.EndpointContract, error) {
	if c.contractDiscovery == nil {
		return nil, fmt.Errorf("contract discovery not enabled")
	}
	return c.contractDiscovery.DiscoverEndpoint(ctx, endpointID)
}

// DiscoverEndpoints discovers endpoints matching a pattern
func (c *Client) DiscoverEndpoints(ctx context.Context, pattern string, version string) ([]contracts.EndpointContract, error) {
	if c.contractDiscovery == nil {
		return nil, fmt.Errorf("contract discovery not enabled")
	}
	return c.contractDiscovery.DiscoverEndpoints(ctx, pattern, version)
}

// Close closes all resources
func (c *Client) Close() error {
	if c.contractDiscovery != nil {
		c.contractDiscovery.Stop()
	}
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
	logger                  *slog.Logger
	enableFIFO              bool
	serviceName             string
	queueBindings           []messaging.QueueBinding
	publishPipeline         *interceptors.Pipeline
	subscribePipeline       *interceptors.Pipeline
	retryPolicy             reliability.RetryPolicy
	dlqHandler              *reliability.DLQHandler
	enableDefaultRetry      bool
	metricsCollector        interceptors.MetricsCollector
	enableDefaultMetrics    bool
	circuitBreaker          *reliability.CircuitBreaker
	enableContractDiscovery bool
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

// WithMetrics sets a custom metrics collector
func WithMetrics(collector interceptors.MetricsCollector) ClientOption {
	return func(cfg *clientConfig) {
		cfg.metricsCollector = collector
	}
}

// WithDefaultMetrics enables default metrics collection
func WithDefaultMetrics() ClientOption {
	return func(cfg *clientConfig) {
		cfg.enableDefaultMetrics = true
	}
}

// WithCircuitBreaker sets the circuit breaker for message publishing
func WithCircuitBreaker(cb *reliability.CircuitBreaker) ClientOption {
	return func(cfg *clientConfig) {
		cfg.circuitBreaker = cb
	}
}

// WithContractDiscovery enables contract discovery
func WithContractDiscovery() ClientOption {
	return func(cfg *clientConfig) {
		cfg.enableContractDiscovery = true
	}
}