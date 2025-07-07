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
	"github.com/glimte/mmate-go/internal/journal"
	"github.com/glimte/mmate-go/monitor"
	"github.com/glimte/mmate-go/schema"
	"github.com/glimte/mmate-go/stageflow"
)

// Client provides the main entry point for mmate-go
type Client struct {
	transport          messaging.Transport
	publisher          *messaging.MessagePublisher
	subscriber         *messaging.MessageSubscriber
	dispatcher         *messaging.MessageDispatcher
	bridge             *bridge.SyncAsyncBridge
	contractDiscovery  *messaging.ContractDiscovery
	serviceName        string
	receiveQueue       string
	metricsCollector   interceptors.MetricsCollector
	connectionString   string // Store for monitoring access
	
	// Enterprise features
	ttlRetryScheduler  *reliability.TTLRetryScheduler
	ackTracker         *messaging.AcknowledgmentTracker
	syncJournal        journal.SyncMutationJournal
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

	// Initialize enterprise features
	var ttlRetryScheduler *reliability.TTLRetryScheduler
	var ackTracker *messaging.AcknowledgmentTracker
	var syncJournal journal.SyncMutationJournal
	
	// Get channel pool for enterprise features
	var channelPool *rabbitmq.ChannelPool
	if cfg.enableTTLRetry || cfg.enableAckTracking {
		// Create a separate connection manager for enterprise features
		connManager := rabbitmq.NewConnectionManager(connectionString)
		if err := connManager.Connect(context.Background()); err != nil {
			return nil, fmt.Errorf("failed to connect for enterprise features: %w", err)
		}
		
		channelPool, err = rabbitmq.NewChannelPool(connManager)
		if err != nil {
			connManager.Close()
			return nil, fmt.Errorf("failed to create channel pool for enterprise features: %w", err)
		}
	}
	
	// Create TTL retry scheduler if enabled
	if cfg.enableTTLRetry && channelPool != nil {
		ttlRetryScheduler = reliability.NewTTLRetryScheduler(channelPool, &reliability.TTLRetrySchedulerOptions{
			Logger: cfg.logger,
		})
		if err := ttlRetryScheduler.Initialize(context.Background()); err != nil {
			return nil, fmt.Errorf("failed to initialize TTL retry scheduler: %w", err)
		}
		cfg.logger.Info("TTL retry scheduler enabled")
	}
	
	// Create sync mutation journal if enabled
	if cfg.enableSyncJournal {
		baseOpts := []journal.InMemoryJournalOption{
			journal.WithMaxEntries(10000),
		}
		syncOpts := append(cfg.syncJournalOptions, journal.WithServiceID(cfg.serviceName))
		syncJournal = journal.NewInMemorySyncMutationJournal(baseOpts, syncOpts...)
		cfg.logger.Info("Sync mutation journal enabled")
	}

	// Create dispatcher (will configure contract extractor later if enabled)
	dispatcher := messaging.NewMessageDispatcher(
		messaging.WithDispatcherLogger(cfg.logger),
	)

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
	
	// Create acknowledgment tracker if enabled (after publisher is created)
	if cfg.enableAckTracking && publisher != nil {
		ackTracker = messaging.NewAcknowledgmentTracker(publisher, &messaging.AcknowledgmentTrackerOptions{
			DefaultTimeout: cfg.ackTimeout,
			Logger:         cfg.logger,
		})
		cfg.logger.Info("Acknowledgment tracking enabled", "timeout", cfg.ackTimeout)
	}

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
	} else if cfg.enableTTLRetry || cfg.enableDefaultRetry || cfg.retryPolicy != nil || sharedMetricsCollector != nil {
		// Create default pipeline with interceptors
		pipeline := interceptors.NewPipeline()
		
		// Add metrics interceptor first (to measure everything)
		if sharedMetricsCollector != nil {
			metricsInterceptor := interceptors.NewMetricsInterceptor(sharedMetricsCollector)
			pipeline.Use(metricsInterceptor)
		}
		
		// Add retry interceptor based on configuration
		if cfg.enableTTLRetry && ttlRetryScheduler != nil {
			// Use TTL-based retry interceptor
			ttlRetryInterceptor := interceptors.NewTTLRetryInterceptor(ttlRetryScheduler, &interceptors.TTLRetryInterceptorOptions{
				RetryPolicy: cfg.ttlRetryPolicy,
				Logger:      cfg.logger,
			})
			pipeline.Use(ttlRetryInterceptor)
		} else if cfg.retryPolicy != nil {
			// Use basic retry interceptor with custom policy
			retryInterceptor := interceptors.NewRetryInterceptor(cfg.retryPolicy).WithLogger(cfg.logger)
			pipeline.Use(retryInterceptor)
		} else if cfg.enableDefaultRetry {
			// Use default basic retry policy
			defaultPolicy := reliability.NewExponentialBackoff(100*time.Millisecond, 30*time.Second, 2.0, 3)
			retryInterceptor := interceptors.NewRetryInterceptor(defaultPolicy).WithLogger(cfg.logger)
			pipeline.Use(retryInterceptor)
		}
		
		// Add processing acknowledgment interceptor if enabled
		if cfg.enableAckTracking && publisher != nil {
			processingAckInterceptor := messaging.NewProcessingAckInterceptor(publisher).WithLogger(cfg.logger)
			pipeline.Use(processingAckInterceptor)
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
	if cfg.enableContractPublishing {
		// Create validator
		validator := schema.NewMessageValidator()
		contractValidator := schema.NewContractValidator(validator)
		
		// Create contract discovery
		contractDiscovery = messaging.NewContractDiscovery(
			transport,
			subscriber,
			publisher,
			messaging.WithServiceName(cfg.serviceName),
			messaging.WithContractValidator(contractValidator),
		)
		
		// Start contract discovery
		if err := contractDiscovery.Start(context.Background()); err != nil {
			return nil, fmt.Errorf("failed to start contract discovery: %w", err)
		}
		
		// Create and set contract extractor on dispatcher
		contractExtractor := messaging.NewContractExtractor(contractDiscovery, cfg.serviceName)
		dispatcher.SetContractExtractor(contractExtractor)
		
		cfg.logger.Info("Contract publishing enabled", "service", cfg.serviceName)
	}

	return &Client{
		transport:         transport,
		publisher:         publisher,
		subscriber:        subscriber,
		dispatcher:        dispatcher,
		bridge:            nil, // Created lazily
		contractDiscovery: contractDiscovery,
		serviceName:       cfg.serviceName,
		receiveQueue:      queueName,
		metricsCollector:  sharedMetricsCollector,
		connectionString:  connectionString,
		
		// Enterprise features
		ttlRetryScheduler: ttlRetryScheduler,
		ackTracker:        ackTracker,
		syncJournal:       syncJournal,
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
func (c *Client) getChannelPoolFromTransport(_ *rabbitmqTransport.Transport) (*rabbitmq.ChannelPool, error) {
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

// Enterprise features accessors

// TTLRetryScheduler returns the TTL retry scheduler if enabled
func (c *Client) TTLRetryScheduler() *reliability.TTLRetryScheduler {
	return c.ttlRetryScheduler
}

// AcknowledgmentTracker returns the acknowledgment tracker if enabled
func (c *Client) AcknowledgmentTracker() *messaging.AcknowledgmentTracker {
	return c.ackTracker
}

// SyncMutationJournal returns the sync mutation journal if enabled
func (c *Client) SyncMutationJournal() journal.SyncMutationJournal {
	return c.syncJournal
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

// PublishEvent publishes an event message
// This is a convenience method that delegates to the publisher
func (c *Client) PublishEvent(ctx context.Context, evt contracts.Event, options ...messaging.PublishOption) error {
	if c.publisher == nil {
		return fmt.Errorf("publisher not initialized")
	}
	return c.publisher.PublishEvent(ctx, evt, options...)
}

// PublishCommand publishes a command message
// This is a convenience method that delegates to the publisher
func (c *Client) PublishCommand(ctx context.Context, cmd contracts.Command, options ...messaging.PublishOption) error {
	if c.publisher == nil {
		return fmt.Errorf("publisher not initialized")
	}
	return c.publisher.PublishCommand(ctx, cmd, options...)
}

// PublishReply publishes a reply message
// This is a convenience method that delegates to the publisher
func (c *Client) PublishReply(ctx context.Context, reply contracts.Reply, replyTo string, options ...messaging.PublishOption) error {
	if c.publisher == nil {
		return fmt.Errorf("publisher not initialized")
	}
	return c.publisher.PublishReply(ctx, reply, replyTo, options...)
}

// ScheduleCommand schedules a command to be delivered at a specific time
// This is a convenience method that delegates to the publisher
func (c *Client) ScheduleCommand(ctx context.Context, cmd contracts.Command, scheduledFor time.Time, options ...messaging.PublishOption) error {
	if c.publisher == nil {
		return fmt.Errorf("publisher not initialized")
	}
	return c.publisher.ScheduleCommand(ctx, cmd, scheduledFor, options...)
}

// ScheduleEvent schedules an event to be delivered at a specific time
// This is a convenience method that delegates to the publisher
func (c *Client) ScheduleEvent(ctx context.Context, evt contracts.Event, scheduledFor time.Time, options ...messaging.PublishOption) error {
	if c.publisher == nil {
		return fmt.Errorf("publisher not initialized")
	}
	return c.publisher.ScheduleEvent(ctx, evt, scheduledFor, options...)
}

// ScheduleQuery schedules a query to be delivered at a specific time
// This is a convenience method that delegates to the publisher
func (c *Client) ScheduleQuery(ctx context.Context, query contracts.Query, scheduledFor time.Time, options ...messaging.PublishOption) error {
	if c.publisher == nil {
		return fmt.Errorf("publisher not initialized")
	}
	return c.publisher.ScheduleQuery(ctx, query, scheduledFor, options...)
}

// PublishWithDelay publishes a message with a delay
// This is a convenience method that uses the WithDelay option
func (c *Client) PublishWithDelay(ctx context.Context, msg contracts.Message, delay time.Duration, options ...messaging.PublishOption) error {
	if c.publisher == nil {
		return fmt.Errorf("publisher not initialized")
	}
	opts := append([]messaging.PublishOption{messaging.WithDelay(delay)}, options...)
	return c.publisher.Publish(ctx, msg, opts...)
}

// NewBatch creates a new batch for batch publishing
// Messages can be added to the batch and published together
func (c *Client) NewBatch() *messaging.Batch {
	if c.publisher == nil {
		return nil
	}
	return c.publisher.NewBatch()
}

// BeginTx begins a new transaction for transactional publishing
// All messages published within the transaction are committed or rolled back together
func (c *Client) BeginTx() (*messaging.Transaction, error) {
	if c.publisher == nil {
		return nil, fmt.Errorf("publisher not initialized")
	}
	return c.publisher.BeginTx()
}

// Enterprise features convenience methods

// SendWithAck sends a message and waits for processing acknowledgment
// Returns the acknowledgment response or an error if acknowledgment tracking is not enabled
func (c *Client) SendWithAck(ctx context.Context, msg contracts.Message, options ...messaging.PublishOption) (*messaging.AckResponse, error) {
	if c.ackTracker == nil {
		return nil, fmt.Errorf("acknowledgment tracking not enabled")
	}
	return c.ackTracker.SendWithAck(ctx, msg, options...)
}

// PublishEventWithAck publishes an event and waits for processing acknowledgment
func (c *Client) PublishEventWithAck(ctx context.Context, evt contracts.Event, options ...messaging.PublishOption) (*messaging.AckResponse, error) {
	if c.ackTracker == nil {
		return nil, fmt.Errorf("acknowledgment tracking not enabled")
	}
	return c.ackTracker.SendWithAck(ctx, evt, options...)
}

// PublishCommandWithAck publishes a command and waits for processing acknowledgment
func (c *Client) PublishCommandWithAck(ctx context.Context, cmd contracts.Command, options ...messaging.PublishOption) (*messaging.AckResponse, error) {
	if c.ackTracker == nil {
		return nil, fmt.Errorf("acknowledgment tracking not enabled")
	}
	return c.ackTracker.SendWithAck(ctx, cmd, options...)
}

// RecordEntityMutation records an entity-level mutation for synchronization
func (c *Client) RecordEntityMutation(ctx context.Context, record *journal.EntityMutationRecord) error {
	if c.syncJournal == nil {
		return fmt.Errorf("sync mutation journal not enabled")
	}
	return c.syncJournal.RecordEntityMutation(ctx, record)
}

// GetEntityMutations retrieves mutations for a specific entity
func (c *Client) GetEntityMutations(ctx context.Context, entityType, entityID string) ([]*journal.EntityMutationRecord, error) {
	if c.syncJournal == nil {
		return nil, fmt.Errorf("sync mutation journal not enabled")
	}
	return c.syncJournal.GetEntityMutations(ctx, entityType, entityID)
}

// GetUnsyncedMutations retrieves mutations that haven't been synced
func (c *Client) GetUnsyncedMutations(ctx context.Context, limit int) ([]*journal.EntityMutationRecord, error) {
	if c.syncJournal == nil {
		return nil, fmt.Errorf("sync mutation journal not enabled")
	}
	return c.syncJournal.GetUnsyncedMutations(ctx, limit)
}

// NewStageFlowEngine creates a new StageFlow engine with contract extraction if enabled
func (c *Client) NewStageFlowEngine(opts ...stageflow.EngineOption) *stageflow.StageFlowEngine {
	engine := stageflow.NewStageFlowEngine(c.publisher, c.subscriber, c.transport, opts...)
	
	// Set contract extractor if contract publishing is enabled
	if c.contractDiscovery != nil {
		contractExtractor := messaging.NewContractExtractor(c.contractDiscovery, c.serviceName)
		engine.SetContractExtractor(contractExtractor)
	}
	
	// Set service queue
	engine.SetServiceQueue(c.receiveQueue)
	
	return engine
}

// Close closes all resources
func (c *Client) Close() error {
	// Close enterprise features first
	if c.ackTracker != nil {
		if err := c.ackTracker.Close(); err != nil {
			// Log error if possible, but don't fail close operation
			slog.Error("Failed to close acknowledgment tracker", "error", err)
		}
	}
	if c.ttlRetryScheduler != nil {
		if err := c.ttlRetryScheduler.Close(); err != nil {
			// Log error if possible, but don't fail close operation
			slog.Error("Failed to close TTL retry scheduler", "error", err)
		}
	}
	
	// Close other components
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
	enableContractPublishing bool
	
	// Enterprise features configuration
	enableTTLRetry          bool
	ttlRetryPolicy          reliability.RetryPolicy
	enableAckTracking       bool
	ackTimeout              time.Duration
	enableSyncJournal       bool
	syncJournalOptions      []journal.SyncJournalOption
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

// WithContractPublishing enables contract publishing and discovery
func WithContractPublishing() ClientOption {
	return func(cfg *clientConfig) {
		cfg.enableContractPublishing = true
	}
}

// Enterprise features configuration options

// WithTTLRetry enables TTL-based retry scheduling using RabbitMQ DLX
func WithTTLRetry(policy ...reliability.RetryPolicy) ClientOption {
	return func(cfg *clientConfig) {
		cfg.enableTTLRetry = true
		if len(policy) > 0 {
			cfg.ttlRetryPolicy = policy[0]
		} else {
			// Default TTL retry policy
			cfg.ttlRetryPolicy = reliability.NewExponentialBackoff(
				100*time.Millisecond,
				30*time.Second,
				2.0,
				5,
			)
		}
	}
}

// WithAcknowledgmentTracking enables application-level acknowledgment tracking
func WithAcknowledgmentTracking(timeout ...time.Duration) ClientOption {
	return func(cfg *clientConfig) {
		cfg.enableAckTracking = true
		if len(timeout) > 0 {
			cfg.ackTimeout = timeout[0]
		} else {
			cfg.ackTimeout = 30 * time.Second
		}
	}
}

// WithSyncMutationJournal enables enhanced mutation journal with sync capabilities
func WithSyncMutationJournal(options ...journal.SyncJournalOption) ClientOption {
	return func(cfg *clientConfig) {
		cfg.enableSyncJournal = true
		cfg.syncJournalOptions = options
	}
}