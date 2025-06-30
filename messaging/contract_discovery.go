package messaging

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/schema"
)

const (
	// Discovery queue and routing
	contractDiscoveryQueue    = "contract.discover"
	contractAnnounceExchange  = "mmate.topic"
	contractAnnounceRoutingKey = "contract.announce"
	
	// Default timeouts
	defaultDiscoveryTimeout   = 5 * time.Second
	defaultAnnouncementInterval = 30 * time.Second
)

// ContractDiscovery handles endpoint contract discovery
type ContractDiscovery struct {
	subscriber  Subscriber
	publisher   Publisher
	contracts   map[string]*contracts.EndpointContract
	validator   *schema.ContractValidator
	serviceName string
	stopChan    chan struct{}
	mu          sync.RWMutex
}

// ContractDiscoveryOption configures contract discovery
type ContractDiscoveryOption func(*ContractDiscovery)

// WithContractValidator sets the contract validator
func WithContractValidator(validator *schema.ContractValidator) ContractDiscoveryOption {
	return func(cd *ContractDiscovery) {
		cd.validator = validator
	}
}

// WithServiceName sets the service name for contracts
func WithServiceName(name string) ContractDiscoveryOption {
	return func(cd *ContractDiscovery) {
		cd.serviceName = name
	}
}

// NewContractDiscovery creates a new contract discovery instance
func NewContractDiscovery(subscriber Subscriber, publisher Publisher, opts ...ContractDiscoveryOption) *ContractDiscovery {
	cd := &ContractDiscovery{
		subscriber:  subscriber,
		publisher:   publisher,
		contracts:   make(map[string]*contracts.EndpointContract),
		serviceName: "unknown-service",
		stopChan:    make(chan struct{}),
	}

	for _, opt := range opts {
		opt(cd)
	}

	// If no validator provided, create one with default message validator
	if cd.validator == nil {
		cd.validator = schema.NewContractValidator(schema.NewMessageValidator())
	}

	return cd
}

// Start starts the contract discovery service
func (cd *ContractDiscovery) Start(ctx context.Context) error {
	// Subscribe to discovery requests
	if err := cd.subscriber.Subscribe(ctx, contractDiscoveryQueue, "ContractDiscoveryRequest", 
		NewAutoAcknowledgingHandler(MessageHandlerFunc(cd.handleDiscoveryRequest))); err != nil {
		return fmt.Errorf("failed to subscribe to discovery queue: %w", err)
	}

	// Subscribe to contract announcements
	if err := cd.subscriber.Subscribe(ctx, contractAnnounceRoutingKey, "ContractAnnouncement",
		NewAutoAcknowledgingHandler(MessageHandlerFunc(cd.handleContractAnnouncement))); err != nil {
		return fmt.Errorf("failed to subscribe to announcements: %w", err)
	}

	// Start periodic announcement of our contracts
	go cd.announceContractsPeriodically(ctx)

	return nil
}

// Stop stops the contract discovery service
func (cd *ContractDiscovery) Stop() error {
	close(cd.stopChan)
	return cd.subscriber.Close()
}

// RegisterEndpoint registers a local endpoint contract
func (cd *ContractDiscovery) RegisterEndpoint(ctx context.Context, contract *contracts.EndpointContract) error {
	if contract == nil {
		return fmt.Errorf("contract cannot be nil")
	}

	// Set service name if not provided
	if contract.ServiceName == "" {
		contract.ServiceName = cd.serviceName
	}

	// Set timestamps
	now := time.Now()
	if contract.CreatedAt.IsZero() {
		contract.CreatedAt = now
	}
	contract.UpdatedAt = now

	// Validate contract
	if err := cd.validator.ValidateContract(ctx, contract); err != nil {
		return fmt.Errorf("contract validation failed: %w", err)
	}

	// Enrich with schemas
	if err := cd.validator.EnrichContractWithSchemas(contract); err != nil {
		// Log warning but don't fail registration
		// Schemas are optional enrichment
	}

	// Store contract
	cd.mu.Lock()
	cd.contracts[contract.EndpointID] = contract
	cd.mu.Unlock()

	// Announce the new contract
	announcement := &contracts.ContractAnnouncement{
		Contracts: []contracts.EndpointContract{*contract},
		Action:    "registered",
	}
	announcement.ID = generateID()
	announcement.Timestamp = time.Now()

	return cd.publisher.Publish(ctx, announcement,
		WithExchange(contractAnnounceExchange),
		WithRoutingKey(contractAnnounceRoutingKey))
}

// UnregisterEndpoint removes an endpoint contract
func (cd *ContractDiscovery) UnregisterEndpoint(ctx context.Context, endpointID string) error {
	cd.mu.Lock()
	contract, exists := cd.contracts[endpointID]
	if !exists {
		cd.mu.Unlock()
		return fmt.Errorf("endpoint %s not found", endpointID)
	}
	delete(cd.contracts, endpointID)
	cd.mu.Unlock()

	// Announce the removal
	announcement := &contracts.ContractAnnouncement{
		Contracts: []contracts.EndpointContract{*contract},
		Action:    "unregistered",
	}
	announcement.ID = generateID()
	announcement.Timestamp = time.Now()

	return cd.publisher.Publish(ctx, announcement,
		WithExchange(contractAnnounceExchange),
		WithRoutingKey(contractAnnounceRoutingKey))
}

// DiscoverEndpoint discovers a specific endpoint
func (cd *ContractDiscovery) DiscoverEndpoint(ctx context.Context, endpointID string) (*contracts.EndpointContract, error) {
	contracts, err := cd.DiscoverEndpoints(ctx, endpointID, "")
	if err != nil {
		return nil, err
	}

	if len(contracts) == 0 {
		return nil, fmt.Errorf("endpoint %s not found", endpointID)
	}

	return &contracts[0], nil
}

// DiscoverEndpoints discovers endpoints matching a pattern
func (cd *ContractDiscovery) DiscoverEndpoints(ctx context.Context, pattern string, version string) ([]contracts.EndpointContract, error) {
	// First check local cache
	localContracts := cd.getMatchingLocalContracts(pattern, version)
	if len(localContracts) > 0 {
		return localContracts, nil
	}

	// Create discovery request
	request := &contracts.ContractDiscoveryRequest{
		Pattern: pattern,
		Version: version,
	}
	request.ID = generateID()
	request.Timestamp = time.Now()

	// Send discovery request and wait for responses
	responseChan := make(chan *contracts.ContractDiscoveryResponse, 10)
	handler := NewAutoAcknowledgingHandler(MessageHandlerFunc(func(ctx context.Context, msg contracts.Message) error {
		if response, ok := msg.(*contracts.ContractDiscoveryResponse); ok {
			select {
			case responseChan <- response:
			case <-ctx.Done():
			}
		}
		return nil
	}))

	// Create temporary reply queue
	replyQueue := fmt.Sprintf("rpl.%s", request.ID)
	if err := cd.subscriber.Subscribe(ctx, replyQueue, "ContractDiscoveryResponse", handler); err != nil {
		return nil, fmt.Errorf("failed to subscribe to reply queue: %w", err)
	}
	defer cd.subscriber.Unsubscribe(replyQueue)

	// Send discovery request
	if err := cd.publisher.Publish(ctx, request, 
		WithDirectQueue(contractDiscoveryQueue),
		WithPublishReplyTo(replyQueue)); err != nil {
		return nil, fmt.Errorf("failed to send discovery request: %w", err)
	}

	// Wait for responses with timeout
	discoveredContracts := make([]contracts.EndpointContract, 0)
	timeout := time.NewTimer(defaultDiscoveryTimeout)
	defer timeout.Stop()

	for {
		select {
		case response := <-responseChan:
			discoveredContracts = append(discoveredContracts, response.Contracts...)
		case <-timeout.C:
			return discoveredContracts, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// GetLocalContracts returns all locally registered contracts
func (cd *ContractDiscovery) GetLocalContracts() []contracts.EndpointContract {
	cd.mu.RLock()
	defer cd.mu.RUnlock()

	contracts := make([]contracts.EndpointContract, 0, len(cd.contracts))
	for _, contract := range cd.contracts {
		contracts = append(contracts, *contract)
	}
	return contracts
}

// handleDiscoveryRequest handles incoming discovery requests
func (cd *ContractDiscovery) handleDiscoveryRequest(ctx context.Context, msg contracts.Message) error {
	request, ok := msg.(*contracts.ContractDiscoveryRequest)
	if !ok {
		return fmt.Errorf("expected ContractDiscoveryRequest, got %T", msg)
	}

	// Get matching contracts
	matchingContracts := cd.getMatchingLocalContracts(request.Pattern, request.Version)

	// Send response if we have matching contracts
	if len(matchingContracts) > 0 {
		response := &contracts.ContractDiscoveryResponse{
			Contracts: matchingContracts,
		}
		response.ID = generateID()
		response.Timestamp = time.Now()
		response.CorrelationID = request.GetID()

		// For now, we don't have access to transport headers
		// In a real implementation, we'd get replyTo from message metadata
		// Skip response for now
		_ = response
	}

	return nil
}

// handleContractAnnouncement handles contract announcements from other services
func (cd *ContractDiscovery) handleContractAnnouncement(ctx context.Context, msg contracts.Message) error {
	_, ok := msg.(*contracts.ContractAnnouncement)
	if !ok {
		return fmt.Errorf("expected ContractAnnouncement, got %T", msg)
	}

	// For now, we just log announcements
	// In a full implementation, we might cache remote contracts
	return nil
}

// getMatchingLocalContracts returns local contracts matching pattern and version
func (cd *ContractDiscovery) getMatchingLocalContracts(pattern string, version string) []contracts.EndpointContract {
	cd.mu.RLock()
	defer cd.mu.RUnlock()

	matching := make([]contracts.EndpointContract, 0)
	for _, contract := range cd.contracts {
		if contract.Matches(pattern, version) {
			matching = append(matching, *contract)
		}
	}
	return matching
}

// announceContractsPeriodically periodically announces local contracts
func (cd *ContractDiscovery) announceContractsPeriodically(ctx context.Context) {
	ticker := time.NewTicker(defaultAnnouncementInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cd.announceAllContracts(ctx)
		case <-cd.stopChan:
			return
		case <-ctx.Done():
			return
		}
	}
}

// announceAllContracts announces all local contracts
func (cd *ContractDiscovery) announceAllContracts(ctx context.Context) {
	localContracts := cd.GetLocalContracts()
	if len(localContracts) == 0 {
		return
	}

	announcement := &contracts.ContractAnnouncement{
		Contracts: localContracts,
		Action:    "registered",
	}
	announcement.ID = generateID()
	announcement.Timestamp = time.Now()

	cd.publisher.Publish(ctx, announcement,
		WithExchange(contractAnnounceExchange),
		WithRoutingKey(contractAnnounceRoutingKey))
}

// generateID generates a unique ID (simplified version)
func generateID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), time.Now().Nanosecond())
}