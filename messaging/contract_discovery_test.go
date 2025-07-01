package messaging

import (
	"context"
	"fmt"
	"testing"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/schema"
)

// Mock implementations for testing
type mockContractPublisher struct {
	published []contracts.Message
}

type mockContractTransport struct {
	queues map[string]QueueOptions
}

func newMockContractTransport() *mockContractTransport {
	return &mockContractTransport{
		queues: make(map[string]QueueOptions),
	}
}

func (m *mockContractTransport) Publisher() TransportPublisher { return nil }
func (m *mockContractTransport) Subscriber() TransportSubscriber { return nil }
func (m *mockContractTransport) CreateQueue(ctx context.Context, name string, options QueueOptions) error {
	m.queues[name] = options
	return nil
}
func (m *mockContractTransport) DeleteQueue(ctx context.Context, name string) error {
	delete(m.queues, name)
	return nil
}
func (m *mockContractTransport) BindQueue(ctx context.Context, queue, exchange, routingKey string) error {
	return nil
}
func (m *mockContractTransport) DeclareQueueWithBindings(ctx context.Context, name string, options QueueOptions, bindings []QueueBinding) error {
	m.queues[name] = options
	return nil
}
func (m *mockContractTransport) Connect(ctx context.Context) error { return nil }
func (m *mockContractTransport) Close() error { return nil }
func (m *mockContractTransport) IsConnected() bool { return true }

func (m *mockContractPublisher) Publish(ctx context.Context, msg contracts.Message, options ...PublishOption) error {
	m.published = append(m.published, msg)
	return nil
}

func (m *mockContractPublisher) PublishEvent(ctx context.Context, event contracts.Event, options ...PublishOption) error {
	m.published = append(m.published, event)
	return nil
}

func (m *mockContractPublisher) PublishCommand(ctx context.Context, command contracts.Command, options ...PublishOption) error {
	m.published = append(m.published, command)
	return nil
}

func (m *mockContractPublisher) Close() error {
	return nil
}

type mockContractSubscriber struct {
	subscriptions map[string]MessageHandler
}

func newMockContractSubscriber() *mockContractSubscriber {
	return &mockContractSubscriber{
		subscriptions: make(map[string]MessageHandler),
	}
}

func (m *mockContractSubscriber) Subscribe(ctx context.Context, queue string, messageType string, handler MessageHandler, options ...SubscriptionOption) error {
	m.subscriptions[queue] = handler
	return nil
}

func (m *mockContractSubscriber) Unsubscribe(queue string) error {
	delete(m.subscriptions, queue)
	return nil
}

func (m *mockContractSubscriber) Close() error {
	return nil
}

// Helper to register test message types
func setupTestTypes(t *testing.T) {
	// Register contract discovery types
	Register("ContractDiscoveryRequest", func() contracts.Message {
		return &contracts.ContractDiscoveryRequest{}
	})
	Register("ContractDiscoveryResponse", func() contracts.Message {
		return &contracts.ContractDiscoveryResponse{}
	})
	Register("ContractAnnouncement", func() contracts.Message {
		return &contracts.ContractAnnouncement{}
	})
	
	// Register test endpoint types
	Register("TestRequest", func() contracts.Message {
		return &TestMessage{Type: "TestRequest"}
	})
	Register("TestResponse", func() contracts.Message {
		return &TestMessage{Type: "TestResponse"}
	})
}

type TestMessage struct {
	contracts.BaseMessage
	Type string
}

func (m *TestMessage) GetType() string {
	return m.Type
}

func TestNewContractDiscovery(t *testing.T) {
	setupTestTypes(t)
	
	transport := newMockContractTransport()
	pub := &mockContractPublisher{}
	sub := newMockContractSubscriber()
	
	cd := NewContractDiscovery(transport, sub, pub, WithServiceName("test-service"))
	
	if cd == nil {
		t.Fatal("NewContractDiscovery returned nil")
	}
	
	if cd.serviceName != "test-service" {
		t.Errorf("Expected service name 'test-service', got %s", cd.serviceName)
	}
	
	if cd.validator == nil {
		t.Error("Expected validator to be initialized")
	}
}

func TestContractDiscovery_RegisterEndpoint(t *testing.T) {
	setupTestTypes(t)
	
	transport := newMockContractTransport()
	pub := &mockContractPublisher{}
	sub := newMockContractSubscriber()
	validator := schema.NewContractValidator(schema.NewMessageValidator())
	
	cd := NewContractDiscovery(transport, sub, pub,
		WithServiceName("test-service"),
		WithContractValidator(validator))
	
	ctx := context.Background()
	
	contract := &contracts.EndpointContract{
		EndpointID: "test.endpoint",
		Queue:      "test.queue",
		InputType:  "TestRequest",
		OutputType: "TestResponse",
		Version:    "1.0.0",
	}
	
	err := cd.RegisterEndpoint(ctx, contract)
	if err != nil {
		t.Fatalf("RegisterEndpoint failed: %v", err)
	}
	
	// Check contract was stored
	localContracts := cd.GetLocalContracts()
	if len(localContracts) != 1 {
		t.Fatalf("Expected 1 local contract, got %d", len(localContracts))
	}
	
	stored := localContracts[0]
	if stored.EndpointID != "test.endpoint" {
		t.Errorf("Expected endpoint ID 'test.endpoint', got %s", stored.EndpointID)
	}
	
	if stored.ServiceName != "test-service" {
		t.Errorf("Expected service name 'test-service', got %s", stored.ServiceName)
	}
	
	// Check announcement was published
	if len(pub.published) != 1 {
		t.Fatalf("Expected 1 published message, got %d", len(pub.published))
	}
	
	announcement, ok := pub.published[0].(*contracts.ContractAnnouncement)
	if !ok {
		t.Fatalf("Expected ContractAnnouncement, got %T", pub.published[0])
	}
	
	if announcement.Action != "registered" {
		t.Errorf("Expected action 'registered', got %s", announcement.Action)
	}
}

func TestContractDiscovery_UnregisterEndpoint(t *testing.T) {
	setupTestTypes(t)
	
	transport := newMockContractTransport()
	pub := &mockContractPublisher{}
	sub := newMockContractSubscriber()
	
	cd := NewContractDiscovery(transport, sub, pub, WithServiceName("test-service"))
	ctx := context.Background()
	
	// Register first
	contract := &contracts.EndpointContract{
		EndpointID:  "test.endpoint",
		Queue:       "test.queue",
		InputType:   "TestRequest",
		OutputType:  "TestResponse",
		ServiceName: "test-service",
	}
	
	err := cd.RegisterEndpoint(ctx, contract)
	if err != nil {
		t.Fatalf("RegisterEndpoint failed: %v", err)
	}
	
	// Clear published messages
	pub.published = nil
	
	// Unregister
	err = cd.UnregisterEndpoint(ctx, "test.endpoint")
	if err != nil {
		t.Fatalf("UnregisterEndpoint failed: %v", err)
	}
	
	// Check contract was removed
	localContracts := cd.GetLocalContracts()
	if len(localContracts) != 0 {
		t.Errorf("Expected 0 local contracts after unregister, got %d", len(localContracts))
	}
	
	// Check unregister announcement
	if len(pub.published) != 1 {
		t.Fatalf("Expected 1 published message, got %d", len(pub.published))
	}
	
	announcement, ok := pub.published[0].(*contracts.ContractAnnouncement)
	if !ok {
		t.Fatalf("Expected ContractAnnouncement, got %T", pub.published[0])
	}
	
	if announcement.Action != "unregistered" {
		t.Errorf("Expected action 'unregistered', got %s", announcement.Action)
	}
}

func TestContractDiscovery_GetMatchingLocalContracts(t *testing.T) {
	setupTestTypes(t)
	
	transport := newMockContractTransport()
	pub := &mockContractPublisher{}
	sub := newMockContractSubscriber()
	
	cd := NewContractDiscovery(transport, sub, pub)
	ctx := context.Background()
	
	// Register multiple contracts
	contracts := []contracts.EndpointContract{
		{
			EndpointID:  "order.validate",
			Queue:       "order.validate",
			InputType:   "TestRequest",
			OutputType:  "TestResponse",
			ServiceName: "order-service",
			Version:     "1.0.0",
		},
		{
			EndpointID:  "order.submit",
			Queue:       "order.submit",
			InputType:   "TestRequest",
			OutputType:  "TestResponse",
			ServiceName: "order-service",
			Version:     "1.0.0",
		},
		{
			EndpointID:  "inventory.check",
			Queue:       "inventory.check",
			InputType:   "TestRequest",
			OutputType:  "TestResponse",
			ServiceName: "inventory-service",
			Version:     "2.0.0",
		},
	}
	
	for _, c := range contracts {
		contract := c // capture loop variable
		err := cd.RegisterEndpoint(ctx, &contract)
		if err != nil {
			t.Fatalf("Failed to register contract: %v", err)
		}
	}
	
	// Test pattern matching
	tests := []struct {
		name    string
		pattern string
		version string
		want    int
	}{
		{
			name:    "match all order endpoints",
			pattern: "order.*",
			version: "",
			want:    2,
		},
		{
			name:    "match specific endpoint",
			pattern: "order.validate",
			version: "",
			want:    1,
		},
		{
			name:    "match with version",
			pattern: "",
			version: "1.0.0",
			want:    2,
		},
		{
			name:    "no matches",
			pattern: "payment.*",
			version: "",
			want:    0,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matching := cd.getMatchingLocalContracts(tt.pattern, tt.version)
			if len(matching) != tt.want {
				t.Errorf("Expected %d matching contracts, got %d", tt.want, len(matching))
			}
		})
	}
}

func TestContractDiscovery_Start(t *testing.T) {
	setupTestTypes(t)
	
	transport := newMockContractTransport()
	pub := &mockContractPublisher{}
	sub := newMockContractSubscriber()
	
	cd := NewContractDiscovery(transport, sub, pub)
	ctx := context.Background()
	
	err := cd.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	
	// Check discovery queue was created
	if _, ok := transport.queues[contractDiscoveryQueue]; !ok {
		t.Error("Expected discovery queue to be created")
	}
	
	// Check subscriptions were created
	if len(sub.subscriptions) != 2 {
		t.Errorf("Expected 2 subscriptions, got %d", len(sub.subscriptions))
	}
	
	// Check discovery queue subscription
	if _, ok := sub.subscriptions[contractDiscoveryQueue]; !ok {
		t.Error("Expected subscription to discovery queue")
	}
	
	// Check announcement subscription - it should be to the service-specific queue
	announcementQueue := fmt.Sprintf("contract.announce.%s", cd.serviceName)
	if _, ok := sub.subscriptions[announcementQueue]; !ok {
		t.Error("Expected subscription to announcement queue")
	}
	
	// Stop the service
	err = cd.Stop()
	if err != nil {
		t.Errorf("Stop failed: %v", err)
	}
}

func TestContractDiscovery_HandleDiscoveryRequest(t *testing.T) {
	setupTestTypes(t)
	
	transport := newMockContractTransport()
	pub := &mockContractPublisher{}
	sub := newMockContractSubscriber()
	
	cd := NewContractDiscovery(transport, sub, pub)
	ctx := context.Background()
	
	// Register a contract
	contract := &contracts.EndpointContract{
		EndpointID:  "test.endpoint",
		Queue:       "test.queue",
		InputType:   "TestRequest",
		OutputType:  "TestResponse",
		ServiceName: "test-service",
		Version:     "1.0.0",
	}
	
	err := cd.RegisterEndpoint(ctx, contract)
	if err != nil {
		t.Fatalf("RegisterEndpoint failed: %v", err)
	}
	
	// Clear published messages
	pub.published = nil
	
	// Create discovery request
	request := &contracts.ContractDiscoveryRequest{
		BaseMessage: contracts.BaseMessage{
			ID:            "req-123",
			CorrelationID: "reply-queue-123",
		},
		Pattern: "test.*",
		Version: "",
	}
	
	// Handle the request
	err = cd.handleDiscoveryRequest(ctx, request)
	if err != nil {
		t.Fatalf("handleDiscoveryRequest failed: %v", err)
	}
	
	// Currently, handleDiscoveryRequest doesn't publish responses
	// because we don't have access to replyTo from transport headers
	// This is a limitation of the current implementation
	if len(pub.published) != 0 {
		t.Errorf("Expected 0 published responses (current limitation), got %d", len(pub.published))
	}
}

func TestContractDiscovery_DiscoverEndpoint(t *testing.T) {
	setupTestTypes(t)
	
	transport := newMockContractTransport()
	pub := &mockContractPublisher{}
	sub := newMockContractSubscriber()
	
	cd := NewContractDiscovery(transport, sub, pub)
	ctx := context.Background()
	
	// Register a local contract
	contract := &contracts.EndpointContract{
		EndpointID:  "test.endpoint",
		Queue:       "test.queue",
		InputType:   "TestRequest",
		OutputType:  "TestResponse",
		ServiceName: "test-service",
		Version:     "1.0.0",
	}
	
	err := cd.RegisterEndpoint(ctx, contract)
	if err != nil {
		t.Fatalf("RegisterEndpoint failed: %v", err)
	}
	
	// Discover from local cache
	discovered, err := cd.DiscoverEndpoint(ctx, "test.endpoint")
	if err != nil {
		t.Fatalf("DiscoverEndpoint failed: %v", err)
	}
	
	if discovered.EndpointID != "test.endpoint" {
		t.Errorf("Expected endpoint ID 'test.endpoint', got %s", discovered.EndpointID)
	}
	
	// Try to discover non-existent endpoint
	// This would normally trigger remote discovery, but with mocks it returns empty
	_, err = cd.DiscoverEndpoint(ctx, "non.existent")
	if err == nil {
		t.Error("Expected error for non-existent endpoint")
	}
}