//go:build integration
// +build integration

package messaging

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockTransport implements Transport interface for testing
type MockTransport struct {
	mu              sync.RWMutex
	connected       bool
	queues          map[string]*mockQueue
	exchanges       map[string]*mockExchange
	bindings        map[string][]QueueBinding
	deliveryChannel chan mockDeliveryWrapper
	publisher       *MockTransportPublisher
	subscriber      *MockTransportSubscriber
	failureMode     bool
	connectionError error
}

type mockQueue struct {
	name      string
	options   QueueOptions
	messages  []*mockMessage
	consumers []mockConsumer
	mu        sync.Mutex
}

type mockExchange struct {
	name string
	typ  string
}

type mockMessage struct {
	envelope *contracts.Envelope
	headers  map[string]interface{}
}

type mockConsumer struct {
	queue   string
	handler func(TransportDelivery) error
	options SubscriptionOptions
}

type mockDeliveryWrapper struct {
	delivery *MockDelivery
	consumer mockConsumer
}

// NewMockTransport creates a new mock transport for testing
func NewMockTransport() *MockTransport {
	mt := &MockTransport{
		queues:          make(map[string]*mockQueue),
		exchanges:       make(map[string]*mockExchange),
		bindings:        make(map[string][]QueueBinding),
		deliveryChannel: make(chan mockDeliveryWrapper, 100),
		connected:       false,
	}
	mt.publisher = &MockTransportPublisher{transport: mt}
	mt.subscriber = &MockTransportSubscriber{transport: mt}
	return mt
}

// Connect simulates connection establishment
func (mt *MockTransport) Connect(ctx context.Context) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if mt.connectionError != nil {
		return mt.connectionError
	}

	if mt.connected {
		return fmt.Errorf("already connected")
	}

	mt.connected = true
	
	// Start delivery processor
	go mt.processDeliveries()
	
	return nil
}

// Close closes all resources
func (mt *MockTransport) Close() error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if !mt.connected {
		return fmt.Errorf("not connected")
	}

	mt.connected = false
	close(mt.deliveryChannel)
	return nil
}

// IsConnected returns connection status
func (mt *MockTransport) IsConnected() bool {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.connected
}

// Publisher returns the transport publisher
func (mt *MockTransport) Publisher() TransportPublisher {
	return mt.publisher
}

// Subscriber returns the transport subscriber
func (mt *MockTransport) Subscriber() TransportSubscriber {
	return mt.subscriber
}

// CreateQueue creates a queue
func (mt *MockTransport) CreateQueue(ctx context.Context, name string, options QueueOptions) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if !mt.connected {
		return fmt.Errorf("not connected")
	}

	if _, exists := mt.queues[name]; exists {
		return fmt.Errorf("queue %s already exists", name)
	}

	mt.queues[name] = &mockQueue{
		name:      name,
		options:   options,
		messages:  make([]*mockMessage, 0),
		consumers: make([]mockConsumer, 0),
	}

	return nil
}

// DeleteQueue deletes a queue
func (mt *MockTransport) DeleteQueue(ctx context.Context, name string) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if !mt.connected {
		return fmt.Errorf("not connected")
	}

	delete(mt.queues, name)
	delete(mt.bindings, name)
	return nil
}

// BindQueue creates a binding
func (mt *MockTransport) BindQueue(ctx context.Context, queue, exchange, routingKey string) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if !mt.connected {
		return fmt.Errorf("not connected")
	}

	if _, exists := mt.queues[queue]; !exists {
		return fmt.Errorf("queue %s does not exist", queue)
	}

	binding := QueueBinding{
		Exchange:   exchange,
		RoutingKey: routingKey,
	}

	mt.bindings[queue] = append(mt.bindings[queue], binding)
	return nil
}

// DeclareQueueWithBindings creates a queue with bindings
func (mt *MockTransport) DeclareQueueWithBindings(ctx context.Context, name string, options QueueOptions, bindings []QueueBinding) error {
	if err := mt.CreateQueue(ctx, name, options); err != nil {
		return err
	}

	for _, binding := range bindings {
		if err := mt.BindQueue(ctx, name, binding.Exchange, binding.RoutingKey); err != nil {
			return err
		}
	}

	return nil
}

// processDeliveries simulates message delivery to consumers
func (mt *MockTransport) processDeliveries() {
	for wrapper := range mt.deliveryChannel {
		go func(w mockDeliveryWrapper) {
			// Simulate async delivery
			time.Sleep(10 * time.Millisecond)
			w.consumer.handler(w.delivery)
		}(wrapper)
	}
}

// MockTransportPublisher implements TransportPublisher
type MockTransportPublisher struct {
	transport *MockTransport
}

// Publish sends a message
func (mtp *MockTransportPublisher) Publish(ctx context.Context, exchange, routingKey string, envelope *contracts.Envelope) error {
	mtp.transport.mu.Lock()
	defer mtp.transport.mu.Unlock()

	if !mtp.transport.connected {
		return fmt.Errorf("not connected")
	}

	if mtp.transport.failureMode {
		return fmt.Errorf("simulated publish failure")
	}

	// Route message to queues based on bindings
	for queueName, bindings := range mtp.transport.bindings {
		for _, binding := range bindings {
			if binding.Exchange == exchange && matchRoutingKey(binding.RoutingKey, routingKey) {
				queue := mtp.transport.queues[queueName]
				if queue != nil {
					msg := &mockMessage{
						envelope: envelope,
						headers:  make(map[string]interface{}),
					}
					
					queue.mu.Lock()
					queue.messages = append(queue.messages, msg)
					
					// Deliver to one consumer (round-robin)
					if len(queue.consumers) > 0 {
						// Use message count as simple round-robin index
						consumerIndex := (len(queue.messages) - 1) % len(queue.consumers)
						consumer := queue.consumers[consumerIndex]
						
						delivery := &MockDelivery{
							body:      envelope.Payload,
							headers:   msg.headers,
							envelope:  envelope,
							acked:     false,
							rejected:  false,
							transport: mtp.transport,
							queue:     queueName,
						}
						
						select {
						case mtp.transport.deliveryChannel <- mockDeliveryWrapper{
							delivery: delivery,
							consumer: consumer,
						}:
						default:
							// Channel full, drop message (simulates overflow)
						}
					}
					queue.mu.Unlock()
				}
			}
		}
	}

	return nil
}

// Close closes the publisher
func (mtp *MockTransportPublisher) Close() error {
	return nil
}

// MockTransportSubscriber implements TransportSubscriber
type MockTransportSubscriber struct {
	transport *MockTransport
}

// Subscribe registers a handler
func (mts *MockTransportSubscriber) Subscribe(ctx context.Context, queue string, handler func(TransportDelivery) error, options SubscriptionOptions) error {
	mts.transport.mu.Lock()
	defer mts.transport.mu.Unlock()

	if !mts.transport.connected {
		return fmt.Errorf("not connected")
	}

	q, exists := mts.transport.queues[queue]
	if !exists {
		return fmt.Errorf("queue %s does not exist", queue)
	}

	consumer := mockConsumer{
		queue:   queue,
		handler: handler,
		options: options,
	}

	q.mu.Lock()
	q.consumers = append(q.consumers, consumer)
	
	// Deliver existing messages
	for _, msg := range q.messages {
		delivery := &MockDelivery{
			body:      msg.envelope.Payload,
			headers:   msg.headers,
			envelope:  msg.envelope,
			acked:     false,
			rejected:  false,
			transport: mts.transport,
			queue:     queue,
		}
		
		select {
		case mts.transport.deliveryChannel <- mockDeliveryWrapper{
			delivery: delivery,
			consumer: consumer,
		}:
		default:
		}
	}
	// Clear messages after delivery to avoid duplicate deliveries
	q.messages = q.messages[:0]
	q.mu.Unlock()

	return nil
}

// Unsubscribe removes a subscription
func (mts *MockTransportSubscriber) Unsubscribe(queue string) error {
	mts.transport.mu.Lock()
	defer mts.transport.mu.Unlock()

	q, exists := mts.transport.queues[queue]
	if !exists {
		return fmt.Errorf("queue %s does not exist", queue)
	}

	q.mu.Lock()
	q.consumers = nil
	q.mu.Unlock()

	return nil
}

// Close closes the subscriber
func (mts *MockTransportSubscriber) Close() error {
	return nil
}

// MockDelivery implements TransportDelivery
type MockDelivery struct {
	body      []byte
	headers   map[string]interface{}
	envelope  *contracts.Envelope
	acked     bool
	rejected  bool
	transport *MockTransport
	queue     string
	mu        sync.Mutex
}

// Body returns the message body
func (md *MockDelivery) Body() []byte {
	return md.body
}

// Acknowledge marks the message as processed
func (md *MockDelivery) Acknowledge() error {
	md.mu.Lock()
	defer md.mu.Unlock()

	if md.acked || md.rejected {
		return fmt.Errorf("message already acknowledged/rejected")
	}

	md.acked = true
	return nil
}

// Reject rejects the message
func (md *MockDelivery) Reject(requeue bool) error {
	md.mu.Lock()
	defer md.mu.Unlock()

	if md.acked || md.rejected {
		return fmt.Errorf("message already acknowledged/rejected")
	}

	md.rejected = true
	
	if requeue {
		// Simulate requeue by redelivering to consumers
		md.transport.mu.RLock()
		if q, exists := md.transport.queues[md.queue]; exists {
			q.mu.Lock()
			// Redeliver to all consumers
			for _, consumer := range q.consumers {
				newDelivery := &MockDelivery{
					body:      md.body,
					headers:   md.headers,
					envelope:  md.envelope,
					acked:     false,
					rejected:  false,
					transport: md.transport,
					queue:     md.queue,
				}
				
				select {
				case md.transport.deliveryChannel <- mockDeliveryWrapper{
					delivery: newDelivery,
					consumer: consumer,
				}:
				default:
				}
			}
			q.mu.Unlock()
		}
		md.transport.mu.RUnlock()
	}

	return nil
}

// Headers returns message headers
func (md *MockDelivery) Headers() map[string]interface{} {
	return md.headers
}

// Helper function to match routing keys with wildcard support
func matchRoutingKey(pattern, key string) bool {
	// Exact match
	if pattern == key {
		return true
	}
	
	// Support * wildcard (matches one word)
	if strings.Contains(pattern, "*") {
		patternParts := strings.Split(pattern, ".")
		keyParts := strings.Split(key, ".")
		
		if len(patternParts) != len(keyParts) {
			return false
		}
		
		for i := range patternParts {
			if patternParts[i] != "*" && patternParts[i] != keyParts[i] {
				return false
			}
		}
		return true
	}
	
	// Support # wildcard (matches zero or more words)
	if strings.Contains(pattern, "#") {
		// Simple implementation: check prefix before #
		prefix := strings.Split(pattern, "#")[0]
		return strings.HasPrefix(key, prefix)
	}
	
	return false
}

// Test methods for simulating various scenarios

// SetFailureMode enables/disables failure simulation
func (mt *MockTransport) SetFailureMode(enabled bool) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.failureMode = enabled
}

// SetConnectionError sets an error to be returned on Connect
func (mt *MockTransport) SetConnectionError(err error) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.connectionError = err
}

// GetQueueMessageCount returns the number of messages in a queue
func (mt *MockTransport) GetQueueMessageCount(queue string) int {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	if q, exists := mt.queues[queue]; exists {
		q.mu.Lock()
		defer q.mu.Unlock()
		return len(q.messages)
	}
	return 0
}

// SimulateConnectionLoss simulates a connection failure
func (mt *MockTransport) SimulateConnectionLoss() {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.connected = false
}

// Integration tests using mock transport

func TestMockTransportIntegration(t *testing.T) {
	t.Run("Basic publish and subscribe", func(t *testing.T) {
		ctx := context.Background()
		transport := NewMockTransport()

		// Connect
		err := transport.Connect(ctx)
		require.NoError(t, err)
		defer transport.Close()

		// Create queue
		err = transport.CreateQueue(ctx, "test-queue", QueueOptions{
			Durable: true,
		})
		require.NoError(t, err)

		// Bind queue
		err = transport.BindQueue(ctx, "test-queue", "test-exchange", "test.key")
		require.NoError(t, err)

		// Subscribe
		received := make(chan *contracts.Envelope, 1)
		subscriber := transport.Subscriber()
		err = subscriber.Subscribe(ctx, "test-queue", func(delivery TransportDelivery) error {
			env := &contracts.Envelope{}
			env.Payload = delivery.Body()
			received <- env
			return delivery.Acknowledge()
		}, SubscriptionOptions{})
		require.NoError(t, err)

		// Publish
		publisher := transport.Publisher()
		envelope := &contracts.Envelope{
			Payload: []byte("test message"),
		}
		err = publisher.Publish(ctx, "test-exchange", "test.key", envelope)
		require.NoError(t, err)

		// Verify receipt
		select {
		case env := <-received:
			assert.Equal(t, "test message", string(env.Payload))
		case <-time.After(2 * time.Second):
			t.Fatal("Message not received")
		}
	})

	t.Run("Multiple consumers on same queue", func(t *testing.T) {
		ctx := context.Background()
		transport := NewMockTransport()

		err := transport.Connect(ctx)
		require.NoError(t, err)
		defer transport.Close()

		// Create queue with bindings
		err = transport.DeclareQueueWithBindings(ctx, "shared-queue", QueueOptions{}, []QueueBinding{
			{Exchange: "events", RoutingKey: "user.*"},
		})
		require.NoError(t, err)

		// Multiple consumers
		var mu sync.Mutex
		receivedMessages := make(map[string]int)
		consumer1Count := 0
		consumer2Count := 0

		subscriber := transport.Subscriber()
		
		// Consumer 1
		err = subscriber.Subscribe(ctx, "shared-queue", func(delivery TransportDelivery) error {
			mu.Lock()
			consumer1Count++
			msg := string(delivery.Body())
			receivedMessages[msg]++
			mu.Unlock()
			return delivery.Acknowledge()
		}, SubscriptionOptions{})
		require.NoError(t, err)

		// Consumer 2
		err = subscriber.Subscribe(ctx, "shared-queue", func(delivery TransportDelivery) error {
			mu.Lock()
			consumer2Count++
			msg := string(delivery.Body())
			receivedMessages[msg]++
			mu.Unlock()
			return delivery.Acknowledge()
		}, SubscriptionOptions{})
		require.NoError(t, err)

		// Publish multiple messages
		publisher := transport.Publisher()
		for i := 0; i < 10; i++ {
			envelope := &contracts.Envelope{
				Payload: []byte(fmt.Sprintf("message-%d", i)),
			}
			err = publisher.Publish(ctx, "events", "user.created", envelope)
			require.NoError(t, err)
		}

		// Wait for messages to be processed
		time.Sleep(500 * time.Millisecond)

		// Verify results
		mu.Lock()
		totalReceived := consumer1Count + consumer2Count
		assert.Equal(t, 10, totalReceived, "Total messages should be 10")
		assert.Equal(t, 10, len(receivedMessages), "Should receive all 10 unique messages")
		
		// Each message should be received exactly once
		for msg, count := range receivedMessages {
			assert.Equal(t, 1, count, "Message %s should be received exactly once", msg)
		}
		
		// Both consumers should have received some messages (load balancing)
		assert.Greater(t, consumer1Count, 0, "Consumer 1 should receive some messages")
		assert.Greater(t, consumer2Count, 0, "Consumer 2 should receive some messages")
		mu.Unlock()
	})

	t.Run("Message rejection and requeue", func(t *testing.T) {
		ctx := context.Background()
		transport := NewMockTransport()

		err := transport.Connect(ctx)
		require.NoError(t, err)
		defer transport.Close()

		// Setup queue
		err = transport.CreateQueue(ctx, "retry-queue", QueueOptions{})
		require.NoError(t, err)
		err = transport.BindQueue(ctx, "retry-queue", "", "retry-queue")
		require.NoError(t, err)

		attempts := 0
		processed := make(chan bool, 1)

		// Subscribe with retry logic
		subscriber := transport.Subscriber()
		err = subscriber.Subscribe(ctx, "retry-queue", func(delivery TransportDelivery) error {
			attempts++
			if attempts < 3 {
				// Reject and requeue
				delivery.Reject(true)
				return fmt.Errorf("simulated error")
			}
			// Success on third attempt
			processed <- true
			return delivery.Acknowledge()
		}, SubscriptionOptions{})
		require.NoError(t, err)

		// Publish message
		publisher := transport.Publisher()
		envelope := &contracts.Envelope{
			Payload: []byte("retry-message"),
		}
		err = publisher.Publish(ctx, "", "retry-queue", envelope)
		require.NoError(t, err)

		// Wait for processing
		select {
		case <-processed:
			assert.Equal(t, 3, attempts)
		case <-time.After(3 * time.Second):
			t.Fatal("Message not processed after retries")
		}
	})

	t.Run("Connection failure handling", func(t *testing.T) {
		ctx := context.Background()
		transport := NewMockTransport()

		// Set connection error
		transport.SetConnectionError(fmt.Errorf("connection refused"))
		
		err := transport.Connect(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection refused")
		assert.False(t, transport.IsConnected())

		// Clear error and connect
		transport.SetConnectionError(nil)
		err = transport.Connect(ctx)
		require.NoError(t, err)
		assert.True(t, transport.IsConnected())

		// Simulate connection loss
		transport.SimulateConnectionLoss()
		assert.False(t, transport.IsConnected())

		// Operations should fail when disconnected
		publisher := transport.Publisher()
		err = publisher.Publish(ctx, "test", "test", &contracts.Envelope{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not connected")
	})

	t.Run("Publish failure simulation", func(t *testing.T) {
		ctx := context.Background()
		transport := NewMockTransport()

		err := transport.Connect(ctx)
		require.NoError(t, err)
		defer transport.Close()

		// Enable failure mode
		transport.SetFailureMode(true)

		publisher := transport.Publisher()
		err = publisher.Publish(ctx, "test", "test", &contracts.Envelope{
			Payload: []byte("should fail"),
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "simulated publish failure")

		// Disable failure mode
		transport.SetFailureMode(false)
		
		err = publisher.Publish(ctx, "test", "test", &contracts.Envelope{
			Payload: []byte("should succeed"),
		})
		assert.NoError(t, err)
	})

	t.Run("Queue management operations", func(t *testing.T) {
		ctx := context.Background()
		transport := NewMockTransport()

		err := transport.Connect(ctx)
		require.NoError(t, err)
		defer transport.Close()

		// Create queue
		err = transport.CreateQueue(ctx, "managed-queue", QueueOptions{
			Durable:    true,
			AutoDelete: false,
		})
		require.NoError(t, err)

		// Try to create same queue again
		err = transport.CreateQueue(ctx, "managed-queue", QueueOptions{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")

		// Delete queue
		err = transport.DeleteQueue(ctx, "managed-queue")
		assert.NoError(t, err)

		// Should be able to create again after deletion
		err = transport.CreateQueue(ctx, "managed-queue", QueueOptions{})
		assert.NoError(t, err)
	})
}

// TestMockTransportConcurrency tests concurrent operations
func TestMockTransportConcurrency(t *testing.T) {
	ctx := context.Background()
	transport := NewMockTransport()

	err := transport.Connect(ctx)
	require.NoError(t, err)
	defer transport.Close()

	// Create test queue
	err = transport.CreateQueue(ctx, "concurrent-queue", QueueOptions{})
	require.NoError(t, err)
	err = transport.BindQueue(ctx, "concurrent-queue", "concurrent", "test")
	require.NoError(t, err)

	// Track received messages
	var receivedMu sync.Mutex
	received := make(map[string]bool)
	done := make(chan bool)

	// Subscribe
	subscriber := transport.Subscriber()
	err = subscriber.Subscribe(ctx, "concurrent-queue", func(delivery TransportDelivery) error {
		msg := string(delivery.Body())
		receivedMu.Lock()
		received[msg] = true
		if len(received) == 100 {
			close(done)
		}
		receivedMu.Unlock()
		return delivery.Acknowledge()
	}, SubscriptionOptions{})
	require.NoError(t, err)

	// Concurrent publishers
	publisher := transport.Publisher()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				envelope := &contracts.Envelope{
					Payload: []byte(fmt.Sprintf("msg-%d-%d", id, j)),
				}
				err := publisher.Publish(ctx, "concurrent", "test", envelope)
				if err != nil {
					t.Errorf("Publish error: %v", err)
				}
			}
		}(i)
	}

	// Wait for all publishes
	wg.Wait()

	// Wait for all messages to be received
	select {
	case <-done:
		assert.Equal(t, 100, len(received))
	case <-time.After(5 * time.Second):
		t.Fatalf("Only received %d/100 messages", len(received))
	}
}