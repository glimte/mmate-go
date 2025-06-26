//go:build integration
// +build integration

package rabbitmq

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testRabbitMQURL string
)

func init() {
	testRabbitMQURL = os.Getenv("RABBITMQ_URL")
	if testRabbitMQURL == "" {
		testRabbitMQURL = "amqp://guest:guest@localhost:5672/"
	}
}

// TestConnectionManagerIntegration tests connection management with real RabbitMQ
func TestConnectionManagerIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	t.Run("Connect and maintain connection", func(t *testing.T) {
		cm := NewConnectionManager(testRabbitMQURL,
			WithReconnectDelay(1*time.Second),
			WithMaxReconnectAttempts(3))

		// Connect
		err := cm.Connect(ctx)
		require.NoError(t, err)
		defer cm.Close()

		// Verify connection
		conn, err := cm.GetConnection()
		assert.NoError(t, err)
		assert.NotNil(t, conn)
		assert.False(t, conn.IsClosed())
	})

	t.Run("Auto-reconnection after connection loss", func(t *testing.T) {
		reconnected := make(chan struct{}, 1)
		disconnected := make(chan struct{}, 1)

		listener := &testConnectionListener{
			onReconnected: func() {
				reconnected <- struct{}{}
			},
			onDisconnected: func(err string) {
				disconnected <- struct{}{}
			},
		}

		cm := NewConnectionManager(testRabbitMQURL,
			WithReconnectDelay(500*time.Millisecond),
			WithMaxReconnectAttempts(5))
		
		cm.AddStateListener(listener)

		// Connect
		err := cm.Connect(ctx)
		require.NoError(t, err)
		defer cm.Close()

		// Get initial connection
		conn1, err := cm.GetConnection()
		require.NoError(t, err)

		// Force close the connection to simulate network failure
		conn1.Close()

		// Wait for disconnection and reconnection
		select {
		case <-disconnected:
			// Good, disconnection detected
		case <-time.After(2 * time.Second):
			t.Fatal("Disconnection not detected")
		}

		select {
		case <-reconnected:
			// Good, reconnection successful
		case <-time.After(5 * time.Second):
			t.Fatal("Reconnection failed")
		}

		// Verify new connection works
		conn2, err := cm.GetConnection()
		assert.NoError(t, err)
		assert.NotNil(t, conn2)
		assert.False(t, conn2.IsClosed())
		assert.NotEqual(t, conn1, conn2) // Should be different connection
	})
}

// TestChannelPoolIntegration tests channel pooling with real RabbitMQ
func TestChannelPoolIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	cm := NewConnectionManager(testRabbitMQURL)
	err := cm.Connect(ctx)
	require.NoError(t, err)
	defer cm.Close()

	conn, err := cm.GetConnection()
	require.NoError(t, err)

	t.Run("Create and use channel pool", func(t *testing.T) {
		pool, err := NewChannelPool(conn,
			WithMaxChannels(5),
			WithChannelIdleTimeout(30*time.Second))
		require.NoError(t, err)
		defer pool.Close()

		// Get multiple channels
		channels := make([]*PooledChannel, 3)
		for i := 0; i < 3; i++ {
			ch, err := pool.Get(ctx)
			require.NoError(t, err)
			require.NotNil(t, ch)
			channels[i] = ch
		}

		// Verify pool size
		assert.Equal(t, 3, pool.Size())

		// Return channels to pool
		for _, ch := range channels {
			pool.Put(ch)
		}

		// Get channel again (should reuse)
		ch, err := pool.Get(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, ch)
		pool.Put(ch)
	})

	t.Run("Channel health check", func(t *testing.T) {
		pool, err := NewChannelPool(conn,
			WithMaxChannels(2),
			WithHealthCheckInterval(1*time.Second))
		require.NoError(t, err)
		defer pool.Close()

		// Get channel
		ch1, err := pool.Get(ctx)
		require.NoError(t, err)

		// Close channel to simulate failure
		ch1.Channel.Close()
		pool.Put(ch1)

		// Wait for health check
		time.Sleep(2 * time.Second)

		// Get channel again (should get new healthy channel)
		ch2, err := pool.Get(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, ch2)
		assert.NotEqual(t, ch1.ID, ch2.ID)
	})
}

// TestPublisherIntegration tests publishing with real RabbitMQ
func TestPublisherIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	cm := NewConnectionManager(testRabbitMQURL)
	err := cm.Connect(ctx)
	require.NoError(t, err)
	defer cm.Close()

	conn, err := cm.GetConnection()
	require.NoError(t, err)

	// Setup test queue
	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	queueName := fmt.Sprintf("test-queue-%d", time.Now().UnixNano())
	_, err = ch.QueueDeclare(queueName, false, true, false, false, nil)
	require.NoError(t, err)

	t.Run("Publish with confirms", func(t *testing.T) {
		publisher, err := NewPublisher(conn,
			WithPublisherConfirms(true),
			WithPublisherMandatory(true))
		require.NoError(t, err)
		defer publisher.Close()

		message := PublishMessage{
			Exchange:   "",
			RoutingKey: queueName,
			Body:       []byte(`{"test": "message"}`),
			Headers: amqp.Table{
				"x-test-header": "test-value",
			},
		}

		// Publish message
		err = publisher.Publish(ctx, message)
		assert.NoError(t, err)

		// Verify message in queue
		delivery, ok, err := ch.Get(queueName, true)
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, message.Body, delivery.Body)
	})

	t.Run("Batch publish", func(t *testing.T) {
		publisher, err := NewPublisher(conn, WithPublisherConfirms(true))
		require.NoError(t, err)
		defer publisher.Close()

		// Create batch
		messages := make([]PublishMessage, 10)
		for i := 0; i < 10; i++ {
			messages[i] = PublishMessage{
				Exchange:   "",
				RoutingKey: queueName,
				Body:       []byte(fmt.Sprintf(`{"batch": %d}`, i)),
			}
		}

		// Publish batch
		results := publisher.PublishBatch(ctx, messages)
		
		// Verify all succeeded
		successCount := 0
		for _, result := range results {
			if result.Error == nil && result.Confirmed {
				successCount++
			}
		}
		assert.Equal(t, 10, successCount)

		// Verify messages in queue
		for i := 0; i < 10; i++ {
			delivery, ok, err := ch.Get(queueName, true)
			assert.NoError(t, err)
			assert.True(t, ok)
			assert.Contains(t, string(delivery.Body), "batch")
		}
	})
}

// TestConsumerIntegration tests consuming with real RabbitMQ
func TestConsumerIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	cm := NewConnectionManager(testRabbitMQURL)
	err := cm.Connect(ctx)
	require.NoError(t, err)
	defer cm.Close()

	conn, err := cm.GetConnection()
	require.NoError(t, err)

	// Setup test queue
	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	queueName := fmt.Sprintf("test-queue-%d", time.Now().UnixNano())
	_, err = ch.QueueDeclare(queueName, false, true, false, false, nil)
	require.NoError(t, err)

	t.Run("Consume messages with acknowledgment", func(t *testing.T) {
		consumer, err := NewConsumer(conn,
			WithConsumerPrefetch(5),
			WithConsumerConcurrency(2))
		require.NoError(t, err)
		defer consumer.Close()

		received := make(chan []byte, 10)
		handler := func(ctx context.Context, delivery Delivery) error {
			received <- delivery.Body
			return delivery.Ack(false)
		}

		// Start consuming
		consumerTag, err := consumer.Subscribe(ctx, queueName, "", handler)
		require.NoError(t, err)

		// Publish test messages
		for i := 0; i < 5; i++ {
			err = ch.PublishWithContext(ctx, "", queueName, false, false,
				amqp.Publishing{
					Body: []byte(fmt.Sprintf("message-%d", i)),
				})
			require.NoError(t, err)
		}

		// Verify all messages received
		for i := 0; i < 5; i++ {
			select {
			case msg := <-received:
				assert.Contains(t, string(msg), "message-")
			case <-time.After(2 * time.Second):
				t.Fatal("Timeout waiting for message")
			}
		}

		// Unsubscribe
		err = consumer.Unsubscribe(consumerTag)
		assert.NoError(t, err)
	})

	t.Run("Consumer error handling and retry", func(t *testing.T) {
		consumer, err := NewConsumer(conn,
			WithConsumerPrefetch(1),
			WithConsumerAckStrategy(AckOnSuccess))
		require.NoError(t, err)
		defer consumer.Close()

		attempts := 0
		handler := func(ctx context.Context, delivery Delivery) error {
			attempts++
			if attempts < 3 {
				return fmt.Errorf("simulated error")
			}
			return nil
		}

		// Start consuming
		_, err = consumer.Subscribe(ctx, queueName, "", handler)
		require.NoError(t, err)

		// Publish test message
		err = ch.PublishWithContext(ctx, "", queueName, false, false,
			amqp.Publishing{
				Body: []byte("retry-test"),
			})
		require.NoError(t, err)

		// Wait for retries
		time.Sleep(3 * time.Second)

		// Verify message was retried
		assert.Equal(t, 3, attempts)
	})
}

// TestDirectPublisherIntegration tests direct publisher with real RabbitMQ
func TestDirectPublisherIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	conn, err := amqp.Dial(testRabbitMQURL)
	require.NoError(t, err)
	defer conn.Close()

	// Setup test exchange and queue
	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	exchangeName := fmt.Sprintf("test-exchange-%d", time.Now().UnixNano())
	queueName := fmt.Sprintf("test-queue-%d", time.Now().UnixNano())

	err = ch.ExchangeDeclare(exchangeName, "direct", false, true, false, false, nil)
	require.NoError(t, err)

	_, err = ch.QueueDeclare(queueName, false, true, false, false, nil)
	require.NoError(t, err)

	err = ch.QueueBind(queueName, "test-key", exchangeName, false, nil)
	require.NoError(t, err)

	t.Run("Direct publish with routing", func(t *testing.T) {
		publisher, err := NewDirectPublisher(conn,
			WithDirectExchange(exchangeName),
			WithReliablePublishing(true))
		require.NoError(t, err)
		defer publisher.Close()

		// Publish to specific routing key
		err = publisher.Publish(ctx, "test-key", []byte("direct-message"), amqp.Table{
			"x-custom": "header",
		})
		assert.NoError(t, err)

		// Verify message received
		delivery, ok, err := ch.Get(queueName, true)
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte("direct-message"), delivery.Body)
		assert.Equal(t, "header", delivery.Headers["x-custom"])
	})
}

// testConnectionListener implements ConnectionStateListener for testing
type testConnectionListener struct {
	onConnected    func()
	onDisconnected func(string)
	onReconnecting func(int)
}

func (l *testConnectionListener) OnConnected() {
	if l.onConnected != nil {
		l.onConnected()
	}
}

func (l *testConnectionListener) OnDisconnected(err string) {
	if l.onDisconnected != nil {
		l.onDisconnected(err)
	}
}

func (l *testConnectionListener) OnReconnecting(attempt int) {
	if l.onReconnecting != nil {
		l.onReconnecting(attempt)
	}
}

// TestTopologyManagerIntegration tests topology management with real RabbitMQ
func TestTopologyManagerIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	conn, err := amqp.Dial(testRabbitMQURL)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	tm := NewTopologyManager(ch)

	t.Run("Declare and setup topology", func(t *testing.T) {
		topology := &Topology{
			Exchanges: []ExchangeDeclaration{
				{
					Name:       fmt.Sprintf("test-commands-%d", time.Now().UnixNano()),
					Type:       "topic",
					Durable:    false,
					AutoDelete: true,
				},
				{
					Name:       fmt.Sprintf("test-events-%d", time.Now().UnixNano()),
					Type:       "fanout",
					Durable:    false,
					AutoDelete: true,
				},
			},
			Queues: []QueueDeclaration{
				{
					Name:       fmt.Sprintf("test-cmd-queue-%d", time.Now().UnixNano()),
					Durable:    false,
					AutoDelete: true,
					Args: amqp.Table{
						"x-message-ttl": 60000,
					},
				},
			},
		}

		// Add binding
		topology.Bindings = []Binding{
			{
				Queue:      topology.Queues[0].Name,
				Exchange:   topology.Exchanges[0].Name,
				RoutingKey: "cmd.#",
			},
		}

		// Declare topology
		err := tm.DeclareTopology(ctx, topology)
		assert.NoError(t, err)
		
		// Verify exchange exists by publishing
		err = ch.PublishWithContext(ctx, topology.Exchanges[0].Name, "cmd.test", false, false,
			amqp.Publishing{Body: []byte("test")})
		assert.NoError(t, err)
	})
}

// TestEndToEndMessaging tests complete message flow with real RabbitMQ
func TestEndToEndMessaging(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	
	// Setup connection
	cm := NewConnectionManager(testRabbitMQURL)
	err := cm.Connect(ctx)
	require.NoError(t, err)
	defer cm.Close()

	conn, err := cm.GetConnection()
	require.NoError(t, err)

	// Setup topology
	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	queueName := fmt.Sprintf("e2e-queue-%d", time.Now().UnixNano())
	dlqName := fmt.Sprintf("dlq-%s", queueName)

	// Create main queue with DLQ
	_, err = ch.QueueDeclare(dlqName, true, false, false, false, nil)
	require.NoError(t, err)

	_, err = ch.QueueDeclare(queueName, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": dlqName,
	})
	require.NoError(t, err)

	t.Run("Message flow with DLQ", func(t *testing.T) {
		// Setup publisher
		publisher, err := NewPublisher(conn, WithPublisherConfirms(true))
		require.NoError(t, err)
		defer publisher.Close()

		// Setup consumer that always fails
		consumer, err := NewConsumer(conn, WithConsumerAckStrategy(AckOnSuccess))
		require.NoError(t, err)
		defer consumer.Close()

		failureCount := 0
		handler := func(ctx context.Context, delivery Delivery) error {
			failureCount++
			// Reject without requeue to send to DLQ
			delivery.Nack(false, false)
			return fmt.Errorf("simulated failure")
		}

		// Start consuming
		_, err = consumer.Subscribe(ctx, queueName, "", handler)
		require.NoError(t, err)

		// Publish message
		msg := PublishMessage{
			RoutingKey: queueName,
			Body:       []byte(`{"test": "dlq-test"}`),
			Headers: amqp.Table{
				"x-original-queue": queueName,
			},
		}
		err = publisher.Publish(ctx, msg)
		require.NoError(t, err)

		// Wait for processing
		time.Sleep(2 * time.Second)

		// Verify message ended up in DLQ
		delivery, ok, err := ch.Get(dlqName, true)
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, msg.Body, delivery.Body)
		
		// Check x-death header
		if xDeath, ok := delivery.Headers["x-death"]; ok {
			// RabbitMQ adds x-death header with rejection info
			assert.NotNil(t, xDeath)
		}
	})
}