package rabbitmq

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConnectionManager(t *testing.T) {
	t.Run("NewConnectionManager creates manager with defaults", func(t *testing.T) {
		manager := NewConnectionManager("amqp://localhost:5672")
		
		assert.Equal(t, "amqp://localhost:5672", manager.url)
		assert.Equal(t, 5*time.Second, manager.reconnectDelay)
		assert.Equal(t, -1, manager.maxRetries) // -1 means infinite retries by default
		assert.NotNil(t, manager.logger)
		assert.False(t, manager.isConnected)
	})
	
	t.Run("NewConnectionManager applies options", func(t *testing.T) {
		logger := slog.Default()
		manager := NewConnectionManager(
			"amqp://test:5672",
			WithReconnectDelay(10*time.Second),
			WithMaxRetries(5),
			WithLogger(logger),
		)
		
		assert.Equal(t, "amqp://test:5672", manager.url)
		assert.Equal(t, 10*time.Second, manager.reconnectDelay)
		assert.Equal(t, 5, manager.maxRetries)
		assert.Equal(t, logger, manager.logger)
	})
	
	t.Run("Connect with invalid URL fails", func(t *testing.T) {
		manager := NewConnectionManager("invalid://url")
		err := manager.Connect(context.Background())
		assert.Error(t, err)
		assert.False(t, manager.IsConnected())
	})
	
	t.Run("GetConnection returns error when not connected", func(t *testing.T) {
		manager := NewConnectionManager("amqp://localhost:5672")
		_, err := manager.GetConnection()
		assert.Error(t, err)
		assert.Equal(t, ErrConnectionNotReady, err)
	})
}

func TestChannelPool(t *testing.T) {
	t.Run("ChannelPool creation fails without connection", func(t *testing.T) {
		manager := NewConnectionManager("amqp://localhost:5672")
		_, err := NewChannelPool(manager)
		assert.Error(t, err)
		// Check that it's a ChannelError
		var chanErr *ChannelError
		assert.ErrorAs(t, err, &chanErr)
		assert.Equal(t, "pool initialization", chanErr.Op)
	})
	
	t.Run("ChannelPool applies options", func(t *testing.T) {
		// Create a pool without actually connecting
		pool := &ChannelPool{
			maxSize:     10,
			minSize:     2,
			idleTimeout: 5 * time.Minute,
		}
		
		// Apply options
		WithMaxSize(20)(pool)
		WithMinSize(5)(pool)
		WithIdleTimeout(10 * time.Minute)(pool)
		
		assert.Equal(t, 20, pool.maxSize)
		assert.Equal(t, 5, pool.minSize)
		assert.Equal(t, 10*time.Minute, pool.idleTimeout)
	})
	
	t.Run("Get from closed pool returns error", func(t *testing.T) {
		pool := &ChannelPool{
			closed: true,
		}
		
		ctx := context.Background()
		_, err := pool.Get(ctx)
		assert.Error(t, err)
		assert.Equal(t, ErrChannelPoolClosed, err)
	})
	
	t.Run("Put to closed pool closes channel", func(t *testing.T) {
		pool := &ChannelPool{
			closed: true,
		}
		
		// This should not panic
		pool.Put(nil)
	})
	
	t.Run("Size returns active count", func(t *testing.T) {
		pool := &ChannelPool{
			activeCount: 5,
		}
		
		assert.Equal(t, 5, pool.Size())
	})
}

func TestPublisher(t *testing.T) {
	t.Run("NewPublisher creates with defaults", func(t *testing.T) {
		pool := &ChannelPool{}
		publisher := NewPublisher(pool)
		
		assert.Equal(t, pool, publisher.pool)
		assert.Equal(t, 5*time.Second, publisher.confirmTimeout)
		assert.Equal(t, 10*time.Second, publisher.publishTimeout)
		assert.Equal(t, 3, publisher.maxRetries)
	})
	
	t.Run("NewPublisher applies options", func(t *testing.T) {
		pool := &ChannelPool{}
		publisher := NewPublisher(
			pool,
			WithConfirmTimeout(3*time.Second),
			WithPublishTimeout(15*time.Second),
			WithPublishRetries(5),
		)
		
		assert.Equal(t, 3*time.Second, publisher.confirmTimeout)
		assert.Equal(t, 15*time.Second, publisher.publishTimeout)
		assert.Equal(t, 5, publisher.maxRetries)
	})
}

func TestTopologyManager(t *testing.T) {
	t.Run("NewTopologyManager creates manager", func(t *testing.T) {
		pool := &ChannelPool{}
		tm := NewTopologyManager(pool)
		
		assert.Equal(t, pool, tm.pool)
	})
	
	t.Run("CreateDefaultTopology returns expected structure", func(t *testing.T) {
		topology := CreateDefaultTopology()
		
		assert.Len(t, topology.Exchanges, 3)
		
		// Check exchanges
		exchangeNames := make(map[string]bool)
		for _, ex := range topology.Exchanges {
			exchangeNames[ex.Name] = true
		}
		
		assert.True(t, exchangeNames["commands"])
		assert.True(t, exchangeNames["events"])
		assert.True(t, exchangeNames["dlx"])
	})
	
	t.Run("ExchangeDeclaration fields", func(t *testing.T) {
		exchange := ExchangeDeclaration{
			Name:       "test.exchange",
			Type:       "topic",
			Durable:    true,
			AutoDelete: false,
		}
		
		assert.Equal(t, "test.exchange", exchange.Name)
		assert.Equal(t, "topic", exchange.Type)
		assert.True(t, exchange.Durable)
		assert.False(t, exchange.AutoDelete)
	})
	
	t.Run("QueueDeclaration fields", func(t *testing.T) {
		queue := QueueDeclaration{
			Name:       "test.queue",
			Durable:    true,
			AutoDelete: false,
			Exclusive:  false,
		}
		
		assert.Equal(t, "test.queue", queue.Name)
		assert.True(t, queue.Durable)
		assert.False(t, queue.AutoDelete)
		assert.False(t, queue.Exclusive)
	})
	
	t.Run("Binding fields", func(t *testing.T) {
		binding := Binding{
			Queue:      "test.queue",
			Exchange:   "test.exchange",
			RoutingKey: "test.key",
		}
		
		assert.Equal(t, "test.queue", binding.Queue)
		assert.Equal(t, "test.exchange", binding.Exchange)
		assert.Equal(t, "test.key", binding.RoutingKey)
	})
}

func TestErrors(t *testing.T) {
	t.Run("PublishMessage structure", func(t *testing.T) {
		msg := PublishMessage{
			Exchange:   "test.exchange",
			RoutingKey: "test.key",
			Mandatory:  true,
			Immediate:  false,
		}
		
		assert.Equal(t, "test.exchange", msg.Exchange)
		assert.Equal(t, "test.key", msg.RoutingKey)
		assert.True(t, msg.Mandatory)
		assert.False(t, msg.Immediate)
	})
}