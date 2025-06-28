package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueueInfo_Fields(t *testing.T) {
	queue := QueueInfo{
		Name:        "test-queue",
		Messages:    100,
		Consumers:   3,
		MessageRate: 5.5,
		Memory:      1024,
		State:       "running",
		Vhost:       "/",
		Durable:     true,
		AutoDelete:  false,
	}
	
	assert.Equal(t, "test-queue", queue.Name)
	assert.Equal(t, 100, queue.Messages)
	assert.Equal(t, 3, queue.Consumers)
	assert.Equal(t, 5.5, queue.MessageRate)
	assert.Equal(t, int64(1024), queue.Memory)
	assert.Equal(t, "running", queue.State)
	assert.Equal(t, "/", queue.Vhost)
	assert.True(t, queue.Durable)
	assert.False(t, queue.AutoDelete)
}

func TestQueueInfo_JSONSerialization(t *testing.T) {
	queue := QueueInfo{
		Name:        "json-queue",
		Messages:    42,
		Consumers:   2,
		MessageRate: 1.5,
		Memory:      2048,
		State:       "idle",
		Vhost:       "/prod",
		Durable:     false,
		AutoDelete:  true,
	}
	
	data, err := json.Marshal(queue)
	require.NoError(t, err)
	
	var unmarshaled QueueInfo
	err = json.Unmarshal(data, &unmarshaled)
	require.NoError(t, err)
	
	assert.Equal(t, queue, unmarshaled)
}

func TestExchangeInfo_Fields(t *testing.T) {
	exchange := ExchangeInfo{
		Name:       "test-exchange",
		Type:       "direct",
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		Vhost:      "/",
	}
	
	assert.Equal(t, "test-exchange", exchange.Name)
	assert.Equal(t, "direct", exchange.Type)
	assert.True(t, exchange.Durable)
	assert.False(t, exchange.AutoDelete)
	assert.False(t, exchange.Internal)
	assert.Equal(t, "/", exchange.Vhost)
}

func TestExchangeInfo_JSONSerialization(t *testing.T) {
	exchange := ExchangeInfo{
		Name:       "topic-exchange",
		Type:       "topic",
		Durable:    false,
		AutoDelete: true,
		Internal:   true,
		Vhost:      "/staging",
	}
	
	data, err := json.Marshal(exchange)
	require.NoError(t, err)
	
	var unmarshaled ExchangeInfo
	err = json.Unmarshal(data, &unmarshaled)
	require.NoError(t, err)
	
	assert.Equal(t, exchange, unmarshaled)
}

func TestConnectionInfo_Fields(t *testing.T) {
	now := time.Now()
	conn := ConnectionInfo{
		Name:      "connection-1",
		Host:      "localhost",
		Port:      5672,
		User:      "guest",
		Vhost:     "/",
		State:     "running",
		Channels:  5,
		Protocol:  "AMQP 0-9-1",
		Connected: now,
	}
	
	assert.Equal(t, "connection-1", conn.Name)
	assert.Equal(t, "localhost", conn.Host)
	assert.Equal(t, 5672, conn.Port)
	assert.Equal(t, "guest", conn.User)
	assert.Equal(t, "/", conn.Vhost)
	assert.Equal(t, "running", conn.State)
	assert.Equal(t, 5, conn.Channels)
	assert.Equal(t, "AMQP 0-9-1", conn.Protocol)
	assert.Equal(t, now, conn.Connected)
}

func TestConnectionInfo_JSONSerialization(t *testing.T) {
	connected := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	conn := ConnectionInfo{
		Name:      "prod-connection",
		Host:      "rabbitmq.example.com",
		Port:      5672,
		User:      "appuser",
		Vhost:     "/prod",
		State:     "running",
		Channels:  10,
		Protocol:  "AMQP 0-9-1",
		Connected: connected,
	}
	
	data, err := json.Marshal(conn)
	require.NoError(t, err)
	
	var unmarshaled ConnectionInfo
	err = json.Unmarshal(data, &unmarshaled)
	require.NoError(t, err)
	
	assert.Equal(t, conn, unmarshaled)
}

func TestMessageStats_Fields(t *testing.T) {
	stats := MessageStats{
		PublishTotal:         1000,
		PublishDetails:       Details{Rate: 10.5},
		DeliverGetTotal:      800,
		DeliverGetDetails:    Details{Rate: 8.2},
		ConfirmTotal:         950,
		ConfirmDetails:       Details{Rate: 9.8},
		ReturnUnroutableTotal: 5,
		ReturnUnroutableDetails: Details{Rate: 0.1},
	}
	
	assert.Equal(t, 1000, stats.PublishTotal)
	assert.Equal(t, 10.5, stats.PublishDetails.Rate)
	assert.Equal(t, 800, stats.DeliverGetTotal)
	assert.Equal(t, 8.2, stats.DeliverGetDetails.Rate)
	assert.Equal(t, 950, stats.ConfirmTotal)
	assert.Equal(t, 9.8, stats.ConfirmDetails.Rate)
	assert.Equal(t, 5, stats.ReturnUnroutableTotal)
	assert.Equal(t, 0.1, stats.ReturnUnroutableDetails.Rate)
}

func TestQueueTotals_Fields(t *testing.T) {
	totals := QueueTotals{
		Messages:        500,
		MessagesReady:   300,
		MessagesUnacked: 200,
	}
	
	assert.Equal(t, 500, totals.Messages)
	assert.Equal(t, 300, totals.MessagesReady)
	assert.Equal(t, 200, totals.MessagesUnacked)
}

func TestObjectTotals_Fields(t *testing.T) {
	totals := ObjectTotals{
		Consumers:   15,
		Queues:      25,
		Exchanges:   10,
		Connections: 5,
		Channels:    30,
	}
	
	assert.Equal(t, 15, totals.Consumers)
	assert.Equal(t, 25, totals.Queues)
	assert.Equal(t, 10, totals.Exchanges)
	assert.Equal(t, 5, totals.Connections)
	assert.Equal(t, 30, totals.Channels)
}

func TestDetails_Fields(t *testing.T) {
	details := Details{Rate: 42.7}
	assert.Equal(t, 42.7, details.Rate)
}

func TestOverview_Fields(t *testing.T) {
	overview := Overview{
		ManagementVersion:   "3.9.0",
		RabbitMQVersion:     "3.9.13",
		ErlangVersion:       "24.2",
		MessageStats: MessageStats{
			PublishTotal:      1000,
			PublishDetails:    Details{Rate: 10.0},
			DeliverGetTotal:   800,
			DeliverGetDetails: Details{Rate: 8.0},
		},
		QueueTotals: QueueTotals{
			Messages:        500,
			MessagesReady:   300,
			MessagesUnacked: 200,
		},
		ObjectTotals: ObjectTotals{
			Consumers:   15,
			Queues:      25,
			Exchanges:   10,
			Connections: 5,
			Channels:    30,
		},
		StatisticsDBNode:       "rabbit@server1",
		Node:                   "rabbit@server1",
		StatisticsDBEventQueue: 0,
	}
	
	assert.Equal(t, "3.9.0", overview.ManagementVersion)
	assert.Equal(t, "3.9.13", overview.RabbitMQVersion)
	assert.Equal(t, "24.2", overview.ErlangVersion)
	assert.Equal(t, 1000, overview.MessageStats.PublishTotal)
	assert.Equal(t, 500, overview.QueueTotals.Messages)
	assert.Equal(t, 15, overview.ObjectTotals.Consumers)
	assert.Equal(t, "rabbit@server1", overview.StatisticsDBNode)
	assert.Equal(t, "rabbit@server1", overview.Node)
	assert.Equal(t, 0, overview.StatisticsDBEventQueue)
}

func TestOverview_JSONSerialization(t *testing.T) {
	overview := Overview{
		ManagementVersion: "3.9.0",
		RabbitMQVersion:   "3.9.13",
		ErlangVersion:     "24.2",
		MessageStats: MessageStats{
			PublishTotal:   1000,
			PublishDetails: Details{Rate: 10.0},
		},
		QueueTotals: QueueTotals{
			Messages: 500,
		},
		ObjectTotals: ObjectTotals{
			Queues: 25,
		},
		StatisticsDBNode: "rabbit@server1",
		Node:             "rabbit@server1",
	}
	
	data, err := json.Marshal(overview)
	require.NoError(t, err)
	
	var unmarshaled Overview
	err = json.Unmarshal(data, &unmarshaled)
	require.NoError(t, err)
	
	assert.Equal(t, overview.ManagementVersion, unmarshaled.ManagementVersion)
	assert.Equal(t, overview.RabbitMQVersion, unmarshaled.RabbitMQVersion)
	assert.Equal(t, overview.MessageStats.PublishTotal, unmarshaled.MessageStats.PublishTotal)
	assert.Equal(t, overview.QueueTotals.Messages, unmarshaled.QueueTotals.Messages)
}

// Test that MockRabbitMQClient implements the RabbitMQClient interface
func TestMockRabbitMQClient_ImplementsInterface(t *testing.T) {
	var client RabbitMQClient = &MockRabbitMQClient{}
	
	// This test verifies that MockRabbitMQClient implements all required methods
	// If any method is missing, this will fail to compile
	assert.NotNil(t, client)
	
	// Verify interface methods exist (compilation check)
	ctx := context.Background()
	
	// These calls will panic since we haven't set up mocks, but that's okay
	// We just want to verify the interface is properly implemented
	defer func() {
		if r := recover(); r != nil {
			// Expected - mock calls will panic without setup
		}
	}()
	
	// Test that all interface methods exist
	_, _ = client.ListQueues(ctx)
	_, _ = client.GetQueues(ctx)
	_, _ = client.GetQueue(ctx, "/", "test")
	_, _ = client.ListExchanges(ctx)
	_, _ = client.GetExchange(ctx, "/", "test")
	_, _ = client.ListConnections(ctx)
	_, _ = client.GetConnection(ctx, "test")
	_, _ = client.GetOverview(ctx)
	_ = client.CheckHealth(ctx)
}

// Test data structure validation
func TestDataStructure_Validation(t *testing.T) {
	t.Run("QueueInfo validation", func(t *testing.T) {
		queue := QueueInfo{
			Name:        "",
			Messages:    -1,
			Consumers:   -1,
			MessageRate: -1.0,
			Memory:      -1,
		}
		
		// These are just structural tests - the actual validation would be in business logic
		assert.Equal(t, "", queue.Name)
		assert.Equal(t, -1, queue.Messages)
		assert.Equal(t, -1, queue.Consumers)
		assert.Equal(t, -1.0, queue.MessageRate)
		assert.Equal(t, int64(-1), queue.Memory)
	})
	
	t.Run("ConnectionInfo validation", func(t *testing.T) {
		conn := ConnectionInfo{
			Host:     "",
			Port:     0,
			User:     "",
			Channels: -1,
		}
		
		assert.Equal(t, "", conn.Host)
		assert.Equal(t, 0, conn.Port)
		assert.Equal(t, "", conn.User)
		assert.Equal(t, -1, conn.Channels)
	})
}

// Test edge cases in JSON serialization
func TestJSONSerialization_EdgeCases(t *testing.T) {
	t.Run("empty values", func(t *testing.T) {
		queue := QueueInfo{}
		data, err := json.Marshal(queue)
		require.NoError(t, err)
		
		var unmarshaled QueueInfo
		err = json.Unmarshal(data, &unmarshaled)
		require.NoError(t, err)
		
		assert.Equal(t, queue, unmarshaled)
	})
	
	t.Run("zero time", func(t *testing.T) {
		conn := ConnectionInfo{
			Connected: time.Time{},
		}
		
		data, err := json.Marshal(conn)
		require.NoError(t, err)
		
		var unmarshaled ConnectionInfo
		err = json.Unmarshal(data, &unmarshaled)
		require.NoError(t, err)
		
		assert.Equal(t, conn.Connected, unmarshaled.Connected)
	})
	
	t.Run("large numbers", func(t *testing.T) {
		overview := Overview{
			MessageStats: MessageStats{
				PublishTotal:    999999999,
				DeliverGetTotal: 888888888,
			},
			QueueTotals: QueueTotals{
				Messages: 777777777,
			},
		}
		
		data, err := json.Marshal(overview)
		require.NoError(t, err)
		
		var unmarshaled Overview
		err = json.Unmarshal(data, &unmarshaled)
		require.NoError(t, err)
		
		assert.Equal(t, overview.MessageStats.PublishTotal, unmarshaled.MessageStats.PublishTotal)
		assert.Equal(t, overview.QueueTotals.Messages, unmarshaled.QueueTotals.Messages)
	})
}

// Test that all data structures work with standard JSON operations
func TestDataStructures_JSONCompatibility(t *testing.T) {
	structures := []interface{}{
		QueueInfo{Name: "test", Messages: 100},
		ExchangeInfo{Name: "test", Type: "direct"},
		ConnectionInfo{Name: "test", Host: "localhost", Port: 5672},
		MessageStats{PublishTotal: 100},
		QueueTotals{Messages: 100},
		ObjectTotals{Queues: 10},
		Details{Rate: 1.5},
		Overview{RabbitMQVersion: "3.9.0"},
	}
	
	for i, structure := range structures {
		t.Run(fmt.Sprintf("structure_%d", i), func(t *testing.T) {
			// Test marshaling
			data, err := json.Marshal(structure)
			require.NoError(t, err)
			assert.Greater(t, len(data), 0)
			
			// Test that the JSON is valid
			var raw map[string]interface{}
			err = json.Unmarshal(data, &raw)
			require.NoError(t, err)
			
			// Test pretty printing
			prettyData, err := json.MarshalIndent(structure, "", "  ")
			require.NoError(t, err)
			assert.Greater(t, len(prettyData), len(data))
		})
	}
}

// Benchmark JSON serialization
func BenchmarkQueueInfo_JSONMarshal(b *testing.B) {
	queue := QueueInfo{
		Name:        "benchmark-queue",
		Messages:    1000,
		Consumers:   5,
		MessageRate: 10.5,
		Memory:      2048,
		State:       "running",
		Vhost:       "/",
		Durable:     true,
		AutoDelete:  false,
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(queue)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOverview_JSONMarshal(b *testing.B) {
	overview := Overview{
		ManagementVersion: "3.9.0",
		RabbitMQVersion:   "3.9.13",
		ErlangVersion:     "24.2",
		MessageStats: MessageStats{
			PublishTotal:      10000,
			PublishDetails:    Details{Rate: 100.5},
			DeliverGetTotal:   8000,
			DeliverGetDetails: Details{Rate: 80.2},
		},
		QueueTotals: QueueTotals{
			Messages:        5000,
			MessagesReady:   3000,
			MessagesUnacked: 2000,
		},
		ObjectTotals: ObjectTotals{
			Consumers:   150,
			Queues:      250,
			Exchanges:   100,
			Connections: 50,
			Channels:    300,
		},
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(overview)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Test concurrent access to data structures (they should be safe to read concurrently)
func TestDataStructures_ConcurrentAccess(t *testing.T) {
	queue := QueueInfo{
		Name:     "concurrent-queue",
		Messages: 1000,
	}
	
	const numGoroutines = 10
	const numReads = 100
	
	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numReads; j++ {
				// Read operations should be safe
				_ = queue.Name
				_ = queue.Messages
				
				// JSON marshaling should be safe
				_, err := json.Marshal(queue)
				assert.NoError(t, err)
			}
		}()
	}
	
	wg.Wait()
}