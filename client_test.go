package mmate

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClientConveniencePublishMethods tests the convenience publish methods on Client
func TestClientConveniencePublishMethods(t *testing.T) {
	// Skip if no RabbitMQ connection string is provided
	connectionString := "amqp://admin:admin@localhost:5672/"
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Create client
	client, err := NewClientWithOptions(connectionString, WithServiceName("test-service"))
	require.NoError(t, err)
	defer client.Close()

	t.Run("PublishEvent", func(t *testing.T) {
		// Create a test event
		event := &testEvent{
			ID:          "test-event-1",
			Type:        "TestEvent",
			Timestamp:   time.Now(),
			AggregateID: "aggregate-1",
			Data:        "test data",
		}

		// Test that PublishEvent works
		err := client.PublishEvent(ctx, event)
		assert.NoError(t, err)
	})

	t.Run("PublishCommand", func(t *testing.T) {
		// Create a test command
		command := &testCommand{
			ID:            "test-cmd-1",
			Type:          "TestCommand",
			Timestamp:     time.Now(),
			TargetService: "target-service",
			Data:          "test data",
		}

		// Test that PublishCommand works
		err := client.PublishCommand(ctx, command)
		assert.NoError(t, err)
	})

	t.Run("PublishReply", func(t *testing.T) {
		// Create a test reply
		reply := &testReply{
			ID:        "test-reply-1",
			Type:      "TestReply",
			Timestamp: time.Now(),
			Success:   true,
			Data:      "test data",
		}

		// Test that PublishReply works with a replyTo queue
		err := client.PublishReply(ctx, reply, "reply-queue")
		assert.NoError(t, err)
	})

	t.Run("PublishWithNilPublisher", func(t *testing.T) {
		// Create a client without publisher
		emptyClient := &Client{}

		event := &testEvent{ID: "test", Type: "test"}
		err := emptyClient.PublishEvent(ctx, event)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "publisher not initialized")

		command := &testCommand{ID: "test", Type: "test"}
		err = emptyClient.PublishCommand(ctx, command)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "publisher not initialized")

		reply := &testReply{ID: "test", Type: "test"}
		err = emptyClient.PublishReply(ctx, reply, "queue")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "publisher not initialized")
	})
}

// Test implementations of contracts

type testEvent struct {
	ID            string
	Type          string
	Timestamp     time.Time
	AggregateID   string
	Data          string
	CorrelationID string
}

func (e *testEvent) GetID() string             { return e.ID }
func (e *testEvent) GetType() string           { return e.Type }
func (e *testEvent) GetMessageType() string    { return e.Type }
func (e *testEvent) GetTimestamp() time.Time   { return e.Timestamp }
func (e *testEvent) GetCorrelationID() string  { return e.CorrelationID }
func (e *testEvent) SetCorrelationID(id string) { e.CorrelationID = id }
func (e *testEvent) GetAggregateID() string    { return e.AggregateID }
func (e *testEvent) GetSequence() int64        { return 1 }

type testCommand struct {
	ID            string
	Type          string
	Timestamp     time.Time
	TargetService string
	Data          string
	CorrelationID string
}

func (c *testCommand) GetID() string             { return c.ID }
func (c *testCommand) GetType() string           { return c.Type }
func (c *testCommand) GetMessageType() string    { return c.Type }
func (c *testCommand) GetTimestamp() time.Time   { return c.Timestamp }
func (c *testCommand) GetCorrelationID() string  { return c.CorrelationID }
func (c *testCommand) SetCorrelationID(id string) { c.CorrelationID = id }
func (c *testCommand) GetTargetService() string  { return c.TargetService }

type testReply struct {
	ID            string
	Type          string
	Timestamp     time.Time
	Success       bool
	Data          string
	CorrelationID string
}

func (r *testReply) GetID() string             { return r.ID }
func (r *testReply) GetType() string           { return r.Type }
func (r *testReply) GetMessageType() string    { return r.Type }
func (r *testReply) GetTimestamp() time.Time   { return r.Timestamp }
func (r *testReply) GetCorrelationID() string  { return r.CorrelationID }
func (r *testReply) SetCorrelationID(id string) { r.CorrelationID = id }
func (r *testReply) IsSuccess() bool           { return r.Success }
func (r *testReply) GetError() error           { return nil }

type testQuery struct {
	ID            string
	Type          string
	Timestamp     time.Time
	Filter        string
	CorrelationID string
}

func (q *testQuery) GetID() string             { return q.ID }
func (q *testQuery) GetType() string           { return q.Type }
func (q *testQuery) GetMessageType() string    { return q.Type }
func (q *testQuery) GetTimestamp() time.Time   { return q.Timestamp }
func (q *testQuery) GetCorrelationID() string  { return q.CorrelationID }
func (q *testQuery) SetCorrelationID(id string) { q.CorrelationID = id }
func (q *testQuery) GetReplyTo() string        { return "" }

// TestClientAdvancedFeatures tests the new advanced publishing features
func TestClientAdvancedFeatures(t *testing.T) {
	// Skip if no RabbitMQ connection string is provided
	connectionString := "amqp://admin:admin@localhost:5672/"
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Create client
	client, err := NewClientWithOptions(connectionString, WithServiceName("test-advanced"))
	require.NoError(t, err)
	defer client.Close()

	t.Run("ScheduleCommand", func(t *testing.T) {
		cmd := &testCommand{
			ID:            "test-cmd-scheduled",
			Type:          "TestScheduledCommand",
			Timestamp:     time.Now(),
			TargetService: "test-service",
			Data:          "scheduled command data",
		}

		scheduledFor := time.Now().Add(2 * time.Second)
		err := client.ScheduleCommand(ctx, cmd, scheduledFor)
		assert.NoError(t, err)
	})

	t.Run("ScheduleEvent", func(t *testing.T) {
		evt := &testEvent{
			ID:          "test-evt-scheduled",
			Type:        "TestScheduledEvent",
			Timestamp:   time.Now(),
			AggregateID: "agg-123",
			Data:        "scheduled event data",
		}

		scheduledFor := time.Now().Add(2 * time.Second)
		err := client.ScheduleEvent(ctx, evt, scheduledFor)
		assert.NoError(t, err)
	})

	t.Run("ScheduleQuery", func(t *testing.T) {
		qry := &testQuery{
			ID:        "test-qry-scheduled",
			Type:      "TestScheduledQuery",
			Timestamp: time.Now(),
			Filter:    "scheduled filter",
		}

		scheduledFor := time.Now().Add(2 * time.Second)
		err := client.ScheduleQuery(ctx, qry, scheduledFor)
		assert.NoError(t, err)
	})

	t.Run("PublishWithDelay", func(t *testing.T) {
		cmd := &testCommand{
			ID:            "test-cmd-delayed",
			Type:          "TestDelayedCommand",
			Timestamp:     time.Now(),
			TargetService: "test-service",
			Data:          "delayed command data",
		}

		err := client.PublishWithDelay(ctx, cmd, 1*time.Second)
		assert.NoError(t, err)
	})

	t.Run("BatchPublishing", func(t *testing.T) {
		batch := client.NewBatch()
		require.NotNil(t, batch)

		// Add multiple messages to batch
		for i := 0; i < 3; i++ {
			cmd := &testCommand{
				ID:            "test-cmd-batch-" + string(rune('0'+i)),
				Type:          "TestBatchCommand",
				Timestamp:     time.Now(),
				TargetService: "test-service",
				Data:          "batch command " + string(rune('0'+i)),
			}
			err := batch.Add(cmd)
			assert.NoError(t, err)
		}

		// Publish the batch
		err := batch.Publish(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 0, batch.Size()) // Batch should be cleared
	})

	t.Run("TransactionalPublishing", func(t *testing.T) {
		// Note: This requires RabbitMQ transport to support transactions
		// Currently our transport doesn't implement TransactionalTransportPublisher
		tx, err := client.BeginTx()
		if err != nil {
			// Expected for now - transport doesn't support transactions yet
			assert.Contains(t, err.Error(), "transport does not support transactions")
			t.Skip("Transport doesn't support transactions yet")
		}

		// If we get here, test transaction
		cmd := &testCommand{
			ID:            "test-cmd-tx",
			Type:          "TestTxCommand",
			Timestamp:     time.Now(),
			TargetService: "test-service",
			Data:          "transactional command",
		}

		err = tx.PublishCommand(ctx, cmd)
		assert.NoError(t, err)

		err = tx.Commit()
		assert.NoError(t, err)
	})
}