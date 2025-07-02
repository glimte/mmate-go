package messaging

import (
	"context"
	"errors"
	"testing"

	"github.com/glimte/mmate-go/contracts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Test types are defined in dispatcher_test.go

// MockTransportPublisher for batch testing
type MockTransportPublisher struct {
	mock.Mock
}

func (m *MockTransportPublisher) Publish(ctx context.Context, exchange, routingKey string, envelope *contracts.Envelope) error {
	args := m.Called(ctx, exchange, routingKey, envelope)
	return args.Error(0)
}

func (m *MockTransportPublisher) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestBatch_Add(t *testing.T) {
	transport := &MockTransportPublisher{}
	publisher := NewMessagePublisher(transport)
	batch := publisher.NewBatch()

	// Test adding a message
	msg := &TestCommand{
		BaseCommand: contracts.NewBaseCommand("TestCommand"),
		Data:        "test",
	}

	err := batch.Add(msg)
	assert.NoError(t, err)
	assert.Equal(t, 1, batch.Size())

	// Test adding nil message
	err = batch.Add(nil)
	assert.Error(t, err)
	assert.Equal(t, 1, batch.Size())
}

func TestBatch_AddTypedMessages(t *testing.T) {
	transport := &MockTransportPublisher{}
	publisher := NewMessagePublisher(transport)
	batch := publisher.NewBatch()

	// Test adding command
	cmd := &TestCommand{
		BaseCommand: contracts.NewBaseCommand("TestCommand"),
		Data:        "test",
	}
	err := batch.AddCommand(cmd)
	assert.NoError(t, err)

	// Test adding event
	evt := &TestEvent{
		BaseEvent: contracts.BaseEvent{
			BaseMessage:  contracts.NewBaseMessage("TestEvent"),
			AggregateID:  "aggregate-123",
			Sequence: 1,
		},
		Data: "event-data",
	}
	err = batch.AddEvent(evt)
	assert.NoError(t, err)

	// Test adding query
	qry := &TestQuery{
		BaseQuery: contracts.BaseQuery{
			BaseMessage: contracts.NewBaseMessage("TestQuery"),
		},
		Filter: "filter",
	}
	err = batch.AddQuery(qry)
	assert.NoError(t, err)

	assert.Equal(t, 3, batch.Size())
}

func TestBatch_Clear(t *testing.T) {
	transport := &MockTransportPublisher{}
	publisher := NewMessagePublisher(transport)
	batch := publisher.NewBatch()

	// Add some messages
	msg := &TestCommand{
		BaseCommand: contracts.NewBaseCommand("TestCommand"),
		Data:        "test",
	}
	batch.Add(msg)
	batch.Add(msg)
	assert.Equal(t, 2, batch.Size())

	// Clear the batch
	batch.Clear()
	assert.Equal(t, 0, batch.Size())
}

func TestBatch_Publish(t *testing.T) {
	transport := &MockTransportPublisher{}
	publisher := NewMessagePublisher(transport)
	batch := publisher.NewBatch()

	// Add messages
	cmd := &TestCommand{
		BaseCommand: contracts.NewBaseCommand("TestCommand"),
		Data:        "test",
	}
	batch.Add(cmd)

	evt := &TestEvent{
		BaseEvent: contracts.BaseEvent{
			BaseMessage:  contracts.NewBaseMessage("TestEvent"),
			AggregateID:  "aggregate-123",
			Sequence: 1,
		},
		Data: "event-data",
	}
	batch.Add(evt)

	// Set up expectations
	transport.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Publish batch
	ctx := context.Background()
	err := batch.Publish(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 0, batch.Size()) // Batch should be cleared

	// Verify all messages were published
	transport.AssertNumberOfCalls(t, "Publish", 2)
}

func TestBatch_PublishWithErrors(t *testing.T) {
	transport := &MockTransportPublisher{}
	publisher := NewMessagePublisher(transport)
	batch := publisher.NewBatch()

	// Add messages
	cmd1 := &TestCommand{
		BaseCommand: contracts.NewBaseCommand("TestCommand1"),
		Data:        "test1",
	}
	batch.Add(cmd1)

	cmd2 := &TestCommand{
		BaseCommand: contracts.NewBaseCommand("TestCommand2"),
		Data:        "test2",
	}
	batch.Add(cmd2)

	// First message always fails (even after retries)
	transport.On("Publish", mock.Anything, "mmate.messages", "cmd..TestCommand1", mock.Anything).Return(errors.New("publish failed"))
	// Second message succeeds immediately  
	transport.On("Publish", mock.Anything, "mmate.messages", "cmd..TestCommand2", mock.Anything).Return(nil).Once()

	// Publish batch
	ctx := context.Background()
	err := batch.Publish(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "batch publish completed with errors: 1/2 succeeded")
	assert.Equal(t, 0, batch.Size()) // Batch should still be cleared
}

func TestBatch_PublishEmpty(t *testing.T) {
	transport := &MockTransportPublisher{}
	publisher := NewMessagePublisher(transport)
	batch := publisher.NewBatch()

	// Publish empty batch
	ctx := context.Background()
	err := batch.Publish(ctx)
	assert.NoError(t, err)

	// No publishes should have been called
	transport.AssertNotCalled(t, "Publish")
}

func TestBatch_ConcurrentOperations(t *testing.T) {
	transport := &MockTransportPublisher{}
	publisher := NewMessagePublisher(transport)
	batch := publisher.NewBatch()

	// Set up transport to always succeed
	transport.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Run concurrent operations
	done := make(chan bool)
	
	// Add messages concurrently
	for i := 0; i < 10; i++ {
		go func(n int) {
			cmd := &TestCommand{
				BaseCommand: contracts.NewBaseCommand("TestCommand"),
				Data:        "test",
			}
			batch.Add(cmd)
			done <- true
		}(i)
	}

	// Wait for all adds to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify size
	assert.Equal(t, 10, batch.Size())

	// Publish
	ctx := context.Background()
	err := batch.Publish(ctx)
	assert.NoError(t, err)
	transport.AssertNumberOfCalls(t, "Publish", 10)
}

func TestBatch_PublishAtomic(t *testing.T) {
	// Set up transactional transport
	transport := &MockTransactionalTransportPublisher{}
	mockTx := &MockTransportTransaction{}
	transport.On("BeginTx", mock.Anything).Return(mockTx, nil)
	
	publisher := NewMessagePublisher(transport)
	batch := publisher.NewBatch()

	// Add messages
	cmd1 := &TestCommand{
		BaseCommand: contracts.NewBaseCommand("TestCommand1"),
		Data:        "test1",
	}
	batch.Add(cmd1)

	cmd2 := &TestCommand{
		BaseCommand: contracts.NewBaseCommand("TestCommand2"),
		Data:        "test2",
	}
	batch.Add(cmd2)

	// Set up transaction expectations
	mockTx.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockTx.On("Commit").Return(nil)
	mockTx.On("Rollback").Return(nil)

	// Publish atomically
	ctx := context.Background()
	err := batch.PublishAtomic(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 0, batch.Size()) // Batch should be cleared

	// Verify transaction was used
	transport.AssertCalled(t, "BeginTx", mock.Anything)
	mockTx.AssertNumberOfCalls(t, "Publish", 2)
	mockTx.AssertCalled(t, "Commit")
}

func TestBatch_PublishAtomicWithError(t *testing.T) {
	// Set up transactional transport
	transport := &MockTransactionalTransportPublisher{}
	mockTx := &MockTransportTransaction{}
	transport.On("BeginTx", mock.Anything).Return(mockTx, nil)
	
	publisher := NewMessagePublisher(transport)
	batch := publisher.NewBatch()

	// Add messages
	cmd1 := &TestCommand{
		BaseCommand: contracts.NewBaseCommand("TestCommand1"),
		Data:        "test1",
	}
	batch.Add(cmd1)

	cmd2 := &TestCommand{
		BaseCommand: contracts.NewBaseCommand("TestCommand2"),
		Data:        "test2",
	}
	batch.Add(cmd2)

	// First publish succeeds, second fails
	mockTx.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	mockTx.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("publish failed")).Once()
	mockTx.On("Rollback").Return(nil)

	// Publish atomically
	ctx := context.Background()
	err := batch.PublishAtomic(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to publish message in transaction")

	// Verify rollback was called
	mockTx.AssertCalled(t, "Rollback")
	mockTx.AssertNotCalled(t, "Commit")
}

func TestBatch_PublishAtomicEmpty(t *testing.T) {
	transport := &MockTransactionalTransportPublisher{}
	publisher := NewMessagePublisher(transport)
	batch := publisher.NewBatch()

	// Publish empty batch atomically
	ctx := context.Background()
	err := batch.PublishAtomic(ctx)
	assert.NoError(t, err)

	// No transaction should have been started
	transport.AssertNotCalled(t, "BeginTx")
}