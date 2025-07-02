package messaging

import (
	"context"
	"errors"
	"testing"

	"github.com/glimte/mmate-go/contracts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockTransportTransaction for testing
type MockTransportTransaction struct {
	mock.Mock
}

func (m *MockTransportTransaction) Publish(ctx context.Context, exchange, routingKey string, envelope *contracts.Envelope) error {
	args := m.Called(ctx, exchange, routingKey, envelope)
	return args.Error(0)
}

func (m *MockTransportTransaction) Commit() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockTransportTransaction) Rollback() error {
	args := m.Called()
	return args.Error(0)
}

// MockTransactionalTransportPublisher for testing
type MockTransactionalTransportPublisher struct {
	MockTransportPublisher
}

func (m *MockTransactionalTransportPublisher) BeginTx(ctx context.Context) (TransportTransaction, error) {
	args := m.Called(ctx)
	if args.Get(0) != nil {
		return args.Get(0).(TransportTransaction), args.Error(1)
	}
	return nil, args.Error(1)
}

func TestTransaction_BeginTx(t *testing.T) {
	// Test with non-transactional transport
	transport := &MockTransportPublisher{}
	publisher := NewMessagePublisher(transport)
	
	tx, err := publisher.BeginTx()
	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "transport does not support transactions")

	// Test with transactional transport
	transactionalTransport := &MockTransactionalTransportPublisher{}
	mockTx := &MockTransportTransaction{}
	transactionalTransport.On("BeginTx", mock.Anything).Return(mockTx, nil)
	
	publisher2 := NewMessagePublisher(transactionalTransport)
	tx2, err := publisher2.BeginTx()
	assert.NoError(t, err)
	assert.NotNil(t, tx2)
}

func TestTransaction_Publish(t *testing.T) {
	// Set up
	transport := &MockTransactionalTransportPublisher{}
	mockTx := &MockTransportTransaction{}
	transport.On("BeginTx", mock.Anything).Return(mockTx, nil)
	
	publisher := NewMessagePublisher(transport)
	tx, _ := publisher.BeginTx()

	// Test publishing a message
	msg := &testCommand{
		BaseCommand: contracts.NewBaseCommand("testCommand"),
		Data:        "test",
	}
	
	mockTx.On("Publish", mock.Anything, "mmate.messages", mock.Anything, mock.Anything).Return(nil)
	
	ctx := context.Background()
	err := tx.Publish(ctx, msg)
	assert.NoError(t, err)
	
	mockTx.AssertCalled(t, "Publish", mock.Anything, "mmate.messages", mock.Anything, mock.Anything)
}

func TestTransaction_PublishTypedMessages(t *testing.T) {
	// Set up
	transport := &MockTransactionalTransportPublisher{}
	mockTx := &MockTransportTransaction{}
	transport.On("BeginTx", mock.Anything).Return(mockTx, nil)
	
	publisher := NewMessagePublisher(transport)
	tx, _ := publisher.BeginTx()
	ctx := context.Background()

	// Test publishing command
	cmd := &testCommand{
		BaseCommand: contracts.NewBaseCommand("testCommand"),
		Data:        "test",
	}
	mockTx.On("Publish", mock.Anything, "mmate.commands", "cmd..testCommand", mock.Anything).Return(nil)
	err := tx.PublishCommand(ctx, cmd)
	assert.NoError(t, err)

	// Test publishing event
	evt := &testEvent{
		BaseEvent: contracts.BaseEvent{
			BaseMessage:  contracts.NewBaseMessage("testEvent"),
			AggregateID:  "aggregate-123",
			Sequence: 1,
		},
		Data: "event-data",
	}
	mockTx.On("Publish", mock.Anything, "mmate.events", "evt.aggregate-123.testEvent", mock.Anything).Return(nil)
	err = tx.PublishEvent(ctx, evt)
	assert.NoError(t, err)

	// Test publishing query
	qry := &testQuery{
		BaseQuery: contracts.BaseQuery{
			BaseMessage: contracts.NewBaseMessage("testQuery"),
		},
		Filter: "filter",
	}
	mockTx.On("Publish", mock.Anything, "mmate.queries", "qry.testQuery", mock.Anything).Return(nil)
	err = tx.PublishQuery(ctx, qry)
	assert.NoError(t, err)
}

func TestTransaction_Commit(t *testing.T) {
	// Set up
	transport := &MockTransactionalTransportPublisher{}
	mockTx := &MockTransportTransaction{}
	transport.On("BeginTx", mock.Anything).Return(mockTx, nil)
	
	publisher := NewMessagePublisher(transport)
	tx, _ := publisher.BeginTx()

	// Test successful commit
	mockTx.On("Commit").Return(nil)
	err := tx.Commit()
	assert.NoError(t, err)
	mockTx.AssertCalled(t, "Commit")

	// Test committing already committed transaction
	err = tx.Commit()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "transaction already committed")
}

func TestTransaction_Rollback(t *testing.T) {
	// Set up
	transport := &MockTransactionalTransportPublisher{}
	mockTx := &MockTransportTransaction{}
	transport.On("BeginTx", mock.Anything).Return(mockTx, nil)
	
	publisher := NewMessagePublisher(transport)
	tx, _ := publisher.BeginTx()

	// Test successful rollback
	mockTx.On("Rollback").Return(nil)
	err := tx.Rollback()
	assert.NoError(t, err)
	mockTx.AssertCalled(t, "Rollback")

	// Test rolling back already rolled back transaction
	err = tx.Rollback()
	assert.NoError(t, err) // Should return nil for idempotency
}

func TestTransaction_CommitAfterRollback(t *testing.T) {
	// Set up
	transport := &MockTransactionalTransportPublisher{}
	mockTx := &MockTransportTransaction{}
	transport.On("BeginTx", mock.Anything).Return(mockTx, nil)
	
	publisher := NewMessagePublisher(transport)
	tx, _ := publisher.BeginTx()

	// Rollback first
	mockTx.On("Rollback").Return(nil)
	tx.Rollback()

	// Try to commit after rollback
	err := tx.Commit()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "transaction already rolled back")
}

func TestTransaction_PublishAfterCommit(t *testing.T) {
	// Set up
	transport := &MockTransactionalTransportPublisher{}
	mockTx := &MockTransportTransaction{}
	transport.On("BeginTx", mock.Anything).Return(mockTx, nil)
	
	publisher := NewMessagePublisher(transport)
	tx, _ := publisher.BeginTx()

	// Commit first
	mockTx.On("Commit").Return(nil)
	tx.Commit()

	// Try to publish after commit
	msg := &testCommand{
		BaseCommand: contracts.NewBaseCommand("testCommand"),
		Data:        "test",
	}
	
	ctx := context.Background()
	err := tx.Publish(ctx, msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "transaction already committed")
}

func TestTransaction_PublishAfterRollback(t *testing.T) {
	// Set up
	transport := &MockTransactionalTransportPublisher{}
	mockTx := &MockTransportTransaction{}
	transport.On("BeginTx", mock.Anything).Return(mockTx, nil)
	
	publisher := NewMessagePublisher(transport)
	tx, _ := publisher.BeginTx()

	// Rollback first
	mockTx.On("Rollback").Return(nil)
	tx.Rollback()

	// Try to publish after rollback
	msg := &testCommand{
		BaseCommand: contracts.NewBaseCommand("testCommand"),
		Data:        "test",
	}
	
	ctx := context.Background()
	err := tx.Publish(ctx, msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "transaction already rolled back")
}

func TestTransaction_ErrorHandling(t *testing.T) {
	// Set up
	transport := &MockTransactionalTransportPublisher{}
	mockTx := &MockTransportTransaction{}
	transport.On("BeginTx", mock.Anything).Return(mockTx, nil)
	
	publisher := NewMessagePublisher(transport)
	tx, _ := publisher.BeginTx()

	// Test publish error
	msg := &testCommand{
		BaseCommand: contracts.NewBaseCommand("testCommand"),
		Data:        "test",
	}
	
	mockTx.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("publish failed"))
	
	ctx := context.Background()
	err := tx.Publish(ctx, msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to publish in transaction")

	// Test commit error
	mockTx.On("Commit").Return(errors.New("commit failed"))
	err = tx.Commit()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to commit transaction")

	// Test rollback error
	transport2 := &MockTransactionalTransportPublisher{}
	mockTx2 := &MockTransportTransaction{}
	transport2.On("BeginTx", mock.Anything).Return(mockTx2, nil)
	
	publisher2 := NewMessagePublisher(transport2)
	tx2, _ := publisher2.BeginTx()
	
	mockTx2.On("Rollback").Return(errors.New("rollback failed"))
	err = tx2.Rollback()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to rollback transaction")
}