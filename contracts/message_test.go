package contracts

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestBaseMessage(t *testing.T) {
	t.Run("NewBaseMessage creates valid message", func(t *testing.T) {
		msg := NewBaseMessage("TestMessage")
		
		assert.NotEmpty(t, msg.ID)
		assert.Equal(t, "TestMessage", msg.Type)
		assert.NotZero(t, msg.Timestamp)
		assert.Empty(t, msg.CorrelationID)
		
		// Verify ID is valid UUID
		_, err := uuid.Parse(msg.ID)
		assert.NoError(t, err)
	})
	
	t.Run("BaseMessage implements Message interface", func(t *testing.T) {
		base := NewBaseMessage("TestMessage")
		
		// Test interface methods
		assert.Equal(t, base.ID, base.GetID())
		assert.Equal(t, base.Type, base.GetType())
		assert.Equal(t, base.Timestamp, base.GetTimestamp())
		assert.Equal(t, base.CorrelationID, base.GetCorrelationID())
		
		// Test SetCorrelationID
		corrID := uuid.New().String()
		base.SetCorrelationID(corrID)
		assert.Equal(t, corrID, base.CorrelationID)
		assert.Equal(t, corrID, base.GetCorrelationID())
	})
}

func TestBaseCommand(t *testing.T) {
	t.Run("BaseCommand has correct fields", func(t *testing.T) {
		cmd := BaseCommand{
			BaseMessage:   NewBaseMessage("CreateUser"),
			TargetService: "user-service",
		}
		
		assert.Equal(t, "CreateUser", cmd.GetType())
		assert.Equal(t, "user-service", cmd.GetTargetService())
	})
	
	t.Run("BaseCommand implements Command interface", func(t *testing.T) {
		cmd := BaseCommand{
			BaseMessage:   NewBaseMessage("TestCommand"),
			TargetService: "test-service",
		}
		
		var c Command = &cmd
		assert.Equal(t, cmd.GetID(), c.GetID())
		assert.Equal(t, cmd.GetType(), c.GetType())
		assert.Equal(t, cmd.GetTargetService(), c.GetTargetService())
	})
}

func TestBaseEvent(t *testing.T) {
	t.Run("BaseEvent has correct fields", func(t *testing.T) {
		evt := BaseEvent{
			BaseMessage: NewBaseMessage("UserCreated"),
			AggregateID: "user-123",
			Sequence:    42,
		}
		
		assert.Equal(t, "UserCreated", evt.GetType())
		assert.Equal(t, "user-123", evt.GetAggregateID())
		assert.Equal(t, int64(42), evt.GetSequence())
	})
	
	t.Run("BaseEvent implements Event interface", func(t *testing.T) {
		evt := BaseEvent{
			BaseMessage: NewBaseMessage("TestEvent"),
			AggregateID: "aggregate-456",
			Sequence:    10,
		}
		
		var e Event = &evt
		assert.Equal(t, evt.GetID(), e.GetID())
		assert.Equal(t, evt.GetType(), e.GetType())
		assert.Equal(t, evt.GetAggregateID(), e.GetAggregateID())
		assert.Equal(t, evt.GetSequence(), e.GetSequence())
	})
}

func TestBaseQuery(t *testing.T) {
	t.Run("BaseQuery has correct fields", func(t *testing.T) {
		query := BaseQuery{
			BaseMessage: NewBaseMessage("GetUser"),
			ReplyTo:     "reply.queue",
		}
		
		assert.Equal(t, "GetUser", query.GetType())
		assert.Equal(t, "reply.queue", query.GetReplyTo())
	})
	
	t.Run("BaseQuery implements Query interface", func(t *testing.T) {
		query := BaseQuery{
			BaseMessage: NewBaseMessage("TestQuery"),
			ReplyTo:     "test.reply.queue",
		}
		
		var q Query = &query
		assert.Equal(t, query.GetID(), q.GetID())
		assert.Equal(t, query.GetType(), q.GetType())
		assert.Equal(t, query.GetReplyTo(), q.GetReplyTo())
	})
}

func TestBaseReply(t *testing.T) {
	t.Run("BaseReply for success", func(t *testing.T) {
		reply := BaseReply{
			BaseMessage: NewBaseMessage("SuccessReply"),
			Success:     true,
		}
		
		assert.Equal(t, "SuccessReply", reply.GetType())
		assert.True(t, reply.IsSuccess())
		assert.Nil(t, reply.GetError())
	})
	
	t.Run("BaseReply implements Reply interface", func(t *testing.T) {
		reply := BaseReply{
			BaseMessage: NewBaseMessage("TestReply"),
			Success:     true,
		}
		
		var r Reply = &reply
		assert.Equal(t, reply.GetID(), r.GetID())
		assert.Equal(t, reply.GetType(), r.GetType())
		assert.True(t, r.IsSuccess())
		assert.Nil(t, r.GetError())
	})
}

func TestErrorReply(t *testing.T) {
	t.Run("NewErrorReply creates error reply", func(t *testing.T) {
		errorReply := NewErrorReply("ErrorType", "ERR001", "Something went wrong")
		
		assert.NotEmpty(t, errorReply.ID)
		assert.Equal(t, "ErrorType", errorReply.Type)
		assert.False(t, errorReply.Success)
		assert.Equal(t, "ERR001", errorReply.ErrorCode)
		assert.Equal(t, "Something went wrong", errorReply.ErrorMessage)
	})
	
	t.Run("ErrorReply implements Reply interface", func(t *testing.T) {
		errorReply := NewErrorReply("TestError", "TEST_ERR", "Test error message")
		
		var r Reply = errorReply
		assert.False(t, r.IsSuccess())
		assert.NotNil(t, r.GetError())
		assert.Contains(t, r.GetError().Error(), "TEST_ERR")
		assert.Contains(t, r.GetError().Error(), "Test error message")
	})
}

func TestCorrelationIDPropagation(t *testing.T) {
	// Create a command
	cmd := BaseCommand{
		BaseMessage:   NewBaseMessage("CreateOrder"),
		TargetService: "order-service",
	}
	corrID := uuid.New().String()
	cmd.SetCorrelationID(corrID)
	
	// Create event in response
	evt := BaseEvent{
		BaseMessage: NewBaseMessage("OrderCreated"),
		AggregateID: "order-123",
	}
	evt.SetCorrelationID(cmd.GetCorrelationID())
	
	// Create reply
	reply := BaseReply{
		BaseMessage: NewBaseMessage("OrderCreatedReply"),
		Success:     true,
	}
	reply.SetCorrelationID(evt.GetCorrelationID())
	
	// All should have same correlation ID
	assert.Equal(t, corrID, cmd.GetCorrelationID())
	assert.Equal(t, corrID, evt.GetCorrelationID())
	assert.Equal(t, corrID, reply.GetCorrelationID())
}

func TestEnvelope(t *testing.T) {
	t.Run("Envelope structure", func(t *testing.T) {
		env := Envelope{
			ID:            "msg-123",
			Type:          "TestMessage",
			Timestamp:     time.Now().UTC().Format(time.RFC3339),
			CorrelationID: "corr-456",
			Headers:       map[string]interface{}{"trace-id": "trace-789"},
		}
		
		assert.Equal(t, "msg-123", env.ID)
		assert.Equal(t, "TestMessage", env.Type)
		assert.NotEmpty(t, env.Timestamp)
		assert.Equal(t, "corr-456", env.CorrelationID)
		assert.Equal(t, "trace-789", env.Headers["trace-id"])
	})
}

func TestMessageMetadata(t *testing.T) {
	t.Run("MessageMetadata fields", func(t *testing.T) {
		metadata := MessageMetadata{
			Queue:        "test.queue",
			Exchange:     "test.exchange",
			RoutingKey:   "test.key",
			Priority:     5,
			DeliveryMode: 2,
			Expiration:   "60000",
			RetryCount:   1,
			MaxRetries:   3,
		}
		
		assert.Equal(t, "test.queue", metadata.Queue)
		assert.Equal(t, "test.exchange", metadata.Exchange)
		assert.Equal(t, "test.key", metadata.RoutingKey)
		assert.Equal(t, uint8(5), metadata.Priority)
		assert.Equal(t, uint8(2), metadata.DeliveryMode)
		assert.Equal(t, "60000", metadata.Expiration)
		assert.Equal(t, 1, metadata.RetryCount)
		assert.Equal(t, 3, metadata.MaxRetries)
	})
}