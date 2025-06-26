package contracts

import (
	"time"

	"github.com/google/uuid"
)

// BaseMessage provides common fields for all message types
type BaseMessage struct {
	ID            string    `json:"id"`
	Timestamp     time.Time `json:"timestamp"`
	Type          string    `json:"type"`
	CorrelationID string    `json:"correlationId,omitempty"`
}

// NewBaseMessage creates a new base message with generated ID and current timestamp
func NewBaseMessage(messageType string) BaseMessage {
	return BaseMessage{
		ID:        uuid.New().String(),
		Timestamp: time.Now().UTC(),
		Type:      messageType,
	}
}

// GetID returns the message ID
func (m BaseMessage) GetID() string {
	return m.ID
}

// GetTimestamp returns the message timestamp
func (m BaseMessage) GetTimestamp() time.Time {
	return m.Timestamp
}

// GetType returns the message type
func (m BaseMessage) GetType() string {
	return m.Type
}

// GetMessageType returns the message type (alias for GetType for compatibility)
func (m BaseMessage) GetMessageType() string {
	return m.Type
}

// GetCorrelationID returns the correlation ID
func (m BaseMessage) GetCorrelationID() string {
	return m.CorrelationID
}

// SetCorrelationID sets the correlation ID
func (m *BaseMessage) SetCorrelationID(correlationID string) {
	m.CorrelationID = correlationID
}

// BaseCommand provides common fields for command messages
type BaseCommand struct {
	BaseMessage
	TargetService string `json:"targetService"`
	ReplyTo       string `json:"replyTo,omitempty"`
}

// GetTargetService returns the target service for the command
func (c BaseCommand) GetTargetService() string {
	return c.TargetService
}

// BaseEvent provides common fields for event messages
type BaseEvent struct {
	BaseMessage
	AggregateID string `json:"aggregateId"`
	Sequence    int64  `json:"sequence"`
	Source      string `json:"source,omitempty"`
}

// GetAggregateID returns the aggregate ID
func (e BaseEvent) GetAggregateID() string {
	return e.AggregateID
}

// GetSequence returns the event sequence number
func (e BaseEvent) GetSequence() int64 {
	return e.Sequence
}

// BaseReply provides common fields for reply messages
type BaseReply struct {
	BaseMessage
	Success bool `json:"success"`
}

// IsSuccess returns whether the reply indicates success
func (r BaseReply) IsSuccess() bool {
	return r.Success
}

// GetError returns nil for successful replies (can be overridden)
func (r BaseReply) GetError() error {
	return nil
}

// BaseQuery provides common fields for query messages
type BaseQuery struct {
	BaseMessage
	ReplyTo string `json:"replyTo"`
}

// GetReplyTo returns the reply-to address
func (q BaseQuery) GetReplyTo() string {
	return q.ReplyTo
}

// NewBaseCommand creates a new command with generated ID and current timestamp
func NewBaseCommand(messageType string) BaseCommand {
	return BaseCommand{
		BaseMessage: NewBaseMessage(messageType),
	}
}

// NewBaseReply creates a new reply with generated ID and current timestamp
func NewBaseReply(requestID, correlationID string) BaseReply {
	reply := BaseReply{
		BaseMessage: NewBaseMessage("Reply"),
		Success:     true,
	}
	reply.SetCorrelationID(correlationID)
	return reply
}