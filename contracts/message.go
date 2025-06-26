package contracts

import (
	"time"
)

// Message is the base interface for all messages
type Message interface {
	GetID() string
	GetTimestamp() time.Time
	GetType() string
	GetMessageType() string // Alias for GetType for compatibility
	GetCorrelationID() string
	SetCorrelationID(correlationID string)
}

// Command represents an action to be performed
type Command interface {
	Message
	GetTargetService() string
}

// Event represents something that has happened
type Event interface {
	Message
	GetAggregateID() string
	GetSequence() int64
}

// Query represents a request for information
type Query interface {
	Message
	GetReplyTo() string
}

// Reply represents a response to a request
type Reply interface {
	Message
	IsSuccess() bool
	GetError() error
}