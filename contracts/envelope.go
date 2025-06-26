package contracts

import (
	"encoding/json"
)

// Envelope wraps messages for transport
type Envelope struct {
	ID            string                 `json:"id"`
	Type          string                 `json:"type"`
	Timestamp     string                 `json:"timestamp"`
	CorrelationID string                 `json:"correlationId,omitempty"`
	ReplyTo       string                 `json:"replyTo,omitempty"`
	Headers       map[string]interface{} `json:"headers,omitempty"`
	Body          json.RawMessage        `json:"body"`
}

// MessageMetadata contains routing and processing information
type MessageMetadata struct {
	Queue        string
	Exchange     string
	RoutingKey   string
	Priority     uint8
	DeliveryMode uint8
	Expiration   string
	RetryCount   int
	MaxRetries   int
}