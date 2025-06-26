package messaging

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/google/uuid"
)

// EnvelopeFactory creates message envelopes with proper metadata
type EnvelopeFactory interface {
	CreateEnvelope(message contracts.Message) (*contracts.Envelope, error)
	CreateEnvelopeWithOptions(message contracts.Message, opts ...EnvelopeOption) (*contracts.Envelope, error)
	ExtractMessage(envelope *contracts.Envelope, messageType interface{}) error
}

// EnvelopeOption configures envelope creation
type EnvelopeOption func(*contracts.Envelope)

// WithEnvelopeID sets a custom envelope ID
func WithEnvelopeID(id string) EnvelopeOption {
	return func(e *contracts.Envelope) {
		e.ID = id
	}
}

// WithEnvelopeTimestamp sets a custom timestamp
func WithEnvelopeTimestamp(timestamp time.Time) EnvelopeOption {
	return func(e *contracts.Envelope) {
		e.Timestamp = timestamp.Format(time.RFC3339)
	}
}

// WithEnvelopeHeaders sets custom headers
func WithEnvelopeHeaders(headers map[string]interface{}) EnvelopeOption {
	return func(e *contracts.Envelope) {
		if e.Headers == nil {
			e.Headers = make(map[string]interface{})
		}
		for k, v := range headers {
			e.Headers[k] = v
		}
	}
}

// WithEnvelopeReplyTo sets the reply-to field
func WithEnvelopeReplyTo(replyTo string) EnvelopeOption {
	return func(e *contracts.Envelope) {
		e.ReplyTo = replyTo
	}
}

// DefaultEnvelopeFactory provides standard envelope creation
type DefaultEnvelopeFactory struct {
	defaultHeaders map[string]interface{}
}

// NewEnvelopeFactory creates a new envelope factory
func NewEnvelopeFactory() *DefaultEnvelopeFactory {
	return &DefaultEnvelopeFactory{
		defaultHeaders: make(map[string]interface{}),
	}
}

// NewEnvelopeFactoryWithDefaults creates a factory with default headers
func NewEnvelopeFactoryWithDefaults(defaultHeaders map[string]interface{}) *DefaultEnvelopeFactory {
	return &DefaultEnvelopeFactory{
		defaultHeaders: defaultHeaders,
	}
}

// CreateEnvelope creates an envelope for a message
func (f *DefaultEnvelopeFactory) CreateEnvelope(message contracts.Message) (*contracts.Envelope, error) {
	return f.CreateEnvelopeWithOptions(message)
}

// CreateEnvelopeWithOptions creates an envelope with custom options
func (f *DefaultEnvelopeFactory) CreateEnvelopeWithOptions(message contracts.Message, opts ...EnvelopeOption) (*contracts.Envelope, error) {
	if message == nil {
		return nil, fmt.Errorf("message cannot be nil")
	}

	// Serialize message to JSON
	body, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}

	// Create envelope
	envelope := &contracts.Envelope{
		ID:            uuid.New().String(),
		Type:          message.GetType(),
		CorrelationID: message.GetCorrelationID(),
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		Headers:       make(map[string]interface{}),
		Body:          body,
	}

	// Add default headers
	for k, v := range f.defaultHeaders {
		envelope.Headers[k] = v
	}

	// Add message type headers
	envelope.Headers["x-message-type"] = message.GetType()
	envelope.Headers["x-message-id"] = message.GetID()

	if envelope.CorrelationID != "" {
		envelope.Headers["x-correlation-id"] = envelope.CorrelationID
	}

	// Add type-specific headers
	switch msg := message.(type) {
	case contracts.Command:
		envelope.Headers["x-message-kind"] = "command"
		if cmd, ok := msg.(*contracts.BaseCommand); ok && cmd.ReplyTo != "" {
			envelope.Headers["x-reply-to"] = cmd.ReplyTo
		}
	case contracts.Event:
		envelope.Headers["x-message-kind"] = "event"
		envelope.Headers["x-aggregate-id"] = msg.GetAggregateID()
		envelope.Headers["x-sequence"] = fmt.Sprintf("%d", msg.GetSequence())
	case contracts.Query:
		envelope.Headers["x-message-kind"] = "query"
		if qry, ok := msg.(*contracts.BaseQuery); ok && qry.ReplyTo != "" {
			envelope.Headers["x-reply-to"] = qry.ReplyTo
		}
	case contracts.Reply:
		envelope.Headers["x-message-kind"] = "reply"
		envelope.Headers["x-success"] = fmt.Sprintf("%t", msg.IsSuccess())
	}

	// Add source information
	envelope.Headers["x-source"] = "mmate-go"
	envelope.Headers["x-version"] = "1.0"

	// Apply options
	for _, opt := range opts {
		opt(envelope)
	}

	return envelope, nil
}

// ExtractMessage extracts a message from an envelope
func (f *DefaultEnvelopeFactory) ExtractMessage(envelope *contracts.Envelope, messageType interface{}) error {
	if envelope == nil {
		return fmt.Errorf("envelope cannot be nil")
	}
	if messageType == nil {
		return fmt.Errorf("messageType cannot be nil")
	}

	// Unmarshal body into the provided message type
	err := json.Unmarshal(envelope.Body, messageType)
	if err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	// Set correlation ID if the message supports it
	if msg, ok := messageType.(contracts.Message); ok && envelope.CorrelationID != "" {
		msg.SetCorrelationID(envelope.CorrelationID)
	}

	return nil
}

// EnvelopeSerializer handles envelope serialization
type EnvelopeSerializer interface {
	Serialize(envelope *contracts.Envelope) ([]byte, error)
	Deserialize(data []byte) (*contracts.Envelope, error)
}

// JSONEnvelopeSerializer provides JSON serialization for envelopes
type JSONEnvelopeSerializer struct{}

// NewJSONEnvelopeSerializer creates a new JSON envelope serializer
func NewJSONEnvelopeSerializer() *JSONEnvelopeSerializer {
	return &JSONEnvelopeSerializer{}
}

// Serialize serializes an envelope to JSON
func (s *JSONEnvelopeSerializer) Serialize(envelope *contracts.Envelope) ([]byte, error) {
	if envelope == nil {
		return nil, fmt.Errorf("envelope cannot be nil")
	}

	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal envelope: %w", err)
	}

	return data, nil
}

// Deserialize deserializes JSON data to an envelope
func (s *JSONEnvelopeSerializer) Deserialize(data []byte) (*contracts.Envelope, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("data cannot be empty")
	}

	var envelope contracts.Envelope
	err := json.Unmarshal(data, &envelope)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal envelope: %w", err)
	}

	return &envelope, nil
}
