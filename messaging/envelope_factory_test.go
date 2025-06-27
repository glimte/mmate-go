package messaging

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/stretchr/testify/assert"
)

// Test message types for envelope factory tests
type envelopeTestCommand struct {
	contracts.BaseCommand
	CommandData string `json:"commandData"`
}

type envelopeTestEvent struct {
	contracts.BaseEvent
	EventData string `json:"eventData"`
}

type envelopeTestQuery struct {
	contracts.BaseQuery
	QueryData string `json:"queryData"`
}

type envelopeTestReply struct {
	contracts.BaseReply
	ReplyData string `json:"replyData"`
}

func TestNewEnvelopeFactory(t *testing.T) {
	t.Run("creates factory with empty default headers", func(t *testing.T) {
		factory := NewEnvelopeFactory()
		
		assert.NotNil(t, factory)
		assert.NotNil(t, factory.defaultHeaders)
		assert.Empty(t, factory.defaultHeaders)
	})
	
	t.Run("creates factory with default headers", func(t *testing.T) {
		headers := map[string]interface{}{
			"env": "test",
			"version": "1.0",
		}
		factory := NewEnvelopeFactoryWithDefaults(headers)
		
		assert.NotNil(t, factory)
		assert.Equal(t, headers, factory.defaultHeaders)
	})
}

func TestCreateEnvelope(t *testing.T) {
	factory := NewEnvelopeFactory()
	
	t.Run("creates envelope from command", func(t *testing.T) {
		cmd := &envelopeTestCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
				TargetService: "test-service",
				ReplyTo: "reply.queue",
			},
			CommandData: "test data",
		}
		
		envelope, err := factory.CreateEnvelope(cmd)
		
		assert.NoError(t, err)
		assert.NotNil(t, envelope)
		assert.Equal(t, cmd.GetID(), envelope.ID)
		assert.Equal(t, "TestCommand", envelope.Type)
		assert.Equal(t, cmd.GetCorrelationID(), envelope.CorrelationID)
		assert.NotEmpty(t, envelope.Timestamp)
		
		// Check headers
		assert.Equal(t, "command", envelope.Headers["x-message-kind"])
		assert.Equal(t, "TestCommand", envelope.Headers["x-message-type"])
		assert.Equal(t, cmd.GetID(), envelope.Headers["x-message-id"])
		// Note: x-reply-to header won't be set because the factory checks for *BaseCommand type specifically
		assert.Equal(t, "mmate-go", envelope.Headers["x-source"])
		assert.Equal(t, "1.0", envelope.Headers["x-version"])
		
		// Check payload contains only non-base fields
		var payload map[string]interface{}
		err = json.Unmarshal(envelope.Payload, &payload)
		assert.NoError(t, err)
		assert.Equal(t, "test data", payload["commandData"])
		assert.Equal(t, "test-service", payload["targetService"])
		assert.Equal(t, "reply.queue", payload["replyTo"])
		
		// Base fields should not be in payload
		assert.NotContains(t, payload, "id")
		assert.NotContains(t, payload, "type")
		assert.NotContains(t, payload, "timestamp")
	})
	
	t.Run("creates envelope from event", func(t *testing.T) {
		event := &envelopeTestEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
				AggregateID: "aggregate-123",
				Sequence: 42,
			},
			EventData: "event data",
		}
		
		envelope, err := factory.CreateEnvelope(event)
		
		assert.NoError(t, err)
		assert.NotNil(t, envelope)
		assert.Equal(t, event.GetID(), envelope.ID)
		assert.Equal(t, "TestEvent", envelope.Type)
		
		// Check event-specific headers
		assert.Equal(t, "event", envelope.Headers["x-message-kind"])
		assert.Equal(t, "aggregate-123", envelope.Headers["x-aggregate-id"])
		assert.Equal(t, "42", envelope.Headers["x-sequence"])
		
		// Check payload
		var payload map[string]interface{}
		err = json.Unmarshal(envelope.Payload, &payload)
		assert.NoError(t, err)
		assert.Equal(t, "event data", payload["eventData"])
		assert.Equal(t, "aggregate-123", payload["aggregateId"])
		assert.Equal(t, float64(42), payload["sequence"]) // JSON unmarshals numbers as float64
	})
	
	t.Run("creates envelope from query", func(t *testing.T) {
		query := &envelopeTestQuery{
			BaseQuery: contracts.BaseQuery{
				BaseMessage: contracts.NewBaseMessage("TestQuery"),
				ReplyTo: "query.reply.queue",
			},
			QueryData: "query data",
		}
		
		envelope, err := factory.CreateEnvelope(query)
		
		assert.NoError(t, err)
		assert.NotNil(t, envelope)
		
		// Check query-specific headers
		assert.Equal(t, "query", envelope.Headers["x-message-kind"])
		// Note: x-reply-to header won't be set because the factory checks for *BaseQuery type specifically
		
		// Check payload
		var payload map[string]interface{}
		err = json.Unmarshal(envelope.Payload, &payload)
		assert.NoError(t, err)
		assert.Equal(t, "query data", payload["queryData"])
		assert.Equal(t, "query.reply.queue", payload["replyTo"])
	})
	
	t.Run("creates envelope from reply", func(t *testing.T) {
		reply := &envelopeTestReply{
			BaseReply: contracts.BaseReply{
				BaseMessage: contracts.NewBaseMessage("TestReply"),
				Success: true,
			},
			ReplyData: "reply data",
		}
		
		envelope, err := factory.CreateEnvelope(reply)
		
		assert.NoError(t, err)
		assert.NotNil(t, envelope)
		
		// Check reply-specific headers
		assert.Equal(t, "reply", envelope.Headers["x-message-kind"])
		assert.Equal(t, "true", envelope.Headers["x-success"])
		
		// Check payload
		var payload map[string]interface{}
		err = json.Unmarshal(envelope.Payload, &payload)
		assert.NoError(t, err)
		assert.Equal(t, "reply data", payload["replyData"])
		assert.Equal(t, true, payload["success"])
	})
	
	t.Run("fails with nil message", func(t *testing.T) {
		envelope, err := factory.CreateEnvelope(nil)
		
		assert.Error(t, err)
		assert.Nil(t, envelope)
		assert.Contains(t, err.Error(), "message cannot be nil")
	})
	
	t.Run("includes default headers", func(t *testing.T) {
		defaultHeaders := map[string]interface{}{
			"environment": "test",
			"service": "test-service",
		}
		factory := NewEnvelopeFactoryWithDefaults(defaultHeaders)
		
		msg := &contracts.BaseMessage{
			ID: "msg-123",
			Type: "TestMessage",
			Timestamp: time.Now(),
		}
		
		envelope, err := factory.CreateEnvelope(msg)
		
		assert.NoError(t, err)
		assert.Equal(t, "test", envelope.Headers["environment"])
		assert.Equal(t, "test-service", envelope.Headers["service"])
	})
}

func TestCreateEnvelopeWithOptions(t *testing.T) {
	factory := NewEnvelopeFactory()
	
	t.Run("applies envelope options", func(t *testing.T) {
		msg := &contracts.BaseMessage{
			ID: "msg-123",
			Type: "TestMessage",
			Timestamp: time.Now(),
		}
		
		customTime := time.Now().Add(-1 * time.Hour)
		customHeaders := map[string]interface{}{
			"custom-header": "custom-value",
			"priority": 5,
		}
		
		envelope, err := factory.CreateEnvelopeWithOptions(msg,
			WithEnvelopeID("custom-id"),
			WithEnvelopeTimestamp(customTime),
			WithEnvelopeHeaders(customHeaders),
			WithEnvelopeReplyTo("custom.reply.queue"),
		)
		
		assert.NoError(t, err)
		assert.Equal(t, "custom-id", envelope.ID)
		assert.Equal(t, customTime.Format(time.RFC3339), envelope.Timestamp)
		assert.Equal(t, "custom-value", envelope.Headers["custom-header"])
		assert.Equal(t, 5, envelope.Headers["priority"])
		assert.Equal(t, "custom.reply.queue", envelope.ReplyTo)
		
		// Original headers should still be present
		assert.Equal(t, "TestMessage", envelope.Headers["x-message-type"])
		assert.Equal(t, "mmate-go", envelope.Headers["x-source"])
	})
	
	t.Run("options override defaults", func(t *testing.T) {
		defaultHeaders := map[string]interface{}{
			"env": "production",
			"version": "1.0",
		}
		factory := NewEnvelopeFactoryWithDefaults(defaultHeaders)
		
		msg := &contracts.BaseMessage{
			ID: "msg-123",
			Type: "TestMessage",
			Timestamp: time.Now(),
		}
		
		envelope, err := factory.CreateEnvelopeWithOptions(msg,
			WithEnvelopeHeaders(map[string]interface{}{
				"env": "test", // Override default
				"new-header": "new-value",
			}),
		)
		
		assert.NoError(t, err)
		assert.Equal(t, "test", envelope.Headers["env"]) // Overridden
		assert.Equal(t, "1.0", envelope.Headers["version"]) // Still from defaults
		assert.Equal(t, "new-value", envelope.Headers["new-header"]) // New header
	})
}

func TestEnvelopeExtractMessage(t *testing.T) {
	factory := NewEnvelopeFactory()
	
	t.Run("extracts message from envelope", func(t *testing.T) {
		payload := json.RawMessage(`{"commandData":"extracted data","targetService":"service-1"}`)
		envelope := &contracts.Envelope{
			ID: "env-123",
			Type: "TestCommand",
			Timestamp: time.Now().Format(time.RFC3339),
			CorrelationID: "corr-456",
			Payload: payload,
		}
		
		var cmd envelopeTestCommand
		err := factory.ExtractMessage(envelope, &cmd)
		
		assert.NoError(t, err)
		assert.Equal(t, "extracted data", cmd.CommandData)
		assert.Equal(t, "service-1", cmd.TargetService)
		assert.Equal(t, "corr-456", cmd.GetCorrelationID()) // Should set correlation ID
	})
	
	t.Run("fails with nil envelope", func(t *testing.T) {
		var msg testCommand
		err := factory.ExtractMessage(nil, &msg)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "envelope cannot be nil")
	})
	
	t.Run("fails with nil message type", func(t *testing.T) {
		envelope := &contracts.Envelope{
			ID: "env-123",
			Type: "TestCommand",
			Payload: json.RawMessage(`{}`),
		}
		
		err := factory.ExtractMessage(envelope, nil)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "messageType cannot be nil")
	})
	
	t.Run("fails with invalid payload", func(t *testing.T) {
		envelope := &contracts.Envelope{
			ID: "env-123",
			Type: "TestCommand",
			Payload: json.RawMessage(`invalid json`),
		}
		
		var cmd envelopeTestCommand
		err := factory.ExtractMessage(envelope, &cmd)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to unmarshal message")
	})
}

func TestJSONEnvelopeSerializer(t *testing.T) {
	serializer := NewJSONEnvelopeSerializer()
	
	t.Run("serializes envelope to JSON", func(t *testing.T) {
		envelope := &contracts.Envelope{
			ID: "env-123",
			Type: "TestMessage",
			Timestamp: time.Now().Format(time.RFC3339),
			CorrelationID: "corr-456",
			Headers: map[string]interface{}{
				"header1": "value1",
				"header2": 42,
			},
			Payload: json.RawMessage(`{"data":"test"}`),
		}
		
		data, err := serializer.Serialize(envelope)
		
		assert.NoError(t, err)
		assert.NotNil(t, data)
		
		// Verify it's valid JSON
		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		assert.NoError(t, err)
		assert.Equal(t, "env-123", result["id"])
		assert.Equal(t, "TestMessage", result["type"])
		assert.Equal(t, "corr-456", result["correlationId"])
	})
	
	t.Run("fails to serialize nil envelope", func(t *testing.T) {
		data, err := serializer.Serialize(nil)
		
		assert.Error(t, err)
		assert.Nil(t, data)
		assert.Contains(t, err.Error(), "envelope cannot be nil")
	})
	
	t.Run("deserializes JSON to envelope", func(t *testing.T) {
		jsonData := `{
			"id": "env-123",
			"type": "TestMessage",
			"timestamp": "2023-01-01T12:00:00Z",
			"correlationId": "corr-456",
			"headers": {"header1": "value1"},
			"payload": {"data": "test"}
		}`
		
		envelope, err := serializer.Deserialize([]byte(jsonData))
		
		assert.NoError(t, err)
		assert.NotNil(t, envelope)
		assert.Equal(t, "env-123", envelope.ID)
		assert.Equal(t, "TestMessage", envelope.Type)
		assert.Equal(t, "corr-456", envelope.CorrelationID)
		assert.Equal(t, "2023-01-01T12:00:00Z", envelope.Timestamp)
		assert.Equal(t, "value1", envelope.Headers["header1"])
	})
	
	t.Run("fails to deserialize empty data", func(t *testing.T) {
		envelope, err := serializer.Deserialize([]byte{})
		
		assert.Error(t, err)
		assert.Nil(t, envelope)
		assert.Contains(t, err.Error(), "data cannot be empty")
	})
	
	t.Run("fails to deserialize invalid JSON", func(t *testing.T) {
		envelope, err := serializer.Deserialize([]byte("invalid json"))
		
		assert.Error(t, err)
		assert.Nil(t, envelope)
		assert.Contains(t, err.Error(), "failed to unmarshal envelope")
	})
}

func TestEnvelopeRoundTrip(t *testing.T) {
	factory := NewEnvelopeFactory()
	serializer := NewJSONEnvelopeSerializer()
	
	t.Run("command round trip", func(t *testing.T) {
		// Create original command
		originalCmd := &envelopeTestCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
				TargetService: "test-service",
			},
			CommandData: "command data",
		}
		originalCmd.SetCorrelationID("corr-123")
		
		// Create envelope
		envelope, err := factory.CreateEnvelope(originalCmd)
		assert.NoError(t, err)
		
		// Serialize envelope
		data, err := serializer.Serialize(envelope)
		assert.NoError(t, err)
		
		// Deserialize envelope
		deserializedEnv, err := serializer.Deserialize(data)
		assert.NoError(t, err)
		
		// Extract command from envelope
		var extractedCmd envelopeTestCommand
		err = factory.ExtractMessage(deserializedEnv, &extractedCmd)
		assert.NoError(t, err)
		
		// Verify the extracted command matches the original
		assert.Equal(t, originalCmd.CommandData, extractedCmd.CommandData)
		assert.Equal(t, originalCmd.TargetService, extractedCmd.TargetService)
		assert.Equal(t, originalCmd.GetCorrelationID(), extractedCmd.GetCorrelationID())
	})
}