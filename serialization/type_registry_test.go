package serialization

import (
	"encoding/json"
	"testing"

	"github.com/glimte/mmate-go/contracts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test message types
type TestCommand struct {
	contracts.BaseMessage
	OrderID    string `json:"orderId"`
	CustomerID string `json:"customerId"`
	Amount     float64 `json:"amount"`
}

type TestEvent struct {
	contracts.BaseMessage
	EventType string `json:"eventType"`
	Payload   map[string]interface{} `json:"payload"`
}

func TestDefaultTypeRegistry(t *testing.T) {
	t.Run("creates new registry", func(t *testing.T) {
		registry := NewTypeRegistry()
		assert.NotNil(t, registry)
		assert.NotNil(t, registry.types)
		assert.NotNil(t, registry.names)
	})
	
	t.Run("registers type with name", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		err := registry.Register("TestCommand", &TestCommand{})
		require.NoError(t, err)
		
		assert.True(t, registry.IsRegistered("TestCommand"))
	})
	
	t.Run("registers type automatically", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		err := registry.RegisterType(&TestCommand{})
		require.NoError(t, err)
		
		// Should be registered with full type name
		types := registry.ListTypes()
		assert.Len(t, types, 1)
		assert.Contains(t, types[0], "TestCommand")
	})
	
	t.Run("rejects empty type name", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		err := registry.Register("", &TestCommand{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "type name cannot be empty")
	})
	
	t.Run("rejects nil type", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		err := registry.Register("Test", nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "message type cannot be nil")
	})
	
	t.Run("rejects non-struct types", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		err := registry.Register("Test", "not a struct")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be a struct")
	})
	
	t.Run("handles duplicate registration of same type", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		err := registry.Register("TestCommand", &TestCommand{})
		require.NoError(t, err)
		
		// Registering same type again should not error
		err = registry.Register("TestCommand", &TestCommand{})
		assert.NoError(t, err)
	})
	
	t.Run("rejects duplicate registration of different type", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		err := registry.Register("TestCommand", &TestCommand{})
		require.NoError(t, err)
		
		// Registering different type with same name should error
		err = registry.Register("TestCommand", &TestEvent{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already registered")
	})
}

func TestDefaultTypeRegistry_CreateInstance(t *testing.T) {
	registry := NewTypeRegistry()
	
	// Register types
	err := registry.Register("TestCommand", &TestCommand{})
	require.NoError(t, err)
	
	err = registry.Register("TestEvent", &TestEvent{})
	require.NoError(t, err)
	
	t.Run("creates instance of registered type", func(t *testing.T) {
		instance, err := registry.CreateInstance("TestCommand")
		require.NoError(t, err)
		
		cmd, ok := instance.(*TestCommand)
		assert.True(t, ok)
		assert.NotNil(t, cmd)
	})
	
	t.Run("returns error for unregistered type", func(t *testing.T) {
		_, err := registry.CreateInstance("UnknownType")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not registered")
	})
}

func TestDefaultTypeRegistry_GetTypeName(t *testing.T) {
	registry := NewTypeRegistry()
	
	// Register type
	err := registry.Register("TestCommand", &TestCommand{})
	require.NoError(t, err)
	
	t.Run("gets type name for registered type", func(t *testing.T) {
		cmd := &TestCommand{}
		typeName, err := registry.GetTypeName(cmd)
		require.NoError(t, err)
		assert.Equal(t, "TestCommand", typeName)
	})
	
	t.Run("returns error for unregistered type", func(t *testing.T) {
		event := &TestEvent{}
		_, err := registry.GetTypeName(event)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not registered")
	})
	
	t.Run("returns error for nil", func(t *testing.T) {
		_, err := registry.GetTypeName(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be nil")
	})
}

func TestJSONSerializer(t *testing.T) {
	t.Run("creates with default options", func(t *testing.T) {
		serializer := NewJSONSerializer()
		assert.NotNil(t, serializer)
		assert.True(t, serializer.includeType)
		assert.False(t, serializer.prettyPrint)
		assert.Equal(t, "_type", serializer.typeFieldName)
	})
	
	t.Run("applies options", func(t *testing.T) {
		registry := NewTypeRegistry()
		serializer := NewJSONSerializer(
			WithTypeRegistry(registry),
			WithIncludeType(false),
			WithPrettyPrint(true),
			WithTypeFieldName("$type"),
		)
		
		assert.Equal(t, registry, serializer.registry)
		assert.False(t, serializer.includeType)
		assert.True(t, serializer.prettyPrint)
		assert.Equal(t, "$type", serializer.typeFieldName)
	})
}

func TestJSONSerializer_Serialize(t *testing.T) {
	registry := NewTypeRegistry()
	registry.Register("TestCommand", &TestCommand{})
	
	serializer := NewJSONSerializer(WithTypeRegistry(registry))
	
	t.Run("serializes message with type info", func(t *testing.T) {
		cmd := &TestCommand{
			BaseMessage: contracts.BaseMessage{
				ID:   "msg-123",
				Type: "TestCommand",
			},
			OrderID:    "order-456",
			CustomerID: "cust-789",
			Amount:     99.99,
		}
		
		data, err := serializer.Serialize(cmd)
		require.NoError(t, err)
		
		// Check that type info is included
		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)
		
		assert.Equal(t, "TestCommand", result["_type"])
		assert.Equal(t, "msg-123", result["id"])
		assert.Equal(t, "order-456", result["orderId"])
	})
	
	t.Run("serializes without type info when disabled", func(t *testing.T) {
		serializer := NewJSONSerializer(
			WithTypeRegistry(registry),
			WithIncludeType(false),
		)
		
		cmd := &TestCommand{
			BaseMessage: contracts.BaseMessage{
				ID:   "msg-123",
				Type: "TestCommand",
			},
		}
		
		data, err := serializer.Serialize(cmd)
		require.NoError(t, err)
		
		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)
		
		_, hasType := result["_type"]
		assert.False(t, hasType)
	})
	
	t.Run("handles pretty print", func(t *testing.T) {
		serializer := NewJSONSerializer(
			WithTypeRegistry(registry),
			WithPrettyPrint(true),
		)
		
		cmd := &TestCommand{
			BaseMessage: contracts.BaseMessage{
				ID: "msg-123",
			},
		}
		
		data, err := serializer.Serialize(cmd)
		require.NoError(t, err)
		
		// Pretty printed JSON should have newlines
		assert.Contains(t, string(data), "\n")
	})
	
	t.Run("returns error for nil message", func(t *testing.T) {
		_, err := serializer.Serialize(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be nil")
	})
}

func TestJSONSerializer_Deserialize(t *testing.T) {
	registry := NewTypeRegistry()
	registry.Register("TestCommand", &TestCommand{})
	
	serializer := NewJSONSerializer(WithTypeRegistry(registry))
	
	t.Run("deserializes with type info", func(t *testing.T) {
		data := []byte(`{
			"_type": "TestCommand",
			"id": "msg-123",
			"type": "TestCommand",
			"orderId": "order-456",
			"customerId": "cust-789",
			"amount": 99.99
		}`)
		
		msg, err := serializer.Deserialize(data)
		require.NoError(t, err)
		
		cmd, ok := msg.(*TestCommand)
		require.True(t, ok)
		assert.Equal(t, "msg-123", cmd.ID)
		assert.Equal(t, "order-456", cmd.OrderID)
		assert.Equal(t, "cust-789", cmd.CustomerID)
		assert.Equal(t, 99.99, cmd.Amount)
	})
	
	t.Run("deserializes without type info as BaseMessage", func(t *testing.T) {
		data := []byte(`{
			"id": "msg-123",
			"type": "SomeCommand",
			"timestamp": "2024-01-01T00:00:00Z"
		}`)
		
		msg, err := serializer.Deserialize(data)
		require.NoError(t, err)
		
		assert.Equal(t, "msg-123", msg.GetID())
		assert.Equal(t, "SomeCommand", msg.GetType())
	})
	
	t.Run("returns error for unknown type", func(t *testing.T) {
		data := []byte(`{
			"_type": "UnknownType",
			"id": "msg-123"
		}`)
		
		_, err := serializer.Deserialize(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create instance")
	})
	
	t.Run("returns error for empty data", func(t *testing.T) {
		_, err := serializer.Deserialize([]byte{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be empty")
	})
	
	t.Run("returns error for invalid JSON", func(t *testing.T) {
		_, err := serializer.Deserialize([]byte("not json"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to unmarshal")
	})
}

func TestJSONSerializer_Envelope(t *testing.T) {
	serializer := NewJSONSerializer()
	
	t.Run("serializes envelope", func(t *testing.T) {
		env := &contracts.Envelope{
			ID:            "msg-123",
			Type:          "TestCommand",
			CorrelationID: "corr-456",
			Headers: map[string]interface{}{
				"source": "test",
			},
		}
		
		data, err := serializer.SerializeEnvelope(env)
		require.NoError(t, err)
		
		var result contracts.Envelope
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)
		
		assert.Equal(t, "msg-123", result.ID)
		assert.Equal(t, "corr-456", result.CorrelationID)
		assert.Equal(t, "test", result.Headers["source"])
	})
	
	t.Run("deserializes envelope", func(t *testing.T) {
		data := []byte(`{
			"id": "msg-123",
			"type": "TestCommand",
			"correlationId": "corr-456",
			"headers": {
				"source": "test"
			}
		}`)
		
		env, err := serializer.DeserializeEnvelope(data)
		require.NoError(t, err)
		
		assert.Equal(t, "msg-123", env.ID)
		assert.Equal(t, "TestCommand", env.Type)
		assert.Equal(t, "corr-456", env.CorrelationID)
		assert.Equal(t, "test", env.Headers["source"])
	})
	
	t.Run("returns error for nil envelope", func(t *testing.T) {
		_, err := serializer.SerializeEnvelope(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be nil")
	})
	
	t.Run("returns error for empty envelope data", func(t *testing.T) {
		_, err := serializer.DeserializeEnvelope([]byte{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be empty")
	})
}