package messaging

import (
	"testing"

	"github.com/glimte/mmate-go/contracts"
	"github.com/stretchr/testify/assert"
)

// Test message types
type TestRegistryMessage struct {
	contracts.BaseMessage
	Data string `json:"data"`
}

type TestRegistryCommand struct {
	contracts.BaseCommand
	Action string `json:"action"`
}

type TestRegistryEvent struct {
	contracts.BaseEvent
	EventData string `json:"eventData"`
}

type TestRegistryQuery struct {
	contracts.BaseQuery
	Filter string `json:"filter"`
}

func TestRegister(t *testing.T) {
	t.Run("Register message type successfully", func(t *testing.T) {
		factory := func() contracts.Message {
			return &TestRegistryMessage{
				BaseMessage: contracts.NewBaseMessage("TestRegistryMessage"),
				Data:        "test",
			}
		}

		err := Register("TestRegistryMessage", factory)

		assert.NoError(t, err)
		
		// Verify registration by checking the global registry
		registry := GetTypeRegistry()
		assert.True(t, registry.IsRegistered("TestRegistryMessage"))
	})

	t.Run("Register command type", func(t *testing.T) {
		factory := func() contracts.Message {
			cmd := &TestRegistryCommand{
				BaseCommand: contracts.NewBaseCommand("TestRegistryCommand"),
				Action:      "execute",
			}
			cmd.TargetService = "test-service"
			return cmd
		}

		err := Register("TestRegistryCommand", factory)

		assert.NoError(t, err)
		
		registry := GetTypeRegistry()
		assert.True(t, registry.IsRegistered("TestRegistryCommand"))
	})

	t.Run("Register event type", func(t *testing.T) {
		factory := func() contracts.Message {
			evt := &TestRegistryEvent{
				EventData: "event occurred",
			}
			evt.BaseEvent.BaseMessage = contracts.NewBaseMessage("TestRegistryEvent")
			evt.BaseEvent.AggregateID = "aggregate-123"
			evt.BaseEvent.Sequence = 1
			return evt
		}

		err := Register("TestRegistryEvent", factory)

		assert.NoError(t, err)
		
		registry := GetTypeRegistry()
		assert.True(t, registry.IsRegistered("TestRegistryEvent"))
	})

	t.Run("Register query type", func(t *testing.T) {
		factory := func() contracts.Message {
			qry := &TestRegistryQuery{
				BaseQuery: contracts.NewBaseQuery("TestRegistryQuery"),
				Filter:    "active",
			}
			qry.ReplyTo = "reply-queue"
			return qry
		}

		err := Register("TestRegistryQuery", factory)

		assert.NoError(t, err)
		
		registry := GetTypeRegistry()
		assert.True(t, registry.IsRegistered("TestRegistryQuery"))
	})
}

func TestRegisterType(t *testing.T) {
	t.Run("Register using struct name", func(t *testing.T) {
		// Create unique type for this test
		type UniqueTestMessage struct {
			contracts.BaseMessage
			UniqueField string `json:"uniqueField"`
		}

		factory := func() contracts.Message {
			return &UniqueTestMessage{
				BaseMessage: contracts.NewBaseMessage("UniqueTestMessage"),
				UniqueField: "unique",
			}
		}

		err := RegisterType(factory)

		assert.NoError(t, err)
		
		// The type should be registered with its full package path
		registry := GetTypeRegistry()
		// We can't predict the exact registered name due to package path
		// but we know it should be registered
		types := registry.ListTypes()
		found := false
		for _, typeName := range types {
			if contains(typeName, "UniqueTestMessage") {
				found = true
				break
			}
		}
		assert.True(t, found, "UniqueTestMessage should be registered")
	})
}

func TestGetTypeRegistry(t *testing.T) {
	t.Run("Returns global registry", func(t *testing.T) {
		registry1 := GetTypeRegistry()
		registry2 := GetTypeRegistry()
		
		// Should return the same instance
		assert.NotNil(t, registry1)
		assert.NotNil(t, registry2)
		// Both should have access to the same registered types
		assert.Equal(t, len(registry1.ListTypes()), len(registry2.ListTypes()))
	})
}

// Helper function to check if string contains substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || (len(s) > len(substr) && containsSubstring(s, substr)))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}