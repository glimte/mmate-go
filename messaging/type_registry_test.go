package messaging

import (
	"testing"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/serialization"
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
		// Reset registry for clean test
		globalRegistry = serialization.NewTypeRegistry()
		
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

	t.Run("Register different message types", func(t *testing.T) {
		// Reset registry for clean test
		globalRegistry = serialization.NewTypeRegistry()

		// Register command
		commandFactory := func() contracts.Message {
			return &TestRegistryCommand{
				BaseCommand: contracts.NewBaseCommand("TestRegistryCommand"),
				Action:      "test",
			}
		}
		err := Register("TestRegistryCommand", commandFactory)
		assert.NoError(t, err)

		// Register event
		eventFactory := func() contracts.Message {
			return &TestRegistryEvent{
				BaseEvent: contracts.BaseEvent{
					BaseMessage: contracts.NewBaseMessage("TestRegistryEvent"),
					AggregateID: "test-aggregate",
					Sequence:    1,
				},
				EventData: "test data",
			}
		}
		err = Register("TestRegistryEvent", eventFactory)
		assert.NoError(t, err)

		// Register query
		queryFactory := func() contracts.Message {
			return &TestRegistryQuery{
				BaseQuery: contracts.NewBaseQuery("TestRegistryQuery"),
				Filter:    "active",
			}
		}
		err = Register("TestRegistryQuery", queryFactory)
		assert.NoError(t, err)

		// Verify all types are registered
		registry := GetTypeRegistry()
		assert.True(t, registry.IsRegistered("TestRegistryCommand"))
		assert.True(t, registry.IsRegistered("TestRegistryEvent"))
		assert.True(t, registry.IsRegistered("TestRegistryQuery"))
	})

	t.Run("Register panics with nil factory", func(t *testing.T) {
		// Reset registry for clean test
		globalRegistry = serialization.NewTypeRegistry()

		// The Register function will panic when calling nil factory
		assert.Panics(t, func() {
			Register("TestType", nil)
		})
	})

	t.Run("Register fails with factory returning nil", func(t *testing.T) {
		// Reset registry for clean test
		globalRegistry = serialization.NewTypeRegistry()

		nilFactory := func() contracts.Message {
			return nil
		}

		err := Register("TestType", nilFactory)

		assert.Error(t, err)
	})

	t.Run("Register duplicate type name fails", func(t *testing.T) {
		// Reset registry for clean test
		globalRegistry = serialization.NewTypeRegistry()

		factory1 := func() contracts.Message {
			return &TestRegistryMessage{
				BaseMessage: contracts.NewBaseMessage("DuplicateType"),
			}
		}

		factory2 := func() contracts.Message {
			return &TestRegistryCommand{
				BaseCommand: contracts.NewBaseCommand("DuplicateType"),
			}
		}

		// First registration should succeed
		err := Register("DuplicateType", factory1)
		assert.NoError(t, err)

		// Second registration with same name but different type should fail
		err = Register("DuplicateType", factory2)
		assert.Error(t, err)
	})
}

func TestRegisterType(t *testing.T) {
	t.Run("RegisterType with message successfully", func(t *testing.T) {
		// Reset registry for clean test
		globalRegistry = serialization.NewTypeRegistry()

		factory := func() contracts.Message {
			return &TestRegistryMessage{
				BaseMessage: contracts.NewBaseMessage("TestRegistryMessage"),
				Data:        "test",
			}
		}

		err := RegisterType(factory)

		assert.NoError(t, err)
		
		// Verify registration - type name will include package path
		registry := GetTypeRegistry()
		types := registry.ListTypes()
		found := false
		for _, typeName := range types {
			if typeName == "github.com/glimte/mmate-go/messaging.TestRegistryMessage" {
				found = true
				break
			}
		}
		assert.True(t, found, "Expected TestRegistryMessage to be registered")
	})

	t.Run("RegisterType with command successfully", func(t *testing.T) {
		// Reset registry for clean test
		globalRegistry = serialization.NewTypeRegistry()

		factory := func() contracts.Message {
			return &TestRegistryCommand{
				BaseCommand: contracts.NewBaseCommand("TestRegistryCommand"),
				Action:      "test",
			}
		}

		err := RegisterType(factory)

		assert.NoError(t, err)
		
		// Verify registration
		registry := GetTypeRegistry()
		types := registry.ListTypes()
		found := false
		for _, typeName := range types {
			if typeName == "github.com/glimte/mmate-go/messaging.TestRegistryCommand" {
				found = true
				break
			}
		}
		assert.True(t, found, "Expected TestRegistryCommand to be registered")
	})

	t.Run("RegisterType with event successfully", func(t *testing.T) {
		// Reset registry for clean test
		globalRegistry = serialization.NewTypeRegistry()

		factory := func() contracts.Message {
			return &TestRegistryEvent{
				BaseEvent: contracts.BaseEvent{
					BaseMessage: contracts.NewBaseMessage("TestRegistryEvent"),
					AggregateID: "test-aggregate",
					Sequence:    1,
				},
				EventData: "test data",
			}
		}

		err := RegisterType(factory)

		assert.NoError(t, err)
		
		// Verify registration
		registry := GetTypeRegistry()
		types := registry.ListTypes()
		found := false
		for _, typeName := range types {
			if typeName == "github.com/glimte/mmate-go/messaging.TestRegistryEvent" {
				found = true
				break
			}
		}
		assert.True(t, found, "Expected TestRegistryEvent to be registered")
	})

	t.Run("RegisterType with query successfully", func(t *testing.T) {
		// Reset registry for clean test
		globalRegistry = serialization.NewTypeRegistry()

		factory := func() contracts.Message {
			return &TestRegistryQuery{
				BaseQuery: contracts.NewBaseQuery("TestRegistryQuery"),
				Filter:    "active",
			}
		}

		err := RegisterType(factory)

		assert.NoError(t, err)
		
		// Verify registration
		registry := GetTypeRegistry()
		types := registry.ListTypes()
		found := false
		for _, typeName := range types {
			if typeName == "github.com/glimte/mmate-go/messaging.TestRegistryQuery" {
				found = true
				break
			}
		}
		assert.True(t, found, "Expected TestRegistryQuery to be registered")
	})

	t.Run("RegisterType panics with nil factory", func(t *testing.T) {
		// Reset registry for clean test
		globalRegistry = serialization.NewTypeRegistry()

		// The RegisterType function will panic when calling nil factory
		assert.Panics(t, func() {
			RegisterType(nil)
		})
	})

	t.Run("RegisterType fails with factory returning nil", func(t *testing.T) {
		// Reset registry for clean test
		globalRegistry = serialization.NewTypeRegistry()

		nilFactory := func() contracts.Message {
			return nil
		}

		err := RegisterType(nilFactory)

		assert.Error(t, err)
	})

	t.Run("RegisterType handles duplicate registrations", func(t *testing.T) {
		// Reset registry for clean test
		globalRegistry = serialization.NewTypeRegistry()

		factory := func() contracts.Message {
			return &TestRegistryMessage{
				BaseMessage: contracts.NewBaseMessage("TestRegistryMessage"),
			}
		}

		// First registration should succeed
		err := RegisterType(factory)
		assert.NoError(t, err)

		// Second registration of same type should not error (same type)
		err = RegisterType(factory)
		assert.NoError(t, err)
	})
}

func TestGetTypeRegistry(t *testing.T) {
	t.Run("GetTypeRegistry returns global registry", func(t *testing.T) {
		registry := GetTypeRegistry()

		assert.NotNil(t, registry)
		assert.Equal(t, globalRegistry, registry)
	})

	t.Run("GetTypeRegistry returns same instance", func(t *testing.T) {
		registry1 := GetTypeRegistry()
		registry2 := GetTypeRegistry()

		assert.Equal(t, registry1, registry2)
		assert.Same(t, registry1, registry2)
	})

	t.Run("Registry persists registrations across calls", func(t *testing.T) {
		// Reset registry for clean test
		globalRegistry = serialization.NewTypeRegistry()

		// Register a type
		factory := func() contracts.Message {
			return &TestRegistryMessage{
				BaseMessage: contracts.NewBaseMessage("PersistentType"),
			}
		}
		err := Register("PersistentType", factory)
		assert.NoError(t, err)

		// Get registry and verify registration persists
		registry := GetTypeRegistry()
		assert.NotNil(t, registry)
		assert.True(t, registry.IsRegistered("PersistentType"))

		// Try to register same type again to verify it's still there
		// The underlying registry allows re-registration of the same type
		err = Register("PersistentType", factory)
		assert.NoError(t, err) // Should succeed because it's the same type
	})
}

func TestGlobalRegistryIntegration(t *testing.T) {
	t.Run("Register and RegisterType work together", func(t *testing.T) {
		// Reset registry for clean test
		globalRegistry = serialization.NewTypeRegistry()

		// Register using Register function
		registerFactory := func() contracts.Message {
			return &TestRegistryMessage{
				BaseMessage: contracts.NewBaseMessage("RegisterMessage"),
			}
		}
		err := Register("RegisterMessage", registerFactory)
		assert.NoError(t, err)

		// Register using RegisterType function
		registerTypeFactory := func() contracts.Message {
			return &TestRegistryCommand{
				BaseCommand: contracts.NewBaseCommand("RegisterTypeCommand"),
			}
		}
		err = RegisterType(registerTypeFactory)
		assert.NoError(t, err)

		// Verify both are accessible through the same registry
		registry := GetTypeRegistry()
		assert.NotNil(t, registry)

		// Both types should be registered
		assert.True(t, registry.IsRegistered("RegisterMessage"))
		
		// RegisterType uses package path, so check with full name
		types := registry.ListTypes()
		found := false
		for _, typeName := range types {
			if typeName == "github.com/glimte/mmate-go/messaging.TestRegistryCommand" {
				found = true
				break
			}
		}
		assert.True(t, found)
	})

	t.Run("Multiple registrations maintain separate types", func(t *testing.T) {
		// Reset registry for clean test
		globalRegistry = serialization.NewTypeRegistry()

		// Register multiple different types
		types := []struct {
			name    string
			factory func() contracts.Message
		}{
			{
				name: "Type1",
				factory: func() contracts.Message {
					return &TestRegistryMessage{
						BaseMessage: contracts.NewBaseMessage("Type1"),
						Data:        "type1_data",
					}
				},
			},
			{
				name: "Type2", 
				factory: func() contracts.Message {
					return &TestRegistryCommand{
						BaseCommand: contracts.NewBaseCommand("Type2"),
						Action:      "type2_action",
					}
				},
			},
			{
				name: "Type3",
				factory: func() contracts.Message {
					return &TestRegistryEvent{
						BaseEvent: contracts.BaseEvent{
							BaseMessage: contracts.NewBaseMessage("Type3"),
							AggregateID: "test-aggregate",
							Sequence:    1,
						},
						EventData: "type3_event",
					}
				},
			},
		}

		// Register all types
		for _, typeInfo := range types {
			err := Register(typeInfo.name, typeInfo.factory)
			assert.NoError(t, err, "Failed to register %s", typeInfo.name)
		}

		// Verify all types are registered
		registry := GetTypeRegistry()
		for _, typeInfo := range types {
			assert.True(t, registry.IsRegistered(typeInfo.name))
		}
	})
}

// Test thread safety (basic test)
func TestTypeRegistryThreadSafety(t *testing.T) {
	t.Run("Concurrent registrations", func(t *testing.T) {
		// Reset registry for clean test
		globalRegistry = serialization.NewTypeRegistry()

		// This is a basic test - the underlying serialization.TypeRegistry
		// should handle thread safety, we're just testing our wrapper doesn't break it
		done := make(chan bool, 2)

		// Goroutine 1
		go func() {
			factory1 := func() contracts.Message {
				return &TestRegistryMessage{
					BaseMessage: contracts.NewBaseMessage("ConcurrentType1"),
				}
			}
			err := Register("ConcurrentType1", factory1)
			assert.NoError(t, err)
			done <- true
		}()

		// Goroutine 2
		go func() {
			factory2 := func() contracts.Message {
				return &TestRegistryCommand{
					BaseCommand: contracts.NewBaseCommand("ConcurrentType2"),
				}
			}
			err := Register("ConcurrentType2", factory2)
			assert.NoError(t, err)
			done <- true
		}()

		// Wait for both to complete
		<-done
		<-done

		// Verify both types are registered
		registry := GetTypeRegistry()
		assert.NotNil(t, registry)
		assert.True(t, registry.IsRegistered("ConcurrentType1"))
		assert.True(t, registry.IsRegistered("ConcurrentType2"))
	})
}

// Test error conditions
func TestTypeRegistryErrorConditions(t *testing.T) {
	t.Run("Factory panics propagate", func(t *testing.T) {
		// Reset registry for clean test
		globalRegistry = serialization.NewTypeRegistry()

		panicFactory := func() contracts.Message {
			panic("test panic")
		}

		// The Register function does not handle panics, they propagate up
		assert.Panics(t, func() {
			Register("PanicType", panicFactory)
		})
	})

	t.Run("Empty type name fails", func(t *testing.T) {
		// Reset registry for clean test
		globalRegistry = serialization.NewTypeRegistry()

		factory := func() contracts.Message {
			return &TestRegistryMessage{
				BaseMessage: contracts.NewBaseMessage("Test"),
			}
		}

		err := Register("", factory)
		assert.Error(t, err)
	})
}

func TestRegistryInteractionWithSerialization(t *testing.T) {
	t.Run("Registered types work with type registry", func(t *testing.T) {
		// Reset registry for clean test
		globalRegistry = serialization.NewTypeRegistry()

		// Register a type
		factory := func() contracts.Message {
			return &TestRegistryMessage{
				BaseMessage: contracts.NewBaseMessage("SerializationTest"),
				Data:        "test data",
			}
		}

		err := Register("SerializationTest", factory)
		assert.NoError(t, err)

		// Create instance using registry
		registry := GetTypeRegistry()
		instance, err := registry.CreateInstance("SerializationTest")
		assert.NoError(t, err)
		assert.NotNil(t, instance)

		// Verify it's the correct type
		msg, ok := instance.(*TestRegistryMessage)
		assert.True(t, ok)
		assert.NotNil(t, msg)
	})

	t.Run("GetTypeName works for registered types", func(t *testing.T) {
		// Reset registry for clean test
		globalRegistry = serialization.NewTypeRegistry()

		// Register a type
		factory := func() contracts.Message {
			return &TestRegistryMessage{
				BaseMessage: contracts.NewBaseMessage("TypeNameTest"),
				Data:        "test",
			}
		}

		err := Register("TypeNameTest", factory)
		assert.NoError(t, err)

		// Create instance and get type name
		msg := factory()
		registry := GetTypeRegistry()
		typeName, err := registry.GetTypeName(msg)
		assert.NoError(t, err)
		assert.Equal(t, "TypeNameTest", typeName)
	})
}