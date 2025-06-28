package serialization

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/stretchr/testify/assert"
)

// Test message types with comprehensive features
type testCommand struct {
	contracts.BaseCommand
	OrderID      string                 `json:"orderId"`
	CustomerID   string                 `json:"customerId"`
	Amount       float64                `json:"amount"`
	Items        []string               `json:"items,omitempty"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
	ProcessAfter *time.Time             `json:"processAfter,omitempty"`
}

type testEvent struct {
	contracts.BaseEvent
	EventType     string                 `json:"eventType"`
	OccurredAt    time.Time              `json:"occurredAt"`
	Payload       map[string]interface{} `json:"payload"`
	SchemaVersion int                    `json:"schemaVersion"`
}

type testQuery struct {
	contracts.BaseQuery
	QueryType   string   `json:"queryType"`
	Filters     []string `json:"filters"`
	Limit       int      `json:"limit"`
	Offset      int      `json:"offset"`
	IncludeData bool     `json:"includeData"`
}

type testReply struct {
	contracts.BaseReply
	ResultCount int                    `json:"resultCount"`
	Results     []interface{}          `json:"results,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	Duration    time.Duration          `json:"duration"`
}

// Complex nested types for advanced testing
type nestedCommand struct {
	contracts.BaseCommand
	Parent struct {
		ID   string `json:"id"`
		Name string `json:"name"`
		Child struct {
			Value  string    `json:"value"`
			Number int       `json:"number"`
			Time   time.Time `json:"time"`
		} `json:"child"`
	} `json:"parent"`
	Tags []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	} `json:"tags"`
}

// Non-message type for negative testing
type nonMessageType struct {
	Value string `json:"value"`
	Count int    `json:"count"`
}

// Type with custom marshaling
type customMarshalCommand struct {
	contracts.BaseCommand
	Secret string `json:"-"` // Should not be serialized
	Public string `json:"public"`
	internal string // unexported field
}

func (c *customMarshalCommand) MarshalJSON() ([]byte, error) {
	type Alias customMarshalCommand
	return json.Marshal(&struct {
		*Alias
		Custom string `json:"custom"`
	}{
		Alias:  (*Alias)(c),
		Custom: "custom-value",
	})
}

// Benchmarks types
type benchmarkCommand struct {
	contracts.BaseCommand
	Field1  string `json:"field1"`
	Field2  string `json:"field2"`
	Field3  int    `json:"field3"`
	Field4  bool   `json:"field4"`
	Field5  float64 `json:"field5"`
}

func TestNewTypeRegistry(t *testing.T) {
	t.Run("creates empty registry with initialized maps", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		assert.NotNil(t, registry)
		assert.NotNil(t, registry.types)
		assert.NotNil(t, registry.names)
		assert.Empty(t, registry.ListTypes())
		
		// Verify mutex is usable
		registry.mu.Lock()
		registry.mu.Unlock()
	})
}

func TestRegister(t *testing.T) {
	t.Run("registers struct type successfully", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		err := registry.Register("TestCommand", &testCommand{})
		
		assert.NoError(t, err)
		assert.True(t, registry.IsRegistered("TestCommand"))
		
		// Verify it's stored correctly
		typ, err := registry.Get("TestCommand")
		assert.NoError(t, err)
		assert.Equal(t, reflect.TypeOf(testCommand{}), typ)
	})
	
	t.Run("handles pointer and non-pointer types identically", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		// Register with pointer
		err := registry.Register("Command1", &testCommand{})
		assert.NoError(t, err)
		
		// Register with non-pointer
		err = registry.Register("Command2", testCommand{})
		assert.NoError(t, err)
		
		// Both should resolve to same underlying type
		type1, _ := registry.Get("Command1")
		type2, _ := registry.Get("Command2")
		assert.Equal(t, type1, type2)
		assert.Equal(t, reflect.TypeOf(testCommand{}), type1)
	})
	
	t.Run("validates type name", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		testCases := []struct {
			name        string
			typeName    string
			shouldError bool
			errorMsg    string
		}{
			{"empty name", "", true, "type name cannot be empty"},
			{"valid name", "ValidType", false, ""},
			{"with dots", "com.example.Type", false, ""},
			{"with numbers", "Type123", false, ""},
			{"with underscore", "Type_Name", false, ""},
		}
		
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := registry.Register(tc.typeName, &testCommand{})
				if tc.shouldError {
					assert.Error(t, err)
					assert.Contains(t, err.Error(), tc.errorMsg)
				} else {
					assert.NoError(t, err)
				}
			})
		}
	})
	
	t.Run("validates message type", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		testCases := []struct {
			name        string
			msgType     interface{}
			shouldError bool
			errorMsg    string
		}{
			{"nil type", nil, true, "message type cannot be nil"},
			{"string type", "not a struct", true, "message type must be a struct"},
			{"int type", 42, true, "message type must be a struct"},
			{"slice type", []string{}, true, "message type must be a struct"},
			{"map type", map[string]int{}, true, "message type must be a struct"},
			{"valid struct", testCommand{}, false, ""},
			{"valid struct pointer", &testCommand{}, false, ""},
		}
		
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := registry.Register("TestType", tc.msgType)
				if tc.shouldError {
					assert.Error(t, err)
					assert.Contains(t, err.Error(), tc.errorMsg)
				} else {
					assert.NoError(t, err)
				}
			})
		}
	})
	
	t.Run("handles duplicate registrations", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		// First registration
		err := registry.Register("TestCommand", &testCommand{})
		assert.NoError(t, err)
		
		// Same type, same name - should succeed (idempotent)
		err = registry.Register("TestCommand", &testCommand{})
		assert.NoError(t, err)
		
		// Different type, same name - should fail
		err = registry.Register("TestCommand", &testEvent{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already registered")
		
		// Verify original registration unchanged
		typ, _ := registry.Get("TestCommand")
		assert.Equal(t, reflect.TypeOf(testCommand{}), typ)
	})
	
	t.Run("maintains bidirectional mapping", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		cmd := &testCommand{}
		err := registry.Register("TestCommand", cmd)
		assert.NoError(t, err)
		
		// Forward lookup
		typ, err := registry.Get("TestCommand")
		assert.NoError(t, err)
		assert.Equal(t, reflect.TypeOf(testCommand{}), typ)
		
		// Reverse lookup
		name, err := registry.GetTypeName(cmd)
		assert.NoError(t, err)
		assert.Equal(t, "TestCommand", name)
	})
}

func TestRegisterType(t *testing.T) {
	t.Run("registers with auto-generated name", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		err := registry.RegisterType(&testCommand{})
		assert.NoError(t, err)
		
		// Should include package path
		types := registry.ListTypes()
		assert.Len(t, types, 1)
		assert.Contains(t, types[0], "testCommand")
		assert.Contains(t, types[0], "serialization")
		
		// Should be retrievable
		typ, err := registry.Get(types[0])
		assert.NoError(t, err)
		assert.Equal(t, reflect.TypeOf(testCommand{}), typ)
	})
	
	t.Run("handles types from different packages", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		// Register our test type
		err := registry.RegisterType(&testCommand{})
		assert.NoError(t, err)
		
		// Register a type from contracts package
		err = registry.RegisterType(&contracts.BaseMessage{})
		assert.NoError(t, err)
		
		types := registry.ListTypes()
		assert.Len(t, types, 2)
		
		// Verify both have package paths
		for _, typeName := range types {
			assert.Contains(t, typeName, ".")
		}
	})
	
	t.Run("fails with anonymous types", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		// Anonymous struct
		err := registry.RegisterType(&struct{}{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot determine type name")
	})
	
	t.Run("fails with nil", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		err := registry.RegisterType(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "message type cannot be nil")
	})
}

func TestGet(t *testing.T) {
	t.Run("retrieves registered types", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		// Register multiple types
		registry.Register("Command", &testCommand{})
		registry.Register("Event", &testEvent{})
		registry.Register("Query", &testQuery{})
		
		// Retrieve each
		cmdType, err := registry.Get("Command")
		assert.NoError(t, err)
		assert.Equal(t, reflect.TypeOf(testCommand{}), cmdType)
		
		eventType, err := registry.Get("Event")
		assert.NoError(t, err)
		assert.Equal(t, reflect.TypeOf(testEvent{}), eventType)
		
		queryType, err := registry.Get("Query")
		assert.NoError(t, err)
		assert.Equal(t, reflect.TypeOf(testQuery{}), queryType)
	})
	
	t.Run("returns error for unregistered type", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		typ, err := registry.Get("UnknownType")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "type UnknownType not registered")
		assert.Nil(t, typ)
	})
	
	t.Run("is case sensitive", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		registry.Register("TestCommand", &testCommand{})
		
		// Exact case works
		_, err := registry.Get("TestCommand")
		assert.NoError(t, err)
		
		// Different case fails
		_, err = registry.Get("testcommand")
		assert.Error(t, err)
		
		_, err = registry.Get("TESTCOMMAND")
		assert.Error(t, err)
	})
}

func TestCreateInstance(t *testing.T) {
	t.Run("creates new instances of registered types", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		registry.Register("TestCommand", &testCommand{})
		
		// Create multiple instances
		instance1, err := registry.CreateInstance("TestCommand")
		assert.NoError(t, err)
		
		instance2, err := registry.CreateInstance("TestCommand")
		assert.NoError(t, err)
		
		// Should be different instances
		assert.NotSame(t, instance1, instance2)
		
		// Should be correct type
		cmd1, ok := instance1.(*testCommand)
		assert.True(t, ok)
		assert.NotNil(t, cmd1)
		
		cmd2, ok := instance2.(*testCommand)
		assert.True(t, ok)
		assert.NotNil(t, cmd2)
		
		// Modifying one shouldn't affect the other
		cmd1.OrderID = "order-123"
		assert.NotEqual(t, cmd1.OrderID, cmd2.OrderID)
	})
	
	t.Run("creates instances with zero values", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		registry.Register("TestCommand", &testCommand{})
		
		instance, err := registry.CreateInstance("TestCommand")
		assert.NoError(t, err)
		
		cmd, ok := instance.(*testCommand)
		assert.True(t, ok)
		
		// Check zero values
		assert.Empty(t, cmd.OrderID)
		assert.Empty(t, cmd.CustomerID)
		assert.Zero(t, cmd.Amount)
		assert.Nil(t, cmd.Items)
		assert.Nil(t, cmd.Metadata)
		assert.Nil(t, cmd.ProcessAfter)
	})
	
	t.Run("works with complex nested types", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		registry.Register("NestedCommand", &nestedCommand{})
		
		instance, err := registry.CreateInstance("NestedCommand")
		assert.NoError(t, err)
		
		cmd, ok := instance.(*nestedCommand)
		assert.True(t, ok)
		assert.NotNil(t, cmd)
		
		// Nested structs should be initialized
		assert.Empty(t, cmd.Parent.ID)
		assert.Empty(t, cmd.Parent.Child.Value)
		assert.Nil(t, cmd.Tags)
	})
	
	t.Run("fails for unregistered type", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		instance, err := registry.CreateInstance("UnknownType")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "type UnknownType not registered")
		assert.Nil(t, instance)
	})
}

func TestGetTypeName(t *testing.T) {
	t.Run("retrieves name for registered types", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		registry.Register("Command", &testCommand{})
		registry.Register("Event", &testEvent{})
		
		// Test with instances
		cmd := &testCommand{OrderID: "123"}
		name, err := registry.GetTypeName(cmd)
		assert.NoError(t, err)
		assert.Equal(t, "Command", name)
		
		event := &testEvent{EventType: "test"}
		name, err = registry.GetTypeName(event)
		assert.NoError(t, err)
		assert.Equal(t, "Event", name)
	})
	
	t.Run("works with pointer and non-pointer values", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		registry.Register("TestCommand", &testCommand{})
		
		// With pointer
		cmdPtr := &testCommand{}
		name, err := registry.GetTypeName(cmdPtr)
		assert.NoError(t, err)
		assert.Equal(t, "TestCommand", name)
		
		// With value
		cmdVal := testCommand{}
		name, err = registry.GetTypeName(cmdVal)
		assert.NoError(t, err)
		assert.Equal(t, "TestCommand", name)
	})
	
	t.Run("handles nil gracefully", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		name, err := registry.GetTypeName(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "message cannot be nil")
		assert.Empty(t, name)
	})
	
	t.Run("fails for unregistered types", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		unregistered := &testCommand{}
		name, err := registry.GetTypeName(unregistered)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not registered")
		assert.Empty(t, name)
	})
	
	t.Run("handles interface types", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		registry.Register("TestCommand", &testCommand{})
		
		// Pass as interface
		var msg contracts.Message = &testCommand{}
		name, err := registry.GetTypeName(msg)
		assert.NoError(t, err)
		assert.Equal(t, "TestCommand", name)
	})
}

func TestIsRegistered(t *testing.T) {
	registry := NewTypeRegistry()
	
	// Register some types
	registry.Register("RegisteredType", &testCommand{})
	
	t.Run("returns true for registered types", func(t *testing.T) {
		assert.True(t, registry.IsRegistered("RegisteredType"))
	})
	
	t.Run("returns false for unregistered types", func(t *testing.T) {
		assert.False(t, registry.IsRegistered("UnregisteredType"))
		assert.False(t, registry.IsRegistered(""))
		assert.False(t, registry.IsRegistered("RegisteredType2")) // Close but not exact
	})
	
	t.Run("is case sensitive", func(t *testing.T) {
		assert.False(t, registry.IsRegistered("registeredtype"))
		assert.False(t, registry.IsRegistered("REGISTEREDTYPE"))
	})
}

func TestListTypes(t *testing.T) {
	t.Run("returns empty slice for empty registry", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		types := registry.ListTypes()
		assert.NotNil(t, types)
		assert.Empty(t, types)
	})
	
	t.Run("returns all registered type names", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		// Register multiple types
		registry.Register("Type1", &testCommand{})
		registry.Register("Type2", &testEvent{})
		registry.Register("Type3", &testQuery{})
		registry.Register("Type4", &testReply{})
		
		types := registry.ListTypes()
		assert.Len(t, types, 4)
		
		// Convert to map for easier testing
		typeMap := make(map[string]bool)
		for _, t := range types {
			typeMap[t] = true
		}
		
		assert.True(t, typeMap["Type1"])
		assert.True(t, typeMap["Type2"])
		assert.True(t, typeMap["Type3"])
		assert.True(t, typeMap["Type4"])
	})
	
	t.Run("returns new slice each time", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		registry.Register("Type1", &testCommand{})
		
		types1 := registry.ListTypes()
		types2 := registry.ListTypes()
		
		// Should be different slices
		assert.NotSame(t, types1, types2)
		
		// But same content
		assert.Equal(t, types1, types2)
		
		// Modifying one shouldn't affect the other
		if len(types1) > 0 {
			types1[0] = "Modified"
			assert.NotEqual(t, types1[0], types2[0])
		}
	})
}

func TestConcurrentAccess(t *testing.T) {
	t.Run("handles concurrent registrations safely", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		const goroutines = 100
		errors := make(chan error, goroutines)
		var wg sync.WaitGroup
		
		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				
				typeName := fmt.Sprintf("Type%d", idx)
				err := registry.Register(typeName, &testCommand{})
				if err != nil {
					errors <- err
				}
			}(i)
		}
		
		wg.Wait()
		close(errors)
		
		// Check for errors
		for err := range errors {
			t.Errorf("Concurrent registration error: %v", err)
		}
		
		// Verify all registered
		types := registry.ListTypes()
		assert.Len(t, types, goroutines)
	})
	
	t.Run("handles concurrent reads safely", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		// Pre-register some types
		registry.Register("Command", &testCommand{})
		registry.Register("Event", &testEvent{})
		registry.Register("Query", &testQuery{})
		
		const goroutines = 100
		var wg sync.WaitGroup
		
		// Concurrent reads
		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				
				// Get
				_, err := registry.Get("Command")
				assert.NoError(t, err)
				
				// IsRegistered
				assert.True(t, registry.IsRegistered("Event"))
				
				// ListTypes
				types := registry.ListTypes()
				assert.Len(t, types, 3)
				
				// GetTypeName
				cmd := &testCommand{}
				_, err = registry.GetTypeName(cmd)
				// May or may not error depending on timing
				
				// CreateInstance
				instance, err := registry.CreateInstance("Query")
				assert.NoError(t, err)
				assert.NotNil(t, instance)
			}()
		}
		
		wg.Wait()
	})
	
	t.Run("handles mixed concurrent operations", func(t *testing.T) {
		registry := NewTypeRegistry()
		
		const operations = 1000
		var wg sync.WaitGroup
		
		// Writer goroutines
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < operations/4; i++ {
				registry.Register(fmt.Sprintf("Type%d", i), &testCommand{})
			}
		}()
		
		// Reader goroutines
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < operations/4; j++ {
					registry.ListTypes()
					registry.IsRegistered(fmt.Sprintf("Type%d", j%10))
				}
			}()
		}
		
		wg.Wait()
		
		// Verify registry is still consistent
		types := registry.ListTypes()
		assert.GreaterOrEqual(t, len(types), operations/4)
	})
}

// JSON Serializer Tests

func TestNewJSONSerializer(t *testing.T) {
	t.Run("creates with default configuration", func(t *testing.T) {
		serializer := NewJSONSerializer()
		
		assert.NotNil(t, serializer)
		assert.NotNil(t, serializer.registry)
		assert.True(t, serializer.includeType)
		assert.False(t, serializer.prettyPrint)
		assert.Equal(t, "_type", serializer.typeFieldName)
	})
	
	t.Run("applies all options correctly", func(t *testing.T) {
		customRegistry := NewTypeRegistry()
		
		serializer := NewJSONSerializer(
			WithTypeRegistry(customRegistry),
			WithIncludeType(false),
			WithPrettyPrint(true),
			WithTypeFieldName("$type"),
		)
		
		assert.Same(t, customRegistry, serializer.registry)
		assert.False(t, serializer.includeType)
		assert.True(t, serializer.prettyPrint)
		assert.Equal(t, "$type", serializer.typeFieldName)
	})
}

func TestJSONSerialize(t *testing.T) {
	t.Run("serializes simple message", func(t *testing.T) {
		serializer := NewJSONSerializer(WithIncludeType(false))
		
		msg := &testCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
			OrderID:    "order-123",
			CustomerID: "customer-456",
			Amount:     99.99,
		}
		
		data, err := serializer.Serialize(msg)
		assert.NoError(t, err)
		
		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		assert.NoError(t, err)
		
		assert.Equal(t, "order-123", result["orderId"])
		assert.Equal(t, "customer-456", result["customerId"])
		assert.Equal(t, 99.99, result["amount"])
	})
	
	t.Run("includes type information when enabled", func(t *testing.T) {
		registry := NewTypeRegistry()
		registry.Register("CustomCommand", &testCommand{})
		
		serializer := NewJSONSerializer(
			WithTypeRegistry(registry),
			WithIncludeType(true),
		)
		
		msg := &testCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
		}
		
		data, err := serializer.Serialize(msg)
		assert.NoError(t, err)
		
		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		assert.NoError(t, err)
		
		assert.Equal(t, "CustomCommand", result["_type"])
	})
	
	t.Run("handles complex nested structures", func(t *testing.T) {
		serializer := NewJSONSerializer()
		
		msg := &nestedCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("NestedCommand"),
			},
		}
		msg.Parent.ID = "parent-123"
		msg.Parent.Name = "Parent Name"
		msg.Parent.Child.Value = "child value"
		msg.Parent.Child.Number = 42
		msg.Parent.Child.Time = time.Now()
		msg.Tags = []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}{
			{Key: "env", Value: "production"},
			{Key: "version", Value: "1.0"},
		}
		
		data, err := serializer.Serialize(msg)
		assert.NoError(t, err)
		
		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		assert.NoError(t, err)
		
		parent := result["parent"].(map[string]interface{})
		assert.Equal(t, "parent-123", parent["id"])
		
		child := parent["child"].(map[string]interface{})
		assert.Equal(t, "child value", child["value"])
		assert.Equal(t, float64(42), child["number"])
		
		tags := result["tags"].([]interface{})
		assert.Len(t, tags, 2)
	})
	
	t.Run("handles nil and empty fields", func(t *testing.T) {
		serializer := NewJSONSerializer()
		
		msg := &testCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
			OrderID: "order-123",
			// Other fields left as zero values
		}
		
		data, err := serializer.Serialize(msg)
		assert.NoError(t, err)
		
		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		assert.NoError(t, err)
		
		assert.Equal(t, "order-123", result["orderId"])
		assert.Equal(t, "", result["customerId"])
		assert.Equal(t, float64(0), result["amount"])
		
		// Omitted fields should not be present
		_, hasItems := result["items"]
		assert.False(t, hasItems)
		
		_, hasMetadata := result["metadata"]
		assert.False(t, hasMetadata)
	})
	
	t.Run("respects JSON tags", func(t *testing.T) {
		type taggedMessage struct {
			contracts.BaseMessage
			FieldOne   string `json:"field_one"`
			FieldTwo   string `json:"fieldTwo"`
			IgnoreThis string `json:"-"`
			OmitEmpty  string `json:"omitEmpty,omitempty"`
		}
		
		registry := NewTypeRegistry()
		registry.Register("TaggedMessage", &taggedMessage{})
		
		serializer := NewJSONSerializer(WithTypeRegistry(registry))
		
		msg := &taggedMessage{
			BaseMessage: contracts.NewBaseMessage("TaggedMessage"),
			FieldOne:    "value1",
			FieldTwo:    "value2",
			IgnoreThis:  "should not appear",
			OmitEmpty:   "", // Should be omitted
		}
		
		data, err := serializer.Serialize(msg)
		assert.NoError(t, err)
		
		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		assert.NoError(t, err)
		
		assert.Equal(t, "value1", result["field_one"])
		assert.Equal(t, "value2", result["fieldTwo"])
		assert.NotContains(t, result, "IgnoreThis")
		assert.NotContains(t, result, "ignoreThis")
		assert.NotContains(t, result, "omitEmpty")
	})
	
	t.Run("pretty prints when enabled", func(t *testing.T) {
		serializer := NewJSONSerializer(WithPrettyPrint(true))
		
		msg := &testCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
			OrderID: "123",
			Amount:  50.0,
		}
		
		data, err := serializer.Serialize(msg)
		assert.NoError(t, err)
		
		dataStr := string(data)
		assert.Contains(t, dataStr, "\n")
		assert.Contains(t, dataStr, "  ") // Indentation
		
		// Should still be valid JSON
		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		assert.NoError(t, err)
	})
	
	t.Run("handles custom type field names", func(t *testing.T) {
		registry := NewTypeRegistry()
		registry.Register("MyCommand", &testCommand{})
		
		testCases := []string{"$type", "@type", "__type__", "messageType"}
		
		for _, fieldName := range testCases {
			t.Run(fieldName, func(t *testing.T) {
				serializer := NewJSONSerializer(
					WithTypeRegistry(registry),
					WithTypeFieldName(fieldName),
				)
				
				msg := &testCommand{
					BaseCommand: contracts.BaseCommand{
						BaseMessage: contracts.NewBaseMessage("TestCommand"),
					},
				}
				
				data, err := serializer.Serialize(msg)
				assert.NoError(t, err)
				
				var result map[string]interface{}
				err = json.Unmarshal(data, &result)
				assert.NoError(t, err)
				
				assert.Equal(t, "MyCommand", result[fieldName])
				assert.NotContains(t, result, "_type") // Default should not be present
			})
		}
	})
	
	t.Run("handles custom marshaling", func(t *testing.T) {
		serializer := NewJSONSerializer()
		
		msg := &customMarshalCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("CustomMarshal"),
			},
			Secret: "secret-value",
			Public: "public-value",
		}
		
		data, err := serializer.Serialize(msg)
		assert.NoError(t, err)
		
		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		assert.NoError(t, err)
		
		assert.Equal(t, "public-value", result["public"])
		assert.Equal(t, "custom-value", result["custom"])
		assert.NotContains(t, result, "Secret")
		assert.NotContains(t, result, "secret")
	})
	
	t.Run("fails with nil message", func(t *testing.T) {
		serializer := NewJSONSerializer()
		
		data, err := serializer.Serialize(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "message cannot be nil")
		assert.Nil(t, data)
	})
	
	t.Run("includes type even for unregistered types", func(t *testing.T) {
		serializer := NewJSONSerializer(WithIncludeType(true))
		
		msg := &testCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
		}
		
		data, err := serializer.Serialize(msg)
		assert.NoError(t, err)
		
		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		assert.NoError(t, err)
		
		// Type field should not be present if type is not registered
		_, hasType := result["_type"]
		assert.False(t, hasType)
	})
}

func TestJSONDeserialize(t *testing.T) {
	t.Run("deserializes with type information", func(t *testing.T) {
		registry := NewTypeRegistry()
		registry.Register("TestCommand", &testCommand{})
		
		serializer := NewJSONSerializer(WithTypeRegistry(registry))
		
		data := []byte(`{
			"_type": "TestCommand",
			"id": "msg-123",
			"type": "TestCommand",
			"timestamp": "2024-01-01T00:00:00Z",
			"orderId": "order-456",
			"customerId": "customer-789",
			"amount": 123.45
		}`)
		
		msg, err := serializer.Deserialize(data)
		assert.NoError(t, err)
		assert.NotNil(t, msg)
		
		cmd, ok := msg.(*testCommand)
		assert.True(t, ok)
		assert.Equal(t, "msg-123", cmd.GetID())
		assert.Equal(t, "order-456", cmd.OrderID)
		assert.Equal(t, "customer-789", cmd.CustomerID)
		assert.Equal(t, 123.45, cmd.Amount)
	})
	
	t.Run("deserializes complex nested structures", func(t *testing.T) {
		registry := NewTypeRegistry()
		registry.Register("NestedCommand", &nestedCommand{})
		
		serializer := NewJSONSerializer(WithTypeRegistry(registry))
		
		data := []byte(`{
			"_type": "NestedCommand",
			"parent": {
				"id": "parent-123",
				"name": "Test Parent",
				"child": {
					"value": "child value",
					"number": 42,
					"time": "2024-01-01T12:00:00Z"
				}
			},
			"tags": [
				{"key": "env", "value": "prod"},
				{"key": "version", "value": "1.0"}
			]
		}`)
		
		msg, err := serializer.Deserialize(data)
		assert.NoError(t, err)
		
		cmd, ok := msg.(*nestedCommand)
		assert.True(t, ok)
		assert.Equal(t, "parent-123", cmd.Parent.ID)
		assert.Equal(t, "Test Parent", cmd.Parent.Name)
		assert.Equal(t, "child value", cmd.Parent.Child.Value)
		assert.Equal(t, 42, cmd.Parent.Child.Number)
		assert.Len(t, cmd.Tags, 2)
		assert.Equal(t, "env", cmd.Tags[0].Key)
		assert.Equal(t, "prod", cmd.Tags[0].Value)
	})
	
	t.Run("deserializes without type info as BaseMessage", func(t *testing.T) {
		serializer := NewJSONSerializer()
		
		data := []byte(`{
			"id": "msg-123",
			"type": "UnknownCommand",
			"timestamp": "2024-01-01T00:00:00Z",
			"correlationId": "corr-456"
		}`)
		
		msg, err := serializer.Deserialize(data)
		assert.NoError(t, err)
		assert.NotNil(t, msg)
		
		// Should be BaseMessage
		baseMsg, ok := msg.(*contracts.BaseMessage)
		assert.True(t, ok)
		assert.Equal(t, "msg-123", baseMsg.GetID())
		assert.Equal(t, "UnknownCommand", baseMsg.GetType())
		assert.Equal(t, "corr-456", baseMsg.GetCorrelationID())
	})
	
	t.Run("handles custom type field names", func(t *testing.T) {
		registry := NewTypeRegistry()
		registry.Register("TestCommand", &testCommand{})
		
		serializer := NewJSONSerializer(
			WithTypeRegistry(registry),
			WithTypeFieldName("$type"),
		)
		
		data := []byte(`{
			"$type": "TestCommand",
			"orderId": "order-123"
		}`)
		
		msg, err := serializer.Deserialize(data)
		assert.NoError(t, err)
		
		cmd, ok := msg.(*testCommand)
		assert.True(t, ok)
		assert.Equal(t, "order-123", cmd.OrderID)
	})
	
	t.Run("validates input data", func(t *testing.T) {
		serializer := NewJSONSerializer()
		
		testCases := []struct {
			name     string
			data     []byte
			errorMsg string
		}{
			{
				name:     "empty data",
				data:     []byte{},
				errorMsg: "data cannot be empty",
			},
			{
				name:     "nil data",
				data:     nil,
				errorMsg: "data cannot be empty",
			},
			{
				name:     "invalid JSON",
				data:     []byte("not json"),
				errorMsg: "failed to unmarshal data",
			},
			{
				name:     "JSON array instead of object",
				data:     []byte(`["not", "an", "object"]`),
				errorMsg: "failed to unmarshal data",
			},
		}
		
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				msg, err := serializer.Deserialize(tc.data)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorMsg)
				assert.Nil(t, msg)
			})
		}
	})
	
	t.Run("handles invalid type field", func(t *testing.T) {
		serializer := NewJSONSerializer()
		
		testCases := []struct {
			name     string
			data     string
			errorMsg string
		}{
			{
				name:     "numeric type field",
				data:     `{"_type": 123}`,
				errorMsg: "invalid type field",
			},
			{
				name:     "boolean type field",
				data:     `{"_type": true}`,
				errorMsg: "invalid type field",
			},
			{
				name:     "null type field",
				data:     `{"_type": null}`,
				errorMsg: "invalid type field",
			},
		}
		
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				msg, err := serializer.Deserialize([]byte(tc.data))
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorMsg)
				assert.Nil(t, msg)
			})
		}
	})
	
	t.Run("handles unregistered types gracefully", func(t *testing.T) {
		serializer := NewJSONSerializer()
		
		data := []byte(`{
			"_type": "UnregisteredType",
			"id": "msg-123"
		}`)
		
		msg, err := serializer.Deserialize(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create instance")
		assert.Nil(t, msg)
	})
	
	t.Run("validates deserialized type implements Message", func(t *testing.T) {
		registry := NewTypeRegistry()
		// Register a non-message type
		registry.Register("NonMessage", &nonMessageType{})
		
		serializer := NewJSONSerializer(WithTypeRegistry(registry))
		
		data := []byte(`{
			"_type": "NonMessage",
			"value": "test"
		}`)
		
		msg, err := serializer.Deserialize(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not implement Message interface")
		assert.Nil(t, msg)
	})
}

func TestSerializeEnvelope(t *testing.T) {
	serializer := NewJSONSerializer()
	
	t.Run("serializes envelope with all fields", func(t *testing.T) {
		envelope := &contracts.Envelope{
			ID:            "env-123",
			Type:          "TestMessage",
			Timestamp:     "2024-01-01T12:00:00Z",
			CorrelationID: "corr-456",
			ReplyTo:       "reply.queue",
			Headers: map[string]interface{}{
				"source":  "test-service",
				"version": "1.0",
				"retry":   3,
			},
			Payload: json.RawMessage(`{"data":"test payload"}`),
		}
		
		data, err := serializer.SerializeEnvelope(envelope)
		assert.NoError(t, err)
		
		// Verify it's valid JSON
		var result contracts.Envelope
		err = json.Unmarshal(data, &result)
		assert.NoError(t, err)
		
		assert.Equal(t, envelope.ID, result.ID)
		assert.Equal(t, envelope.Type, result.Type)
		assert.Equal(t, envelope.Timestamp, result.Timestamp)
		assert.Equal(t, envelope.CorrelationID, result.CorrelationID)
		assert.Equal(t, envelope.ReplyTo, result.ReplyTo)
		assert.Equal(t, "test-service", result.Headers["source"])
		assert.Equal(t, "1.0", result.Headers["version"])
		assert.Equal(t, float64(3), result.Headers["retry"]) // JSON numbers are float64
		assert.JSONEq(t, `{"data":"test payload"}`, string(result.Payload))
	})
	
	t.Run("handles empty envelope", func(t *testing.T) {
		envelope := &contracts.Envelope{}
		
		data, err := serializer.SerializeEnvelope(envelope)
		assert.NoError(t, err)
		
		var result contracts.Envelope
		err = json.Unmarshal(data, &result)
		assert.NoError(t, err)
		
		assert.Empty(t, result.ID)
		assert.Empty(t, result.Type)
		// Headers and Payload might be empty map or null depending on JSON handling
		if result.Headers != nil {
			assert.Empty(t, result.Headers)
		}
		// Payload might be null or empty JSON
		if result.Payload != nil {
			assert.True(t, len(result.Payload) == 0 || string(result.Payload) == "null")
		}
	})
	
	t.Run("pretty prints envelope", func(t *testing.T) {
		serializer := NewJSONSerializer(WithPrettyPrint(true))
		
		envelope := &contracts.Envelope{
			ID:   "env-123",
			Type: "TestMessage",
			Headers: map[string]interface{}{
				"test": "value",
			},
		}
		
		data, err := serializer.SerializeEnvelope(envelope)
		assert.NoError(t, err)
		
		dataStr := string(data)
		assert.Contains(t, dataStr, "\n")
		assert.Contains(t, dataStr, "  ")
	})
	
	t.Run("fails with nil envelope", func(t *testing.T) {
		data, err := serializer.SerializeEnvelope(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "envelope cannot be nil")
		assert.Nil(t, data)
	})
}

func TestDeserializeEnvelope(t *testing.T) {
	serializer := NewJSONSerializer()
	
	t.Run("deserializes complete envelope", func(t *testing.T) {
		data := []byte(`{
			"id": "env-123",
			"type": "TestMessage",
			"timestamp": "2024-01-01T12:00:00Z",
			"correlationId": "corr-456",
			"replyTo": "reply.queue",
			"headers": {
				"source": "test-service",
				"version": "1.0",
				"retry": 3
			},
			"payload": {"data": "test payload"}
		}`)
		
		envelope, err := serializer.DeserializeEnvelope(data)
		assert.NoError(t, err)
		assert.NotNil(t, envelope)
		
		assert.Equal(t, "env-123", envelope.ID)
		assert.Equal(t, "TestMessage", envelope.Type)
		assert.Equal(t, "2024-01-01T12:00:00Z", envelope.Timestamp)
		assert.Equal(t, "corr-456", envelope.CorrelationID)
		assert.Equal(t, "reply.queue", envelope.ReplyTo)
		assert.Equal(t, "test-service", envelope.Headers["source"])
		assert.JSONEq(t, `{"data":"test payload"}`, string(envelope.Payload))
	})
	
	t.Run("handles minimal envelope", func(t *testing.T) {
		data := []byte(`{"id": "env-123"}`)
		
		envelope, err := serializer.DeserializeEnvelope(data)
		assert.NoError(t, err)
		assert.NotNil(t, envelope)
		assert.Equal(t, "env-123", envelope.ID)
	})
	
	t.Run("validates input", func(t *testing.T) {
		testCases := []struct {
			name     string
			data     []byte
			errorMsg string
		}{
			{
				name:     "empty data",
				data:     []byte{},
				errorMsg: "data cannot be empty",
			},
			{
				name:     "invalid JSON",
				data:     []byte("not json"),
				errorMsg: "failed to unmarshal envelope",
			},
			{
				name:     "JSON array",
				data:     []byte(`[]`),
				errorMsg: "failed to unmarshal envelope",
			},
		}
		
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				envelope, err := serializer.DeserializeEnvelope(tc.data)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorMsg)
				assert.Nil(t, envelope)
			})
		}
	})
}

func TestRoundTrip(t *testing.T) {
	t.Run("message round trip preserves all data", func(t *testing.T) {
		registry := NewTypeRegistry()
		registry.Register("TestCommand", &testCommand{})
		
		serializer := NewJSONSerializer(WithTypeRegistry(registry))
		
		// Create message with all fields
		original := &testCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
			OrderID:    "order-123",
			CustomerID: "customer-456",
			Amount:     99.99,
			Items:      []string{"item1", "item2", "item3"},
			Metadata: map[string]interface{}{
				"priority": "high",
				"retry":    3,
				"tags":     []string{"urgent", "vip"},
			},
		}
		now := time.Now().UTC().Truncate(time.Second)
		original.ProcessAfter = &now
		original.SetCorrelationID("corr-789")
		
		// Serialize
		data, err := serializer.Serialize(original)
		assert.NoError(t, err)
		
		// Deserialize
		deserialized, err := serializer.Deserialize(data)
		assert.NoError(t, err)
		
		// Verify
		result, ok := deserialized.(*testCommand)
		assert.True(t, ok)
		assert.Equal(t, original.OrderID, result.OrderID)
		assert.Equal(t, original.CustomerID, result.CustomerID)
		assert.Equal(t, original.Amount, result.Amount)
		assert.Equal(t, original.Items, result.Items)
		assert.Equal(t, original.GetID(), result.GetID())
		assert.Equal(t, original.GetCorrelationID(), result.GetCorrelationID())
		
		// Check metadata
		assert.Equal(t, "high", result.Metadata["priority"])
		assert.Equal(t, float64(3), result.Metadata["retry"]) // JSON numbers
		
		// Check time (may lose precision)
		assert.NotNil(t, result.ProcessAfter)
		assert.WithinDuration(t, now, *result.ProcessAfter, time.Second)
	})
	
	t.Run("envelope round trip", func(t *testing.T) {
		serializer := NewJSONSerializer()
		
		original := &contracts.Envelope{
			ID:            "env-123",
			Type:          "TestMessage",
			Timestamp:     time.Now().UTC().Format(time.RFC3339),
			CorrelationID: "corr-456",
			ReplyTo:       "reply.queue",
			Headers: map[string]interface{}{
				"string":  "value",
				"number":  42,
				"float":   3.14,
				"bool":    true,
				"array":   []string{"a", "b", "c"},
				"object":  map[string]string{"key": "value"},
			},
			Payload: json.RawMessage(`{"complex":{"nested":{"data":"value"}}}`),
		}
		
		// Serialize
		data, err := serializer.SerializeEnvelope(original)
		assert.NoError(t, err)
		
		// Deserialize
		deserialized, err := serializer.DeserializeEnvelope(data)
		assert.NoError(t, err)
		
		// Verify basic fields
		assert.Equal(t, original.ID, deserialized.ID)
		assert.Equal(t, original.Type, deserialized.Type)
		assert.Equal(t, original.Timestamp, deserialized.Timestamp)
		assert.Equal(t, original.CorrelationID, deserialized.CorrelationID)
		assert.Equal(t, original.ReplyTo, deserialized.ReplyTo)
		
		// Verify headers (JSON numbers become float64)
		assert.Equal(t, "value", deserialized.Headers["string"])
		assert.Equal(t, float64(42), deserialized.Headers["number"])
		assert.Equal(t, 3.14, deserialized.Headers["float"])
		assert.Equal(t, true, deserialized.Headers["bool"])
		
		// Verify payload
		assert.JSONEq(t, string(original.Payload), string(deserialized.Payload))
	})
}

func TestComplexScenarios(t *testing.T) {
	t.Run("handles unicode and special characters", func(t *testing.T) {
		registry := NewTypeRegistry()
		registry.Register("TestCommand", &testCommand{})
		
		serializer := NewJSONSerializer(WithTypeRegistry(registry))
		
		msg := &testCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
			OrderID:    "order-123",
			CustomerID: "customer-世界-🌍",
			Metadata: map[string]interface{}{
				"unicode":  "Hello 世界",
				"emoji":    "🚀🌟✨",
				"special":  `Special "chars" \n \t \r`,
				"symbols":  "< > & ' \" / \\",
			},
		}
		
		// Serialize
		data, err := serializer.Serialize(msg)
		assert.NoError(t, err)
		
		// Deserialize
		deserialized, err := serializer.Deserialize(data)
		assert.NoError(t, err)
		
		result, ok := deserialized.(*testCommand)
		assert.True(t, ok)
		assert.Equal(t, msg.CustomerID, result.CustomerID)
		assert.Equal(t, "Hello 世界", result.Metadata["unicode"])
		assert.Equal(t, "🚀🌟✨", result.Metadata["emoji"])
		assert.Equal(t, `Special "chars" \n \t \r`, result.Metadata["special"])
	})
	
	t.Run("handles large payloads", func(t *testing.T) {
		registry := NewTypeRegistry()
		registry.Register("TestEvent", &testEvent{})
		
		serializer := NewJSONSerializer(WithTypeRegistry(registry))
		
		// Create large payload
		largeData := make(map[string]interface{})
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("field_%d", i)
			largeData[key] = strings.Repeat("x", 100)
		}
		
		msg := &testEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
			},
			EventType: "LargeEvent",
			Payload:   largeData,
		}
		
		// Serialize
		data, err := serializer.Serialize(msg)
		assert.NoError(t, err)
		assert.Greater(t, len(data), 100000) // Should be > 100KB
		
		// Deserialize
		deserialized, err := serializer.Deserialize(data)
		assert.NoError(t, err)
		
		result, ok := deserialized.(*testEvent)
		assert.True(t, ok)
		assert.Len(t, result.Payload, 1000)
	})
	
	t.Run("preserves type safety across serialization", func(t *testing.T) {
		registry := NewTypeRegistry()
		registry.Register("TestReply", &testReply{})
		
		serializer := NewJSONSerializer(WithTypeRegistry(registry))
		
		original := &testReply{
			BaseReply: contracts.BaseReply{
				BaseMessage: contracts.NewBaseMessage("TestReply"),
				Success:     true,
			},
			ResultCount: 3,
			Results: []interface{}{
				"string result",
				42,
				map[string]interface{}{"nested": "object"},
			},
			Duration: 1500 * time.Millisecond,
		}
		
		// Round trip
		data, _ := serializer.Serialize(original)
		deserialized, _ := serializer.Deserialize(data)
		
		result, ok := deserialized.(*testReply)
		assert.True(t, ok)
		
		// Verify type-specific fields
		assert.True(t, result.IsSuccess())
		assert.Equal(t, 3, result.ResultCount)
		assert.Len(t, result.Results, 3)
		
		// JSON unmarshaling converts numbers to float64
		assert.Equal(t, "string result", result.Results[0])
		assert.Equal(t, float64(42), result.Results[1])
		
		nestedObj, ok := result.Results[2].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, "object", nestedObj["nested"])
	})
}

// Benchmarks

func BenchmarkTypeRegistry(b *testing.B) {
	registry := NewTypeRegistry()
	
	// Pre-register some types
	registry.Register("Command1", &benchmarkCommand{})
	registry.Register("Command2", &benchmarkCommand{})
	registry.Register("Command3", &benchmarkCommand{})
	
	b.Run("Register", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			typeName := fmt.Sprintf("Type%d", i)
			registry.Register(typeName, &benchmarkCommand{})
		}
	})
	
	b.Run("Get", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			registry.Get("Command1")
		}
	})
	
	b.Run("CreateInstance", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			registry.CreateInstance("Command1")
		}
	})
	
	b.Run("IsRegistered", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			registry.IsRegistered("Command1")
		}
	})
	
	b.Run("GetTypeName", func(b *testing.B) {
		cmd := &benchmarkCommand{}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			registry.GetTypeName(cmd)
		}
	})
}

func BenchmarkJSONSerializer(b *testing.B) {
	registry := NewTypeRegistry()
	registry.Register("BenchmarkCommand", &benchmarkCommand{})
	
	serializer := NewJSONSerializer(WithTypeRegistry(registry))
	
	msg := &benchmarkCommand{
		BaseCommand: contracts.BaseCommand{
			BaseMessage: contracts.NewBaseMessage("BenchmarkCommand"),
		},
		Field1: "value1",
		Field2: "value2",
		Field3: 42,
		Field4: true,
		Field5: 3.14159,
	}
	
	b.Run("Serialize", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := serializer.Serialize(msg)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	
	b.Run("Deserialize", func(b *testing.B) {
		data, _ := serializer.Serialize(msg)
		b.ResetTimer()
		
		for i := 0; i < b.N; i++ {
			_, err := serializer.Deserialize(data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	
	b.Run("RoundTrip", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			data, err := serializer.Serialize(msg)
			if err != nil {
				b.Fatal(err)
			}
			
			_, err = serializer.Deserialize(data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}