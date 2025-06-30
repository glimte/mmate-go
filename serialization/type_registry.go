package serialization

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"github.com/glimte/mmate-go/contracts"
)

// TypeRegistry manages message type registrations for serialization
type TypeRegistry interface {
	// Register registers a message type with a type name
	Register(typeName string, msgType interface{}) error

	// RegisterType registers a message type using its struct name
	RegisterType(msgType interface{}) error

	// Get retrieves the type for a given type name
	Get(typeName string) (reflect.Type, error)

	// CreateInstance creates a new instance of the registered type
	CreateInstance(typeName string) (interface{}, error)

	// GetTypeName gets the registered type name for a value
	GetTypeName(msg interface{}) (string, error)

	// IsRegistered checks if a type is registered
	IsRegistered(typeName string) bool

	// ListTypes returns all registered type names
	ListTypes() []string

	// GetFactory returns a factory function for the given type name
	GetFactory(typeName string) (func() contracts.Message, error)
}

// DefaultTypeRegistry is the default implementation of TypeRegistry
type DefaultTypeRegistry struct {
	types map[string]reflect.Type
	names map[reflect.Type]string
	mu    sync.RWMutex
}

// NewTypeRegistry creates a new type registry
func NewTypeRegistry() *DefaultTypeRegistry {
	return &DefaultTypeRegistry{
		types: make(map[string]reflect.Type),
		names: make(map[reflect.Type]string),
	}
}

// Register registers a message type with a type name
func (r *DefaultTypeRegistry) Register(typeName string, msgType interface{}) error {
	if typeName == "" {
		return fmt.Errorf("type name cannot be empty")
	}

	if msgType == nil {
		return fmt.Errorf("message type cannot be nil")
	}

	// Get the underlying type
	t := reflect.TypeOf(msgType)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// Ensure it's a struct
	if t.Kind() != reflect.Struct {
		return fmt.Errorf("message type must be a struct, got %v", t.Kind())
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check for duplicate registration
	if existing, exists := r.types[typeName]; exists {
		if existing == t {
			// Same type, ignore
			return nil
		}
		return fmt.Errorf("type name %s already registered to %v", typeName, existing)
	}

	r.types[typeName] = t
	r.names[t] = typeName

	return nil
}

// RegisterType registers a message type using its struct name
func (r *DefaultTypeRegistry) RegisterType(msgType interface{}) error {
	if msgType == nil {
		return fmt.Errorf("message type cannot be nil")
	}

	t := reflect.TypeOf(msgType)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	typeName := t.Name()
	if typeName == "" {
		return fmt.Errorf("cannot determine type name for %v", t)
	}

	// Include package path for uniqueness
	if t.PkgPath() != "" {
		typeName = t.PkgPath() + "." + typeName
	}

	return r.Register(typeName, msgType)
}

// Get retrieves the type for a given type name
func (r *DefaultTypeRegistry) Get(typeName string) (reflect.Type, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	t, exists := r.types[typeName]
	if !exists {
		return nil, fmt.Errorf("type %s not registered", typeName)
	}

	return t, nil
}

// CreateInstance creates a new instance of the registered type
func (r *DefaultTypeRegistry) CreateInstance(typeName string) (interface{}, error) {
	t, err := r.Get(typeName)
	if err != nil {
		return nil, err
	}

	// Create a new instance as a pointer
	return reflect.New(t).Interface(), nil
}

// GetTypeName gets the registered type name for a value
func (r *DefaultTypeRegistry) GetTypeName(msg interface{}) (string, error) {
	if msg == nil {
		return "", fmt.Errorf("message cannot be nil")
	}

	t := reflect.TypeOf(msg)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	name, exists := r.names[t]
	if !exists {
		return "", fmt.Errorf("type %v not registered", t)
	}

	return name, nil
}

// IsRegistered checks if a type is registered
func (r *DefaultTypeRegistry) IsRegistered(typeName string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, exists := r.types[typeName]
	return exists
}

// ListTypes returns all registered type names
func (r *DefaultTypeRegistry) ListTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]string, 0, len(r.types))
	for typeName := range r.types {
		types = append(types, typeName)
	}

	return types
}

// MessageSerializer handles message serialization with type information
type MessageSerializer interface {
	// Serialize serializes a message to bytes
	Serialize(msg contracts.Message) ([]byte, error)

	// Deserialize deserializes bytes to a message
	Deserialize(data []byte) (contracts.Message, error)

	// SerializeEnvelope serializes an envelope
	SerializeEnvelope(env *contracts.Envelope) ([]byte, error)

	// DeserializeEnvelope deserializes an envelope
	DeserializeEnvelope(data []byte) (*contracts.Envelope, error)
}

// JSONSerializer implements MessageSerializer using JSON
type JSONSerializer struct {
	registry      TypeRegistry
	includeType   bool
	prettyPrint   bool
	typeFieldName string
}

// JSONSerializerOption configures the JSON serializer
type JSONSerializerOption func(*JSONSerializer)

// WithTypeRegistry sets the type registry
func WithTypeRegistry(registry TypeRegistry) JSONSerializerOption {
	return func(s *JSONSerializer) {
		s.registry = registry
	}
}

// WithIncludeType includes type information in serialized data
func WithIncludeType(include bool) JSONSerializerOption {
	return func(s *JSONSerializer) {
		s.includeType = include
	}
}

// WithPrettyPrint enables pretty printing
func WithPrettyPrint(pretty bool) JSONSerializerOption {
	return func(s *JSONSerializer) {
		s.prettyPrint = pretty
	}
}

// WithTypeFieldName sets the field name for type information
func WithTypeFieldName(name string) JSONSerializerOption {
	return func(s *JSONSerializer) {
		s.typeFieldName = name
	}
}

// NewJSONSerializer creates a new JSON serializer
func NewJSONSerializer(opts ...JSONSerializerOption) *JSONSerializer {
	s := &JSONSerializer{
		registry:      NewTypeRegistry(),
		includeType:   true,
		prettyPrint:   false,
		typeFieldName: "_type",
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Serialize serializes a message to bytes
func (s *JSONSerializer) Serialize(msg contracts.Message) ([]byte, error) {
	if msg == nil {
		return nil, fmt.Errorf("message cannot be nil")
	}

	data := make(map[string]interface{})

	// Convert message to map
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}

	if err := json.Unmarshal(msgBytes, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal to map: %w", err)
	}

	// Add type information if needed
	if s.includeType {
		typeName, err := s.registry.GetTypeName(msg)
		if err == nil {
			data[s.typeFieldName] = typeName
		}
	}

	// Serialize
	if s.prettyPrint {
		return json.MarshalIndent(data, "", "  ")
	}
	return json.Marshal(data)
}

// Deserialize deserializes bytes to a message
func (s *JSONSerializer) Deserialize(data []byte) (contracts.Message, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("data cannot be empty")
	}

	// First, unmarshal to map to get type information
	var rawData map[string]interface{}
	if err := json.Unmarshal(data, &rawData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal data: %w", err)
	}

	// Check for type information
	if typeNameRaw, exists := rawData[s.typeFieldName]; exists {
		typeName, ok := typeNameRaw.(string)
		if !ok {
			return nil, fmt.Errorf("invalid type field: expected string, got %T", typeNameRaw)
		}

		// Create instance of the type
		instance, err := s.registry.CreateInstance(typeName)
		if err != nil {
			return nil, fmt.Errorf("failed to create instance: %w", err)
		}

		// Unmarshal into the instance
		if err := json.Unmarshal(data, instance); err != nil {
			return nil, fmt.Errorf("failed to unmarshal into type %s: %w", typeName, err)
		}

		// Ensure it implements Message interface
		msg, ok := instance.(contracts.Message)
		if !ok {
			return nil, fmt.Errorf("type %s does not implement Message interface", typeName)
		}

		return msg, nil
	}

	// No type information, try to deserialize as generic message
	msg := &contracts.BaseMessage{}
	if err := json.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal as BaseMessage: %w", err)
	}

	return msg, nil
}

// SerializeEnvelope serializes an envelope
func (s *JSONSerializer) SerializeEnvelope(env *contracts.Envelope) ([]byte, error) {
	if env == nil {
		return nil, fmt.Errorf("envelope cannot be nil")
	}

	if s.prettyPrint {
		return json.MarshalIndent(env, "", "  ")
	}
	return json.Marshal(env)
}

// DeserializeEnvelope deserializes an envelope
func (s *JSONSerializer) DeserializeEnvelope(data []byte) (*contracts.Envelope, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("data cannot be empty")
	}

	var env contracts.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("failed to unmarshal envelope: %w", err)
	}

	return &env, nil
}

// GetFactory returns a factory function for the given type name
func (r *DefaultTypeRegistry) GetFactory(typeName string) (func() contracts.Message, error) {
	t, err := r.Get(typeName)
	if err != nil {
		return nil, err
	}

	return func() contracts.Message {
		instance := reflect.New(t).Interface()
		msg, _ := instance.(contracts.Message)
		return msg
	}, nil
}

// Global registry instance
var globalRegistry = NewTypeRegistry()

// GetGlobalRegistry returns the global type registry
func GetGlobalRegistry() TypeRegistry {
	return globalRegistry
}
