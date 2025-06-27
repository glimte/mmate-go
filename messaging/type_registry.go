package messaging

import (
	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/serialization"
)

// Global type registry for message serialization
var globalRegistry = serialization.NewTypeRegistry()

// Register registers a message type with the global type registry
func Register(typeName string, factory func() contracts.Message) error {
	msg := factory()
	return globalRegistry.Register(typeName, msg)
}

// RegisterType registers a message type using its struct name
func RegisterType(msgFactory func() contracts.Message) error {
	msg := msgFactory()
	return globalRegistry.RegisterType(msg)
}

// GetTypeRegistry returns the global type registry
func GetTypeRegistry() serialization.TypeRegistry {
	return globalRegistry
}