package messaging

import (
	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/serialization"
)

// Register registers a message type with the global type registry
func Register(typeName string, factory func() contracts.Message) error {
	msg := factory()
	return serialization.GetGlobalRegistry().Register(typeName, msg)
}

// RegisterType registers a message type using its struct name
func RegisterType(msgFactory func() contracts.Message) error {
	msg := msgFactory()
	return serialization.GetGlobalRegistry().RegisterType(msg)
}

// GetTypeRegistry returns the global type registry
func GetTypeRegistry() serialization.TypeRegistry {
	return serialization.GetGlobalRegistry()
}