// Package contracts provides the core message types and interfaces for the mmate messaging framework.
//
// This package defines the base contracts for messages that flow through the system:
//   - Message: Base interface for all messages
//   - Command: Represents an action to be performed
//   - Event: Represents something that has happened
//   - Query: Represents a request for information
//   - Reply: Represents a response to a request
//
// All message types are designed to be serializable and compatible with the .NET implementation
// of mmate for cross-platform messaging.
package contracts