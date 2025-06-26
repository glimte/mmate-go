// Package messaging provides the core messaging capabilities for the mmate framework.
//
// This package implements the primary messaging patterns:
//   - MessageDispatcher: Routes messages to registered handlers with middleware support
//   - MessagePublisher: High-level message publishing with reliability patterns
//   - MessageSubscriber: Message consumption with error handling and acknowledgment strategies
//   - Handler interfaces: Type-safe handlers for Commands, Events, Queries, and Replies
//   - HandlerRegistry: Centralized handler registration and management
//   - Saga support: Long-running business process coordination
//
// Key features:
//   - Type-safe message handling with specialized handler interfaces
//   - Middleware chain for cross-cutting concerns
//   - Built-in reliability patterns (circuit breaker, retry policies)
//   - Flexible acknowledgment strategies for message consumption
//   - Support for request-reply patterns with automatic correlation
//   - Saga pattern implementation for complex workflows
//   - Thread-safe implementations suitable for concurrent use
//
// Example usage:
//
//	// Create dispatcher and register handlers
//	dispatcher := messaging.NewMessageDispatcher()
//	registry := messaging.NewHandlerRegistry(dispatcher, publisher)
//	
//	// Register a command handler
//	err := registry.RegisterCommandHandlerFunc(&CreateUserCommand{}, 
//		func(ctx context.Context, cmd contracts.Command) error {
//			// Handle the command
//			return nil
//		})
//	
//	// Create publisher and publish a command
//	publisher := messaging.NewMessagePublisher(rabbitPublisher)
//	err = publisher.PublishCommand(ctx, &CreateUserCommand{
//		BaseCommand: contracts.BaseCommand{
//			BaseMessage: contracts.NewBaseMessage("CreateUserCommand"),
//			TargetService: "user-service",
//		},
//		Username: "john.doe",
//	})
//	
//	// Create subscriber and subscribe to events
//	subscriber := messaging.NewMessageSubscriber(consumer, dispatcher)
//	err = subscriber.Subscribe(ctx, "user.events", "UserCreated", eventHandler)
//
// The messaging package integrates with the interceptors package to provide
// cross-cutting concerns like logging, metrics, tracing, and validation.
package messaging