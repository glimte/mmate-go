package messaging

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"sync"

	"github.com/glimte/mmate-go/contracts"
)

// MessageHandler processes a specific message type
type MessageHandler interface {
	Handle(ctx context.Context, msg contracts.Message) error
}

// MessageHandlerFunc is a function adapter for MessageHandler
type MessageHandlerFunc func(ctx context.Context, msg contracts.Message) error

// Handle implements MessageHandler
func (f MessageHandlerFunc) Handle(ctx context.Context, msg contracts.Message) error {
	return f(ctx, msg)
}

// HandlerRegistration represents a registered handler
type HandlerRegistration struct {
	Handler     MessageHandler
	MessageType reflect.Type
	Options     HandlerOptions
}

// HandlerOptions configures handler behavior
type HandlerOptions struct {
	Concurrency int    // Max concurrent executions (0 = unlimited)
	Queue       string // Queue name for this handler
	Durable     bool   // Should the handler survive server restarts
	Exclusive   bool   // Is this an exclusive handler
}

// MessageDispatcher routes messages to appropriate handlers
type MessageDispatcher struct {
	handlers   map[string][]HandlerRegistration
	mu         sync.RWMutex
	logger     *slog.Logger
	middleware []MiddlewareFunc
}

// MiddlewareFunc processes messages before they reach handlers
type MiddlewareFunc func(ctx context.Context, msg contracts.Message, next MessageHandler) error

// DispatcherOption configures the MessageDispatcher
type DispatcherOption func(*MessageDispatcher)

// WithDispatcherLogger sets the logger
func WithDispatcherLogger(logger *slog.Logger) DispatcherOption {
	return func(d *MessageDispatcher) {
		d.logger = logger
	}
}

// WithMiddleware adds middleware to the dispatcher
func WithMiddleware(middleware ...MiddlewareFunc) DispatcherOption {
	return func(d *MessageDispatcher) {
		d.middleware = append(d.middleware, middleware...)
	}
}

// NewMessageDispatcher creates a new message dispatcher
func NewMessageDispatcher(options ...DispatcherOption) *MessageDispatcher {
	d := &MessageDispatcher{
		handlers: make(map[string][]HandlerRegistration),
		logger:   slog.Default(),
	}

	for _, opt := range options {
		opt(d)
	}

	return d
}

// RegisterHandler registers a handler for a specific message type
func (d *MessageDispatcher) RegisterHandler(messageType contracts.Message, handler MessageHandler, options ...HandlerOption) error {
	if messageType == nil {
		return fmt.Errorf("messageType cannot be nil")
	}
	if handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}

	msgType := reflect.TypeOf(messageType)
	if msgType.Kind() == reflect.Ptr {
		msgType = msgType.Elem()
	}

	typeName := msgType.Name()
	if typeName == "" {
		return fmt.Errorf("message type must have a name")
	}

	// Apply options
	opts := HandlerOptions{
		Concurrency: 1, // Default to single concurrent execution
		Queue:       fmt.Sprintf("handler.%s", typeName),
		Durable:     true,
		Exclusive:   false,
	}

	for _, opt := range options {
		opt(&opts)
	}

	registration := HandlerRegistration{
		Handler:     handler,
		MessageType: msgType,
		Options:     opts,
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	d.handlers[typeName] = append(d.handlers[typeName], registration)

	d.logger.Info("registered message handler",
		"messageType", typeName,
		"queue", opts.Queue,
		"concurrency", opts.Concurrency,
	)

	return nil
}

// RegisterHandlerFunc registers a function as a handler
func (d *MessageDispatcher) RegisterHandlerFunc(messageType contracts.Message, handler MessageHandlerFunc, options ...HandlerOption) error {
	return d.RegisterHandler(messageType, handler, options...)
}

// UnregisterHandler removes a handler for a message type
func (d *MessageDispatcher) UnregisterHandler(messageType contracts.Message, handler MessageHandler) error {
	msgType := reflect.TypeOf(messageType)
	if msgType.Kind() == reflect.Ptr {
		msgType = msgType.Elem()
	}

	typeName := msgType.Name()

	d.mu.Lock()
	defer d.mu.Unlock()

	handlers, exists := d.handlers[typeName]
	if !exists {
		return fmt.Errorf("no handlers registered for message type: %s", typeName)
	}

	// Find and remove the specific handler
	for i, reg := range handlers {
		if reg.Handler == handler {
			d.handlers[typeName] = append(handlers[:i], handlers[i+1:]...)

			// Remove the type entirely if no more handlers
			if len(d.handlers[typeName]) == 0 {
				delete(d.handlers, typeName)
			}

			d.logger.Info("unregistered message handler", "messageType", typeName)
			return nil
		}
	}

	return fmt.Errorf("handler not found for message type: %s", typeName)
}

// Handle implements the MessageHandler interface by dispatching to registered handlers
func (d *MessageDispatcher) Handle(ctx context.Context, msg contracts.Message) error {
	return d.Dispatch(ctx, msg)
}

// Dispatch sends a message to all registered handlers
func (d *MessageDispatcher) Dispatch(ctx context.Context, msg contracts.Message) error {
	if msg == nil {
		return fmt.Errorf("message cannot be nil")
	}

	msgType := reflect.TypeOf(msg)
	if msgType.Kind() == reflect.Ptr {
		msgType = msgType.Elem()
	}

	typeName := msgType.Name()

	d.mu.RLock()
	handlers, exists := d.handlers[typeName]
	d.mu.RUnlock()

	if !exists {
		d.logger.Warn("no handlers registered for message type", "messageType", typeName)
		return fmt.Errorf("no handlers registered for message type: %s", typeName)
	}

	// Create a copy of handlers to avoid holding the lock
	handlersCopy := make([]HandlerRegistration, len(handlers))
	copy(handlersCopy, handlers)

	// Dispatch to all handlers
	var wg sync.WaitGroup
	errChan := make(chan error, len(handlersCopy))

	for _, registration := range handlersCopy {
		wg.Add(1)
		go func(reg HandlerRegistration) {
			defer wg.Done()

			// Apply middleware chain
			handler := d.buildMiddlewareChain(reg.Handler)

			if err := handler.Handle(ctx, msg); err != nil {
				d.logger.Error("handler failed",
					"messageType", typeName,
					"messageId", msg.GetID(),
					"error", err,
				)
				errChan <- fmt.Errorf("handler failed for message %s: %w", msg.GetID(), err)
			}
		}(registration)
	}

	wg.Wait()
	close(errChan)

	// Collect any errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return fmt.Errorf("dispatch failed with %d errors: %v", len(errors), errors)
	}

	d.logger.Debug("message dispatched successfully",
		"messageType", typeName,
		"messageId", msg.GetID(),
		"handlerCount", len(handlersCopy),
	)

	return nil
}

// GetHandlers returns all registered handlers for a message type
func (d *MessageDispatcher) GetHandlers(messageType contracts.Message) []HandlerRegistration {
	msgType := reflect.TypeOf(messageType)
	if msgType.Kind() == reflect.Ptr {
		msgType = msgType.Elem()
	}

	typeName := msgType.Name()

	d.mu.RLock()
	defer d.mu.RUnlock()

	handlers, exists := d.handlers[typeName]
	if !exists {
		return nil
	}

	// Return a copy to prevent external modification
	result := make([]HandlerRegistration, len(handlers))
	copy(result, handlers)
	return result
}

// GetRegisteredTypes returns all message types that have handlers
func (d *MessageDispatcher) GetRegisteredTypes() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	types := make([]string, 0, len(d.handlers))
	for typeName := range d.handlers {
		types = append(types, typeName)
	}
	return types
}

// buildMiddlewareChain builds the middleware execution chain
func (d *MessageDispatcher) buildMiddlewareChain(handler MessageHandler) MessageHandler {
	if len(d.middleware) == 0 {
		return handler
	}

	// Build chain in reverse order
	result := handler
	for i := len(d.middleware) - 1; i >= 0; i-- {
		middleware := d.middleware[i]
		next := result
		result = MessageHandlerFunc(func(ctx context.Context, msg contracts.Message) error {
			return middleware(ctx, msg, next)
		})
	}

	return result
}

// HandlerOption configures handler registration
type HandlerOption func(*HandlerOptions)

// WithConcurrency sets the maximum concurrent executions for a handler
func WithConcurrency(concurrency int) HandlerOption {
	return func(opts *HandlerOptions) {
		opts.Concurrency = concurrency
	}
}

// WithQueue sets the queue name for a handler
func WithQueue(queue string) HandlerOption {
	return func(opts *HandlerOptions) {
		opts.Queue = queue
	}
}

// WithDurable sets whether the handler should survive server restarts
func WithDurable(durable bool) HandlerOption {
	return func(opts *HandlerOptions) {
		opts.Durable = durable
	}
}

// WithExclusive sets whether this is an exclusive handler
func WithExclusive(exclusive bool) HandlerOption {
	return func(opts *HandlerOptions) {
		opts.Exclusive = exclusive
	}
}
