package messaging

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"time"

	"github.com/glimte/mmate-go/contracts"
)

// KeystoneHandler is a foundational handler pattern that provides
// common functionality for message processing including:
// - Type-safe message handling
// - Automatic error recovery
// - Metrics collection
// - Logging
// - Retry logic
type KeystoneHandler struct {
	handlerName    string
	messageType    reflect.Type
	processFunc    interface{}
	logger         *slog.Logger
	metrics        MetricsCollector
	errorHandler   ErrorHandler
	retryPolicy    RetryPolicy
	preProcessors  []MessageProcessor
	postProcessors []MessageProcessor
	mu             sync.RWMutex
}

// KeystoneOption configures a KeystoneHandler
type KeystoneOption func(*KeystoneHandler)

// WithKeystoneLogger sets the logger
func WithKeystoneLogger(logger *slog.Logger) KeystoneOption {
	return func(h *KeystoneHandler) {
		h.logger = logger
	}
}

// WithKeystoneMetrics sets the metrics collector
func WithKeystoneMetrics(metrics MetricsCollector) KeystoneOption {
	return func(h *KeystoneHandler) {
		h.metrics = metrics
	}
}

// WithKeystoneErrorHandler sets the error handler
func WithKeystoneErrorHandler(handler ErrorHandler) KeystoneOption {
	return func(h *KeystoneHandler) {
		h.errorHandler = handler
	}
}

// WithKeystoneRetryPolicy sets the retry policy
func WithKeystoneRetryPolicy(policy RetryPolicy) KeystoneOption {
	return func(h *KeystoneHandler) {
		h.retryPolicy = policy
	}
}

// WithPreProcessor adds a pre-processor
func WithPreProcessor(processor MessageProcessor) KeystoneOption {
	return func(h *KeystoneHandler) {
		h.preProcessors = append(h.preProcessors, processor)
	}
}

// WithPostProcessor adds a post-processor
func WithPostProcessor(processor MessageProcessor) KeystoneOption {
	return func(h *KeystoneHandler) {
		h.postProcessors = append(h.postProcessors, processor)
	}
}

// NewKeystoneHandler creates a new keystone handler for a specific message type
// The processFunc should have signature: func(ctx context.Context, msg *ConcreteType) error
func NewKeystoneHandler(handlerName string, messageType interface{}, processFunc interface{}, opts ...KeystoneOption) (*KeystoneHandler, error) {
	// Validate processFunc signature
	funcType := reflect.TypeOf(processFunc)
	if funcType.Kind() != reflect.Func {
		return nil, fmt.Errorf("processFunc must be a function")
	}

	if funcType.NumIn() != 2 {
		return nil, fmt.Errorf("processFunc must accept exactly 2 parameters")
	}

	if funcType.NumOut() != 1 {
		return nil, fmt.Errorf("processFunc must return exactly 1 value")
	}

	// Validate context parameter
	if !funcType.In(0).Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
		return nil, fmt.Errorf("first parameter must be context.Context")
	}

	// Validate error return
	if !funcType.Out(0).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		return nil, fmt.Errorf("return value must be error")
	}

	msgType := reflect.TypeOf(messageType)
	if msgType.Kind() == reflect.Ptr {
		msgType = msgType.Elem()
	}

	handler := &KeystoneHandler{
		handlerName:    handlerName,
		messageType:    msgType,
		processFunc:    processFunc,
		logger:         slog.Default(),
		preProcessors:  make([]MessageProcessor, 0),
		postProcessors: make([]MessageProcessor, 0),
	}

	for _, opt := range opts {
		opt(handler)
	}

	return handler, nil
}

// Handle processes a message using the keystone pattern
func (h *KeystoneHandler) Handle(ctx context.Context, msg contracts.Message) error {
	startTime := time.Now()

	// Log start
	h.logger.Info("keystone handler processing message",
		"handler", h.handlerName,
		"messageId", msg.GetID(),
		"messageType", msg.GetType())

	// Record metrics
	if h.metrics != nil {
		defer func() {
			h.metrics.RecordMessage(msg.GetType(), time.Since(startTime), true, "")
		}()
	}

	// Run pre-processors
	for _, processor := range h.preProcessors {
		processedMsg, err := processor.Process(ctx, msg)
		if err != nil {
			return h.handleError(ctx, msg, fmt.Errorf("pre-processor failed: %w", err))
		}
		msg = processedMsg
	}

	// Type assertion to concrete type
	msgValue := reflect.New(h.messageType)
	msgInterface := msgValue.Interface()

	// Try to cast the message to the expected type
	if typedMsg, ok := msg.(contracts.Message); ok {
		// Use reflection to copy fields
		if err := h.copyMessage(typedMsg, msgInterface); err != nil {
			return h.handleError(ctx, msg, fmt.Errorf("failed to convert message: %w", err))
		}
	} else {
		return h.handleError(ctx, msg, fmt.Errorf("invalid message type: expected %s, got %T", h.messageType, msg))
	}

	// Call the process function with retry logic
	err := h.callProcessFunc(ctx, msgInterface)
	if err != nil {
		if h.retryPolicy != nil {
			err = h.retryProcess(ctx, msgInterface)
		}
		if err != nil {
			return h.handleError(ctx, msg, err)
		}
	}

	// Run post-processors
	for _, processor := range h.postProcessors {
		if _, err := processor.Process(ctx, msg); err != nil {
			h.logger.Warn("post-processor failed",
				"handler", h.handlerName,
				"error", err)
			// Don't fail the message for post-processor errors
		}
	}

	h.logger.Info("keystone handler completed successfully",
		"handler", h.handlerName,
		"messageId", msg.GetID(),
		"duration", time.Since(startTime))

	return nil
}

// callProcessFunc calls the process function using reflection
func (h *KeystoneHandler) callProcessFunc(ctx context.Context, msg interface{}) error {
	funcValue := reflect.ValueOf(h.processFunc)
	results := funcValue.Call([]reflect.Value{
		reflect.ValueOf(ctx),
		reflect.ValueOf(msg),
	})

	if len(results) > 0 && !results[0].IsNil() {
		return results[0].Interface().(error)
	}

	return nil
}

// retryProcess retries the process function according to retry policy
func (h *KeystoneHandler) retryProcess(ctx context.Context, msg interface{}) error {
	attempt := 1
	var lastErr error

	for {
		err := h.callProcessFunc(ctx, msg)
		if err == nil {
			return nil
		}

		lastErr = err

		if !h.retryPolicy.ShouldRetry(attempt, err) {
			break
		}

		delay := h.retryPolicy.NextDelay(attempt)
		h.logger.Warn("retrying message processing",
			"handler", h.handlerName,
			"attempt", attempt,
			"delay", delay,
			"error", err)

		select {
		case <-time.After(delay):
			attempt++
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return lastErr
}

// handleError handles processing errors
func (h *KeystoneHandler) handleError(ctx context.Context, msg contracts.Message, err error) error {
	h.logger.Error("keystone handler error",
		"handler", h.handlerName,
		"messageId", msg.GetID(),
		"error", err)

	if h.metrics != nil {
		h.metrics.RecordError(h.handlerName, "processing_error", err.Error())
	}

	if h.errorHandler != nil {
		action := h.errorHandler.HandleError(ctx, msg, err)
		switch action {
		case Acknowledge:
			return nil // Suppress error
		case Retry:
			return fmt.Errorf("retry requested: %w", err)
		case Reject:
			return fmt.Errorf("rejected: %w", err)
		}
	}

	return err
}

// copyMessage copies message data using reflection
func (h *KeystoneHandler) copyMessage(src contracts.Message, dst interface{}) error {
	// This is a simplified version - in production you'd use a proper mapper
	// For now, we'll assume the destination implements contracts.Message
	if dstMsg, ok := dst.(contracts.Message); ok {
		dstMsg.SetCorrelationID(src.GetCorrelationID())
		return nil
	}
	return fmt.Errorf("destination does not implement contracts.Message")
}

// GetHandlerName returns the handler name
func (h *KeystoneHandler) GetHandlerName() string {
	return h.handlerName
}

// GetMessageType returns the handled message type
func (h *KeystoneHandler) GetMessageType() reflect.Type {
	return h.messageType
}

// KeystoneHandlerBuilder provides a fluent API for building keystone handlers
type KeystoneHandlerBuilder struct {
	handlerName string
	messageType interface{}
	processFunc interface{}
	options     []KeystoneOption
}

// NewKeystoneHandlerBuilder creates a new builder
func NewKeystoneHandlerBuilder(handlerName string) *KeystoneHandlerBuilder {
	return &KeystoneHandlerBuilder{
		handlerName: handlerName,
		options:     make([]KeystoneOption, 0),
	}
}

// ForMessage sets the message type
func (b *KeystoneHandlerBuilder) ForMessage(messageType interface{}) *KeystoneHandlerBuilder {
	b.messageType = messageType
	return b
}

// WithProcessor sets the process function
func (b *KeystoneHandlerBuilder) WithProcessor(processFunc interface{}) *KeystoneHandlerBuilder {
	b.processFunc = processFunc
	return b
}

// WithLogger adds logger option
func (b *KeystoneHandlerBuilder) WithLogger(logger *slog.Logger) *KeystoneHandlerBuilder {
	b.options = append(b.options, WithKeystoneLogger(logger))
	return b
}

// WithMetrics adds metrics option
func (b *KeystoneHandlerBuilder) WithMetrics(metrics MetricsCollector) *KeystoneHandlerBuilder {
	b.options = append(b.options, WithKeystoneMetrics(metrics))
	return b
}

// WithErrorHandler adds error handler option
func (b *KeystoneHandlerBuilder) WithErrorHandler(handler ErrorHandler) *KeystoneHandlerBuilder {
	b.options = append(b.options, WithKeystoneErrorHandler(handler))
	return b
}

// WithRetryPolicy adds retry policy option
func (b *KeystoneHandlerBuilder) WithRetryPolicy(policy RetryPolicy) *KeystoneHandlerBuilder {
	b.options = append(b.options, WithKeystoneRetryPolicy(policy))
	return b
}

// AddPreProcessor adds a pre-processor
func (b *KeystoneHandlerBuilder) AddPreProcessor(processor MessageProcessor) *KeystoneHandlerBuilder {
	b.options = append(b.options, WithPreProcessor(processor))
	return b
}

// AddPostProcessor adds a post-processor
func (b *KeystoneHandlerBuilder) AddPostProcessor(processor MessageProcessor) *KeystoneHandlerBuilder {
	b.options = append(b.options, WithPostProcessor(processor))
	return b
}

// Build creates the keystone handler
func (b *KeystoneHandlerBuilder) Build() (*KeystoneHandler, error) {
	if b.messageType == nil {
		return nil, fmt.Errorf("message type is required")
	}
	if b.processFunc == nil {
		return nil, fmt.Errorf("process function is required")
	}

	return NewKeystoneHandler(b.handlerName, b.messageType, b.processFunc, b.options...)
}
