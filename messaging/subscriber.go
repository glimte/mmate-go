package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/interceptors"
	"github.com/glimte/mmate-go/internal/reliability"
)

// MessageSubscriber manages message consumption and routing to handlers
type MessageSubscriber struct {
	transport       TransportSubscriber
	dispatcher      *MessageDispatcher
	logger          *slog.Logger
	subscriptions   map[string]*Subscription
	mu              sync.RWMutex
	errorHandler    ErrorHandler
	deadLetterQueue string
	pipeline        *interceptors.Pipeline
	dlqHandler      *reliability.DLQHandler
}

// Subscription represents an active message subscription
type Subscription struct {
	Queue       string
	MessageType string
	Handler     MessageHandler
	Options     SubscriptionOptions
	cancelFunc  context.CancelFunc
	consumerTag string
}

// SubscriptionOptions configures subscription behavior
type SubscriptionOptions struct {
	PrefetchCount      int
	AutoAck            bool
	Exclusive          bool
	Durable            bool
	AutoDelete         bool
	Arguments          map[string]interface{}
	RetryPolicy        string
	MaxRetries         int
	DeadLetterExchange string
}

// ErrorHandler handles processing errors
type ErrorHandler interface {
	HandleError(ctx context.Context, msg contracts.Message, err error) ErrorAction
}

// ErrorAction determines what to do with failed messages
type ErrorAction int

const (
	Acknowledge ErrorAction = iota // Acknowledge and discard the message
	Retry                          // Nack and requeue for retry
	Reject                         // Reject without requeue (send to DLQ)
)

// DefaultErrorHandler provides default error handling behavior
type DefaultErrorHandler struct {
	MaxRetries int
	Logger     *slog.Logger
}

// HandleError implements ErrorHandler
func (h *DefaultErrorHandler) HandleError(ctx context.Context, msg contracts.Message, err error) ErrorAction {
	if h.Logger != nil {
		h.Logger.Error("message processing failed",
			"messageId", msg.GetID(),
			"messageType", msg.GetType(),
			"error", err,
		)
	}

	// Simple retry logic - you could extract retry count from headers
	return Reject // Send to DLQ by default
}

// SubscriberOption configures the MessageSubscriber
type SubscriberOption func(*MessageSubscriber)

// WithSubscriberLogger sets the logger
func WithSubscriberLogger(logger *slog.Logger) SubscriberOption {
	return func(s *MessageSubscriber) {
		s.logger = logger
	}
}

// WithErrorHandler sets the error handler
func WithErrorHandler(errorHandler ErrorHandler) SubscriberOption {
	return func(s *MessageSubscriber) {
		s.errorHandler = errorHandler
	}
}

// WithDeadLetterQueue sets the dead letter queue name
func WithDeadLetterQueue(dlq string) SubscriberOption {
	return func(s *MessageSubscriber) {
		s.deadLetterQueue = dlq
	}
}

// WithSubscriberInterceptors sets the interceptor pipeline for message consumption
func WithSubscriberInterceptors(pipeline *interceptors.Pipeline) SubscriberOption {
	return func(s *MessageSubscriber) {
		s.pipeline = pipeline
	}
}

// WithDLQHandler sets the DLQ handler for failed message processing
func WithDLQHandler(dlqHandler *reliability.DLQHandler) SubscriberOption {
	return func(s *MessageSubscriber) {
		s.dlqHandler = dlqHandler
	}
}


// NewMessageSubscriber creates a new message subscriber
func NewMessageSubscriber(transport TransportSubscriber, dispatcher *MessageDispatcher, options ...SubscriberOption) *MessageSubscriber {
	s := &MessageSubscriber{
		transport:     transport,
		dispatcher:    dispatcher,
		logger:        slog.Default(),
		subscriptions: make(map[string]*Subscription),
		errorHandler: &DefaultErrorHandler{
			MaxRetries: 3,
			Logger:     slog.Default(),
		},
		deadLetterQueue: "mmate.dlq",
	}

	for _, opt := range options {
		opt(s)
	}

	return s
}

// SubscriptionOption configures subscription behavior
type SubscriptionOption func(*SubscriptionOptions)

// WithPrefetchCount sets the prefetch count
func WithPrefetchCount(count int) SubscriptionOption {
	return func(opts *SubscriptionOptions) {
		opts.PrefetchCount = count
	}
}

// WithAutoAck enables auto-acknowledgment
func WithAutoAck(autoAck bool) SubscriptionOption {
	return func(opts *SubscriptionOptions) {
		opts.AutoAck = autoAck
	}
}

// WithSubscriberExclusive sets exclusive consumption
func WithSubscriberExclusive(exclusive bool) SubscriptionOption {
	return func(opts *SubscriptionOptions) {
		opts.Exclusive = exclusive
	}
}

// WithSubscriberDurable sets queue durability
func WithSubscriberDurable(durable bool) SubscriptionOption {
	return func(opts *SubscriptionOptions) {
		opts.Durable = durable
	}
}

// WithAutoDelete sets auto-delete behavior
func WithAutoDelete(autoDelete bool) SubscriptionOption {
	return func(opts *SubscriptionOptions) {
		opts.AutoDelete = autoDelete
	}
}

// WithMaxRetries sets the maximum number of retries
func WithMaxRetries(maxRetries int) SubscriptionOption {
	return func(opts *SubscriptionOptions) {
		opts.MaxRetries = maxRetries
	}
}

// WithDeadLetterExchange sets the dead letter exchange
func WithDeadLetterExchange(exchange string) SubscriptionOption {
	return func(opts *SubscriptionOptions) {
		opts.DeadLetterExchange = exchange
	}
}

// Subscribe subscribes to messages of a specific type
func (s *MessageSubscriber) Subscribe(ctx context.Context, queue string, messageType string, handler MessageHandler, options ...SubscriptionOption) error {
	if queue == "" {
		return fmt.Errorf("queue name cannot be empty")
	}
	if messageType == "" {
		return fmt.Errorf("message type cannot be empty")
	}
	if handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}

	// Apply default options
	opts := SubscriptionOptions{
		PrefetchCount:      10,
		AutoAck:            false,
		Exclusive:          false,
		Durable:            true,
		AutoDelete:         false,
		Arguments:          make(map[string]interface{}),
		MaxRetries:         3,
		DeadLetterExchange: "mmate.dlx",
	}

	for _, opt := range options {
		opt(&opts)
	}

	// Check if already subscribed
	s.mu.Lock()
	if _, exists := s.subscriptions[queue]; exists {
		s.mu.Unlock()
		return fmt.Errorf("already subscribed to queue: %s", queue)
	}
	s.mu.Unlock()

	// Create subscription context
	subCtx, cancel := context.WithCancel(ctx)

	subscription := &Subscription{
		Queue:       queue,
		MessageType: messageType,
		Handler:     handler,
		Options:     opts,
		cancelFunc:  cancel,
		consumerTag: fmt.Sprintf("subscriber-%s-%d", queue, time.Now().Unix()),
	}

	// Start consuming
	err := s.transport.Subscribe(subCtx, queue, s.createDeliveryHandler(subscription), opts)
	if err != nil {
		cancel()
		return fmt.Errorf("failed to subscribe to queue %s: %w", queue, err)
	}

	// Store subscription
	s.mu.Lock()
	s.subscriptions[queue] = subscription
	s.mu.Unlock()

	// Message processing is handled by the delivery handler

	s.logger.Info("subscribed to queue",
		"queue", queue,
		"messageType", messageType,
		"consumerTag", subscription.consumerTag,
	)

	return nil
}

// Unsubscribe stops consuming from a queue
func (s *MessageSubscriber) Unsubscribe(queue string) error {
	s.mu.Lock()
	subscription, exists := s.subscriptions[queue]
	if !exists {
		s.mu.Unlock()
		return fmt.Errorf("not subscribed to queue: %s", queue)
	}
	delete(s.subscriptions, queue)
	s.mu.Unlock()

	// Cancel the subscription
	subscription.cancelFunc()

	// Unsubscribe from transport
	err := s.transport.Unsubscribe(queue)
	if err != nil {
		s.logger.Warn("failed to unsubscribe from transport",
			"queue", queue,
			"error", err,
		)
	}

	s.logger.Info("unsubscribed from queue", "queue", queue)
	return nil
}

// GetSubscriptions returns all active subscriptions
func (s *MessageSubscriber) GetSubscriptions() map[string]*Subscription {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]*Subscription)
	for k, v := range s.subscriptions {
		result[k] = v
	}
	return result
}

// Close closes all subscriptions
func (s *MessageSubscriber) Close() error {
	s.mu.Lock()
	subscriptions := make([]*Subscription, 0, len(s.subscriptions))
	for _, sub := range s.subscriptions {
		subscriptions = append(subscriptions, sub)
	}
	s.subscriptions = make(map[string]*Subscription)
	s.mu.Unlock()

	// Cancel all subscriptions
	for _, sub := range subscriptions {
		sub.cancelFunc()
	}

	s.logger.Info("closed all subscriptions")
	return nil
}


// handleDelivery handles a single message delivery
func (s *MessageSubscriber) handleDelivery(ctx context.Context, delivery TransportDelivery, subscription *Subscription) {
	// Parse envelope
	var envelope contracts.Envelope
	if err := json.Unmarshal(delivery.Body(), &envelope); err != nil {
		s.logger.Error("failed to parse message envelope",
			"queue", subscription.Queue,
			"error", err,
		)
		if !subscription.Options.AutoAck {
			delivery.Reject(false) // Don't requeue invalid messages
		}
		return
	}

	// Extract message from payload
	msg, err := s.extractMessage(&envelope)
	if err != nil {
		s.logger.Error("failed to extract message",
			"messageId", envelope.ID,
			"messageType", envelope.Type,
			"error", err,
		)
		if !subscription.Options.AutoAck {
			delivery.Reject(false)
		}
		return
	}

	// Process message through interceptor pipeline if configured
	if s.pipeline != nil {
		err = s.pipeline.Execute(ctx, msg, subscription.Handler)
	} else {
		err = subscription.Handler.Handle(ctx, msg)
	}
	
	if err != nil {
		s.logger.Error("handler failed",
			"messageId", msg.GetID(),
			"messageType", msg.GetType(),
			"queue", subscription.Queue,
			"error", err,
		)

		// Handle error based on error handler policy
		if !subscription.Options.AutoAck {
			action := s.errorHandler.HandleError(ctx, msg, err)
			switch action {
			case Acknowledge:
				delivery.Acknowledge()
			case Retry:
				delivery.Reject(true) // Requeue for retry
			case Reject:
				delivery.Reject(false) // Send to DLQ
			}
		}
		return
	}

	// Success - acknowledge message only if not using auto-ack
	if !subscription.Options.AutoAck {
		delivery.Acknowledge()
	}

	s.logger.Debug("message processed successfully",
		"messageId", msg.GetID(),
		"messageType", msg.GetType(),
		"queue", subscription.Queue,
	)
}

// extractMessage extracts the concrete message from the envelope
func (s *MessageSubscriber) extractMessage(envelope *contracts.Envelope) (contracts.Message, error) {
	// Get the global type registry
	registry := GetTypeRegistry()
	
	// Create instance of the registered type
	instance, err := registry.CreateInstance(envelope.Type)
	if err != nil {
		// Fallback to base message if type not registered
		msg := contracts.NewBaseMessage(envelope.Type)
		msg.ID = envelope.ID
		msg.CorrelationID = envelope.CorrelationID
		if envelope.Timestamp != "" {
			if timestamp, err := time.Parse(time.RFC3339, envelope.Timestamp); err == nil {
				msg.Timestamp = timestamp
			}
		}
		return &msg, nil
	}
	
	// First, unmarshal the payload directly into the typed instance
	// This preserves embedded struct relationships and handles JSON properly
	if envelope.Payload != nil {
		if err := json.Unmarshal(envelope.Payload, instance); err != nil {
			return nil, fmt.Errorf("failed to unmarshal payload into %s: %w", envelope.Type, err)
		}
	}
	
	// Then set the envelope fields directly on the instance using reflection
	// This ensures BaseMessage fields from envelope override payload values
	instanceValue := reflect.ValueOf(instance)
	if instanceValue.Kind() == reflect.Ptr {
		instanceValue = instanceValue.Elem()
	}
	
	// Set ID field
	if idField := findFieldByJSONTag(instanceValue, "id"); idField.IsValid() && idField.CanSet() {
		if idField.Kind() == reflect.String {
			idField.SetString(envelope.ID)
		}
	}
	
	// Set Type field  
	if typeField := findFieldByJSONTag(instanceValue, "type"); typeField.IsValid() && typeField.CanSet() {
		if typeField.Kind() == reflect.String {
			typeField.SetString(envelope.Type)
		}
	}
	
	// Set Timestamp field
	if timestampField := findFieldByJSONTag(instanceValue, "timestamp"); timestampField.IsValid() && timestampField.CanSet() {
		if timestampField.Type() == reflect.TypeOf(time.Time{}) {
			if envelope.Timestamp != "" {
				if timestamp, err := time.Parse(time.RFC3339, envelope.Timestamp); err == nil {
					timestampField.Set(reflect.ValueOf(timestamp))
				}
			}
		}
	}
	
	// Set CorrelationID field
	if envelope.CorrelationID != "" {
		if corrField := findFieldByJSONTag(instanceValue, "correlationId"); corrField.IsValid() && corrField.CanSet() {
			if corrField.Kind() == reflect.String {
				corrField.SetString(envelope.CorrelationID)
			}
		}
	}
	
	// Ensure it's a message
	msg, ok := instance.(contracts.Message)
	if !ok {
		return nil, fmt.Errorf("type %s does not implement Message interface", envelope.Type)
	}
	
	return msg, nil
}

// findFieldByJSONTag searches for a field with the given JSON tag, including in embedded structs
func findFieldByJSONTag(structValue reflect.Value, jsonTag string) reflect.Value {
	structType := structValue.Type()
	
	// Search direct fields first
	for i := 0; i < structValue.NumField(); i++ {
		field := structType.Field(i)
		fieldValue := structValue.Field(i)
		
		// Check if this field has the JSON tag we're looking for
		tag := field.Tag.Get("json")
		if tag != "" {
			// Parse the tag (format: "name,omitempty" or just "name")
			tagName := strings.Split(tag, ",")[0]
			if tagName == jsonTag {
				return fieldValue
			}
		}
		
		// If this is an embedded struct, search recursively
		if field.Anonymous && fieldValue.Kind() == reflect.Struct {
			if foundField := findFieldByJSONTag(fieldValue, jsonTag); foundField.IsValid() {
				return foundField
			}
		}
	}
	
	// Not found
	return reflect.Value{}
}

// createDeliveryHandler creates a delivery handler for the transport
func (s *MessageSubscriber) createDeliveryHandler(subscription *Subscription) func(delivery TransportDelivery) error {
	return func(delivery TransportDelivery) error {
		// Create a context for this delivery
		ctx := context.Background()
		s.handleDelivery(ctx, delivery, subscription)
		return nil
	}
}

