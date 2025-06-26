package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/internal/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// MessageSubscriber manages message consumption and routing to handlers
type MessageSubscriber struct {
	consumer        *rabbitmq.Consumer
	dispatcher      *MessageDispatcher
	logger          *slog.Logger
	subscriptions   map[string]*Subscription
	mu              sync.RWMutex
	errorHandler    ErrorHandler
	deadLetterQueue string
	ackStrategy     rabbitmq.AcknowledgmentStrategy
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

// WithSubscriberAckStrategy sets the acknowledgment strategy
func WithSubscriberAckStrategy(strategy rabbitmq.AcknowledgmentStrategy) SubscriberOption {
	return func(s *MessageSubscriber) {
		s.ackStrategy = strategy
	}
}

// NewMessageSubscriber creates a new message subscriber
func NewMessageSubscriber(consumer *rabbitmq.Consumer, dispatcher *MessageDispatcher, options ...SubscriberOption) *MessageSubscriber {
	s := &MessageSubscriber{
		consumer:      consumer,
		dispatcher:    dispatcher,
		logger:        slog.Default(),
		subscriptions: make(map[string]*Subscription),
		errorHandler: &DefaultErrorHandler{
			MaxRetries: 3,
			Logger:     slog.Default(),
		},
		deadLetterQueue: "mmate.dlq",
		ackStrategy:     rabbitmq.AckOnSuccess,
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
	err := s.consumer.Subscribe(subCtx, queue, s.createDeliveryHandler(subscription))
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

	// Unsubscribe from consumer
	err := s.consumer.Unsubscribe(queue)
	if err != nil {
		s.logger.Warn("failed to unsubscribe from consumer",
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

// processMessages processes incoming messages for a subscription
func (s *MessageSubscriber) processMessages(ctx context.Context, deliveryChan <-chan amqp.Delivery, subscription *Subscription) {
	for {
		select {
		case <-ctx.Done():
			s.logger.Debug("stopping message processing",
				"queue", subscription.Queue,
				"reason", ctx.Err(),
			)
			return
		case delivery, ok := <-deliveryChan:
			if !ok {
				s.logger.Warn("delivery channel closed", "queue", subscription.Queue)
				return
			}

			s.handleDelivery(ctx, delivery, subscription)
		}
	}
}

// handleDelivery handles a single message delivery
func (s *MessageSubscriber) handleDelivery(ctx context.Context, delivery amqp.Delivery, subscription *Subscription) {
	// Parse envelope
	var envelope contracts.Envelope
	if err := json.Unmarshal(delivery.Body, &envelope); err != nil {
		s.logger.Error("failed to parse message envelope",
			"queue", subscription.Queue,
			"error", err,
		)
		s.nackMessage(delivery, false) // Don't requeue invalid messages
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
		s.nackMessage(delivery, false)
		return
	}

	// Process message through handler
	err = subscription.Handler.Handle(ctx, msg)
	if err != nil {
		s.logger.Error("handler failed",
			"messageId", msg.GetID(),
			"messageType", msg.GetType(),
			"queue", subscription.Queue,
			"error", err,
		)

		// Handle error based on error handler policy
		action := s.errorHandler.HandleError(ctx, msg, err)
		switch action {
		case Acknowledge:
			s.ackMessage(delivery)
		case Retry:
			s.nackMessage(delivery, true) // Requeue for retry
		case Reject:
			s.nackMessage(delivery, false) // Send to DLQ
		}
		return
	}

	// Success - acknowledge message
	s.ackMessage(delivery)

	s.logger.Debug("message processed successfully",
		"messageId", msg.GetID(),
		"messageType", msg.GetType(),
		"queue", subscription.Queue,
	)
}

// extractMessage extracts the concrete message from the envelope
func (s *MessageSubscriber) extractMessage(envelope *contracts.Envelope) (contracts.Message, error) {
	// This is a simplified implementation
	// In a real implementation, you would need message type registration
	// to properly deserialize to the correct concrete type

	// Create base message and populate fields
	msg := contracts.NewBaseMessage(envelope.Type)
	msg.ID = envelope.ID
	msg.CorrelationID = envelope.CorrelationID

	if envelope.Timestamp != "" {
		if timestamp, err := time.Parse(time.RFC3339, envelope.Timestamp); err == nil {
			msg.Timestamp = timestamp
		}
	}

	// Return as base message for now
	// In a real implementation, you'd deserialize to the specific type
	return &msg, nil
}

// createDeliveryHandler creates a delivery handler for the consumer
func (s *MessageSubscriber) createDeliveryHandler(subscription *Subscription) func(ctx context.Context, delivery amqp.Delivery) error {
	return func(ctx context.Context, delivery amqp.Delivery) error {
		s.handleDelivery(ctx, delivery, subscription)
		return nil
	}
}

// ackMessage acknowledges a message
func (s *MessageSubscriber) ackMessage(delivery amqp.Delivery) {
	if err := delivery.Ack(false); err != nil {
		s.logger.Error("failed to ack message",
			"deliveryTag", delivery.DeliveryTag,
			"error", err,
		)
	}
}

// nackMessage negatively acknowledges a message
func (s *MessageSubscriber) nackMessage(delivery amqp.Delivery, requeue bool) {
	if err := delivery.Nack(false, requeue); err != nil {
		s.logger.Error("failed to nack message",
			"deliveryTag", delivery.DeliveryTag,
			"requeue", requeue,
			"error", err,
		)
	}
}
