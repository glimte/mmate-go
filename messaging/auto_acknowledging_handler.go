package messaging

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/glimte/mmate-go/contracts"
)

// AcknowledgmentStrategy defines how messages are acknowledged
type AcknowledgmentStrategy int

const (
	// AckOnSuccess acknowledges only on successful processing
	AckOnSuccess AcknowledgmentStrategy = iota
	// AckAlways acknowledges regardless of processing result
	AckAlways
	// AckManual requires manual acknowledgment
	AckManual
)

// Acknowledger provides message acknowledgment capabilities
type Acknowledger interface {
	Ack() error
	Nack(requeue bool) error
	Reject(requeue bool) error
}

// AutoAcknowledgingHandler provides automatic message acknowledgment
type AutoAcknowledgingHandler struct {
	handler  MessageHandler
	strategy AcknowledgmentStrategy
	logger   *slog.Logger
	onError  func(ctx context.Context, msg contracts.Message, err error) ErrorAction
}

// AutoAckOption configures the auto-acknowledging handler
type AutoAckOption func(*AutoAcknowledgingHandler)

// WithAckStrategy sets the acknowledgment strategy
func WithAckStrategy(strategy AcknowledgmentStrategy) AutoAckOption {
	return func(h *AutoAcknowledgingHandler) {
		h.strategy = strategy
	}
}

// WithAckLogger sets the logger
func WithAckLogger(logger *slog.Logger) AutoAckOption {
	return func(h *AutoAcknowledgingHandler) {
		h.logger = logger
	}
}

// WithAckErrorHandler sets the error handler
func WithAckErrorHandler(handler func(ctx context.Context, msg contracts.Message, err error) ErrorAction) AutoAckOption {
	return func(h *AutoAcknowledgingHandler) {
		h.onError = handler
	}
}

// NewAutoAcknowledgingHandler creates a new auto-acknowledging handler
func NewAutoAcknowledgingHandler(handler MessageHandler, opts ...AutoAckOption) *AutoAcknowledgingHandler {
	h := &AutoAcknowledgingHandler{
		handler:  handler,
		strategy: AckOnSuccess,
		logger:   slog.Default(),
		onError: func(ctx context.Context, msg contracts.Message, err error) ErrorAction {
			return Retry // Default to retry on error
		},
	}

	for _, opt := range opts {
		opt(h)
	}

	return h
}

// Handle processes the message with automatic acknowledgment
func (h *AutoAcknowledgingHandler) Handle(ctx context.Context, msg contracts.Message) error {
	// Extract acknowledger from context
	ack, ok := ctx.Value(acknowledgerKey{}).(Acknowledger)
	if !ok {
		// No acknowledger in context, process normally
		return h.handler.Handle(ctx, msg)
	}

	// Process message
	err := h.handler.Handle(ctx, msg)

	// Handle acknowledgment based on strategy
	switch h.strategy {
	case AckAlways:
		if ackErr := ack.Ack(); ackErr != nil {
			h.logger.Error("failed to acknowledge message",
				"messageId", msg.GetID(),
				"error", ackErr)
		}
		return err

	case AckOnSuccess:
		if err != nil {
			action := h.onError(ctx, msg, err)
			switch action {
			case Retry:
				if nackErr := ack.Nack(true); nackErr != nil {
					h.logger.Error("failed to nack message",
						"messageId", msg.GetID(),
						"error", nackErr)
				}
			case Reject:
				if rejectErr := ack.Reject(false); rejectErr != nil {
					h.logger.Error("failed to reject message",
						"messageId", msg.GetID(),
						"error", rejectErr)
				}
			case Acknowledge:
				if ackErr := ack.Ack(); ackErr != nil {
					h.logger.Error("failed to acknowledge failed message",
						"messageId", msg.GetID(),
						"error", ackErr)
				}
			}
			return err
		}

		if ackErr := ack.Ack(); ackErr != nil {
			h.logger.Error("failed to acknowledge message",
				"messageId", msg.GetID(),
				"error", ackErr)
			return fmt.Errorf("failed to acknowledge: %w", ackErr)
		}
		return nil

	case AckManual:
		// Manual acknowledgment - do nothing
		return err

	default:
		return fmt.Errorf("unknown acknowledgment strategy: %v", h.strategy)
	}
}

// acknowledgerKey is used to store acknowledger in context
type acknowledgerKey struct{}

// WithAcknowledger adds an acknowledger to the context
func WithAcknowledger(ctx context.Context, ack Acknowledger) context.Context {
	return context.WithValue(ctx, acknowledgerKey{}, ack)
}

// GetAcknowledger retrieves the acknowledger from context
func GetAcknowledger(ctx context.Context) (Acknowledger, bool) {
	ack, ok := ctx.Value(acknowledgerKey{}).(Acknowledger)
	return ack, ok
}

// MessageContext provides message handling context with acknowledgment
type MessageContext struct {
	context.Context
	Message      contracts.Message
	Acknowledger Acknowledger
}

// NewMessageContext creates a new message context
func NewMessageContext(ctx context.Context, msg contracts.Message, ack Acknowledger) *MessageContext {
	return &MessageContext{
		Context:      ctx,
		Message:      msg,
		Acknowledger: ack,
	}
}

// Ack acknowledges the message
func (mc *MessageContext) Ack() error {
	if mc.Acknowledger == nil {
		return fmt.Errorf("no acknowledger available")
	}
	return mc.Acknowledger.Ack()
}

// Nack negatively acknowledges the message
func (mc *MessageContext) Nack(requeue bool) error {
	if mc.Acknowledger == nil {
		return fmt.Errorf("no acknowledger available")
	}
	return mc.Acknowledger.Nack(requeue)
}

// Reject rejects the message
func (mc *MessageContext) Reject(requeue bool) error {
	if mc.Acknowledger == nil {
		return fmt.Errorf("no acknowledger available")
	}
	return mc.Acknowledger.Reject(requeue)
}
