package messaging

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/interceptors"
)

// ProcessingAckHandler wraps a message handler to send processing acknowledgments
type ProcessingAckHandler struct {
	handler     interceptors.MessageHandler
	publisher   *MessagePublisher
	logger      *slog.Logger
	processorID string
}

// ProcessingAckHandlerOptions configures the processing acknowledgment handler
type ProcessingAckHandlerOptions struct {
	Logger      *slog.Logger
	ProcessorID string
}

// NewProcessingAckHandler creates a new processing acknowledgment handler
func NewProcessingAckHandler(handler interceptors.MessageHandler, publisher *MessagePublisher, opts *ProcessingAckHandlerOptions) *ProcessingAckHandler {
	if opts == nil {
		opts = &ProcessingAckHandlerOptions{}
	}

	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	if opts.ProcessorID == "" {
		hostname, _ := os.Hostname()
		opts.ProcessorID = fmt.Sprintf("%s-%d", hostname, os.Getpid())
	}

	return &ProcessingAckHandler{
		handler:     handler,
		publisher:   publisher,
		logger:      opts.Logger,
		processorID: opts.ProcessorID,
	}
}

// Handle processes the message and sends acknowledgment
func (h *ProcessingAckHandler) Handle(ctx context.Context, msg contracts.Message) error {
	startTime := time.Now()
	
	// Check if message expects acknowledgment
	replyTo := h.getReplyTo(ctx, msg)
	if replyTo == "" {
		// No acknowledgment expected, process normally
		return h.handler.Handle(ctx, msg)
	}

	correlationID := msg.GetCorrelationID()
	if correlationID == "" {
		// No correlation ID, process normally
		return h.handler.Handle(ctx, msg)
	}

	h.logger.Debug("Processing message with acknowledgment",
		"messageId", msg.GetID(),
		"correlationId", correlationID,
		"replyTo", replyTo)

	// Process the message
	err := h.handler.Handle(ctx, msg)
	processingTime := time.Since(startTime)

	// Create acknowledgment
	ack := NewAcknowledgmentMessage(correlationID, msg.GetID(), msg.GetType(), err == nil)
	ack.ProcessingTime = processingTime
	ack.ProcessorID = h.processorID

	if err != nil {
		ack.ErrorMessage = err.Error()
	}

	// Add processing metadata
	ack.Metadata = map[string]interface{}{
		"processingStartedAt": startTime,
		"processingEndedAt":   time.Now(),
		"processorVersion":    "mmate-go/1.0",
	}

	// Send acknowledgment
	ackErr := h.publisher.PublishReply(ctx, ack, replyTo)
	if ackErr != nil {
		h.logger.Error("Failed to send processing acknowledgment",
			"messageId", msg.GetID(),
			"correlationId", correlationID,
			"replyTo", replyTo,
			"error", ackErr)
	} else {
		h.logger.Info("Sent processing acknowledgment",
			"messageId", msg.GetID(),
			"correlationId", correlationID,
			"success", err == nil,
			"processingTime", processingTime)
	}

	return err
}

// getReplyTo extracts the reply-to queue from context or message headers
func (h *ProcessingAckHandler) getReplyTo(ctx context.Context, msg contracts.Message) string {
	// Try to get from interceptor context first
	if intercCtx, ok := interceptors.GetInterceptorContext(ctx); ok {
		if replyTo, exists := intercCtx.Get("reply-to"); exists {
			if replyToStr, ok := replyTo.(string); ok {
				return replyToStr
			}
		}
	}

	// Try to get from message if it implements ReplyTo interface
	if replyMsg, ok := msg.(interface{ GetReplyTo() string }); ok {
		return replyMsg.GetReplyTo()
	}

	return ""
}

// AutoAckMessageHandlerFunc is a function adapter for processing acknowledgment handlers
type AutoAckMessageHandlerFunc func(ctx context.Context, msg contracts.Message) error

// Handle implements MessageHandler
func (f AutoAckMessageHandlerFunc) Handle(ctx context.Context, msg contracts.Message) error {
	return f(ctx, msg)
}

// WithProcessingAck wraps a handler function to automatically send processing acknowledgments
func WithProcessingAck(publisher *MessagePublisher, handlerFunc AutoAckMessageHandlerFunc, opts *ProcessingAckHandlerOptions) interceptors.MessageHandler {
	return NewProcessingAckHandler(handlerFunc, publisher, opts)
}

// ProcessingAckInterceptor automatically adds processing acknowledgment to handlers
type ProcessingAckInterceptor struct {
	publisher *MessagePublisher
	logger    *slog.Logger
	enabled   bool
}

// NewProcessingAckInterceptor creates a new processing acknowledgment interceptor
func NewProcessingAckInterceptor(publisher *MessagePublisher) *ProcessingAckInterceptor {
	return &ProcessingAckInterceptor{
		publisher: publisher,
		logger:    slog.Default(),
		enabled:   true,
	}
}

// Intercept implements the Interceptor interface
func (i *ProcessingAckInterceptor) Intercept(ctx context.Context, msg contracts.Message, next interceptors.MessageHandler) error {
	if !i.enabled {
		return next.Handle(ctx, msg)
	}

	// Wrap the next handler with processing acknowledgment
	ackHandler := NewProcessingAckHandler(next, i.publisher, &ProcessingAckHandlerOptions{
		Logger: i.logger,
	})

	return ackHandler.Handle(ctx, msg)
}

// Name returns the interceptor name
func (i *ProcessingAckInterceptor) Name() string {
	return "ProcessingAckInterceptor"
}

// SetEnabled enables or disables the interceptor
func (i *ProcessingAckInterceptor) SetEnabled(enabled bool) {
	i.enabled = enabled
}

// WithLogger sets the logger for the interceptor
func (i *ProcessingAckInterceptor) WithLogger(logger *slog.Logger) *ProcessingAckInterceptor {
	i.logger = logger
	return i
}