package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/internal/reliability"
)

// ServerRequestHandler handles incoming requests and returns replies
type ServerRequestHandler interface {
	HandleRequest(ctx context.Context, request contracts.Message) (contracts.Reply, error)
}

// ServerRequestHandlerFunc is a function that implements ServerRequestHandler
type ServerRequestHandlerFunc func(ctx context.Context, request contracts.Message) (contracts.Reply, error)

// HandleRequest implements ServerRequestHandler
func (f ServerRequestHandlerFunc) HandleRequest(ctx context.Context, request contracts.Message) (contracts.Reply, error) {
	return f(ctx, request)
}

// Server handles incoming requests and sends replies
type Server interface {
	// RegisterHandler registers a handler for a specific message type
	RegisterHandler(messageType string, handler ServerRequestHandler) error

	// RegisterCommandHandler registers a handler for a command type
	RegisterCommandHandler(commandType string, handler func(ctx context.Context, cmd contracts.Command) (contracts.Reply, error)) error

	// RegisterQueryHandler registers a handler for a query type
	RegisterQueryHandler(queryType string, handler func(ctx context.Context, query contracts.Query) (contracts.Reply, error)) error

	// Start starts the server
	Start(ctx context.Context) error

	// Stop stops the server
	Stop() error

	// GetHandlerCount returns the number of registered handlers
	GetHandlerCount() int
}

// DefaultRequestReplyServer is the default implementation of Server
type DefaultRequestReplyServer struct {
	subscriber    Subscriber
	publisher     Publisher
	handlers      map[string]ServerRequestHandler
	subscriptions map[string]string // messageType -> queue
	logger        *slog.Logger
	errorHandler  ErrorHandler
	retryPolicy   reliability.RetryPolicy
	queuePrefix   string
	mu            sync.RWMutex
	running       bool
	ctx           context.Context
	cancel        context.CancelFunc
}

// RequestReplyServerConfig configures the request-reply server
type RequestReplyServerConfig struct {
	QueuePrefix  string
	Logger       *slog.Logger
	ErrorHandler ErrorHandler
	RetryPolicy  reliability.RetryPolicy
}

// RequestReplyServerOption configures the server
type RequestReplyServerOption func(*RequestReplyServerConfig)

// WithServerQueuePrefix sets the queue prefix
func WithServerQueuePrefix(prefix string) RequestReplyServerOption {
	return func(c *RequestReplyServerConfig) {
		c.QueuePrefix = prefix
	}
}

// WithServerLogger sets the logger
func WithServerLogger(logger *slog.Logger) RequestReplyServerOption {
	return func(c *RequestReplyServerConfig) {
		c.Logger = logger
	}
}

// WithServerErrorHandler sets the error handler
func WithServerErrorHandler(handler ErrorHandler) RequestReplyServerOption {
	return func(c *RequestReplyServerConfig) {
		c.ErrorHandler = handler
	}
}

// WithServerRetryPolicy sets the retry policy
func WithServerRetryPolicy(policy reliability.RetryPolicy) RequestReplyServerOption {
	return func(c *RequestReplyServerConfig) {
		c.RetryPolicy = policy
	}
}

// NewRequestReplyServer creates a new request-reply server
func NewRequestReplyServer(subscriber Subscriber, publisher Publisher, opts ...RequestReplyServerOption) (*DefaultRequestReplyServer, error) {
	if subscriber == nil {
		return nil, fmt.Errorf("subscriber cannot be nil")
	}
	if publisher == nil {
		return nil, fmt.Errorf("publisher cannot be nil")
	}

	config := &RequestReplyServerConfig{
		QueuePrefix:  "mmate.rpc",
		Logger:       slog.Default(),
		ErrorHandler: &DefaultErrorHandler{Logger: slog.Default()},
		RetryPolicy:  reliability.NewExponentialBackoff(time.Second, 5*time.Second, 2.0, 3),
	}

	for _, opt := range opts {
		opt(config)
	}

	server := &DefaultRequestReplyServer{
		subscriber:    subscriber,
		publisher:     publisher,
		handlers:      make(map[string]ServerRequestHandler),
		subscriptions: make(map[string]string),
		logger:        config.Logger,
		errorHandler:  config.ErrorHandler,
		retryPolicy:   config.RetryPolicy,
		queuePrefix:   config.QueuePrefix,
	}

	return server, nil
}

// RegisterHandler registers a handler for a specific message type
func (s *DefaultRequestReplyServer) RegisterHandler(messageType string, handler ServerRequestHandler) error {
	if messageType == "" {
		return fmt.Errorf("message type cannot be empty")
	}
	if handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("cannot register handler while server is running")
	}

	if _, exists := s.handlers[messageType]; exists {
		return fmt.Errorf("handler already registered for message type: %s", messageType)
	}

	s.handlers[messageType] = handler
	s.logger.Info("registered request handler",
		"messageType", messageType,
	)

	return nil
}

// RegisterCommandHandler registers a handler for a command type
func (s *DefaultRequestReplyServer) RegisterCommandHandler(commandType string, handler func(ctx context.Context, cmd contracts.Command) (contracts.Reply, error)) error {
	return s.RegisterHandler(commandType, ServerRequestHandlerFunc(func(ctx context.Context, msg contracts.Message) (contracts.Reply, error) {
		cmd, ok := msg.(contracts.Command)
		if !ok {
			return nil, fmt.Errorf("expected command, got %T", msg)
		}
		return handler(ctx, cmd)
	}))
}

// RegisterQueryHandler registers a handler for a query type
func (s *DefaultRequestReplyServer) RegisterQueryHandler(queryType string, handler func(ctx context.Context, query contracts.Query) (contracts.Reply, error)) error {
	return s.RegisterHandler(queryType, ServerRequestHandlerFunc(func(ctx context.Context, msg contracts.Message) (contracts.Reply, error) {
		query, ok := msg.(contracts.Query)
		if !ok {
			return nil, fmt.Errorf("expected query, got %T", msg)
		}
		return handler(ctx, query)
	}))
}

// Start starts the server
func (s *DefaultRequestReplyServer) Start(ctx context.Context) error {
	s.mu.Lock()

	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("server already running")
	}

	if len(s.handlers) == 0 {
		s.mu.Unlock()
		return fmt.Errorf("no handlers registered")
	}

	s.ctx, s.cancel = context.WithCancel(ctx)

	// Subscribe to queues for each message type
	for messageType := range s.handlers {
		queue := fmt.Sprintf("%s.%s", s.queuePrefix, messageType)

		err := s.subscriber.Subscribe(s.ctx, queue, messageType,
			MessageHandlerFunc(s.handleMessage),
			WithAutoAck(false), // Manual ack for reliability
		)
		if err != nil {
			// Cleanup already subscribed queues
			for mt, q := range s.subscriptions {
				s.subscriber.Unsubscribe(q)
				delete(s.subscriptions, mt)
			}
			s.mu.Unlock()
			return fmt.Errorf("failed to subscribe to queue %s: %w", queue, err)
		}

		s.subscriptions[messageType] = queue
		s.logger.Info("subscribed to request queue",
			"queue", queue,
			"messageType", messageType,
		)
	}

	s.running = true
	s.mu.Unlock()

	s.logger.Info("request-reply server started",
		"handlers", len(s.handlers),
	)

	return nil
}

// Stop stops the server
func (s *DefaultRequestReplyServer) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return fmt.Errorf("server not running")
	}

	// Cancel context
	if s.cancel != nil {
		s.cancel()
	}

	// Unsubscribe from all queues
	var errs []error
	for messageType, queue := range s.subscriptions {
		if err := s.subscriber.Unsubscribe(queue); err != nil {
			errs = append(errs, fmt.Errorf("failed to unsubscribe from %s: %w", queue, err))
		}
		delete(s.subscriptions, messageType)
	}

	s.running = false
	s.logger.Info("request-reply server stopped")

	if len(errs) > 0 {
		return fmt.Errorf("errors during shutdown: %v", errs)
	}

	return nil
}

// GetHandlerCount returns the number of registered handlers
func (s *DefaultRequestReplyServer) GetHandlerCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.handlers)
}

// handleMessage handles incoming request messages
func (s *DefaultRequestReplyServer) handleMessage(ctx context.Context, msg contracts.Message) error {
	startTime := time.Now()

	s.logger.Debug("received request",
		"messageId", msg.GetID(),
		"messageType", msg.GetType(),
		"correlationId", msg.GetCorrelationID(),
	)

	// Get handler for message type
	s.mu.RLock()
	handler, exists := s.handlers[msg.GetType()]
	s.mu.RUnlock()

	if !exists {
		s.logger.Error("no handler for message type",
			"messageType", msg.GetType(),
		)
		return s.sendErrorReply(ctx, msg, fmt.Errorf("no handler for message type: %s", msg.GetType()))
	}

	// Execute handler with retry
	var reply contracts.Reply
	var err error

	for attempt := 0; attempt <= s.retryPolicy.MaxRetries(); attempt++ {
		reply, err = handler.HandleRequest(ctx, msg)
		if err == nil {
			break
		}

		shouldRetry, delay := s.retryPolicy.ShouldRetry(attempt+1, err)
		if !shouldRetry {
			break
		}

		s.logger.Warn("retrying request handler",
			"messageId", msg.GetID(),
			"attempt", attempt+1,
			"delay", delay,
			"error", err,
		)

		select {
		case <-time.After(delay):
			// Continue retry
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if err != nil {
		s.logger.Error("request handler failed",
			"messageId", msg.GetID(),
			"messageType", msg.GetType(),
			"error", err,
			"duration", time.Since(startTime),
		)
		return s.sendErrorReply(ctx, msg, err)
	}

	// Send reply
	err = s.sendReply(ctx, msg, reply)
	if err != nil {
		s.logger.Error("failed to send reply",
			"messageId", msg.GetID(),
			"error", err,
		)
		return err
	}

	s.logger.Info("request processed successfully",
		"messageId", msg.GetID(),
		"messageType", msg.GetType(),
		"duration", time.Since(startTime),
	)

	return nil
}

// sendReply sends a reply message
func (s *DefaultRequestReplyServer) sendReply(ctx context.Context, request contracts.Message, reply contracts.Reply) error {
	// Set correlation ID
	if reply != nil {
		reply.SetCorrelationID(request.GetCorrelationID())
	}

	// Determine reply queue
	var replyTo string

	// Check if request is a command with ReplyTo
	if cmd, ok := request.(contracts.Command); ok {
		// Use reflection to get ReplyTo field
		v := reflect.ValueOf(cmd)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}

		// Look for embedded BaseCommand
		if baseField := v.FieldByName("BaseCommand"); baseField.IsValid() {
			if replyToField := baseField.FieldByName("ReplyTo"); replyToField.IsValid() && replyToField.Kind() == reflect.String {
				replyTo = replyToField.String()
			}
		}
	}

	// Check if request is a query with ReplyTo
	if replyTo == "" {
		if query, ok := request.(contracts.Query); ok {
			replyTo = query.GetReplyTo()
		}
	}

	if replyTo == "" {
		return fmt.Errorf("no reply-to address in request")
	}

	// Publish reply
	return s.publisher.Publish(ctx, reply,
		WithRoutingKey(replyTo),
		WithCorrelationID(request.GetCorrelationID()),
	)
}

// sendErrorReply sends an error reply
func (s *DefaultRequestReplyServer) sendErrorReply(ctx context.Context, request contracts.Message, err error) error {
	errorReply := &ErrorReply{
		BaseReply: contracts.BaseReply{
			BaseMessage: contracts.NewBaseMessage("ErrorReply"),
			Success:     false,
		},
		ErrorMessage: err.Error(),
		ErrorCode:    "REQUEST_FAILED",
		Timestamp:    time.Now().UTC(),
	}

	return s.sendReply(ctx, request, errorReply)
}

// ErrorReply represents an error reply message
type ErrorReply struct {
	contracts.BaseReply
	ErrorMessage string    `json:"errorMessage"`
	ErrorCode    string    `json:"errorCode"`
	Timestamp    time.Time `json:"timestamp"`
}

// GetError returns the error
func (r *ErrorReply) GetError() error {
	return fmt.Errorf("%s: %s", r.ErrorCode, r.ErrorMessage)
}

// MarshalJSON custom JSON marshaling
func (r *ErrorReply) MarshalJSON() ([]byte, error) {
	type Alias ErrorReply
	return json.Marshal(&struct {
		*Alias
		Timestamp string `json:"timestamp"`
	}{
		Alias:     (*Alias)(r),
		Timestamp: r.Timestamp.Format(time.RFC3339),
	})
}
