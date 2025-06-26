package bridge

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/internal/reliability"
	"github.com/glimte/mmate-go/messaging"
	"github.com/google/uuid"
)

// Publisher defines the interface for publishing messages
type Publisher interface {
	PublishCommand(ctx context.Context, cmd contracts.Command, opts ...messaging.PublishOption) error
	PublishQuery(ctx context.Context, query contracts.Query, opts ...messaging.PublishOption) error
}

// Subscriber defines the interface for subscribing to messages
type Subscriber interface {
	Subscribe(ctx context.Context, queueName string, messageType string, handler messaging.MessageHandler, opts ...messaging.SubscriptionOption) error
	Unsubscribe(queueName string) error
}

// PendingRequest represents a request waiting for response
type PendingRequest struct {
	ID         string
	ResponseCh chan contracts.Reply
	Timeout    time.Time
	Context    context.Context
	Cancel     context.CancelFunc
}

// SyncAsyncBridge enables synchronous request-response over async messaging
type SyncAsyncBridge struct {
	publisher       Publisher
	subscriber      Subscriber
	pendingRequests map[string]*PendingRequest
	mu              sync.RWMutex
	replyQueue      string
	circuitBreaker  *reliability.CircuitBreaker
	retryPolicy     reliability.RetryPolicy
	cleanupTicker   *time.Ticker
	done            chan struct{}
	defaultTimeout  time.Duration
	logger          interface{} // Accept logger but don't use it
}

// BridgeOption configures the sync-async bridge
type BridgeOption func(*BridgeConfig)

// BridgeConfig holds configuration for the bridge
type BridgeConfig struct {
	ReplyQueue         string
	CleanupInterval    time.Duration
	CircuitBreaker     *reliability.CircuitBreaker
	RetryPolicy        reliability.RetryPolicy
	MaxPendingRequests int
	DefaultTimeout     time.Duration
	Logger             interface{}
}

// WithReplyQueue sets a custom reply queue name
func WithReplyQueue(queueName string) BridgeOption {
	return func(c *BridgeConfig) {
		c.ReplyQueue = queueName
	}
}

// WithCleanupInterval sets the interval for cleaning up expired requests
func WithCleanupInterval(interval time.Duration) BridgeOption {
	return func(c *BridgeConfig) {
		c.CleanupInterval = interval
	}
}

// WithBridgeCircuitBreaker sets the circuit breaker for requests
func WithBridgeCircuitBreaker(cb *reliability.CircuitBreaker) BridgeOption {
	return func(c *BridgeConfig) {
		c.CircuitBreaker = cb
	}
}

// WithBridgeRetryPolicy sets the retry policy for failed requests
func WithBridgeRetryPolicy(policy reliability.RetryPolicy) BridgeOption {
	return func(c *BridgeConfig) {
		c.RetryPolicy = policy
	}
}

// WithMaxPendingRequests sets the maximum number of concurrent pending requests
func WithMaxPendingRequests(max int) BridgeOption {
	return func(c *BridgeConfig) {
		c.MaxPendingRequests = max
	}
}

// WithDefaultTimeout sets the default timeout for requests
func WithDefaultTimeout(timeout time.Duration) BridgeOption {
	return func(c *BridgeConfig) {
		c.DefaultTimeout = timeout
	}
}

// WithRetryPolicy sets the retry policy (alias for WithBridgeRetryPolicy)
func WithRetryPolicy(policy reliability.RetryPolicy) BridgeOption {
	return func(c *BridgeConfig) {
		c.RetryPolicy = policy
	}
}

// NewSyncAsyncBridge creates a new sync-async bridge
func NewSyncAsyncBridge(publisher Publisher, subscriber Subscriber, logger interface{}, opts ...BridgeOption) (*SyncAsyncBridge, error) {
	if publisher == nil {
		return nil, fmt.Errorf("publisher cannot be nil")
	}
	if subscriber == nil {
		return nil, fmt.Errorf("subscriber cannot be nil")
	}

	config := &BridgeConfig{
		ReplyQueue:         fmt.Sprintf("bridge.reply.%s", uuid.New().String()[:8]),
		CleanupInterval:    30 * time.Second,
		MaxPendingRequests: 1000,
		DefaultTimeout:     30 * time.Second,
		Logger:             logger,
	}

	for _, opt := range opts {
		opt(config)
	}

	bridge := &SyncAsyncBridge{
		publisher:       publisher,
		subscriber:      subscriber,
		pendingRequests: make(map[string]*PendingRequest),
		replyQueue:      config.ReplyQueue,
		circuitBreaker:  config.CircuitBreaker,
		retryPolicy:     config.RetryPolicy,
		cleanupTicker:   time.NewTicker(config.CleanupInterval),
		done:            make(chan struct{}),
		defaultTimeout:  config.DefaultTimeout,
		logger:          config.Logger,
	}

	// Subscribe to reply queue
	ctx := context.Background()
	err := subscriber.Subscribe(ctx, config.ReplyQueue, "reply", messaging.MessageHandlerFunc(bridge.handleReply))
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to reply queue: %w", err)
	}

	// Start cleanup routine
	go bridge.cleanupRoutine()

	return bridge, nil
}

// SendAndWait sends a message and waits for reply - main method used by examples
func (b *SyncAsyncBridge) SendAndWait(ctx context.Context, msg contracts.Message, routingKey string, timeout time.Duration) (contracts.Reply, error) {
	if msg == nil {
		return nil, fmt.Errorf("message cannot be nil")
	}

	// Determine message type and call appropriate method
	switch m := msg.(type) {
	case contracts.Command:
		// Set reply queue if it's a BaseCommand
		if baseCmd, ok := m.(*contracts.BaseCommand); ok {
			baseCmd.ReplyTo = b.replyQueue
		}
		return b.RequestCommand(ctx, m, timeout)
	case contracts.Query:
		// Set reply queue if it's a BaseQuery
		if baseQuery, ok := m.(*contracts.BaseQuery); ok {
			baseQuery.ReplyTo = b.replyQueue
		}
		return b.RequestQuery(ctx, m, timeout)
	default:
		return nil, fmt.Errorf("message must be a Command or Query, got %T", msg)
	}
}

// RequestCommand sends a command and waits for a reply
func (b *SyncAsyncBridge) RequestCommand(ctx context.Context, cmd contracts.Command, timeout time.Duration) (contracts.Reply, error) {
	if cmd == nil {
		return nil, fmt.Errorf("command cannot be nil")
	}

	// Set reply queue in command
	if baseCmd, ok := cmd.(*contracts.BaseCommand); ok {
		baseCmd.ReplyTo = b.replyQueue
	}

	correlationID := uuid.New().String()
	if msg, ok := cmd.(contracts.Message); ok {
		msg.SetCorrelationID(correlationID)
	}

	return b.sendRequest(ctx, func(ctx context.Context) error {
		return b.publisher.PublishCommand(ctx, cmd)
	}, correlationID, timeout)
}

// RequestQuery sends a query and waits for a reply
func (b *SyncAsyncBridge) RequestQuery(ctx context.Context, query contracts.Query, timeout time.Duration) (contracts.Reply, error) {
	if query == nil {
		return nil, fmt.Errorf("query cannot be nil")
	}

	// Set reply queue in query
	if baseQuery, ok := query.(*contracts.BaseQuery); ok {
		baseQuery.ReplyTo = b.replyQueue
	}

	correlationID := uuid.New().String()
	if msg, ok := query.(contracts.Message); ok {
		msg.SetCorrelationID(correlationID)
	}

	return b.sendRequest(ctx, func(ctx context.Context) error {
		return b.publisher.PublishQuery(ctx, query)
	}, correlationID, timeout)
}

// sendRequest handles the common request logic
func (b *SyncAsyncBridge) sendRequest(ctx context.Context, publishFunc func(context.Context) error, correlationID string, timeout time.Duration) (contracts.Reply, error) {
	// Check pending request limit
	b.mu.RLock()
	if len(b.pendingRequests) >= 1000 { // Default max
		b.mu.RUnlock()
		return nil, fmt.Errorf("too many pending requests")
	}
	b.mu.RUnlock()

	// Create request context with timeout
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Create pending request
	pending := &PendingRequest{
		ID:         correlationID,
		ResponseCh: make(chan contracts.Reply, 1),
		Timeout:    time.Now().Add(timeout),
		Context:    requestCtx,
		Cancel:     cancel,
	}

	// Register pending request
	b.mu.Lock()
	b.pendingRequests[correlationID] = pending
	b.mu.Unlock()

	// Ensure cleanup on exit
	defer func() {
		b.mu.Lock()
		delete(b.pendingRequests, correlationID)
		b.mu.Unlock()
	}()

	// Execute with circuit breaker if configured
	var err error
	if b.circuitBreaker != nil {
		err = b.circuitBreaker.Execute(requestCtx, func() error {
			return b.executeWithRetry(requestCtx, publishFunc)
		})
	} else {
		err = b.executeWithRetry(requestCtx, publishFunc)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	// Wait for response or timeout
	select {
	case reply := <-pending.ResponseCh:
		return reply, nil
	case <-requestCtx.Done():
		return nil, fmt.Errorf("request timeout or cancelled: %w", requestCtx.Err())
	}
}

// executeWithRetry executes the publish function with retry policy
func (b *SyncAsyncBridge) executeWithRetry(ctx context.Context, publishFunc func(context.Context) error) error {
	if b.retryPolicy != nil {
		return reliability.Retry(ctx, b.retryPolicy, func() error {
			return publishFunc(ctx)
		})
	}
	return publishFunc(ctx)
}

// handleReply processes incoming reply messages
func (b *SyncAsyncBridge) handleReply(ctx context.Context, msg contracts.Message) error {
	reply, ok := msg.(contracts.Reply)
	if !ok {
		return fmt.Errorf("received non-reply message: %T", msg)
	}

	correlationID := msg.GetCorrelationID()
	if correlationID == "" {
		return fmt.Errorf("reply missing correlation ID")
	}

	b.mu.RLock()
	pending, exists := b.pendingRequests[correlationID]
	b.mu.RUnlock()

	if !exists {
		// Request may have timed out or been cleaned up
		return nil
	}

	// Send reply to waiting goroutine
	select {
	case pending.ResponseCh <- reply:
		return nil
	case <-pending.Context.Done():
		return nil // Request was cancelled
	default:
		return fmt.Errorf("failed to deliver reply for correlation ID: %s", correlationID)
	}
}

// cleanupRoutine periodically removes expired requests
func (b *SyncAsyncBridge) cleanupRoutine() {
	for {
		select {
		case <-b.cleanupTicker.C:
			b.cleanupExpiredRequests()
		case <-b.done:
			return
		}
	}
}

// cleanupExpiredRequests removes requests that have exceeded their timeout
func (b *SyncAsyncBridge) cleanupExpiredRequests() {
	now := time.Now()
	var expiredIDs []string

	b.mu.RLock()
	for id, req := range b.pendingRequests {
		if now.After(req.Timeout) {
			expiredIDs = append(expiredIDs, id)
		}
	}
	b.mu.RUnlock()

	if len(expiredIDs) > 0 {
		b.mu.Lock()
		for _, id := range expiredIDs {
			if req, exists := b.pendingRequests[id]; exists {
				req.Cancel() // Cancel the request context
				delete(b.pendingRequests, id)
			}
		}
		b.mu.Unlock()
	}
}

// GetPendingRequestCount returns the number of pending requests
func (b *SyncAsyncBridge) GetPendingRequestCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.pendingRequests)
}

// Close shuts down the bridge and cleans up resources
func (b *SyncAsyncBridge) Close() error {
	// Stop cleanup routine
	close(b.done)
	b.cleanupTicker.Stop()

	// Cancel all pending requests
	b.mu.Lock()
	for _, req := range b.pendingRequests {
		req.Cancel()
	}
	b.pendingRequests = make(map[string]*PendingRequest)
	b.mu.Unlock()

	// Unsubscribe from reply queue
	return b.subscriber.Unsubscribe(b.replyQueue)
}
