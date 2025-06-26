package messaging

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/google/uuid"
)

// RequestStatus represents the status of a request
type RequestStatus string

const (
	RequestStatusPending   RequestStatus = "pending"
	RequestStatusSent      RequestStatus = "sent"
	RequestStatusReceived  RequestStatus = "received"
	RequestStatusTimeout   RequestStatus = "timeout"
	RequestStatusFailed    RequestStatus = "failed"
	RequestStatusCompleted RequestStatus = "completed"
)

// TrackedRequest represents a request being tracked
type TrackedRequest struct {
	ID            string
	CorrelationID string
	Status        RequestStatus
	Request       contracts.Message
	Response      contracts.Reply
	Error         error
	SentAt        time.Time
	ReceivedAt    *time.Time
	Timeout       time.Duration
}

// RequestTracker tracks the status of requests
type RequestTracker interface {
	TrackRequest(request *TrackedRequest) error
	UpdateStatus(correlationID string, status RequestStatus) error
	GetRequest(correlationID string) (*TrackedRequest, error)
	GetActiveRequests() []*TrackedRequest
	CompleteRequest(correlationID string, response contracts.Reply) error
	FailRequest(correlationID string, err error) error
	CleanupExpired() int
}

// RequestReplyClient sends requests and waits for replies
type RequestReplyClient interface {
	SendAndReceive(ctx context.Context, request contracts.Message, timeout time.Duration) (contracts.Reply, error)
	SendCommand(ctx context.Context, command contracts.Command, timeout time.Duration) (contracts.Reply, error)
	SendQuery(ctx context.Context, query contracts.Query, timeout time.Duration) (contracts.Reply, error)
	GetTracker() RequestTracker
	Close() error
}

// RequestReplyServer handles incoming requests and sends replies
type RequestReplyServer interface {
	RegisterHandler(messageType string, handler RequestHandler) error
	Start(ctx context.Context) error
	Stop() error
}

// RequestHandler handles incoming requests and returns replies
type RequestHandler interface {
	HandleRequest(ctx context.Context, request contracts.Message) (contracts.Reply, error)
}

// RequestHandlerFunc is a function adapter for RequestHandler
type RequestHandlerFunc func(ctx context.Context, request contracts.Message) (contracts.Reply, error)

func (f RequestHandlerFunc) HandleRequest(ctx context.Context, request contracts.Message) (contracts.Reply, error) {
	return f(ctx, request)
}

// InMemoryRequestTracker provides in-memory request tracking
type InMemoryRequestTracker struct {
	requests map[string]*TrackedRequest
	mu       sync.RWMutex
}

// NewInMemoryRequestTracker creates a new in-memory request tracker
func NewInMemoryRequestTracker() *InMemoryRequestTracker {
	return &InMemoryRequestTracker{
		requests: make(map[string]*TrackedRequest),
	}
}

// TrackRequest adds a request to tracking
func (t *InMemoryRequestTracker) TrackRequest(request *TrackedRequest) error {
	if request == nil {
		return fmt.Errorf("request cannot be nil")
	}
	if request.CorrelationID == "" {
		return fmt.Errorf("correlation ID is required")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.requests[request.CorrelationID] = request
	return nil
}

// UpdateStatus updates the status of a tracked request
func (t *InMemoryRequestTracker) UpdateStatus(correlationID string, status RequestStatus) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	request, exists := t.requests[correlationID]
	if !exists {
		return fmt.Errorf("request not found: %s", correlationID)
	}

	request.Status = status
	return nil
}

// GetRequest retrieves a tracked request
func (t *InMemoryRequestTracker) GetRequest(correlationID string) (*TrackedRequest, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	request, exists := t.requests[correlationID]
	if !exists {
		return nil, fmt.Errorf("request not found: %s", correlationID)
	}

	return request, nil
}

// GetActiveRequests returns all active requests
func (t *InMemoryRequestTracker) GetActiveRequests() []*TrackedRequest {
	t.mu.RLock()
	defer t.mu.RUnlock()

	active := make([]*TrackedRequest, 0)
	for _, req := range t.requests {
		if req.Status == RequestStatusPending || req.Status == RequestStatusSent {
			active = append(active, req)
		}
	}

	return active
}

// CompleteRequest marks a request as completed with response
func (t *InMemoryRequestTracker) CompleteRequest(correlationID string, response contracts.Reply) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	request, exists := t.requests[correlationID]
	if !exists {
		return fmt.Errorf("request not found: %s", correlationID)
	}

	now := time.Now()
	request.Status = RequestStatusCompleted
	request.Response = response
	request.ReceivedAt = &now

	return nil
}

// FailRequest marks a request as failed
func (t *InMemoryRequestTracker) FailRequest(correlationID string, err error) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	request, exists := t.requests[correlationID]
	if !exists {
		return fmt.Errorf("request not found: %s", correlationID)
	}

	request.Status = RequestStatusFailed
	request.Error = err

	return nil
}

// CleanupExpired removes expired requests
func (t *InMemoryRequestTracker) CleanupExpired() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	removed := 0

	for correlationID, req := range t.requests {
		if req.Status == RequestStatusPending || req.Status == RequestStatusSent {
			if now.Sub(req.SentAt) > req.Timeout {
				req.Status = RequestStatusTimeout
				removed++
			}
		}

		// Remove completed/failed requests older than 5 minutes
		if req.Status == RequestStatusCompleted || req.Status == RequestStatusFailed || req.Status == RequestStatusTimeout {
			if now.Sub(req.SentAt) > 5*time.Minute {
				delete(t.requests, correlationID)
			}
		}
	}

	return removed
}

// DefaultRequestReplyClient provides request/reply functionality
type DefaultRequestReplyClient struct {
	publisher   Publisher
	subscriber  Subscriber
	tracker     RequestTracker
	replyQueue  string
	pendingChan map[string]chan contracts.Reply
	mu          sync.RWMutex
	done        chan struct{}
}

// RequestReplyClientOption configures the client
type RequestReplyClientOption func(*RequestReplyClientConfig)

// RequestReplyClientConfig holds client configuration
type RequestReplyClientConfig struct {
	ReplyQueue string
	Tracker    RequestTracker
}

// WithReplyQueue sets the reply queue name
func WithReplyQueue(queue string) RequestReplyClientOption {
	return func(c *RequestReplyClientConfig) {
		c.ReplyQueue = queue
	}
}

// WithRequestTracker sets a custom request tracker
func WithRequestTracker(tracker RequestTracker) RequestReplyClientOption {
	return func(c *RequestReplyClientConfig) {
		c.Tracker = tracker
	}
}

// NewRequestReplyClient creates a new request/reply client
func NewRequestReplyClient(publisher Publisher, subscriber Subscriber, opts ...RequestReplyClientOption) (*DefaultRequestReplyClient, error) {
	config := &RequestReplyClientConfig{
		ReplyQueue: fmt.Sprintf("reply.%s", uuid.New().String()[:8]),
		Tracker:    NewInMemoryRequestTracker(),
	}

	for _, opt := range opts {
		opt(config)
	}

	client := &DefaultRequestReplyClient{
		publisher:   publisher,
		subscriber:  subscriber,
		tracker:     config.Tracker,
		replyQueue:  config.ReplyQueue,
		pendingChan: make(map[string]chan contracts.Reply),
		done:        make(chan struct{}),
	}

	// Subscribe to reply queue
	err := subscriber.Subscribe(context.Background(), config.ReplyQueue,
		"reply", // message type
		MessageHandlerFunc(client.handleReply),
		WithAutoAck(true),
		WithSubscriberExclusive(true),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to reply queue: %w", err)
	}

	// Start cleanup routine
	go client.cleanupRoutine()

	return client, nil
}

// SendAndReceive sends a request and waits for reply
func (c *DefaultRequestReplyClient) SendAndReceive(ctx context.Context, request contracts.Message, timeout time.Duration) (contracts.Reply, error) {
	correlationID := uuid.New().String()
	request.SetCorrelationID(correlationID)

	// Create response channel
	responseChan := make(chan contracts.Reply, 1)

	c.mu.Lock()
	c.pendingChan[correlationID] = responseChan
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pendingChan, correlationID)
		c.mu.Unlock()
	}()

	// Track request
	tracked := &TrackedRequest{
		ID:            uuid.New().String(),
		CorrelationID: correlationID,
		Status:        RequestStatusPending,
		Request:       request,
		SentAt:        time.Now(),
		Timeout:       timeout,
	}

	err := c.tracker.TrackRequest(tracked)
	if err != nil {
		return nil, fmt.Errorf("failed to track request: %w", err)
	}

	// Send request
	err = c.publisher.Publish(ctx, request,
		WithReplyTo(c.replyQueue),
		WithCorrelationID(correlationID),
	)
	if err != nil {
		c.tracker.FailRequest(correlationID, err)
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	c.tracker.UpdateStatus(correlationID, RequestStatusSent)

	// Wait for response
	select {
	case response := <-responseChan:
		c.tracker.CompleteRequest(correlationID, response)
		return response, nil
	case <-time.After(timeout):
		c.tracker.UpdateStatus(correlationID, RequestStatusTimeout)
		return nil, fmt.Errorf("request timeout after %v", timeout)
	case <-ctx.Done():
		c.tracker.FailRequest(correlationID, ctx.Err())
		return nil, ctx.Err()
	}
}

// SendCommand sends a command and waits for reply
func (c *DefaultRequestReplyClient) SendCommand(ctx context.Context, command contracts.Command, timeout time.Duration) (contracts.Reply, error) {
	return c.SendAndReceive(ctx, command, timeout)
}

// SendQuery sends a query and waits for reply
func (c *DefaultRequestReplyClient) SendQuery(ctx context.Context, query contracts.Query, timeout time.Duration) (contracts.Reply, error) {
	return c.SendAndReceive(ctx, query, timeout)
}

// GetTracker returns the request tracker
func (c *DefaultRequestReplyClient) GetTracker() RequestTracker {
	return c.tracker
}

// handleReply processes incoming replies
func (c *DefaultRequestReplyClient) handleReply(ctx context.Context, msg contracts.Message) error {
	reply, ok := msg.(contracts.Reply)
	if !ok {
		return fmt.Errorf("received non-reply message: %T", msg)
	}

	correlationID := msg.GetCorrelationID()
	if correlationID == "" {
		return fmt.Errorf("reply missing correlation ID")
	}

	c.mu.RLock()
	responseChan, exists := c.pendingChan[correlationID]
	c.mu.RUnlock()

	if exists {
		select {
		case responseChan <- reply:
			c.tracker.UpdateStatus(correlationID, RequestStatusReceived)
		default:
			// Channel full or closed
		}
	}

	return nil
}

// cleanupRoutine periodically cleans up expired requests
func (c *DefaultRequestReplyClient) cleanupRoutine() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.tracker.CleanupExpired()
		case <-c.done:
			return
		}
	}
}

// Close shuts down the client
func (c *DefaultRequestReplyClient) Close() error {
	close(c.done)
	return c.subscriber.Unsubscribe(c.replyQueue)
}
