package reliability

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// MessagePublisher interface for republishing messages
type MessagePublisher interface {
	Publish(ctx context.Context, exchange, routingKey string, body []byte, headers map[string]interface{}) error
}

// DLQHandler handles dead letter queue operations
type DLQHandler struct {
	logger           *slog.Logger
	maxRetries       int
	retryDelay       time.Duration
	errorStore       ErrorStore
	metricsCollector MetricsCollector
	publisher        MessagePublisher
}

// DLQOption configures the DLQ handler
type DLQOption func(*DLQHandler)

// WithDLQLogger sets the logger
func WithDLQLogger(logger *slog.Logger) DLQOption {
	return func(h *DLQHandler) {
		h.logger = logger
	}
}

// WithDLQMaxRetries sets the maximum retries for DLQ messages
func WithDLQMaxRetries(retries int) DLQOption {
	return func(h *DLQHandler) {
		h.maxRetries = retries
	}
}

// WithDLQRetryDelay sets the retry delay
func WithDLQRetryDelay(delay time.Duration) DLQOption {
	return func(h *DLQHandler) {
		h.retryDelay = delay
	}
}

// WithErrorStore sets the error store for persisting failed messages
func WithErrorStore(store ErrorStore) DLQOption {
	return func(h *DLQHandler) {
		h.errorStore = store
	}
}

// WithMetricsCollector sets the metrics collector
func WithMetricsCollector(collector MetricsCollector) DLQOption {
	return func(h *DLQHandler) {
		h.metricsCollector = collector
	}
}

// WithPublisher sets the message publisher
func WithPublisher(publisher MessagePublisher) DLQOption {
	return func(h *DLQHandler) {
		h.publisher = publisher
	}
}

// NewDLQHandler creates a new DLQ handler
func NewDLQHandler(options ...DLQOption) *DLQHandler {
	h := &DLQHandler{
		logger:     slog.Default(),
		maxRetries: 3,
		retryDelay: time.Minute,
	}

	for _, opt := range options {
		opt(h)
	}

	return h
}

// ProcessDLQMessage processes a message from the DLQ
func (h *DLQHandler) ProcessDLQMessage(ctx context.Context, msg amqp.Delivery) error {
	// Extract metadata from headers
	metadata := h.extractMetadata(msg)
	
	h.logger.Info("Processing DLQ message",
		"messageId", msg.MessageId,
		"retryCount", metadata.RetryCount,
		"originalQueue", metadata.OriginalQueue,
		"lastError", metadata.LastError,
	)

	// Check if we should retry
	if metadata.RetryCount < h.maxRetries {
		return h.retryMessage(ctx, msg, metadata)
	}

	// Max retries exceeded, store in error store if available
	if h.errorStore != nil {
		if err := h.storeFailedMessage(ctx, msg, metadata); err != nil {
			h.logger.Error("Failed to store message in error store",
				"error", err,
				"messageId", msg.MessageId,
			)
		}
	}

	// Record metrics if collector is available
	if h.metricsCollector != nil {
		h.metricsCollector.RecordDLQMessage(metadata.OriginalQueue, "max_retries_exceeded")
	}

	// Acknowledge the message to remove it from DLQ
	return msg.Ack(false)
}

// ProcessDLQBatch processes multiple DLQ messages
func (h *DLQHandler) ProcessDLQBatch(ctx context.Context, messages []amqp.Delivery) error {
	for _, msg := range messages {
		if err := h.ProcessDLQMessage(ctx, msg); err != nil {
			h.logger.Error("Failed to process DLQ message",
				"error", err,
				"messageId", msg.MessageId,
			)
			// Continue processing other messages
		}
	}
	return nil
}

// extractMetadata extracts metadata from message headers
func (h *DLQHandler) extractMetadata(msg amqp.Delivery) DLQMetadata {
	metadata := DLQMetadata{
		OriginalQueue: h.getHeaderString(msg.Headers, "x-original-queue"),
		LastError:     h.getHeaderString(msg.Headers, "x-last-error"),
		RetryCount:    h.getHeaderInt(msg.Headers, "x-retry-count"),
		FirstDeathAt:  h.getHeaderTime(msg.Headers, "x-first-death-time"),
	}

	// Extract from x-death header if available
	if xDeath, ok := msg.Headers["x-death"].([]interface{}); ok && len(xDeath) > 0 {
		if death, ok := xDeath[0].(amqp.Table); ok {
			if queue, ok := death["queue"].(string); ok && metadata.OriginalQueue == "" {
				metadata.OriginalQueue = queue
			}
			if reason, ok := death["reason"].(string); ok && metadata.LastError == "" {
				metadata.LastError = reason
			}
			if count, ok := death["count"].(int64); ok {
				metadata.RetryCount = int(count)
			}
		}
	}

	return metadata
}

// retryMessage attempts to retry a message
func (h *DLQHandler) retryMessage(ctx context.Context, msg amqp.Delivery, metadata DLQMetadata) error {
	// Wait before retry
	select {
	case <-time.After(h.retryDelay):
	case <-ctx.Done():
		return ctx.Err()
	}

	// Update retry count
	metadata.RetryCount++
	
	// Update headers
	headers := msg.Headers
	if headers == nil {
		headers = amqp.Table{}
	}
	headers["x-retry-count"] = metadata.RetryCount
	headers["x-last-retry-time"] = time.Now().Unix()

	// Republish message if publisher is available
	if h.publisher != nil {
		err := h.publisher.Publish(ctx, msg.Exchange, metadata.OriginalQueue, msg.Body, headers)
		if err != nil {
			h.logger.Error("Failed to republish message",
				"error", err,
				"messageId", msg.MessageId,
				"retryCount", metadata.RetryCount,
			)
			return fmt.Errorf("failed to republish: %w", err)
		}
		
		h.logger.Info("Message republished for retry",
			"messageId", msg.MessageId,
			"retryCount", metadata.RetryCount,
			"originalQueue", metadata.OriginalQueue,
		)
	} else {
		h.logger.Warn("No publisher configured, cannot retry message",
			"messageId", msg.MessageId,
			"retryCount", metadata.RetryCount,
		)
	}

	// Record metrics
	if h.metricsCollector != nil {
		h.metricsCollector.RecordDLQMessage(metadata.OriginalQueue, "retried")
	}

	// Acknowledge the DLQ message
	return msg.Ack(false)
}

// storeFailedMessage stores a permanently failed message
func (h *DLQHandler) storeFailedMessage(ctx context.Context, msg amqp.Delivery, metadata DLQMetadata) error {
	failedMessage := FailedMessage{
		ID:            msg.MessageId,
		Queue:         metadata.OriginalQueue,
		Exchange:      msg.Exchange,
		RoutingKey:    msg.RoutingKey,
		Headers:       msg.Headers,
		Body:          msg.Body,
		Error:         metadata.LastError,
		RetryCount:    metadata.RetryCount,
		FirstFailedAt: metadata.FirstDeathAt,
		LastFailedAt:  time.Now(),
		ContentType:   msg.ContentType,
		CorrelationID: msg.CorrelationId,
	}

	return h.errorStore.Store(ctx, failedMessage)
}

// getHeaderString safely extracts a string from headers
func (h *DLQHandler) getHeaderString(headers amqp.Table, key string) string {
	if headers == nil {
		return ""
	}
	if val, ok := headers[key].(string); ok {
		return val
	}
	return ""
}

// getHeaderInt safely extracts an int from headers
func (h *DLQHandler) getHeaderInt(headers amqp.Table, key string) int {
	if headers == nil {
		return 0
	}
	switch val := headers[key].(type) {
	case int:
		return val
	case int32:
		return int(val)
	case int64:
		return int(val)
	case float64:
		return int(val)
	}
	return 0
}

// getHeaderTime safely extracts a time from headers
func (h *DLQHandler) getHeaderTime(headers amqp.Table, key string) time.Time {
	if headers == nil {
		return time.Time{}
	}
	switch val := headers[key].(type) {
	case int64:
		return time.Unix(val, 0)
	case float64:
		return time.Unix(int64(val), 0)
	case time.Time:
		return val
	}
	return time.Time{}
}

// DLQMetadata contains metadata about a DLQ message
type DLQMetadata struct {
	OriginalQueue string
	LastError     string
	RetryCount    int
	FirstDeathAt  time.Time
}

// ErrorStore interface for persisting failed messages
type ErrorStore interface {
	Store(ctx context.Context, message FailedMessage) error
	Get(ctx context.Context, id string) (*FailedMessage, error)
	List(ctx context.Context, filter ErrorFilter) ([]FailedMessage, error)
	Delete(ctx context.Context, id string) error
}

// FailedMessage represents a message that failed processing
type FailedMessage struct {
	ID            string
	Queue         string
	Exchange      string
	RoutingKey    string
	Headers       amqp.Table
	Body          []byte
	Error         string
	RetryCount    int
	FirstFailedAt time.Time
	LastFailedAt  time.Time
	ContentType   string
	CorrelationID string
}

// ErrorFilter filters failed messages
type ErrorFilter struct {
	Queue      string
	StartTime  time.Time
	EndTime    time.Time
	MaxResults int
}

// MetricsCollector interface for collecting DLQ metrics
type MetricsCollector interface {
	RecordDLQMessage(queue string, action string)
	RecordRetryAttempt(queue string, success bool)
	RecordErrorStoreOperation(operation string, success bool)
}

// InMemoryErrorStore provides a simple in-memory error store
type InMemoryErrorStore struct {
	messages map[string]FailedMessage
}

// NewInMemoryErrorStore creates a new in-memory error store
func NewInMemoryErrorStore() *InMemoryErrorStore {
	return &InMemoryErrorStore{
		messages: make(map[string]FailedMessage),
	}
}

// Store implements ErrorStore
func (s *InMemoryErrorStore) Store(ctx context.Context, message FailedMessage) error {
	s.messages[message.ID] = message
	return nil
}

// Get implements ErrorStore
func (s *InMemoryErrorStore) Get(ctx context.Context, id string) (*FailedMessage, error) {
	msg, ok := s.messages[id]
	if !ok {
		return nil, fmt.Errorf("message not found: %s", id)
	}
	return &msg, nil
}

// List implements ErrorStore
func (s *InMemoryErrorStore) List(ctx context.Context, filter ErrorFilter) ([]FailedMessage, error) {
	var results []FailedMessage
	
	for _, msg := range s.messages {
		// Apply filters
		if filter.Queue != "" && msg.Queue != filter.Queue {
			continue
		}
		if !filter.StartTime.IsZero() && msg.LastFailedAt.Before(filter.StartTime) {
			continue
		}
		if !filter.EndTime.IsZero() && msg.LastFailedAt.After(filter.EndTime) {
			continue
		}
		
		results = append(results, msg)
		
		if filter.MaxResults > 0 && len(results) >= filter.MaxResults {
			break
		}
	}
	
	return results, nil
}

// Delete implements ErrorStore
func (s *InMemoryErrorStore) Delete(ctx context.Context, id string) error {
	delete(s.messages, id)
	return nil
}

// DLQMessage represents a message with DLQ-specific information
type DLQMessage struct {
	amqp.Delivery
	Metadata DLQMetadata
}

// MarshalJSON marshals a failed message to JSON
func (f FailedMessage) MarshalJSON() ([]byte, error) {
	type Alias FailedMessage
	return json.Marshal(&struct {
		*Alias
		Body string `json:"body"`
	}{
		Alias: (*Alias)(&f),
		Body:  string(f.Body),
	})
}