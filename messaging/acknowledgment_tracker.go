package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/google/uuid"
)

// ProcessingAcknowledment represents an application-level processing acknowledgment
type ProcessingAcknowledment struct {
	CorrelationID   string                 `json:"correlationId"`
	MessageID       string                 `json:"messageId"`
	MessageType     string                 `json:"messageType"`
	Success         bool                   `json:"success"`
	ErrorMessage    string                 `json:"errorMessage,omitempty"`
	ProcessingTime  time.Duration          `json:"processingTime"`
	ProcessedAt     time.Time              `json:"processedAt"`
	ProcessorID     string                 `json:"processorId"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
}

// AckResponse represents a pending acknowledgment response
type AckResponse struct {
	CorrelationID string
	ch            chan *ProcessingAcknowledment
	timeout       time.Duration
	createdAt     time.Time
	mu            sync.Mutex
	completed     bool
}

// NewAckResponse creates a new acknowledgment response
func NewAckResponse(correlationID string, timeout time.Duration) *AckResponse {
	return &AckResponse{
		CorrelationID: correlationID,
		ch:            make(chan *ProcessingAcknowledment, 1),
		timeout:       timeout,
		createdAt:     time.Now(),
	}
}

// WaitForAcknowledgment waits for the processing acknowledgment with timeout
func (r *AckResponse) WaitForAcknowledgment(ctx context.Context) (*ProcessingAcknowledment, error) {
	select {
	case ack := <-r.ch:
		return ack, nil
	case <-time.After(r.timeout):
		r.mu.Lock()
		r.completed = true
		r.mu.Unlock()
		return nil, fmt.Errorf("acknowledgment timeout after %v for correlation %s", r.timeout, r.CorrelationID)
	case <-ctx.Done():
		r.mu.Lock()
		r.completed = true
		r.mu.Unlock()
		return nil, ctx.Err()
	}
}

// IsCompleted returns true if the acknowledgment was received or timed out
func (r *AckResponse) IsCompleted() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.completed
}

// complete delivers the acknowledgment
func (r *AckResponse) complete(ack *ProcessingAcknowledment) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	if r.completed {
		return false
	}
	
	r.completed = true
	select {
	case r.ch <- ack:
		return true
	default:
		return false
	}
}

// AcknowledgmentTracker manages application-level acknowledgments
type AcknowledgmentTracker struct {
	publisher        Publisher
	ackQueue         string
	pendingAcks      map[string]*AckResponse
	defaultTimeout   time.Duration
	cleanupInterval  time.Duration
	logger          *slog.Logger
	mu              sync.RWMutex
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
}

// AcknowledgmentTrackerOptions configures the acknowledgment tracker
type AcknowledgmentTrackerOptions struct {
	AckQueue        string
	DefaultTimeout  time.Duration
	CleanupInterval time.Duration
	Logger          *slog.Logger
}

// NewAcknowledgmentTracker creates a new acknowledgment tracker
func NewAcknowledgmentTracker(publisher Publisher, opts *AcknowledgmentTrackerOptions) *AcknowledgmentTracker {
	if opts == nil {
		opts = &AcknowledgmentTrackerOptions{}
	}

	if opts.AckQueue == "" {
		opts.AckQueue = "mmate.acknowledgments"
	}
	if opts.DefaultTimeout == 0 {
		opts.DefaultTimeout = 30 * time.Second
	}
	if opts.CleanupInterval == 0 {
		opts.CleanupInterval = 5 * time.Minute
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	ctx, cancel := context.WithCancel(context.Background())

	tracker := &AcknowledgmentTracker{
		publisher:       publisher,
		ackQueue:        opts.AckQueue,
		pendingAcks:     make(map[string]*AckResponse),
		defaultTimeout:  opts.DefaultTimeout,
		cleanupInterval: opts.CleanupInterval,
		logger:          opts.Logger,
		ctx:             ctx,
		cancel:          cancel,
	}

	// Start cleanup routine
	tracker.wg.Add(1)
	go tracker.cleanupRoutine()

	return tracker
}

// SendWithAck sends a message and returns an acknowledgment response
func (t *AcknowledgmentTracker) SendWithAck(ctx context.Context, msg contracts.Message, options ...PublishOption) (*AckResponse, error) {
	// Generate correlation ID if not present
	correlationID := msg.GetCorrelationID()
	if correlationID == "" {
		correlationID = uuid.New().String()
		msg.SetCorrelationID(correlationID)
	}

	// Create acknowledgment response
	ackResponse := NewAckResponse(correlationID, t.defaultTimeout)

	// Register pending acknowledgment
	t.mu.Lock()
	t.pendingAcks[correlationID] = ackResponse
	t.mu.Unlock()

	t.logger.Debug("Registered pending acknowledgment",
		"correlationId", correlationID,
		"messageId", msg.GetID())

	// Add reply-to header for acknowledgment
	options = append(options, WithReplyTo(t.ackQueue))

	// Publish message
	err := t.publisher.Publish(ctx, msg, options...)
	if err != nil {
		// Remove pending ack on publish failure
		t.mu.Lock()
		delete(t.pendingAcks, correlationID)
		t.mu.Unlock()
		return nil, fmt.Errorf("failed to publish message with ack: %w", err)
	}

	t.logger.Info("Message sent with acknowledgment tracking",
		"messageId", msg.GetID(),
		"correlationId", correlationID)

	return ackResponse, nil
}

// HandleAcknowledgment processes an incoming acknowledgment
func (t *AcknowledgmentTracker) HandleAcknowledgment(ctx context.Context, ackMsg contracts.Message) error {
	// Deserialize acknowledgment
	var ack ProcessingAcknowledment
	if err := json.Unmarshal([]byte(ackMsg.GetType()), &ack); err != nil {
		// Try to extract from message content if type parsing fails
		if ackMessage, ok := ackMsg.(*AcknowledgmentMessage); ok {
			ack = ProcessingAcknowledment{
				CorrelationID:  ackMessage.CorrelationID,
				MessageID:      ackMessage.MessageID,
				MessageType:    ackMessage.MessageType,
				Success:        ackMessage.Success,
				ErrorMessage:   ackMessage.ErrorMessage,
				ProcessingTime: ackMessage.ProcessingTime,
				ProcessedAt:    ackMessage.ProcessedAt,
				ProcessorID:    ackMessage.ProcessorID,
				Metadata:       ackMessage.Metadata,
			}
		} else {
			return fmt.Errorf("failed to deserialize acknowledgment: %w", err)
		}
	}

	correlationID := ack.CorrelationID
	if correlationID == "" {
		correlationID = ackMsg.GetCorrelationID()
	}

	if correlationID == "" {
		return fmt.Errorf("acknowledgment missing correlation ID")
	}

	// Find pending acknowledgment
	t.mu.Lock()
	ackResponse, exists := t.pendingAcks[correlationID]
	if exists {
		delete(t.pendingAcks, correlationID)
	}
	t.mu.Unlock()

	if !exists {
		t.logger.Warn("Received acknowledgment for unknown correlation ID",
			"correlationId", correlationID)
		return nil
	}

	// Complete the acknowledgment
	if ackResponse.complete(&ack) {
		t.logger.Info("Acknowledgment received",
			"correlationId", correlationID,
			"success", ack.Success,
			"processingTime", ack.ProcessingTime)
	} else {
		t.logger.Warn("Failed to deliver acknowledgment (already completed)",
			"correlationId", correlationID)
	}

	return nil
}

// GetPendingAcksCount returns the number of pending acknowledgments
func (t *AcknowledgmentTracker) GetPendingAcksCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.pendingAcks)
}

// cleanupRoutine removes expired pending acknowledgments
func (t *AcknowledgmentTracker) cleanupRoutine() {
	defer t.wg.Done()
	
	ticker := time.NewTicker(t.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			t.cleanup()
		case <-t.ctx.Done():
			return
		}
	}
}

// cleanup removes expired acknowledgments
func (t *AcknowledgmentTracker) cleanup() {
	now := time.Now()
	var expired []string

	t.mu.RLock()
	for correlationID, ackResponse := range t.pendingAcks {
		if now.Sub(ackResponse.createdAt) > ackResponse.timeout*2 {
			expired = append(expired, correlationID)
		}
	}
	t.mu.RUnlock()

	if len(expired) > 0 {
		t.mu.Lock()
		for _, correlationID := range expired {
			delete(t.pendingAcks, correlationID)
		}
		t.mu.Unlock()

		t.logger.Info("Cleaned up expired acknowledgments", "count", len(expired))
	}
}

// Close shuts down the acknowledgment tracker
func (t *AcknowledgmentTracker) Close() error {
	t.cancel()
	t.wg.Wait()

	// Complete any remaining pending acknowledgments with timeout error
	t.mu.Lock()
	for correlationID, ackResponse := range t.pendingAcks {
		ackResponse.complete(&ProcessingAcknowledment{
			CorrelationID: correlationID,
			Success:       false,
			ErrorMessage:  "acknowledgment tracker closed",
			ProcessedAt:   time.Now(),
		})
	}
	t.pendingAcks = make(map[string]*AckResponse)
	t.mu.Unlock()

	return nil
}

// AcknowledgmentMessage represents an acknowledgment message
type AcknowledgmentMessage struct {
	contracts.BaseMessage
	CorrelationID   string                 `json:"correlationId"`
	MessageID       string                 `json:"messageId"`
	MessageType     string                 `json:"messageType"`
	Success         bool                   `json:"success"`
	ErrorMessage    string                 `json:"errorMessage,omitempty"`
	ProcessingTime  time.Duration          `json:"processingTime"`
	ProcessedAt     time.Time              `json:"processedAt"`
	ProcessorID     string                 `json:"processorId"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
}

// NewAcknowledgmentMessage creates a new acknowledgment message
func NewAcknowledgmentMessage(correlationID, messageID, messageType string, success bool) *AcknowledgmentMessage {
	return &AcknowledgmentMessage{
		BaseMessage: contracts.BaseMessage{
			ID:            uuid.New().String(),
			Type:          "ProcessingAcknowledgment",
			Timestamp:     time.Now(),
			CorrelationID: correlationID,
		},
		CorrelationID: correlationID,
		MessageID:     messageID,
		MessageType:   messageType,
		Success:       success,
		ProcessedAt:   time.Now(),
	}
}

// IsSuccess implements the Reply interface
func (a *AcknowledgmentMessage) IsSuccess() bool {
	return a.Success
}

// GetError implements the Reply interface
func (a *AcknowledgmentMessage) GetError() error {
	if a.ErrorMessage == "" {
		return nil
	}
	return fmt.Errorf(a.ErrorMessage)
}