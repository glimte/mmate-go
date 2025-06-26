package reliability

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/google/uuid"
)

// ErrorSeverity represents the severity of an error
type ErrorSeverity string

const (
	SeverityLow      ErrorSeverity = "low"
	SeverityMedium   ErrorSeverity = "medium"
	SeverityHigh     ErrorSeverity = "high"
	SeverityCritical ErrorSeverity = "critical"
)

// MessageError represents a message processing error
type MessageError struct {
	ID             string                 `json:"id"`
	MessageID      string                 `json:"messageId"`
	MessageType    string                 `json:"messageType"`
	CorrelationID  string                 `json:"correlationId,omitempty"`
	QueueName      string                 `json:"queueName"`
	Error          string                 `json:"error"`
	ErrorCode      string                 `json:"errorCode,omitempty"`
	Severity       ErrorSeverity          `json:"severity"`
	RetryCount     int                    `json:"retryCount"`
	MaxRetries     int                    `json:"maxRetries"`
	FirstOccurred  time.Time              `json:"firstOccurred"`
	LastOccurred   time.Time              `json:"lastOccurred"`
	NextRetry      *time.Time             `json:"nextRetry,omitempty"`
	Resolved       bool                   `json:"resolved"`
	ResolvedAt     *time.Time             `json:"resolvedAt,omitempty"`
	ResolvedBy     string                 `json:"resolvedBy,omitempty"`
	Message        json.RawMessage        `json:"message"`
	Headers        map[string]interface{} `json:"headers,omitempty"`
	StackTrace     string                 `json:"stackTrace,omitempty"`
	AdditionalData map[string]interface{} `json:"additionalData,omitempty"`
}

// MessageErrorStore provides persistent storage for message errors
type MessageErrorStore interface {
	// Store saves a message error
	Store(ctx context.Context, error *MessageError) error

	// Get retrieves a message error by ID
	Get(ctx context.Context, id string) (*MessageError, error)

	// GetByMessageID retrieves all errors for a message
	GetByMessageID(ctx context.Context, messageID string) ([]*MessageError, error)

	// GetUnresolved retrieves all unresolved errors
	GetUnresolved(ctx context.Context, limit int) ([]*MessageError, error)

	// GetByQueue retrieves errors for a specific queue
	GetByQueue(ctx context.Context, queueName string, limit int) ([]*MessageError, error)

	// Update updates an existing error
	Update(ctx context.Context, error *MessageError) error

	// Resolve marks an error as resolved
	Resolve(ctx context.Context, id string, resolvedBy string) error

	// Delete removes an error
	Delete(ctx context.Context, id string) error

	// GetStats returns error statistics
	GetStats(ctx context.Context) (*ErrorStats, error)

	// Cleanup removes old resolved errors
	Cleanup(ctx context.Context, olderThan time.Duration) (int, error)
}

// ErrorStats provides statistics about stored errors
type ErrorStats struct {
	TotalErrors      int                   `json:"totalErrors"`
	UnresolvedErrors int                   `json:"unresolvedErrors"`
	ResolvedErrors   int                   `json:"resolvedErrors"`
	ErrorsByQueue    map[string]int        `json:"errorsByQueue"`
	ErrorsBySeverity map[ErrorSeverity]int `json:"errorsBySeverity"`
	ErrorsByType     map[string]int        `json:"errorsByType"`
	LastUpdated      time.Time             `json:"lastUpdated"`
}

// InMemoryMessageErrorStore provides in-memory error storage
type InMemoryMessageErrorStore struct {
	errors      map[string]*MessageError
	byMessageID map[string][]string
	byQueue     map[string][]string
	mu          sync.RWMutex
}

// NewInMemoryMessageErrorStore creates a new in-memory error store
func NewInMemoryMessageErrorStore() *InMemoryMessageErrorStore {
	return &InMemoryMessageErrorStore{
		errors:      make(map[string]*MessageError),
		byMessageID: make(map[string][]string),
		byQueue:     make(map[string][]string),
	}
}

// Store saves a message error
func (s *InMemoryMessageErrorStore) Store(ctx context.Context, error *MessageError) error {
	if error == nil {
		return fmt.Errorf("error cannot be nil")
	}

	if error.ID == "" {
		error.ID = uuid.New().String()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Store the error
	s.errors[error.ID] = error

	// Update indexes
	if error.MessageID != "" {
		s.byMessageID[error.MessageID] = append(s.byMessageID[error.MessageID], error.ID)
	}

	if error.QueueName != "" {
		s.byQueue[error.QueueName] = append(s.byQueue[error.QueueName], error.ID)
	}

	return nil
}

// Get retrieves a message error by ID
func (s *InMemoryMessageErrorStore) Get(ctx context.Context, id string) (*MessageError, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	error, exists := s.errors[id]
	if !exists {
		return nil, fmt.Errorf("error not found: %s", id)
	}

	// Return a copy to prevent external modifications
	errorCopy := *error
	return &errorCopy, nil
}

// GetByMessageID retrieves all errors for a message
func (s *InMemoryMessageErrorStore) GetByMessageID(ctx context.Context, messageID string) ([]*MessageError, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	errorIDs, exists := s.byMessageID[messageID]
	if !exists {
		return []*MessageError{}, nil
	}

	errors := make([]*MessageError, 0, len(errorIDs))
	for _, id := range errorIDs {
		if error, exists := s.errors[id]; exists {
			errorCopy := *error
			errors = append(errors, &errorCopy)
		}
	}

	return errors, nil
}

// GetUnresolved retrieves all unresolved errors
func (s *InMemoryMessageErrorStore) GetUnresolved(ctx context.Context, limit int) ([]*MessageError, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	errors := make([]*MessageError, 0)
	count := 0

	for _, error := range s.errors {
		if !error.Resolved {
			errorCopy := *error
			errors = append(errors, &errorCopy)
			count++

			if limit > 0 && count >= limit {
				break
			}
		}
	}

	return errors, nil
}

// GetByQueue retrieves errors for a specific queue
func (s *InMemoryMessageErrorStore) GetByQueue(ctx context.Context, queueName string, limit int) ([]*MessageError, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	errorIDs, exists := s.byQueue[queueName]
	if !exists {
		return []*MessageError{}, nil
	}

	errors := make([]*MessageError, 0)
	count := 0

	for _, id := range errorIDs {
		if error, exists := s.errors[id]; exists {
			errorCopy := *error
			errors = append(errors, &errorCopy)
			count++

			if limit > 0 && count >= limit {
				break
			}
		}
	}

	return errors, nil
}

// Update updates an existing error
func (s *InMemoryMessageErrorStore) Update(ctx context.Context, error *MessageError) error {
	if error == nil || error.ID == "" {
		return fmt.Errorf("error and error ID are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.errors[error.ID]; !exists {
		return fmt.Errorf("error not found: %s", error.ID)
	}

	s.errors[error.ID] = error
	return nil
}

// Resolve marks an error as resolved
func (s *InMemoryMessageErrorStore) Resolve(ctx context.Context, id string, resolvedBy string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	error, exists := s.errors[id]
	if !exists {
		return fmt.Errorf("error not found: %s", id)
	}

	now := time.Now()
	error.Resolved = true
	error.ResolvedAt = &now
	error.ResolvedBy = resolvedBy

	return nil
}

// Delete removes an error
func (s *InMemoryMessageErrorStore) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	error, exists := s.errors[id]
	if !exists {
		return fmt.Errorf("error not found: %s", id)
	}

	// Remove from indexes
	if error.MessageID != "" {
		ids := s.byMessageID[error.MessageID]
		s.removeFromIndex(&ids, id)
		s.byMessageID[error.MessageID] = ids
	}

	if error.QueueName != "" {
		ids := s.byQueue[error.QueueName]
		s.removeFromIndex(&ids, id)
		s.byQueue[error.QueueName] = ids
	}

	// Delete the error
	delete(s.errors, id)

	return nil
}

// GetStats returns error statistics
func (s *InMemoryMessageErrorStore) GetStats(ctx context.Context) (*ErrorStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := &ErrorStats{
		TotalErrors:      len(s.errors),
		UnresolvedErrors: 0,
		ResolvedErrors:   0,
		ErrorsByQueue:    make(map[string]int),
		ErrorsBySeverity: make(map[ErrorSeverity]int),
		ErrorsByType:     make(map[string]int),
		LastUpdated:      time.Now(),
	}

	for _, error := range s.errors {
		if error.Resolved {
			stats.ResolvedErrors++
		} else {
			stats.UnresolvedErrors++
		}

		stats.ErrorsByQueue[error.QueueName]++
		stats.ErrorsBySeverity[error.Severity]++
		stats.ErrorsByType[error.MessageType]++
	}

	return stats, nil
}

// Cleanup removes old resolved errors
func (s *InMemoryMessageErrorStore) Cleanup(ctx context.Context, olderThan time.Duration) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-olderThan)
	toDelete := make([]string, 0)

	for id, error := range s.errors {
		if error.Resolved && error.ResolvedAt != nil && error.ResolvedAt.Before(cutoff) {
			toDelete = append(toDelete, id)
		}
	}

	for _, id := range toDelete {
		error := s.errors[id]

		// Remove from indexes
		if error.MessageID != "" {
			ids := s.byMessageID[error.MessageID]
			s.removeFromIndex(&ids, id)
			s.byMessageID[error.MessageID] = ids
		}

		if error.QueueName != "" {
			ids := s.byQueue[error.QueueName]
			s.removeFromIndex(&ids, id)
			s.byQueue[error.QueueName] = ids
		}

		// Delete the error
		delete(s.errors, id)
	}

	return len(toDelete), nil
}

// removeFromIndex removes an ID from a slice
func (s *InMemoryMessageErrorStore) removeFromIndex(slice *[]string, id string) {
	for i, v := range *slice {
		if v == id {
			*slice = append((*slice)[:i], (*slice)[i+1:]...)
			break
		}
	}
}

// MessageErrorRecorder records message errors with context
type MessageErrorRecorder struct {
	store           MessageErrorStore
	queueName       string
	maxRetries      int
	defaultSeverity ErrorSeverity
}

// NewMessageErrorRecorder creates a new error recorder
func NewMessageErrorRecorder(store MessageErrorStore, queueName string, maxRetries int) *MessageErrorRecorder {
	return &MessageErrorRecorder{
		store:           store,
		queueName:       queueName,
		maxRetries:      maxRetries,
		defaultSeverity: SeverityMedium,
	}
}

// RecordError records a message processing error
func (r *MessageErrorRecorder) RecordError(ctx context.Context, msg contracts.Message, err error, retryCount int) error {
	if msg == nil || err == nil {
		return fmt.Errorf("message and error are required")
	}

	// Serialize the message
	msgData, _ := json.Marshal(msg)

	// Determine severity based on retry count
	severity := r.defaultSeverity
	if retryCount >= r.maxRetries {
		severity = SeverityHigh
	} else if retryCount > r.maxRetries/2 {
		severity = SeverityMedium
	}

	now := time.Now()
	messageError := &MessageError{
		ID:            uuid.New().String(),
		MessageID:     msg.GetID(),
		MessageType:   msg.GetType(),
		CorrelationID: msg.GetCorrelationID(),
		QueueName:     r.queueName,
		Error:         err.Error(),
		Severity:      severity,
		RetryCount:    retryCount,
		MaxRetries:    r.maxRetries,
		FirstOccurred: now,
		LastOccurred:  now,
		Message:       msgData,
	}

	// Check if this is an update to existing error
	existingErrors, _ := r.store.GetByMessageID(ctx, msg.GetID())
	for _, existing := range existingErrors {
		if !existing.Resolved && existing.Error == err.Error() {
			// Update existing error
			existing.RetryCount = retryCount
			existing.LastOccurred = now
			existing.Severity = severity
			return r.store.Update(ctx, existing)
		}
	}

	// Store new error
	return r.store.Store(ctx, messageError)
}

// RecordCriticalError records a critical error that should not be retried
func (r *MessageErrorRecorder) RecordCriticalError(ctx context.Context, msg contracts.Message, err error) error {
	if msg == nil || err == nil {
		return fmt.Errorf("message and error are required")
	}

	// Serialize the message
	msgData, _ := json.Marshal(msg)

	now := time.Now()
	messageError := &MessageError{
		ID:            uuid.New().String(),
		MessageID:     msg.GetID(),
		MessageType:   msg.GetType(),
		CorrelationID: msg.GetCorrelationID(),
		QueueName:     r.queueName,
		Error:         err.Error(),
		Severity:      SeverityCritical,
		RetryCount:    0,
		MaxRetries:    0, // No retries for critical errors
		FirstOccurred: now,
		LastOccurred:  now,
		Message:       msgData,
		Resolved:      false,
	}

	return r.store.Store(ctx, messageError)
}
