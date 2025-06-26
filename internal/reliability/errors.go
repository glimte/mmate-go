package reliability

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// Circuit breaker errors
	ErrCircuitOpen         = errors.New("circuit breaker: circuit is open")
	ErrCircuitHalfOpenLimit = errors.New("circuit breaker: half-open request limit reached")
	ErrUnknownState        = errors.New("circuit breaker: unknown state")
	
	// Retry errors
	ErrMaxRetriesExceeded  = errors.New("retry: maximum attempts exceeded")
	ErrRetryTimeout        = errors.New("retry: operation timeout")
	ErrNonRetryable        = errors.New("retry: error is not retryable")
	
	// Dead letter queue errors
	ErrDLQFull             = errors.New("dlq: dead letter queue is full")
	ErrDLQProcessingFailed = errors.New("dlq: failed to process dead letter")
	ErrInvalidDLQMessage   = errors.New("dlq: invalid dead letter message")
	
	// Error store errors
	ErrErrorNotFound       = errors.New("error store: error not found")
	ErrErrorAlreadyResolved = errors.New("error store: error already resolved")
	ErrInvalidErrorData    = errors.New("error store: invalid error data")
)

// CircuitBreakerError represents a circuit breaker error with context
type CircuitBreakerError struct {
	State            State
	Op               string
	Failures         int
	FailureThreshold int
	LastFailure      time.Time
	NextRetry        time.Time
}

func (e *CircuitBreakerError) Error() string {
	switch e.State {
	case StateOpen:
		retryIn := time.Until(e.NextRetry).Round(time.Second)
		return fmt.Sprintf("circuit breaker open: %s blocked (failures=%d/%d, retry in %v)",
			e.Op, e.Failures, e.FailureThreshold, retryIn)
	case StateHalfOpen:
		return fmt.Sprintf("circuit breaker half-open: %s limited", e.Op)
	default:
		return fmt.Sprintf("circuit breaker error: %s in state %v", e.Op, e.State)
	}
}

// RetryError represents a retry operation error
type RetryError struct {
	Op           string
	Attempts     int
	MaxAttempts  int
	LastError    error
	Duration     time.Duration
}

func (e *RetryError) Error() string {
	return fmt.Sprintf("retry failed: %s after %d/%d attempts over %v: %v",
		e.Op, e.Attempts, e.MaxAttempts, e.Duration.Round(time.Millisecond), e.LastError)
}

func (e *RetryError) Unwrap() error {
	return e.LastError
}

// DLQError represents a dead letter queue error
type DLQError struct {
	Queue     string
	MessageID string
	Op        string
	Err       error
	Timestamp time.Time
}

func (e *DLQError) Error() string {
	return fmt.Sprintf("dlq error: %s failed for message %s in queue %s: %v",
		e.Op, e.MessageID, e.Queue, e.Err)
}

func (e *DLQError) Unwrap() error {
	return e.Err
}

// ErrorStoreError represents an error store operation error
type ErrorStoreError struct {
	Op        string
	ErrorID   string
	MessageID string
	Err       error
}

func (e *ErrorStoreError) Error() string {
	if e.MessageID != "" {
		return fmt.Sprintf("error store: %s failed for message %s: %v", e.Op, e.MessageID, e.Err)
	}
	if e.ErrorID != "" {
		return fmt.Sprintf("error store: %s failed for error %s: %v", e.Op, e.ErrorID, e.Err)
	}
	return fmt.Sprintf("error store: %s failed: %v", e.Op, e.Err)
}

func (e *ErrorStoreError) Unwrap() error {
	return e.Err
}

// IsRetryableError checks if an error should be retried
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}
	
	// Check for specific non-retryable errors
	switch {
	case errors.Is(err, ErrNonRetryable):
		return false
	case errors.Is(err, ErrMaxRetriesExceeded):
		return false
	case errors.Is(err, ErrInvalidDLQMessage):
		return false
	case errors.Is(err, ErrInvalidErrorData):
		return false
	}
	
	// Circuit breaker errors might be retryable after timeout
	var cbErr *CircuitBreakerError
	if errors.As(err, &cbErr) {
		return cbErr.State != StateOpen || time.Now().After(cbErr.NextRetry)
	}
	
	return true
}

// ErrorMetrics tracks error metrics
type ErrorMetrics struct {
	TotalErrors      int64
	RetryableErrors  int64
	FatalErrors      int64
	ResolvedErrors   int64
	LastErrorTime    time.Time
	ErrorsByType     map[string]int64
	mu               sync.RWMutex
}

// NewErrorMetrics creates a new error metrics tracker
func NewErrorMetrics() *ErrorMetrics {
	return &ErrorMetrics{
		ErrorsByType: make(map[string]int64),
	}
}

// RecordError records an error in metrics
func (m *ErrorMetrics) RecordError(err error, retryable bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.TotalErrors++
	m.LastErrorTime = time.Now()
	
	if retryable {
		m.RetryableErrors++
	} else {
		m.FatalErrors++
	}
	
	// Track by error type
	errType := fmt.Sprintf("%T", err)
	m.ErrorsByType[errType]++
}

// RecordResolution records error resolution
func (m *ErrorMetrics) RecordResolution() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ResolvedErrors++
}

// GetSnapshot returns a snapshot of current metrics
func (m *ErrorMetrics) GetSnapshot() ErrorMetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	typesCopy := make(map[string]int64)
	for k, v := range m.ErrorsByType {
		typesCopy[k] = v
	}
	
	return ErrorMetricsSnapshot{
		TotalErrors:     m.TotalErrors,
		RetryableErrors: m.RetryableErrors,
		FatalErrors:     m.FatalErrors,
		ResolvedErrors:  m.ResolvedErrors,
		LastErrorTime:   m.LastErrorTime,
		ErrorsByType:    typesCopy,
		Timestamp:       time.Now(),
	}
}

// ErrorMetricsSnapshot represents a point-in-time snapshot of error metrics
type ErrorMetricsSnapshot struct {
	TotalErrors     int64
	RetryableErrors int64
	FatalErrors     int64
	ResolvedErrors  int64
	LastErrorTime   time.Time
	ErrorsByType    map[string]int64
	Timestamp       time.Time
}