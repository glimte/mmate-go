package reliability

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// State represents the circuit breaker state
type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// StateChangeListener receives circuit breaker state change notifications
type StateChangeListener interface {
	OnStateChange(from, to State, reason string)
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	mu              sync.RWMutex
	state           State
	failures        int
	successes       int
	lastFailureTime time.Time
	totalRequests   int64
	totalFailures   int64
	totalSuccesses  int64

	// Configuration
	failureThreshold   int
	successThreshold   int
	timeout            time.Duration
	halfOpenRequests   int
	currentHalfOpen    int
	name               string
	
	// Listeners
	listeners []StateChangeListener
}

// CircuitBreakerOption configures the circuit breaker
type CircuitBreakerOption func(*CircuitBreaker)

// WithFailureThreshold sets the failure threshold
func WithFailureThreshold(threshold int) CircuitBreakerOption {
	return func(cb *CircuitBreaker) {
		cb.failureThreshold = threshold
	}
}

// WithSuccessThreshold sets the success threshold for half-open state
func WithSuccessThreshold(threshold int) CircuitBreakerOption {
	return func(cb *CircuitBreaker) {
		cb.successThreshold = threshold
	}
}

// WithTimeout sets the timeout for open state
func WithTimeout(timeout time.Duration) CircuitBreakerOption {
	return func(cb *CircuitBreaker) {
		cb.timeout = timeout
	}
}

// WithHalfOpenRequests sets the max requests in half-open state
func WithHalfOpenRequests(requests int) CircuitBreakerOption {
	return func(cb *CircuitBreaker) {
		cb.halfOpenRequests = requests
	}
}

// WithName sets the circuit breaker name for identification
func WithName(name string) CircuitBreakerOption {
	return func(cb *CircuitBreaker) {
		cb.name = name
	}
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(options ...CircuitBreakerOption) *CircuitBreaker {
	cb := &CircuitBreaker{
		state:              StateClosed,
		failureThreshold:   5,
		successThreshold:   3,
		timeout:            30 * time.Second,
		halfOpenRequests:   3,
		name:               "default",
		listeners:          make([]StateChangeListener, 0),
	}

	for _, opt := range options {
		opt(cb)
	}

	return cb
}

// Execute runs a function with circuit breaker protection
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func() error) error {
	// Increment total requests
	cb.mu.Lock()
	cb.totalRequests++
	cb.mu.Unlock()

	if err := cb.canExecute(); err != nil {
		return err
	}

	// Check context before execution
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	err := fn()
	cb.recordResult(err)
	return err
}

// GetState returns the current state
func (cb *CircuitBreaker) GetState() State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// GetStats returns circuit breaker statistics
func (cb *CircuitBreaker) GetStats() (failures, successes int, lastFailure time.Time) {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.failures, cb.successes, cb.lastFailureTime
}

// Reset resets the circuit breaker
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	cb.state = StateClosed
	cb.failures = 0
	cb.successes = 0
	cb.currentHalfOpen = 0
}

// canExecute checks if execution is allowed
func (cb *CircuitBreaker) canExecute() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return nil

	case StateOpen:
		nextRetry := cb.lastFailureTime.Add(cb.timeout)
		if time.Now().After(nextRetry) {
			// Transition to half-open
			oldState := cb.state
			cb.state = StateHalfOpen
			cb.currentHalfOpen = 0
			cb.successes = 0
			cb.notifyStateChange(oldState, cb.state, "timeout expired")
			return nil
		}
		return &CircuitBreakerError{
			State:            cb.state,
			Op:               "execute",
			Failures:         cb.failures,
			FailureThreshold: cb.failureThreshold,
			LastFailure:      cb.lastFailureTime,
			NextRetry:        nextRetry,
		}

	case StateHalfOpen:
		if cb.currentHalfOpen >= cb.halfOpenRequests {
			return &CircuitBreakerError{
				State:            cb.state,
				Op:               "execute",
				Failures:         cb.failures,
				FailureThreshold: cb.failureThreshold,
				LastFailure:      cb.lastFailureTime,
				NextRetry:        time.Now().Add(time.Second), // Retry soon
			}
		}
		cb.currentHalfOpen++
		return nil

	default:
		return ErrUnknownState
	}
}

// recordResult records the result of an execution
func (cb *CircuitBreaker) recordResult(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.failures++
		cb.totalFailures++
		cb.lastFailureTime = time.Now()
		oldState := cb.state

		switch cb.state {
		case StateClosed:
			if cb.failures >= cb.failureThreshold {
				cb.state = StateOpen
				cb.notifyStateChange(oldState, cb.state, 
					fmt.Sprintf("failure threshold reached (%d/%d)", cb.failures, cb.failureThreshold))
			}

		case StateHalfOpen:
			// Single failure in half-open moves back to open
			cb.state = StateOpen
			cb.currentHalfOpen = 0
			cb.notifyStateChange(oldState, cb.state, "failure in half-open state")
		}

		// Don't reset success counter on failure in closed state
		if cb.state != StateClosed {
			cb.successes = 0
		}

	} else {
		cb.successes++
		cb.totalSuccesses++
		oldState := cb.state

		switch cb.state {
		case StateHalfOpen:
			if cb.successes >= cb.successThreshold {
				// Transition to closed
				cb.state = StateClosed
				cb.failures = 0
				cb.currentHalfOpen = 0
				cb.notifyStateChange(oldState, cb.state,
					fmt.Sprintf("success threshold reached (%d/%d)", cb.successes, cb.successThreshold))
			}

		case StateClosed:
			// Reset failure counter on success in closed state
			if cb.failures > 0 {
				cb.failures = 0
			}
		}
	}
}

// AddListener adds a state change listener
func (cb *CircuitBreaker) AddListener(listener StateChangeListener) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.listeners = append(cb.listeners, listener)
}

// RemoveListener removes a state change listener
func (cb *CircuitBreaker) RemoveListener(listener StateChangeListener) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	for i, l := range cb.listeners {
		if l == listener {
			cb.listeners = append(cb.listeners[:i], cb.listeners[i+1:]...)
			break
		}
	}
}

// notifyStateChange notifies all listeners of state change
func (cb *CircuitBreaker) notifyStateChange(from, to State, reason string) {
	// Make a copy of listeners to avoid holding lock during callbacks
	listeners := make([]StateChangeListener, len(cb.listeners))
	copy(listeners, cb.listeners)
	
	// Notify listeners in goroutines to avoid blocking
	for _, listener := range listeners {
		go listener.OnStateChange(from, to, reason)
	}
}

// GetMetrics returns circuit breaker metrics
func (cb *CircuitBreaker) GetMetrics() CircuitBreakerMetrics {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	
	return CircuitBreakerMetrics{
		Name:             cb.name,
		State:            cb.state,
		TotalRequests:    cb.totalRequests,
		TotalFailures:    cb.totalFailures,
		TotalSuccesses:   cb.totalSuccesses,
		CurrentFailures:  cb.failures,
		CurrentSuccesses: cb.successes,
		LastFailureTime:  cb.lastFailureTime,
		Timestamp:        time.Now(),
	}
}

// CircuitBreakerMetrics represents circuit breaker metrics
type CircuitBreakerMetrics struct {
	Name             string
	State            State
	TotalRequests    int64
	TotalFailures    int64
	TotalSuccesses   int64
	CurrentFailures  int
	CurrentSuccesses int
	LastFailureTime  time.Time
	Timestamp        time.Time
}