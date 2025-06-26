package rabbitmq

import (
	"errors"
	"fmt"
	"time"
)

var (
	// Connection errors
	ErrConnectionClosed     = errors.New("rabbitmq: connection is closed")
	ErrConnectionNotReady   = errors.New("rabbitmq: connection not ready")
	ErrMaxRetriesExceeded   = errors.New("rabbitmq: maximum reconnection attempts exceeded")
	ErrConnectionTimeout    = errors.New("rabbitmq: connection timeout")
	
	// Channel errors
	ErrChannelClosed        = errors.New("rabbitmq: channel is closed")
	ErrChannelPoolClosed    = errors.New("rabbitmq: channel pool is closed")
	ErrChannelPoolExhausted = errors.New("rabbitmq: channel pool exhausted")
	ErrChannelCreationFailed = errors.New("rabbitmq: failed to create channel")
	
	// Publisher errors
	ErrPublisherClosed      = errors.New("rabbitmq: publisher is closed")
	ErrPublishTimeout       = errors.New("rabbitmq: publish timeout")
	ErrPublishNotConfirmed  = errors.New("rabbitmq: publish not confirmed")
	ErrMandatoryFailed      = errors.New("rabbitmq: mandatory publish failed")
	
	// Consumer errors
	ErrConsumerClosed       = errors.New("rabbitmq: consumer is closed")
	ErrConsumerCancelled    = errors.New("rabbitmq: consumer cancelled")
	ErrInvalidDelivery      = errors.New("rabbitmq: invalid delivery")
	
	// Topology errors
	ErrTopologyDeclarationFailed = errors.New("rabbitmq: topology declaration failed")
	ErrInvalidTopology          = errors.New("rabbitmq: invalid topology configuration")
	
	// General errors
	ErrInvalidConfiguration = errors.New("rabbitmq: invalid configuration")
	ErrOperationCancelled   = errors.New("rabbitmq: operation cancelled")
)

// ConnectionError represents a connection-related error
type ConnectionError struct {
	Op         string    // Operation that failed
	URL        string    // Connection URL (sanitized)
	Err        error     // Underlying error
	Timestamp  time.Time // When the error occurred
	Attempts   int       // Number of attempts made
}

func (e *ConnectionError) Error() string {
	if e.Attempts > 0 {
		return fmt.Sprintf("rabbitmq connection error: %s failed after %d attempts: %v", e.Op, e.Attempts, e.Err)
	}
	return fmt.Sprintf("rabbitmq connection error: %s failed: %v", e.Op, e.Err)
}

func (e *ConnectionError) Unwrap() error {
	return e.Err
}

// ChannelError represents a channel-related error
type ChannelError struct {
	Op        string    // Operation that failed
	ChannelID string    // Channel identifier
	Err       error     // Underlying error
	Timestamp time.Time // When the error occurred
}

func (e *ChannelError) Error() string {
	return fmt.Sprintf("rabbitmq channel error: %s on channel %s: %v", e.Op, e.ChannelID, e.Err)
}

func (e *ChannelError) Unwrap() error {
	return e.Err
}

// PublishError represents a publish operation error
type PublishError struct {
	Exchange   string    // Target exchange
	RoutingKey string    // Routing key used
	Mandatory  bool      // Whether mandatory flag was set
	Err        error     // Underlying error
	Timestamp  time.Time // When the error occurred
}

func (e *PublishError) Error() string {
	return fmt.Sprintf("rabbitmq publish error: failed to publish to %s/%s (mandatory=%v): %v", 
		e.Exchange, e.RoutingKey, e.Mandatory, e.Err)
}

func (e *PublishError) Unwrap() error {
	return e.Err
}

// ConsumerError represents a consumer-related error
type ConsumerError struct {
	Queue        string    // Queue name
	ConsumerTag  string    // Consumer tag
	Op           string    // Operation that failed
	Err          error     // Underlying error
	Timestamp    time.Time // When the error occurred
}

func (e *ConsumerError) Error() string {
	return fmt.Sprintf("rabbitmq consumer error: %s failed for consumer %s on queue %s: %v",
		e.Op, e.ConsumerTag, e.Queue, e.Err)
}

func (e *ConsumerError) Unwrap() error {
	return e.Err
}

// TopologyError represents a topology-related error
type TopologyError struct {
	Component string    // Component type (exchange, queue, binding)
	Name      string    // Component name
	Op        string    // Operation that failed
	Err       error     // Underlying error
	Timestamp time.Time // When the error occurred
}

func (e *TopologyError) Error() string {
	return fmt.Sprintf("rabbitmq topology error: failed to %s %s '%s': %v",
		e.Op, e.Component, e.Name, e.Err)
}

func (e *TopologyError) Unwrap() error {
	return e.Err
}

// IsRetryable determines if an error is retryable
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	
	// Check for specific non-retryable errors
	switch {
	case errors.Is(err, ErrInvalidConfiguration):
		return false
	case errors.Is(err, ErrMaxRetriesExceeded):
		return false
	case errors.Is(err, ErrOperationCancelled):
		return false
	}
	
	// Connection and channel errors are generally retryable
	var connErr *ConnectionError
	if errors.As(err, &connErr) {
		return true
	}
	
	var chanErr *ChannelError
	if errors.As(err, &chanErr) {
		return true
	}
	
	// Default to retryable for unknown errors
	return true
}

// IsFatal determines if an error is fatal and should not be retried
func IsFatal(err error) bool {
	return !IsRetryable(err)
}

// SanitizeURL removes sensitive information from connection URLs
func SanitizeURL(url string) string {
	// Simple implementation - in production, use proper URL parsing
	// to remove password but keep structure
	if len(url) > 20 {
		return url[:10] + "***" + url[len(url)-10:]
	}
	return "***"
}