package rabbitmq

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	// ErrNotConnected is returned when the connection is not established
	ErrNotConnected = errors.New("not connected")
)

// ConnectionStateListener receives connection state change notifications
type ConnectionStateListener interface {
	OnConnected()
	OnDisconnected(err error)
	OnReconnecting(attempt int)
}

// ConnectionManager manages the RabbitMQ connection with automatic reconnection
type ConnectionManager struct {
	url             string
	conn            *amqp.Connection
	mu              sync.RWMutex
	reconnectDelay  time.Duration
	maxRetries      int
	logger          *slog.Logger
	notifyClose     chan *amqp.Error
	isConnected     bool
	done            chan bool
	stateListeners  []ConnectionStateListener
	listenersMu     sync.RWMutex
}

// ConnectionOption configures the ConnectionManager
type ConnectionOption func(*ConnectionManager)

// WithLogger sets the logger
func WithLogger(logger *slog.Logger) ConnectionOption {
	return func(cm *ConnectionManager) {
		cm.logger = logger
	}
}

// WithConnectionLogger is an alias for WithLogger for backwards compatibility
func WithConnectionLogger(logger *slog.Logger) ConnectionOption {
	return WithLogger(logger)
}

// WithReconnectDelay sets the reconnection delay
func WithReconnectDelay(delay time.Duration) ConnectionOption {
	return func(cm *ConnectionManager) {
		cm.reconnectDelay = delay
	}
}

// WithMaxRetries sets the maximum number of reconnection attempts
func WithMaxRetries(retries int) ConnectionOption {
	return func(cm *ConnectionManager) {
		cm.maxRetries = retries
	}
}

// NewConnectionManager creates a new connection manager
func NewConnectionManager(url string, options ...ConnectionOption) *ConnectionManager {
	cm := &ConnectionManager{
		url:            url,
		reconnectDelay: 5 * time.Second,
		maxRetries:     -1, // infinite retries by default
		logger:         slog.Default(),
		done:           make(chan bool),
	}

	for _, opt := range options {
		opt(cm)
	}

	return cm
}

// Connect establishes the initial connection
func (cm *ConnectionManager) Connect(ctx context.Context) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.isConnected {
		return nil
	}

	// Create connection with context timeout
	connCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	connChan := make(chan *amqp.Connection, 1)
	errChan := make(chan error, 1)

	go func() {
		conn, err := amqp.Dial(cm.url)
		if err != nil {
			errChan <- err
			return
		}
		connChan <- conn
	}()

	select {
	case conn := <-connChan:
		cm.conn = conn
		cm.isConnected = true
		cm.notifyClose = make(chan *amqp.Error)
		cm.conn.NotifyClose(cm.notifyClose)

		cm.logger.Info("connected to RabbitMQ", 
			"url", SanitizeURL(cm.url))
		
		// Notify listeners
		cm.notifyConnected()

		// Start reconnection handler
		go cm.handleReconnect()

		return nil

	case err := <-errChan:
		return &ConnectionError{
			Op:        "connect",
			URL:       SanitizeURL(cm.url),
			Err:       err,
			Timestamp: time.Now(),
			Attempts:  1,
		}

	case <-connCtx.Done():
		return &ConnectionError{
			Op:        "connect",
			URL:       SanitizeURL(cm.url),
			Err:       ErrConnectionTimeout,
			Timestamp: time.Now(),
			Attempts:  1,
		}
	}
}

// GetConnection returns the current connection
func (cm *ConnectionManager) GetConnection() (*amqp.Connection, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if !cm.isConnected || cm.conn == nil {
		return nil, ErrConnectionNotReady
	}

	// Check if connection is actually closed
	if cm.conn.IsClosed() {
		return nil, ErrConnectionClosed
	}

	return cm.conn, nil
}

// IsConnected returns the connection status
func (cm *ConnectionManager) IsConnected() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.isConnected
}

// Close closes the connection
func (cm *ConnectionManager) Close() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if !cm.isConnected {
		return nil
	}

	close(cm.done)
	cm.isConnected = false

	if cm.conn != nil {
		err := cm.conn.Close()
		cm.conn = nil
		return err
	}

	return nil
}

// handleReconnect monitors the connection and reconnects if necessary
func (cm *ConnectionManager) handleReconnect() {
	for {
		select {
		case err := <-cm.notifyClose:
			if err != nil {
				cm.logger.Error("connection closed", "error", err)
			}

			cm.mu.Lock()
			cm.isConnected = false
			cm.conn = nil
			cm.mu.Unlock()

			// Notify listeners
			cm.notifyDisconnected(err)

			// Attempt to reconnect
			cm.reconnect()

		case <-cm.done:
			cm.logger.Info("connection manager shutting down")
			return
		}
	}
}

// reconnect attempts to reconnect to RabbitMQ
func (cm *ConnectionManager) reconnect() {
	retries := 0
	startTime := time.Now()

	for {
		select {
		case <-cm.done:
			return
		default:
		}

		if cm.maxRetries > 0 && retries >= cm.maxRetries {
			cm.logger.Error("max reconnection attempts reached",
				"attempts", retries,
				"duration", time.Since(startTime))
			
			// Notify listeners with detailed error
			err := &ConnectionError{
				Op:        "reconnect",
				URL:       SanitizeURL(cm.url),
				Err:       ErrMaxRetriesExceeded,
				Timestamp: time.Now(),
				Attempts:  retries,
			}
			cm.notifyDisconnected(err)
			return
		}

		cm.logger.Info("attempting to reconnect", 
			"attempt", retries+1,
			"maxRetries", cm.maxRetries)
		
		// Notify listeners
		cm.notifyReconnecting(retries + 1)

		// Use exponential backoff with jitter
		delay := cm.calculateBackoff(retries)
		
		// Wait before attempting
		if retries > 0 {
			select {
			case <-time.After(delay):
			case <-cm.done:
				return
			}
		}

		// Attempt connection with timeout
		connCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		connChan := make(chan *amqp.Connection, 1)
		errChan := make(chan error, 1)

		go func() {
			conn, err := amqp.Dial(cm.url)
			if err != nil {
				errChan <- err
				return
			}
			connChan <- conn
		}()

		select {
		case conn := <-connChan:
			cancel()
			
			cm.mu.Lock()
			cm.conn = conn
			cm.isConnected = true
			cm.notifyClose = make(chan *amqp.Error)
			cm.conn.NotifyClose(cm.notifyClose)
			cm.mu.Unlock()

			cm.logger.Info("successfully reconnected to RabbitMQ",
				"attempts", retries+1,
				"duration", time.Since(startTime))
			
			// Notify listeners
			cm.notifyConnected()
			
			// Reset retries for next disconnection
			return
			
		case err := <-errChan:
			cancel()
			cm.logger.Error("reconnection failed", 
				"error", err, 
				"attempt", retries+1,
				"nextRetryIn", delay)
			retries++
			
		case <-connCtx.Done():
			cancel()
			cm.logger.Error("reconnection timeout",
				"attempt", retries+1)
			retries++
			
		case <-cm.done:
			cancel()
			return
		}
	}
}

// AddStateListener adds a connection state listener
func (cm *ConnectionManager) AddStateListener(listener ConnectionStateListener) {
	cm.listenersMu.Lock()
	defer cm.listenersMu.Unlock()
	cm.stateListeners = append(cm.stateListeners, listener)
}

// RemoveStateListener removes a connection state listener
func (cm *ConnectionManager) RemoveStateListener(listener ConnectionStateListener) {
	cm.listenersMu.Lock()
	defer cm.listenersMu.Unlock()
	
	for i, l := range cm.stateListeners {
		if l == listener {
			cm.stateListeners = append(cm.stateListeners[:i], cm.stateListeners[i+1:]...)
			break
		}
	}
}

// notifyConnected notifies all listeners of successful connection
func (cm *ConnectionManager) notifyConnected() {
	cm.listenersMu.RLock()
	defer cm.listenersMu.RUnlock()
	
	for _, listener := range cm.stateListeners {
		go listener.OnConnected()
	}
}

// notifyDisconnected notifies all listeners of disconnection
func (cm *ConnectionManager) notifyDisconnected(err error) {
	cm.listenersMu.RLock()
	defer cm.listenersMu.RUnlock()
	
	for _, listener := range cm.stateListeners {
		go listener.OnDisconnected(err)
	}
}

// notifyReconnecting notifies all listeners of reconnection attempt
func (cm *ConnectionManager) notifyReconnecting(attempt int) {
	cm.listenersMu.RLock()
	defer cm.listenersMu.RUnlock()
	
	for _, listener := range cm.stateListeners {
		go listener.OnReconnecting(attempt)
	}
}

// calculateBackoff calculates the backoff duration with jitter
func (cm *ConnectionManager) calculateBackoff(attempt int) time.Duration {
	// Exponential backoff with jitter
	base := cm.reconnectDelay
	if base == 0 {
		base = 5 * time.Second
	}
	
	// Cap at 5 minutes
	maxDelay := 5 * time.Minute
	
	// Calculate exponential delay
	delay := base * time.Duration(1<<uint(attempt))
	if delay > maxDelay {
		delay = maxDelay
	}
	
	// Add jitter (±25%)
	jitter := time.Duration(float64(delay) * 0.25)
	delay = delay - jitter/2 + time.Duration(time.Now().UnixNano()%int64(jitter))
	
	return delay
}