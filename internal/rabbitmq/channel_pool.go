package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/google/uuid"
)

// ChannelPool manages a pool of AMQP channels
type ChannelPool struct {
	manager      *ConnectionManager
	channels     chan *PooledChannel
	maxSize      int
	minSize      int
	idleTimeout  time.Duration
	mu           sync.Mutex
	closed       bool
	activeCount  int
}

// PooledChannel wraps an AMQP channel with pool metadata
type PooledChannel struct {
	*amqp.Channel
	pool     *ChannelPool
	lastUsed time.Time
	id       string
}

// ChannelPoolOption configures the channel pool
type ChannelPoolOption func(*ChannelPool)

// WithMaxSize sets the maximum pool size
func WithMaxSize(size int) ChannelPoolOption {
	return func(cp *ChannelPool) {
		cp.maxSize = size
	}
}

// WithMinSize sets the minimum pool size
func WithMinSize(size int) ChannelPoolOption {
	return func(cp *ChannelPool) {
		cp.minSize = size
	}
}

// WithIdleTimeout sets the idle timeout for channels
func WithIdleTimeout(timeout time.Duration) ChannelPoolOption {
	return func(cp *ChannelPool) {
		cp.idleTimeout = timeout
	}
}

// WithMaxChannels is an alias for WithMaxSize for backwards compatibility
func WithMaxChannels(size int) ChannelPoolOption {
	return WithMaxSize(size)
}

// WithChannelLogger adds logging to the channel pool (placeholder for compatibility)
func WithChannelLogger(logger *slog.Logger) ChannelPoolOption {
	return func(cp *ChannelPool) {
		// Channel pool doesn't have logger field, but we accept it for compatibility
	}
}

// NewChannelPool creates a new channel pool
func NewChannelPool(manager *ConnectionManager, options ...ChannelPoolOption) (*ChannelPool, error) {
	if manager == nil {
		return nil, ErrInvalidConfiguration
	}

	pool := &ChannelPool{
		manager:     manager,
		maxSize:     10,
		minSize:     2,
		idleTimeout: 5 * time.Minute,
	}

	for _, opt := range options {
		opt(pool)
	}

	// Validate configuration
	if pool.maxSize < 1 {
		return nil, fmt.Errorf("%w: max size must be at least 1", ErrInvalidConfiguration)
	}
	if pool.minSize < 0 || pool.minSize > pool.maxSize {
		return nil, fmt.Errorf("%w: min size must be between 0 and max size", ErrInvalidConfiguration)
	}

	pool.channels = make(chan *PooledChannel, pool.maxSize)

	// Pre-populate with minimum channels
	var createdChannels []*PooledChannel
	for i := 0; i < pool.minSize; i++ {
		ch, err := pool.createChannel()
		if err != nil {
			// Clean up any created channels
			for _, created := range createdChannels {
				created.Channel.Close()
			}
			return nil, &ChannelError{
				Op:        "pool initialization",
				ChannelID: fmt.Sprintf("init-%d", i),
				Err:       err,
				Timestamp: time.Now(),
			}
		}
		createdChannels = append(createdChannels, ch)
	}

	// Add channels to pool
	for _, ch := range createdChannels {
		pool.channels <- ch
	}

	// Start idle cleanup routine
	go pool.cleanupIdle()

	return pool, nil
}

// Get retrieves a channel from the pool
func (cp *ChannelPool) Get(ctx context.Context) (*PooledChannel, error) {
	cp.mu.Lock()
	if cp.closed {
		cp.mu.Unlock()
		return nil, ErrChannelPoolClosed
	}
	cp.mu.Unlock()

	select {
	case ch := <-cp.channels:
		// Check if channel is still valid
		if ch.Channel.IsClosed() {
			cp.mu.Lock()
			cp.activeCount--
			cp.mu.Unlock()
			
			// Create a new channel
			return cp.createAndGet(ctx)
		}
		
		ch.lastUsed = time.Now()
		return ch, nil

	default:
		// No channels available, create new one if under max
		cp.mu.Lock()
		if cp.activeCount < cp.maxSize {
			cp.mu.Unlock()
			return cp.createAndGet(ctx)
		}
		cp.mu.Unlock()

		// Wait for a channel to become available
		select {
		case ch := <-cp.channels:
			if ch.Channel.IsClosed() {
				cp.mu.Lock()
				cp.activeCount--
				cp.mu.Unlock()
				return cp.createAndGet(ctx)
			}
			ch.lastUsed = time.Now()
			return ch, nil
			
		case <-ctx.Done():
			return nil, &ChannelError{
				Op:        "get channel",
				ChannelID: "pool",
				Err:       ctx.Err(),
				Timestamp: time.Now(),
			}
			
		case <-time.After(5 * time.Second):
			return nil, &ChannelError{
				Op:        "get channel",
				ChannelID: "pool",
				Err:       ErrChannelPoolExhausted,
				Timestamp: time.Now(),
			}
		}
	}
}

// Put returns a channel to the pool
func (cp *ChannelPool) Put(ch *PooledChannel) {
	if ch == nil {
		return
	}

	cp.mu.Lock()
	if cp.closed {
		cp.mu.Unlock()
		ch.Channel.Close()
		return
	}
	cp.mu.Unlock()

	// Check if channel is still valid
	if ch.Channel.IsClosed() {
		cp.mu.Lock()
		cp.activeCount--
		cp.mu.Unlock()
		return
	}

	ch.lastUsed = time.Now()

	select {
	case cp.channels <- ch:
		// Channel returned to pool
	default:
		// Pool is full, close the channel
		ch.Channel.Close()
		cp.mu.Lock()
		cp.activeCount--
		cp.mu.Unlock()
	}
}

// Close closes all channels in the pool
func (cp *ChannelPool) Close() error {
	cp.mu.Lock()
	if cp.closed {
		cp.mu.Unlock()
		return nil
	}
	cp.closed = true
	cp.mu.Unlock()

	close(cp.channels)

	// Close all channels
	for ch := range cp.channels {
		if ch != nil && !ch.Channel.IsClosed() {
			ch.Channel.Close()
		}
	}

	return nil
}

// createChannel creates a new pooled channel
func (cp *ChannelPool) createChannel() (*PooledChannel, error) {
	conn, err := cp.manager.GetConnection()
	if err != nil {
		return nil, &ChannelError{
			Op:        "create channel",
			ChannelID: "new",
			Err:       err,
			Timestamp: time.Now(),
		}
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, &ChannelError{
			Op:        "create channel",
			ChannelID: "new",
			Err:       fmt.Errorf("%w: %v", ErrChannelCreationFailed, err),
			Timestamp: time.Now(),
		}
	}

	pooledCh := &PooledChannel{
		Channel:  ch,
		pool:     cp,
		lastUsed: time.Now(),
		id:       uuid.New().String(),
	}

	cp.mu.Lock()
	cp.activeCount++
	cp.mu.Unlock()

	return pooledCh, nil
}

// createAndGet creates a new channel and returns it
func (cp *ChannelPool) createAndGet(ctx context.Context) (*PooledChannel, error) {
	// Check context before creating
	select {
	case <-ctx.Done():
		return nil, &ChannelError{
			Op:        "create channel",
			ChannelID: "new",
			Err:       ctx.Err(),
			Timestamp: time.Now(),
		}
	default:
	}

	ch, err := cp.createChannel()
	if err != nil {
		return nil, err
	}
	return ch, nil
}

// cleanupIdle removes idle channels
func (cp *ChannelPool) cleanupIdle() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		cp.mu.Lock()
		if cp.closed {
			cp.mu.Unlock()
			return
		}
		cp.mu.Unlock()

		// Check channels for idle timeout
		timeout := time.Now().Add(-cp.idleTimeout)
		
		// Temporarily collect channels to check
		var activeChannels []*PooledChannel
		
		// Drain current channels
		drainLoop:
		for {
			select {
			case ch := <-cp.channels:
				if ch.lastUsed.Before(timeout) && cp.activeCount > cp.minSize {
					// Close idle channel
					ch.Channel.Close()
					cp.mu.Lock()
					cp.activeCount--
					cp.mu.Unlock()
				} else {
					activeChannels = append(activeChannels, ch)
				}
			default:
				break drainLoop
			}
		}
		
		// Return active channels to pool
		for _, ch := range activeChannels {
			select {
			case cp.channels <- ch:
			default:
				// This shouldn't happen, but handle it
				ch.Channel.Close()
				cp.mu.Lock()
				cp.activeCount--
				cp.mu.Unlock()
			}
		}
	}
}

// Size returns the current number of channels in the pool
func (cp *ChannelPool) Size() int {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return cp.activeCount
}

// Execute runs a function with a channel from the pool
func (cp *ChannelPool) Execute(ctx context.Context, fn func(*amqp.Channel) error) error {
	ch, err := cp.Get(ctx)
	if err != nil {
		return err
	}
	defer cp.Put(ch)

	// Run function with panic recovery
	var execErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				execErr = fmt.Errorf("panic in channel execution: %v", r)
			}
		}()
		execErr = fn(ch.Channel)
	}()

	return execErr
}