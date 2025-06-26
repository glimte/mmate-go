package health

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"time"

	"github.com/glimte/mmate-go/internal/rabbitmq"
)

// RabbitMQChecker checks RabbitMQ connection health
type RabbitMQChecker struct {
	connManager *rabbitmq.ConnectionManager
	logger      *slog.Logger
}

// NewRabbitMQChecker creates a new RabbitMQ health checker
func NewRabbitMQChecker(connManager *rabbitmq.ConnectionManager, logger *slog.Logger) *RabbitMQChecker {
	return &RabbitMQChecker{
		connManager: connManager,
		logger:      logger,
	}
}

func (c *RabbitMQChecker) Name() string {
	return "rabbitmq"
}

func (c *RabbitMQChecker) Check(ctx context.Context) CheckResult {
	start := time.Now()
	result := CheckResult{
		Name:      c.Name(),
		Timestamp: start,
		Details:   make(map[string]interface{}),
	}

	// Check connection
	conn, err := c.connManager.GetConnection()
	if err != nil {
		result.Status = StatusUnhealthy
		result.Message = "Failed to get connection"
		result.Error = err.Error()
		result.Duration = time.Since(start)
		return result
	}

	if conn.IsClosed() {
		result.Status = StatusUnhealthy
		result.Message = "Connection is closed"
		result.Duration = time.Since(start)
		return result
	}

	// Try to create a channel to test the connection
	ch, err := conn.Channel()
	if err != nil {
		result.Status = StatusUnhealthy
		result.Message = "Failed to create channel"
		result.Error = err.Error()
		result.Duration = time.Since(start)
		return result
	}
	defer ch.Close()

	// Perform a simple operation
	err = ch.ExchangeDeclarePassive(
		"amq.direct", // name
		"direct",     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		result.Status = StatusDegraded
		result.Message = "Exchange check failed"
		result.Error = err.Error()
	} else {
		result.Status = StatusHealthy
		result.Message = "Connection is healthy"
	}

	result.Duration = time.Since(start)
	result.Details["connection_open"] = !conn.IsClosed()
	result.Details["response_time_ms"] = result.Duration.Milliseconds()

	return result
}

// ChannelPoolChecker checks the health of a channel pool
type ChannelPoolChecker struct {
	pool   *rabbitmq.ChannelPool
	logger *slog.Logger
}

// NewChannelPoolChecker creates a new channel pool health checker
func NewChannelPoolChecker(pool *rabbitmq.ChannelPool, logger *slog.Logger) *ChannelPoolChecker {
	return &ChannelPoolChecker{
		pool:   pool,
		logger: logger,
	}
}

func (c *ChannelPoolChecker) Name() string {
	return "channel_pool"
}

func (c *ChannelPoolChecker) Check(ctx context.Context) CheckResult {
	start := time.Now()
	result := CheckResult{
		Name:      c.Name(),
		Timestamp: start,
		Details:   make(map[string]interface{}),
	}

	// Get pool size
	size := c.pool.Size()
	result.Details["pool_size"] = size

	// Try to get a channel
	ch, err := c.pool.Get(ctx)
	if err != nil {
		result.Status = StatusUnhealthy
		result.Message = "Failed to get channel from pool"
		result.Error = err.Error()
		result.Duration = time.Since(start)
		return result
	}

	// Return the channel
	c.pool.Put(ch)

	result.Status = StatusHealthy
	result.Message = "Channel pool is healthy"
	result.Duration = time.Since(start)
	result.Details["response_time_ms"] = result.Duration.Milliseconds()

	return result
}

// QueueChecker checks if a specific queue exists and is accessible
type QueueChecker struct {
	queueName   string
	channelPool *rabbitmq.ChannelPool
	logger      *slog.Logger
}

// NewQueueChecker creates a new queue health checker
func NewQueueChecker(queueName string, channelPool *rabbitmq.ChannelPool, logger *slog.Logger) *QueueChecker {
	return &QueueChecker{
		queueName:   queueName,
		channelPool: channelPool,
		logger:      logger,
	}
}

func (c *QueueChecker) Name() string {
	return fmt.Sprintf("queue_%s", c.queueName)
}

func (c *QueueChecker) Check(ctx context.Context) CheckResult {
	start := time.Now()
	result := CheckResult{
		Name:      c.Name(),
		Timestamp: start,
		Details:   make(map[string]interface{}),
	}

	// Get channel
	ch, err := c.channelPool.Get(ctx)
	if err != nil {
		result.Status = StatusUnhealthy
		result.Message = "Failed to get channel"
		result.Error = err.Error()
		result.Duration = time.Since(start)
		return result
	}
	defer c.channelPool.Put(ch)

	// Check if queue exists using QueueInspect
	queue, err := ch.QueueInspect(c.queueName)
	if err != nil {
		result.Status = StatusUnhealthy
		result.Message = fmt.Sprintf("Queue %s not accessible", c.queueName)
		result.Error = err.Error()
		result.Duration = time.Since(start)
		return result
	}

	result.Status = StatusHealthy
	result.Message = fmt.Sprintf("Queue %s is accessible", c.queueName)
	result.Duration = time.Since(start)
	result.Details["queue_name"] = queue.Name
	result.Details["message_count"] = queue.Messages
	result.Details["consumer_count"] = queue.Consumers
	result.Details["response_time_ms"] = result.Duration.Milliseconds()

	// Check if queue has too many messages (warning threshold)
	if queue.Messages > 10000 {
		result.Status = StatusDegraded
		result.Message = fmt.Sprintf("Queue %s has high message count", c.queueName)
	}

	return result
}

// MemoryChecker checks system memory usage
type MemoryChecker struct {
	warningThreshold  float64 // percentage
	criticalThreshold float64 // percentage
}

// NewMemoryChecker creates a new memory checker
func NewMemoryChecker(warningThreshold, criticalThreshold float64) *MemoryChecker {
	return &MemoryChecker{
		warningThreshold:  warningThreshold,
		criticalThreshold: criticalThreshold,
	}
}

func (c *MemoryChecker) Name() string {
	return "memory"
}

func (c *MemoryChecker) Check(ctx context.Context) CheckResult {
	start := time.Now()
	result := CheckResult{
		Name:      c.Name(),
		Timestamp: start,
		Details:   make(map[string]interface{}),
	}

	// This is a simplified check - in production you'd use runtime.MemStats
	// or system-specific memory information
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Calculate memory usage percentage (simplified)
	usedMB := float64(m.Sys) / 1024 / 1024
	result.Details["memory_used_mb"] = usedMB
	result.Details["gc_runs"] = m.NumGC
	result.Details["goroutines"] = runtime.NumGoroutine()

	// For this example, we'll just check if we have too many goroutines
	goroutines := runtime.NumGoroutine()
	if goroutines > 1000 {
		result.Status = StatusUnhealthy
		result.Message = fmt.Sprintf("Too many goroutines: %d", goroutines)
	} else if goroutines > 500 {
		result.Status = StatusDegraded
		result.Message = fmt.Sprintf("High goroutine count: %d", goroutines)
	} else {
		result.Status = StatusHealthy
		result.Message = "Memory usage is normal"
	}

	result.Duration = time.Since(start)
	return result
}

// ComponentChecker allows checking custom components
type ComponentChecker struct {
	name    string
	checker func(ctx context.Context) (Status, string, map[string]interface{}, error)
}

// NewComponentChecker creates a checker for custom components
func NewComponentChecker(name string, checker func(ctx context.Context) (Status, string, map[string]interface{}, error)) *ComponentChecker {
	return &ComponentChecker{
		name:    name,
		checker: checker,
	}
}

func (c *ComponentChecker) Name() string {
	return c.name
}

func (c *ComponentChecker) Check(ctx context.Context) CheckResult {
	start := time.Now()
	result := CheckResult{
		Name:      c.Name(),
		Timestamp: start,
		Details:   make(map[string]interface{}),
	}

	status, message, details, err := c.checker(ctx)

	result.Status = status
	result.Message = message
	if details != nil {
		result.Details = details
	}
	if err != nil {
		result.Error = err.Error()
	}
	result.Duration = time.Since(start)

	return result
}
