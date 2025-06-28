package monitor

import (
	"context"
	"fmt"

	"github.com/glimte/mmate-go/internal/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// QueueInspector provides AMQP-based queue inspection for service-scoped monitoring
// This replaces the HTTP management API client for basic queue information
type QueueInspector struct {
	channelPool *rabbitmq.ChannelPool
	vhost       string
}

// NewQueueInspector creates a new AMQP-based queue inspector
func NewQueueInspector(channelPool *rabbitmq.ChannelPool) *QueueInspector {
	return &QueueInspector{
		channelPool: channelPool,
		vhost:       "/", // default vhost
	}
}

// WithVhost sets a custom vhost for queue inspection
func (qi *QueueInspector) WithVhost(vhost string) *QueueInspector {
	qi.vhost = vhost
	return qi
}

// InspectQueue inspects a single queue using AMQP QueueInspect
// This provides basic queue information without requiring HTTP management API access
func (qi *QueueInspector) InspectQueue(ctx context.Context, queueName string) (*QueueInfo, error) {
	var queue amqp.Queue
	err := qi.channelPool.Execute(ctx, func(ch *amqp.Channel) error {
		var err error
		queue, err = ch.QueueInspect(queueName)
		return err
	})
	
	if err != nil {
		return nil, fmt.Errorf("failed to inspect queue %s: %w", queueName, err)
	}

	// Convert AMQP queue info to our QueueInfo structure
	queueInfo := &QueueInfo{
		Name:      queue.Name,
		Messages:  queue.Messages,
		Consumers: queue.Consumers,
		// Note: AMQP QueueInspect provides limited information compared to HTTP API
		// but this is sufficient for service-scoped monitoring
	}

	return queueInfo, nil
}

// CheckQueueExists checks if a queue exists without getting full details
func (qi *QueueInspector) CheckQueueExists(ctx context.Context, queueName string) (bool, error) {
	err := qi.channelPool.Execute(ctx, func(ch *amqp.Channel) error {
		_, err := ch.QueueInspect(queueName)
		return err
	})
	
	if err != nil {
		// Check if it's a "not found" error
		if amqpErr, ok := err.(*amqp.Error); ok && amqpErr.Code == 404 {
			return false, nil
		}
		return false, fmt.Errorf("failed to check queue existence: %w", err)
	}
	
	return true, nil
}

// GetServiceQueueHealth performs basic health assessment using AMQP inspection
func (qi *QueueInspector) GetServiceQueueHealth(ctx context.Context, queueName string) (*QueueHealth, error) {
	queueInfo, err := qi.InspectQueue(ctx, queueName)
	if err != nil {
		return &QueueHealth{
			QueueName: queueName,
			Status:    StatusUnhealthy,
			Message:   fmt.Sprintf("Failed to inspect queue: %v", err),
		}, err
	}

	health := &QueueHealth{
		QueueName: queueName,
		Messages:  queueInfo.Messages,
		Consumers: queueInfo.Consumers,
	}

	// Determine health status based on basic queue metrics
	switch {
	case queueInfo.Messages > 10000:
		health.Status = StatusDegraded
		health.Message = fmt.Sprintf("High message count: %d messages", queueInfo.Messages)
	case queueInfo.Messages > 1000:
		health.Status = StatusDegraded
		health.Message = fmt.Sprintf("Elevated message count: %d messages", queueInfo.Messages)
	case queueInfo.Consumers == 0 && queueInfo.Messages > 0:
		health.Status = StatusUnhealthy
		health.Message = fmt.Sprintf("No consumers for %d messages", queueInfo.Messages)
	default:
		health.Status = StatusHealthy
		health.Message = "Queue is healthy"
	}

	return health, nil
}

// QueueHealth represents basic queue health information from AMQP inspection
type QueueHealth struct {
	QueueName string `json:"queue_name"`
	Status    Status `json:"status"`
	Message   string `json:"message"`
	Messages  int    `json:"messages"`
	Consumers int    `json:"consumers"`
}