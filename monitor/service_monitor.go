package monitor

import (
	"context"
	"fmt"
	"time"

	"github.com/glimte/mmate-go/internal/rabbitmq"
)

// ServiceMonitor provides service-scoped monitoring capabilities
// This prevents "mastodon" usage by restricting monitoring to service-owned resources
type ServiceMonitor struct {
	serviceName    string
	serviceQueue   string
	queueInspector *QueueInspector
	vhost          string
}

// NewServiceMonitor creates a service-scoped monitor using AMQP queue inspection
// This ensures monitoring is limited to the service's own resources
func NewServiceMonitor(serviceName, serviceQueue string, channelPool *rabbitmq.ChannelPool) *ServiceMonitor {
	return &ServiceMonitor{
		serviceName:    serviceName,
		serviceQueue:   serviceQueue,
		queueInspector: NewQueueInspector(channelPool),
		vhost:          "/", // default vhost
	}
}

// WithVhost sets a custom vhost for this service
func (sm *ServiceMonitor) WithVhost(vhost string) *ServiceMonitor {
	sm.vhost = vhost
	sm.queueInspector = sm.queueInspector.WithVhost(vhost)
	return sm
}

// ServiceQueueInfo returns information about this service's queue ONLY
func (sm *ServiceMonitor) ServiceQueueInfo(ctx context.Context) (*QueueInfo, error) {
	return sm.queueInspector.InspectQueue(ctx, sm.serviceQueue)
}

// ServiceQueueHealth checks if this service's queue is healthy
func (sm *ServiceMonitor) ServiceQueueHealth(ctx context.Context) (*ServiceHealth, error) {
	queueInfo, err := sm.ServiceQueueInfo(ctx)
	if err != nil {
		return &ServiceHealth{
			ServiceName: sm.serviceName,
			QueueName:   sm.serviceQueue,
			Status:      StatusUnhealthy,
			Message:     fmt.Sprintf("Failed to get queue info: %v", err),
			CheckedAt:   time.Now(),
		}, err
	}

	health := &ServiceHealth{
		ServiceName: sm.serviceName,
		QueueName:   sm.serviceQueue,
		CheckedAt:   time.Now(),
		QueueInfo:   queueInfo,
	}

	// Determine health status based on queue metrics
	if queueInfo.Messages > 10000 {
		health.Status = StatusDegraded
		health.Message = fmt.Sprintf("High message count: %d messages in queue", queueInfo.Messages)
	} else if queueInfo.Messages > 1000 {
		health.Status = StatusDegraded
		health.Message = fmt.Sprintf("Elevated message count: %d messages in queue", queueInfo.Messages)
	} else {
		health.Status = StatusHealthy
		health.Message = "Queue is healthy"
	}

	return health, nil
}

// ServiceOwnedQueues returns queues that belong to this service
// For service-scoped monitoring, we only check known service queues
// This prevents "mastodon" behavior by not listing all queues
func (sm *ServiceMonitor) ServiceOwnedQueues(ctx context.Context) ([]QueueInfo, error) {
	// For service-scoped monitoring, we inspect only the service's main queue
	// Additional service queues would need to be explicitly registered
	var serviceQueues []QueueInfo
	
	// Check main service queue
	mainQueue, err := sm.ServiceQueueInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get main service queue: %w", err)
	}
	serviceQueues = append(serviceQueues, *mainQueue)
	
	// Additional service queues could be added here if configured
	// This prevents scanning all queues ("mastodon" behavior)
	
	return serviceQueues, nil
}

// ServiceMetrics combines local metrics with queue monitoring for this service only
func (sm *ServiceMonitor) ServiceMetrics(ctx context.Context, localMetrics *MetricsSummary) (*ServiceMetrics, error) {
	queueInfo, err := sm.ServiceQueueInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get service queue info: %w", err)
	}

	serviceQueues, err := sm.ServiceOwnedQueues(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get service-owned queues: %w", err)
	}

	// Get consumer stats for this service
	consumerStats, err := sm.GetMyConsumerStats(ctx)
	if err != nil {
		// Log error but don't fail - consumer stats are optional
		consumerStats = nil
	}

	return &ServiceMetrics{
		ServiceName:   sm.serviceName,
		MainQueue:     *queueInfo,
		OwnedQueues:   serviceQueues,
		LocalMetrics:  localMetrics,
		ConsumerStats: consumerStats,
		CollectedAt:   time.Now(),
	}, nil
}

// GetMyConsumerStats returns consumer statistics for this service's queues only
func (sm *ServiceMonitor) GetMyConsumerStats(ctx context.Context) (*ConsumerStats, error) {
	serviceQueues, err := sm.ServiceOwnedQueues(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get service queues: %w", err)
	}

	stats := &ConsumerStats{
		ServiceName:     sm.serviceName,
		ConsumerDetails: make([]ConsumerInfo, 0),
	}

	totalUtilization := 0.0
	totalRate := 0.0

	for _, queue := range serviceQueues {
		// Get consumer information from queue statistics
		// Note: This is simplified - in a full implementation you'd use
		// RabbitMQ management API to get detailed consumer info
		if queue.Consumers > 0 {
			stats.TotalConsumers += queue.Consumers
			
			// Estimate consumer info from queue stats
			consumerInfo := ConsumerInfo{
				QueueName:       queue.Name,
				ConsumerTag:     fmt.Sprintf("%s-consumer", sm.serviceName),
				MessagesReady:   queue.Messages,
				MessageRate:     queue.MessageRate,
				LastActivity:    time.Now(),
			}
			
			stats.ConsumerDetails = append(stats.ConsumerDetails, consumerInfo)
			totalRate += queue.MessageRate
			
			// Simple utilization estimate based on message rate
			if queue.MessageRate > 0 {
				stats.ActiveConsumers++
				totalUtilization += 1.0 // Assume full utilization if processing
			}
		}
	}

	if stats.TotalConsumers > 0 {
		stats.AverageUtilization = totalUtilization / float64(stats.TotalConsumers)
	}
	stats.TotalProcessingRate = totalRate

	return stats, nil
}

// BrokerHealth checks basic connectivity (this is safe for all services to call)
// Note: This no longer provides full broker overview to maintain service-scoped architecture
func (sm *ServiceMonitor) BrokerHealth(ctx context.Context) (*BasicConnectivityHealth, error) {
	// Test basic connectivity by checking if our service queue exists
	exists, err := sm.queueInspector.CheckQueueExists(ctx, sm.serviceQueue)
	if err != nil {
		return &BasicConnectivityHealth{
			Connected: false,
			Message:   fmt.Sprintf("Failed to check connectivity: %v", err),
		}, err
	}
	
	return &BasicConnectivityHealth{
		Connected: exists,
		Message:   "Service can communicate with RabbitMQ",
	}, nil
}

// ServiceHealth represents the health status of a specific service
type ServiceHealth struct {
	ServiceName string     `json:"service_name"`
	QueueName   string     `json:"queue_name"`
	Status      Status     `json:"status"`
	Message     string     `json:"message"`
	CheckedAt   time.Time  `json:"checked_at"`
	QueueInfo   *QueueInfo `json:"queue_info,omitempty"`
}

// ServiceMetrics combines local application metrics with service-scoped queue metrics
type ServiceMetrics struct {
	ServiceName    string            `json:"service_name"`
	MainQueue      QueueInfo         `json:"main_queue"`
	OwnedQueues    []QueueInfo       `json:"owned_queues"`
	LocalMetrics   *MetricsSummary   `json:"local_metrics"`
	ConsumerStats  *ConsumerStats    `json:"consumer_stats,omitempty"`
	CollectedAt    time.Time         `json:"collected_at"`
}

// ConsumerStats represents statistics about consumers for this service
type ConsumerStats struct {
	ServiceName       string         `json:"service_name"`
	TotalConsumers    int            `json:"total_consumers"`
	ActiveConsumers   int            `json:"active_consumers"`
	ConsumerDetails   []ConsumerInfo `json:"consumer_details"`
	AverageUtilization float64       `json:"average_utilization"`
	TotalProcessingRate float64      `json:"total_processing_rate"`
}

// ConsumerInfo represents detailed information about a single consumer
type ConsumerInfo struct {
	QueueName       string    `json:"queue_name"`
	ConsumerTag     string    `json:"consumer_tag"`
	ChannelNumber   int       `json:"channel_number"`
	ConnectionName  string    `json:"connection_name"`
	PrefetchCount   int       `json:"prefetch_count"`
	MessagesReady   int       `json:"messages_ready"`
	MessagesUnacked int       `json:"messages_unacked"`
	MessageRate     float64   `json:"message_rate"`
	LastActivity    time.Time `json:"last_activity"`
}

// GetTotalQueuedMessages returns the total number of messages across all service-owned queues
func (sm *ServiceMetrics) GetTotalQueuedMessages() int {
	total := 0
	for _, queue := range sm.OwnedQueues {
		total += queue.Messages
	}
	return total
}

// GetTotalConsumers returns the total number of consumers across all service-owned queues
func (sm *ServiceMetrics) GetTotalConsumers() int {
	total := 0
	for _, queue := range sm.OwnedQueues {
		total += queue.Consumers
	}
	return total
}

// NewServiceMonitorFromChannelPool creates a ServiceMonitor from a channel pool
// This is the recommended way to create service monitors
func NewServiceMonitorFromChannelPool(serviceName, serviceQueue string, channelPool *rabbitmq.ChannelPool) *ServiceMonitor {
	return NewServiceMonitor(serviceName, serviceQueue, channelPool)
}

// BasicConnectivityHealth represents basic broker connectivity health
type BasicConnectivityHealth struct {
	Connected bool   `json:"connected"`
	Message   string `json:"message"`
}