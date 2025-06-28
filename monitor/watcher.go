package monitor

import (
	"context"
	"sort"
	"strings"
	"time"
)

// QueueMetrics represents queue monitoring data
type QueueMetrics struct {
	Name         string    `json:"name"`
	Messages     int       `json:"messages"`
	Consumers    int       `json:"consumers"`
	MessageRate  float64   `json:"message_rate"`
	Memory       int64     `json:"memory"`
	State        string    `json:"state"`
	Timestamp    time.Time `json:"timestamp"`
}

// QueueSummary represents aggregated queue metrics
type QueueSummary struct {
	TotalQueues    int           `json:"total_queues"`
	TotalMessages  int           `json:"total_messages"`
	TotalConsumers int           `json:"total_consumers"`
	Queues         []QueueMetrics `json:"queues"`
	Timestamp      time.Time     `json:"timestamp"`
}

// QueueMonitor provides queue monitoring functionality (library only)
type QueueMonitor struct {
	client RabbitMQClient
}

// NewQueueMonitor creates a new queue monitor
func NewQueueMonitor(client RabbitMQClient) *QueueMonitor {
	return &QueueMonitor{
		client: client,
	}
}

// GetQueueMetrics retrieves current queue metrics
func (m *QueueMonitor) GetQueueMetrics(ctx context.Context, queueFilters []string) (*QueueSummary, error) {
	queues, err := m.client.ListQueues(ctx)
	if err != nil {
		return nil, err
	}

	// Convert to QueueMetrics and filter
	metrics := make([]QueueMetrics, 0)
	for _, q := range queues {
		if len(queueFilters) == 0 || m.matchesFilter(q.Name, queueFilters) {
			metrics = append(metrics, QueueMetrics{
				Name:        q.Name,
				Messages:    q.Messages,
				Consumers:   q.Consumers,
				MessageRate: q.MessageRate,
				Memory:      q.Memory,
				State:       q.State,
				Timestamp:   time.Now(),
			})
		}
	}

	// Sort by message count (descending)
	sort.Slice(metrics, func(i, j int) bool {
		return metrics[i].Messages > metrics[j].Messages
	})

	// Calculate summary
	summary := &QueueSummary{
		TotalQueues: len(metrics),
		Queues:      metrics,
		Timestamp:   time.Now(),
	}

	for _, q := range metrics {
		summary.TotalMessages += q.Messages
		summary.TotalConsumers += q.Consumers
	}

	return summary, nil
}

// matchesFilter checks if queue name matches any of the filters
func (m *QueueMonitor) matchesFilter(queueName string, filters []string) bool {
	for _, filter := range filters {
		if matched, _ := MatchPattern(queueName, filter); matched {
			return true
		}
	}
	return false
}

// MatchPattern checks if a name matches a pattern (supports * wildcard)
func MatchPattern(name, pattern string) (bool, error) {
	// Simple wildcard matching
	if !strings.Contains(pattern, "*") {
		return name == pattern, nil
	}
	
	// Convert pattern to regex-like matching
	pattern = strings.ReplaceAll(pattern, ".", "\\.")
	pattern = strings.ReplaceAll(pattern, "*", ".*")
	pattern = "^" + pattern + "$"
	
	// For simplicity, we'll use string matching
	// In production, you'd use regexp
	if strings.HasPrefix(pattern, "^") && strings.HasSuffix(pattern, "$") {
		pattern = pattern[1 : len(pattern)-1]
	}
	
	parts := strings.Split(pattern, ".*")
	if len(parts) == 2 {
		return strings.HasPrefix(name, parts[0]) && strings.HasSuffix(name, parts[1]), nil
	}
	
	return false, nil
}