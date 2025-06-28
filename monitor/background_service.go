package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// AlertLevel represents the severity of an alert
type AlertLevel string

const (
	AlertLevelInfo     AlertLevel = "info"
	AlertLevelWarning  AlertLevel = "warning"
	AlertLevelCritical AlertLevel = "critical"
)

// Alert represents a monitoring alert
type Alert struct {
	ID          string                 `json:"id"`
	Level       AlertLevel             `json:"level"`
	Service     string                 `json:"service"`
	Component   string                 `json:"component"`
	Message     string                 `json:"message"`
	Details     map[string]interface{} `json:"details,omitempty"`
	Timestamp   time.Time              `json:"timestamp"`
	Resolved    bool                   `json:"resolved"`
	ResolvedAt  *time.Time             `json:"resolvedAt,omitempty"`
	Occurrences int                    `json:"occurrences"`
	FirstSeen   time.Time              `json:"firstSeen"`
	LastSeen    time.Time              `json:"lastSeen"`
}

// AlertHandler defines the interface for handling alerts
type AlertHandler interface {
	HandleAlert(ctx context.Context, alert *Alert) error
	Name() string
}

// BrokerHealthMonitoringService provides continuous health monitoring
type BrokerHealthMonitoringService struct {
	client          RabbitMQClient
	healthRegistry  *Registry
	alertHandlers   []AlertHandler
	activeAlerts    map[string]*Alert
	alertsMutex     sync.RWMutex
	logger          *slog.Logger
	checkInterval   time.Duration
	alertThresholds map[string]AlertThreshold
	running         bool
	runningMutex    sync.RWMutex
	ctx             context.Context
	cancel          context.CancelFunc
}

// AlertThreshold defines when to trigger alerts
type AlertThreshold struct {
	Component        string        `json:"component"`
	MetricName       string        `json:"metricName"`
	WarningValue     float64       `json:"warningValue"`
	CriticalValue    float64       `json:"criticalValue"`
	CheckDuration    time.Duration `json:"checkDuration"`
	ConsecutiveFails int           `json:"consecutiveFails"`
}

// ServiceConfig configures the monitoring service
type ServiceConfig struct {
	CheckInterval   time.Duration
	AlertThresholds map[string]AlertThreshold
}

// NewBrokerHealthMonitoringService creates a new monitoring service
func NewBrokerHealthMonitoringService(client RabbitMQClient, healthRegistry *Registry, logger *slog.Logger) *BrokerHealthMonitoringService {
	ctx, cancel := context.WithCancel(context.Background())

	return &BrokerHealthMonitoringService{
		client:          client,
		healthRegistry:  healthRegistry,
		alertHandlers:   make([]AlertHandler, 0),
		activeAlerts:    make(map[string]*Alert),
		logger:          logger,
		checkInterval:   30 * time.Second,
		alertThresholds: defaultAlertThresholds(),
		ctx:             ctx,
		cancel:          cancel,
	}
}

// defaultAlertThresholds returns default monitoring thresholds
func defaultAlertThresholds() map[string]AlertThreshold {
	return map[string]AlertThreshold{
		"queue_depth": {
			Component:        "queue",
			MetricName:       "message_count",
			WarningValue:     1000,
			CriticalValue:    5000,
			CheckDuration:    5 * time.Minute,
			ConsecutiveFails: 3,
		},
		"consumer_lag": {
			Component:        "consumer",
			MetricName:       "lag_seconds",
			WarningValue:     300,  // 5 minutes
			CriticalValue:    1800, // 30 minutes
			CheckDuration:    2 * time.Minute,
			ConsecutiveFails: 2,
		},
		"memory_usage": {
			Component:        "broker",
			MetricName:       "memory_used_percent",
			WarningValue:     80.0,
			CriticalValue:    95.0,
			CheckDuration:    1 * time.Minute,
			ConsecutiveFails: 2,
		},
		"connection_count": {
			Component:        "broker",
			MetricName:       "connection_count",
			WarningValue:     1000,
			CriticalValue:    1500,
			CheckDuration:    2 * time.Minute,
			ConsecutiveFails: 3,
		},
	}
}

// AddAlertHandler adds an alert handler
func (s *BrokerHealthMonitoringService) AddAlertHandler(handler AlertHandler) {
	s.alertHandlers = append(s.alertHandlers, handler)
	s.logger.Info("Alert handler added", "handler", handler.Name())
}

// SetCheckInterval sets the monitoring check interval
func (s *BrokerHealthMonitoringService) SetCheckInterval(interval time.Duration) {
	s.checkInterval = interval
}

// SetAlertThreshold sets a custom alert threshold
func (s *BrokerHealthMonitoringService) SetAlertThreshold(name string, threshold AlertThreshold) {
	s.alertThresholds[name] = threshold
}

// Start begins the monitoring service
func (s *BrokerHealthMonitoringService) Start() error {
	s.runningMutex.Lock()
	defer s.runningMutex.Unlock()

	if s.running {
		return fmt.Errorf("monitoring service is already running")
	}

	s.running = true
	s.logger.Info("Starting broker health monitoring service", "interval", s.checkInterval)

	go s.monitoringLoop()
	go s.alertCleanupLoop()

	return nil
}

// Stop stops the monitoring service
func (s *BrokerHealthMonitoringService) Stop() error {
	s.runningMutex.Lock()
	defer s.runningMutex.Unlock()

	if !s.running {
		return fmt.Errorf("monitoring service is not running")
	}

	s.logger.Info("Stopping broker health monitoring service")
	s.cancel()
	s.running = false

	return nil
}

// IsRunning returns whether the service is running
func (s *BrokerHealthMonitoringService) IsRunning() bool {
	s.runningMutex.RLock()
	defer s.runningMutex.RUnlock()
	return s.running
}

// GetActiveAlerts returns all active alerts
func (s *BrokerHealthMonitoringService) GetActiveAlerts() []*Alert {
	s.alertsMutex.RLock()
	defer s.alertsMutex.RUnlock()

	alerts := make([]*Alert, 0, len(s.activeAlerts))
	for _, alert := range s.activeAlerts {
		alerts = append(alerts, alert)
	}

	return alerts
}

// monitoringLoop runs the main monitoring loop
func (s *BrokerHealthMonitoringService) monitoringLoop() {
	ticker := time.NewTicker(s.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.performHealthChecks()
		}
	}
}

// performHealthChecks executes all health checks and evaluates alerts
func (s *BrokerHealthMonitoringService) performHealthChecks() {
	// Check overall health
	healthCtx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()

	overallHealth := s.healthRegistry.Check(healthCtx)

	// Evaluate health check results for alerts
	for name, checkResult := range overallHealth.Checks {
		s.evaluateHealthCheckAlert(name, checkResult)
	}

	// Check broker-specific metrics
	s.checkBrokerMetrics()

	// Check queue metrics
	s.checkQueueMetrics()
}

// evaluateHealthCheckAlert evaluates a health check result for alerts
func (s *BrokerHealthMonitoringService) evaluateHealthCheckAlert(checkName string, result CheckResult) {
	alertKey := fmt.Sprintf("health_check_%s", checkName)

	switch result.Status {
	case StatusUnhealthy:
		s.triggerAlert(alertKey, AlertLevelCritical, "health_check", checkName,
			fmt.Sprintf("Health check %s is unhealthy: %s", checkName, result.Message),
			map[string]interface{}{
				"check_name": checkName,
				"duration":   result.Duration.String(),
				"error":      result.Error,
				"details":    result.Details,
			})

	case StatusDegraded:
		s.triggerAlert(alertKey, AlertLevelWarning, "health_check", checkName,
			fmt.Sprintf("Health check %s is degraded: %s", checkName, result.Message),
			map[string]interface{}{
				"check_name": checkName,
				"duration":   result.Duration.String(),
				"details":    result.Details,
			})

	case StatusHealthy:
		s.resolveAlert(alertKey)
	}
}

// checkBrokerMetrics checks broker-level metrics
func (s *BrokerHealthMonitoringService) checkBrokerMetrics() {
	// Get broker overview
	ctx := context.Background()
	overview, err := s.client.GetOverview(ctx)
	if err != nil {
		s.triggerAlert("broker_overview", AlertLevelCritical, "broker", "overview",
			fmt.Sprintf("Failed to get broker overview: %v", err),
			map[string]interface{}{"error": err.Error()})
		return
	}

	// Check message rates
	if overview.MessageStats.PublishDetails.Rate < 0.1 && overview.MessageStats.DeliverGetDetails.Rate < 0.1 {
		s.triggerAlert("broker_activity", AlertLevelWarning, "broker", "activity",
			"Broker appears inactive - very low message rates",
			map[string]interface{}{
				"publish_rate": overview.MessageStats.PublishDetails.Rate,
				"deliver_rate": overview.MessageStats.DeliverGetDetails.Rate,
			})
	} else {
		s.resolveAlert("broker_activity")
	}

	// Check connection count
	if threshold, exists := s.alertThresholds["connection_count"]; exists {
		connectionCount := float64(overview.ObjectTotals.Connections)
		if connectionCount >= threshold.CriticalValue {
			s.triggerAlert("connection_count", AlertLevelCritical, "broker", "connections",
				fmt.Sprintf("Critical connection count: %.0f", connectionCount),
				map[string]interface{}{
					"current_connections": connectionCount,
					"critical_threshold":  threshold.CriticalValue,
				})
		} else if connectionCount >= threshold.WarningValue {
			s.triggerAlert("connection_count", AlertLevelWarning, "broker", "connections",
				fmt.Sprintf("High connection count: %.0f", connectionCount),
				map[string]interface{}{
					"current_connections": connectionCount,
					"warning_threshold":   threshold.WarningValue,
				})
		} else {
			s.resolveAlert("connection_count")
		}
	}
}

// checkQueueMetrics checks queue-level metrics
func (s *BrokerHealthMonitoringService) checkQueueMetrics() {
	ctx := context.Background()
	queues, err := s.client.GetQueues(ctx)
	if err != nil {
		s.triggerAlert("queue_metrics", AlertLevelCritical, "queue", "list",
			fmt.Sprintf("Failed to get queue metrics: %v", err),
			map[string]interface{}{"error": err.Error()})
		return
	}

	threshold, exists := s.alertThresholds["queue_depth"]
	if !exists {
		return
	}

	for _, queue := range queues {
		alertKey := fmt.Sprintf("queue_depth_%s", queue.Name)
		messageCount := float64(queue.Messages)

		if messageCount >= threshold.CriticalValue {
			s.triggerAlert(alertKey, AlertLevelCritical, "queue", queue.Name,
				fmt.Sprintf("Critical queue depth: %.0f messages", messageCount),
				map[string]interface{}{
					"queue_name":         queue.Name,
					"message_count":      messageCount,
					"critical_threshold": threshold.CriticalValue,
					"consumers":          queue.Consumers,
				})
		} else if messageCount >= threshold.WarningValue {
			s.triggerAlert(alertKey, AlertLevelWarning, "queue", queue.Name,
				fmt.Sprintf("High queue depth: %.0f messages", messageCount),
				map[string]interface{}{
					"queue_name":        queue.Name,
					"message_count":     messageCount,
					"warning_threshold": threshold.WarningValue,
					"consumers":         queue.Consumers,
				})
		} else {
			s.resolveAlert(alertKey)
		}
	}
}

// triggerAlert creates or updates an alert
func (s *BrokerHealthMonitoringService) triggerAlert(key string, level AlertLevel, service, component, message string, details map[string]interface{}) {
	s.alertsMutex.Lock()
	defer s.alertsMutex.Unlock()

	now := time.Now()

	if existing, exists := s.activeAlerts[key]; exists {
		// Update existing alert
		existing.Occurrences++
		existing.LastSeen = now
		existing.Message = message
		existing.Details = details

		s.logger.Debug("Alert updated",
			"key", key,
			"level", level,
			"occurrences", existing.Occurrences,
		)
	} else {
		// Create new alert
		alert := &Alert{
			ID:          fmt.Sprintf("%s_%d", key, now.Unix()),
			Level:       level,
			Service:     service,
			Component:   component,
			Message:     message,
			Details:     details,
			Timestamp:   now,
			Resolved:    false,
			Occurrences: 1,
			FirstSeen:   now,
			LastSeen:    now,
		}

		s.activeAlerts[key] = alert

		s.logger.Warn("New alert triggered",
			"key", key,
			"level", level,
			"service", service,
			"component", component,
			"message", message,
		)

		// Send to alert handlers
		go s.sendToHandlers(alert)
	}
}

// resolveAlert marks an alert as resolved
func (s *BrokerHealthMonitoringService) resolveAlert(key string) {
	s.alertsMutex.Lock()
	defer s.alertsMutex.Unlock()

	if alert, exists := s.activeAlerts[key]; exists && !alert.Resolved {
		now := time.Now()
		alert.Resolved = true
		alert.ResolvedAt = &now

		s.logger.Info("Alert resolved",
			"key", key,
			"duration", now.Sub(alert.FirstSeen).String(),
			"occurrences", alert.Occurrences,
		)

		// Send resolution to alert handlers
		go s.sendToHandlers(alert)
	}
}

// sendToHandlers sends an alert to all registered handlers
func (s *BrokerHealthMonitoringService) sendToHandlers(alert *Alert) {
	for _, handler := range s.alertHandlers {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := handler.HandleAlert(ctx, alert); err != nil {
			s.logger.Error("Alert handler failed",
				"handler", handler.Name(),
				"alert", alert.ID,
				"error", err,
			)
		}
		cancel()
	}
}

// alertCleanupLoop cleans up old resolved alerts
func (s *BrokerHealthMonitoringService) alertCleanupLoop() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.cleanupResolvedAlerts()
		}
	}
}

// cleanupResolvedAlerts removes old resolved alerts
func (s *BrokerHealthMonitoringService) cleanupResolvedAlerts() {
	s.alertsMutex.Lock()
	defer s.alertsMutex.Unlock()

	cutoff := time.Now().Add(-24 * time.Hour)
	toDelete := make([]string, 0)

	for key, alert := range s.activeAlerts {
		if alert.Resolved && alert.ResolvedAt != nil && alert.ResolvedAt.Before(cutoff) {
			toDelete = append(toDelete, key)
		}
	}

	for _, key := range toDelete {
		delete(s.activeAlerts, key)
	}

	if len(toDelete) > 0 {
		s.logger.Debug("Cleaned up resolved alerts", "count", len(toDelete))
	}
}

// GetMetrics returns monitoring metrics
func (s *BrokerHealthMonitoringService) GetMetrics() map[string]interface{} {
	s.alertsMutex.RLock()
	defer s.alertsMutex.RUnlock()

	metrics := map[string]interface{}{
		"active_alerts":  len(s.activeAlerts),
		"running":        s.IsRunning(),
		"check_interval": s.checkInterval.String(),
		"alert_handlers": len(s.alertHandlers),
	}

	// Count alerts by level
	levelCounts := map[AlertLevel]int{
		AlertLevelInfo:     0,
		AlertLevelWarning:  0,
		AlertLevelCritical: 0,
	}

	for _, alert := range s.activeAlerts {
		if !alert.Resolved {
			levelCounts[alert.Level]++
		}
	}

	metrics["alerts_by_level"] = levelCounts

	return metrics
}

// MarshalJSON implements json.Marshaler for the service
func (s *BrokerHealthMonitoringService) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"running":        s.IsRunning(),
		"check_interval": s.checkInterval.String(),
		"active_alerts":  s.GetActiveAlerts(),
		"metrics":        s.GetMetrics(),
	})
}
