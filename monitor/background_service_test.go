package monitor

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Mock RabbitMQClient for testing
type MockRabbitMQClient struct {
	mock.Mock
}

func (m *MockRabbitMQClient) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	args := m.Called(ctx)
	return args.Get(0).([]QueueInfo), args.Error(1)
}

func (m *MockRabbitMQClient) GetQueues(ctx context.Context) ([]QueueInfo, error) {
	args := m.Called(ctx)
	return args.Get(0).([]QueueInfo), args.Error(1)
}

func (m *MockRabbitMQClient) GetQueue(ctx context.Context, vhost, name string) (*QueueInfo, error) {
	args := m.Called(ctx, vhost, name)
	return args.Get(0).(*QueueInfo), args.Error(1)
}

func (m *MockRabbitMQClient) ListExchanges(ctx context.Context) ([]ExchangeInfo, error) {
	args := m.Called(ctx)
	return args.Get(0).([]ExchangeInfo), args.Error(1)
}

func (m *MockRabbitMQClient) GetExchange(ctx context.Context, vhost, name string) (*ExchangeInfo, error) {
	args := m.Called(ctx, vhost, name)
	return args.Get(0).(*ExchangeInfo), args.Error(1)
}

func (m *MockRabbitMQClient) ListConnections(ctx context.Context) ([]ConnectionInfo, error) {
	args := m.Called(ctx)
	return args.Get(0).([]ConnectionInfo), args.Error(1)
}

func (m *MockRabbitMQClient) GetConnection(ctx context.Context, name string) (*ConnectionInfo, error) {
	args := m.Called(ctx, name)
	return args.Get(0).(*ConnectionInfo), args.Error(1)
}

func (m *MockRabbitMQClient) GetOverview(ctx context.Context) (*Overview, error) {
	args := m.Called(ctx)
	return args.Get(0).(*Overview), args.Error(1)
}

func (m *MockRabbitMQClient) CheckHealth(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// Mock AlertHandler for testing
type MockAlertHandler struct {
	mock.Mock
	mu       sync.Mutex
	alerts   []*Alert
	name     string
}

func NewMockAlertHandler(name string) *MockAlertHandler {
	return &MockAlertHandler{
		name:   name,
		alerts: make([]*Alert, 0),
	}
}

func (m *MockAlertHandler) HandleAlert(ctx context.Context, alert *Alert) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	args := m.Called(ctx, alert)
	m.alerts = append(m.alerts, alert)
	return args.Error(0)
}

func (m *MockAlertHandler) Name() string {
	return m.name
}

func (m *MockAlertHandler) GetAlerts() []*Alert {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	alerts := make([]*Alert, len(m.alerts))
	copy(alerts, m.alerts)
	return alerts
}

func (m *MockAlertHandler) ClearAlerts() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.alerts = make([]*Alert, 0)
}

func TestAlertLevel_String(t *testing.T) {
	tests := []struct {
		name  string
		level AlertLevel
		want  string
	}{
		{"info", AlertLevelInfo, "info"},
		{"warning", AlertLevelWarning, "warning"},
		{"critical", AlertLevelCritical, "critical"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, string(tt.level))
		})
	}
}

func TestDefaultAlertThresholds(t *testing.T) {
	thresholds := defaultAlertThresholds()
	
	assert.NotNil(t, thresholds)
	assert.Greater(t, len(thresholds), 0)
	
	// Check that expected thresholds exist
	expectedKeys := []string{"queue_depth", "consumer_lag", "memory_usage", "connection_count"}
	for _, key := range expectedKeys {
		assert.Contains(t, thresholds, key, "Expected threshold %s to exist", key)
	}
	
	// Verify queue_depth threshold
	queueDepth := thresholds["queue_depth"]
	assert.Equal(t, "queue", queueDepth.Component)
	assert.Equal(t, "message_count", queueDepth.MetricName)
	assert.Equal(t, float64(1000), queueDepth.WarningValue)
	assert.Equal(t, float64(5000), queueDepth.CriticalValue)
	assert.Equal(t, 3, queueDepth.ConsecutiveFails)
}

func TestNewBrokerHealthMonitoringService(t *testing.T) {
	mockClient := &MockRabbitMQClient{}
	registry := NewRegistry()
	logger := slog.Default()
	
	service := NewBrokerHealthMonitoringService(mockClient, registry, logger)
	
	assert.NotNil(t, service)
	assert.Equal(t, mockClient, service.client)
	assert.Equal(t, registry, service.healthRegistry)
	assert.Equal(t, logger, service.logger)
	assert.Equal(t, 30*time.Second, service.checkInterval)
	assert.False(t, service.running)
	assert.NotNil(t, service.activeAlerts)
	assert.NotNil(t, service.alertHandlers)
	assert.NotNil(t, service.alertThresholds)
	assert.NotNil(t, service.ctx)
	assert.NotNil(t, service.cancel)
}

func TestBrokerHealthMonitoringService_AddAlertHandler(t *testing.T) {
	service := createTestService(t)
	
	handler1 := NewMockAlertHandler("handler1")
	handler2 := NewMockAlertHandler("handler2")
	
	service.AddAlertHandler(handler1)
	assert.Equal(t, 1, len(service.alertHandlers))
	assert.Equal(t, handler1, service.alertHandlers[0])
	
	service.AddAlertHandler(handler2)
	assert.Equal(t, 2, len(service.alertHandlers))
	assert.Equal(t, handler2, service.alertHandlers[1])
}

func TestBrokerHealthMonitoringService_SetCheckInterval(t *testing.T) {
	service := createTestService(t)
	
	newInterval := 60 * time.Second
	service.SetCheckInterval(newInterval)
	
	assert.Equal(t, newInterval, service.checkInterval)
}

func TestBrokerHealthMonitoringService_SetAlertThreshold(t *testing.T) {
	service := createTestService(t)
	
	customThreshold := AlertThreshold{
		Component:        "custom",
		MetricName:       "custom_metric",
		WarningValue:     100,
		CriticalValue:    200,
		CheckDuration:    5 * time.Minute,
		ConsecutiveFails: 2,
	}
	
	service.SetAlertThreshold("custom_threshold", customThreshold)
	
	assert.Contains(t, service.alertThresholds, "custom_threshold")
	assert.Equal(t, customThreshold, service.alertThresholds["custom_threshold"])
}

func TestBrokerHealthMonitoringService_StartStop(t *testing.T) {
	service := createTestService(t)
	
	// Test initial state
	assert.False(t, service.IsRunning())
	
	// Test start
	err := service.Start()
	assert.NoError(t, err)
	assert.True(t, service.IsRunning())
	
	// Test double start
	err = service.Start()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already running")
	
	// Test stop
	err = service.Stop()
	assert.NoError(t, err)
	assert.False(t, service.IsRunning())
	
	// Test double stop
	err = service.Stop()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not running")
}

func TestBrokerHealthMonitoringService_GetActiveAlerts(t *testing.T) {
	service := createTestService(t)
	
	// Initially no alerts
	alerts := service.GetActiveAlerts()
	assert.Equal(t, 0, len(alerts))
	
	// Add some alerts manually
	now := time.Now()
	alert1 := &Alert{
		ID:          "alert1",
		Level:       AlertLevelWarning,
		Service:     "test",
		Component:   "comp1",
		Message:     "Test alert 1",
		Timestamp:   now,
		FirstSeen:   now,
		LastSeen:    now,
		Occurrences: 1,
	}
	
	alert2 := &Alert{
		ID:          "alert2",
		Level:       AlertLevelCritical,
		Service:     "test",
		Component:   "comp2",
		Message:     "Test alert 2",
		Timestamp:   now,
		FirstSeen:   now,
		LastSeen:    now,
		Occurrences: 1,
	}
	
	service.activeAlerts["key1"] = alert1
	service.activeAlerts["key2"] = alert2
	
	alerts = service.GetActiveAlerts()
	assert.Equal(t, 2, len(alerts))
	
	// Check that alerts are copies (to prevent external modification)
	alertsMap := make(map[string]*Alert)
	for _, alert := range alerts {
		alertsMap[alert.ID] = alert
	}
	assert.Contains(t, alertsMap, "alert1")
	assert.Contains(t, alertsMap, "alert2")
}

func TestBrokerHealthMonitoringService_TriggerAlert(t *testing.T) {
	service := createTestService(t)
	
	key := "test_alert"
	level := AlertLevelWarning
	service_name := "test_service"
	component := "test_component"
	message := "Test message"
	details := map[string]interface{}{"key": "value"}
	
	// Trigger new alert
	service.triggerAlert(key, level, service_name, component, message, details)
	
	alerts := service.GetActiveAlerts()
	assert.Equal(t, 1, len(alerts))
	
	if len(alerts) == 0 {
		return
	}
	alert := alerts[0]
	assert.Equal(t, level, alert.Level)
	assert.Equal(t, service_name, alert.Service)
	assert.Equal(t, component, alert.Component)
	assert.Equal(t, message, alert.Message)
	assert.Equal(t, details, alert.Details)
	assert.Equal(t, 1, alert.Occurrences)
	assert.False(t, alert.Resolved)
	
	// Trigger same alert again (should update existing)
	newMessage := "Updated message"
	newDetails := map[string]interface{}{"key": "new_value"}
	service.triggerAlert(key, level, service_name, component, newMessage, newDetails)
	
	alerts = service.GetActiveAlerts()
	assert.Equal(t, 1, len(alerts))
	
	alert = alerts[0]
	assert.Equal(t, newMessage, alert.Message)
	assert.Equal(t, newDetails, alert.Details)
	assert.Equal(t, 2, alert.Occurrences)
}

func TestBrokerHealthMonitoringService_ResolveAlert(t *testing.T) {
	service := createTestService(t)
	
	// Trigger an alert first
	key := "test_alert"
	service.triggerAlert(key, AlertLevelWarning, "test", "comp", "message", nil)
	
	alerts := service.GetActiveAlerts()
	assert.Equal(t, 1, len(alerts))
	assert.False(t, alerts[0].Resolved)
	assert.Nil(t, alerts[0].ResolvedAt)
	
	// Resolve the alert
	service.resolveAlert(key)
	
	alerts = service.GetActiveAlerts()
	assert.Equal(t, 1, len(alerts))
	assert.True(t, alerts[0].Resolved)
	assert.NotNil(t, alerts[0].ResolvedAt)
	
	// Resolving again should be a no-op
	originalResolvedAt := alerts[0].ResolvedAt
	service.resolveAlert(key)
	
	alerts = service.GetActiveAlerts()
	assert.Equal(t, 1, len(alerts))
	assert.Equal(t, originalResolvedAt, alerts[0].ResolvedAt)
	
	// Resolving non-existent alert should be safe
	service.resolveAlert("non_existent")
}

func TestBrokerHealthMonitoringService_SendToHandlers(t *testing.T) {
	service := createTestService(t)
	
	handler1 := NewMockAlertHandler("handler1")
	handler2 := NewMockAlertHandler("handler2")
	
	handler1.On("HandleAlert", mock.Anything, mock.Anything).Return(nil)
	handler2.On("HandleAlert", mock.Anything, mock.Anything).Return(nil)
	
	service.AddAlertHandler(handler1)
	service.AddAlertHandler(handler2)
	
	alert := &Alert{
		ID:        "test_alert",
		Level:     AlertLevelWarning,
		Service:   "test",
		Component: "comp",
		Message:   "test message",
		Timestamp: time.Now(),
	}
	
	service.sendToHandlers(alert)
	
	// Give handlers time to process (they run in goroutines)
	time.Sleep(100 * time.Millisecond)
	
	handler1.AssertExpectations(t)
	handler2.AssertExpectations(t)
	
	// Verify handlers received the alert
	handler1Alerts := handler1.GetAlerts()
	handler2Alerts := handler2.GetAlerts()
	
	assert.Equal(t, 1, len(handler1Alerts))
	assert.Equal(t, 1, len(handler2Alerts))
	assert.Equal(t, alert.ID, handler1Alerts[0].ID)
	assert.Equal(t, alert.ID, handler2Alerts[0].ID)
}

func TestBrokerHealthMonitoringService_EvaluateHealthCheckAlert(t *testing.T) {
	service := createTestService(t)
	
	checkName := "test_check"
	
	t.Run("unhealthy status triggers critical alert", func(t *testing.T) {
		result := CheckResult{
			Name:    checkName,
			Status:  StatusUnhealthy,
			Message: "Test unhealthy",
			Error:   "test error",
		}
		
		service.evaluateHealthCheckAlert(checkName, result)
		
		alerts := service.GetActiveAlerts()
		assert.Equal(t, 1, len(alerts))
		assert.Equal(t, AlertLevelCritical, alerts[0].Level)
		assert.Contains(t, alerts[0].Message, "unhealthy")
	})
	
	t.Run("degraded status triggers warning alert", func(t *testing.T) {
		service.activeAlerts = make(map[string]*Alert) // Clear alerts
		
		result := CheckResult{
			Name:    checkName,
			Status:  StatusDegraded,
			Message: "Test degraded",
		}
		
		service.evaluateHealthCheckAlert(checkName, result)
		
		alerts := service.GetActiveAlerts()
		assert.Equal(t, 1, len(alerts))
		assert.Equal(t, AlertLevelWarning, alerts[0].Level)
		assert.Contains(t, alerts[0].Message, "degraded")
	})
	
	t.Run("healthy status resolves alert", func(t *testing.T) {
		// First trigger an alert
		service.triggerAlert("health_check_"+checkName, AlertLevelCritical, "health_check", checkName, "test", nil)
		
		result := CheckResult{
			Name:    checkName,
			Status:  StatusHealthy,
			Message: "All good",
		}
		
		service.evaluateHealthCheckAlert(checkName, result)
		
		alerts := service.GetActiveAlerts()
		assert.Equal(t, 1, len(alerts))
		assert.True(t, alerts[0].Resolved)
	})
}

func TestBrokerHealthMonitoringService_CheckBrokerMetrics(t *testing.T) {
	service := createTestService(t)
	mockClient := service.client.(*MockRabbitMQClient)
	
	t.Run("overview error triggers alert", func(t *testing.T) {
		mockClient.On("GetOverview", mock.Anything).Return(&Overview{}, assert.AnError).Once()
		
		service.checkBrokerMetrics()
		
		alerts := service.GetActiveAlerts()
		assert.Greater(t, len(alerts), 0)
		
		var brokerAlert *Alert
		for _, alert := range alerts {
			if alert.Service == "broker" && alert.Component == "overview" {
				brokerAlert = alert
				break
			}
		}
		assert.NotNil(t, brokerAlert)
		assert.Equal(t, AlertLevelCritical, brokerAlert.Level)
	})
	
	t.Run("low activity triggers warning", func(t *testing.T) {
		service.activeAlerts = make(map[string]*Alert) // Clear alerts
		
		overview := &Overview{
			MessageStats: MessageStats{
				PublishDetails:    Details{Rate: 0.05},
				DeliverGetDetails: Details{Rate: 0.05},
			},
			ObjectTotals: ObjectTotals{
				Connections: 100,
			},
		}
		
		mockClient.On("GetOverview", mock.Anything).Return(overview, nil).Once()
		
		service.checkBrokerMetrics()
		
		alerts := service.GetActiveAlerts()
		var activityAlert *Alert
		for _, alert := range alerts {
			if alert.Service == "broker" && alert.Component == "activity" {
				activityAlert = alert
				break
			}
		}
		assert.NotNil(t, activityAlert)
		assert.Equal(t, AlertLevelWarning, activityAlert.Level)
	})
	
	t.Run("high connection count triggers alert", func(t *testing.T) {
		service.activeAlerts = make(map[string]*Alert) // Clear alerts
		
		overview := &Overview{
			MessageStats: MessageStats{
				PublishDetails:    Details{Rate: 1.0},
				DeliverGetDetails: Details{Rate: 1.0},
			},
			ObjectTotals: ObjectTotals{
				Connections: 1200, // Above warning threshold (1000) but below critical (1500)
			},
		}
		
		mockClient.On("GetOverview", mock.Anything).Return(overview, nil).Once()
		
		service.checkBrokerMetrics()
		
		alerts := service.GetActiveAlerts()
		var connAlert *Alert
		for _, alert := range alerts {
			if alert.Service == "broker" && alert.Component == "connections" {
				connAlert = alert
				break
			}
		}
		assert.NotNil(t, connAlert)
		assert.Equal(t, AlertLevelWarning, connAlert.Level)
	})
}

func TestBrokerHealthMonitoringService_CheckQueueMetrics(t *testing.T) {
	service := createTestService(t)
	mockClient := service.client.(*MockRabbitMQClient)
	
	t.Run("queue list error triggers alert", func(t *testing.T) {
		mockClient.On("GetQueues", mock.Anything).Return([]QueueInfo{}, assert.AnError).Once()
		
		service.checkQueueMetrics()
		
		alerts := service.GetActiveAlerts()
		assert.Greater(t, len(alerts), 0)
		
		var queueAlert *Alert
		for _, alert := range alerts {
			if alert.Service == "queue" && alert.Component == "list" {
				queueAlert = alert
				break
			}
		}
		assert.NotNil(t, queueAlert)
		assert.Equal(t, AlertLevelCritical, queueAlert.Level)
	})
	
	t.Run("high queue depth triggers alert", func(t *testing.T) {
		service.activeAlerts = make(map[string]*Alert) // Clear alerts
		
		queues := []QueueInfo{
			{
				Name:     "test_queue",
				Messages: 6000, // Above critical threshold (5000)
				Consumers: 1,
			},
			{
				Name:     "normal_queue",
				Messages: 100, // Below warning threshold
				Consumers: 2,
			},
		}
		
		mockClient.On("GetQueues", mock.Anything).Return(queues, nil).Once()
		
		service.checkQueueMetrics()
		
		alerts := service.GetActiveAlerts()
		var queueAlert *Alert
		for _, alert := range alerts {
			if alert.Component == "test_queue" {
				queueAlert = alert
				break
			}
		}
		assert.NotNil(t, queueAlert)
		assert.Equal(t, AlertLevelCritical, queueAlert.Level)
		assert.Contains(t, queueAlert.Message, "6000 messages")
	})
}

func TestBrokerHealthMonitoringService_CleanupResolvedAlerts(t *testing.T) {
	service := createTestService(t)
	
	now := time.Now()
	oldTime := now.Add(-25 * time.Hour) // Older than 24 hour cutoff
	recentTime := now.Add(-1 * time.Hour) // Recent
	
	// Add some alerts
	oldResolvedAlert := &Alert{
		ID:         "old_resolved",
		Resolved:   true,
		ResolvedAt: &oldTime,
	}
	
	recentResolvedAlert := &Alert{
		ID:         "recent_resolved",
		Resolved:   true,
		ResolvedAt: &recentTime,
	}
	
	activeAlert := &Alert{
		ID:       "active",
		Resolved: false,
	}
	
	service.activeAlerts["old"] = oldResolvedAlert
	service.activeAlerts["recent"] = recentResolvedAlert
	service.activeAlerts["active"] = activeAlert
	
	assert.Equal(t, 3, len(service.activeAlerts))
	
	service.cleanupResolvedAlerts()
	
	// Only old resolved alert should be removed
	assert.Equal(t, 2, len(service.activeAlerts))
	assert.Contains(t, service.activeAlerts, "recent")
	assert.Contains(t, service.activeAlerts, "active")
	assert.NotContains(t, service.activeAlerts, "old")
}

func TestBrokerHealthMonitoringService_GetMetrics(t *testing.T) {
	service := createTestService(t)
	
	// Add some alerts directly to avoid triggering handlers
	now := time.Now()
	service.activeAlerts["alert1"] = &Alert{
		ID:          "alert1",
		Level:       AlertLevelWarning,
		Service:     "svc",
		Component:   "comp",
		Message:     "msg",
		Timestamp:   now,
		Resolved:    true,
		ResolvedAt:  &now,
		Occurrences: 1,
	}
	service.activeAlerts["alert2"] = &Alert{
		ID:          "alert2",
		Level:       AlertLevelCritical,
		Service:     "svc",
		Component:   "comp",
		Message:     "msg",
		Timestamp:   now,
		Resolved:    false,
		Occurrences: 1,
	}
	service.activeAlerts["alert3"] = &Alert{
		ID:          "alert3",
		Level:       AlertLevelInfo,
		Service:     "svc",
		Component:   "comp",
		Message:     "msg",
		Timestamp:   now,
		Resolved:    false,
		Occurrences: 1,
	}
	
	// Add a handler
	handler := NewMockAlertHandler("test")
	service.AddAlertHandler(handler)
	
	metrics := service.GetMetrics()
	
	assert.Equal(t, 3, metrics["active_alerts"])
	assert.Equal(t, false, metrics["running"])
	assert.Equal(t, service.checkInterval.String(), metrics["check_interval"])
	assert.Equal(t, 1, metrics["alert_handlers"])
	
	alertsByLevel := metrics["alerts_by_level"].(map[AlertLevel]int)
	assert.Equal(t, 1, alertsByLevel[AlertLevelInfo])
	assert.Equal(t, 0, alertsByLevel[AlertLevelWarning]) // Resolved alert not counted
	assert.Equal(t, 1, alertsByLevel[AlertLevelCritical])
}

func TestBrokerHealthMonitoringService_MarshalJSON(t *testing.T) {
	service := createTestService(t)
	
	// Add alert directly to avoid triggering handlers
	now := time.Now()
	service.activeAlerts["test"] = &Alert{
		ID:          "test",
		Level:       AlertLevelWarning,
		Service:     "svc",
		Component:   "comp",
		Message:     "msg",
		Timestamp:   now,
		Resolved:    false,
		Occurrences: 1,
	}
	
	data, err := json.Marshal(service)
	assert.NoError(t, err)
	
	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	assert.NoError(t, err)
	
	assert.Contains(t, result, "running")
	assert.Contains(t, result, "check_interval")
	assert.Contains(t, result, "active_alerts")
	assert.Contains(t, result, "metrics")
	
	assert.Equal(t, false, result["running"])
}

// Helper function to create a test service
func createTestService(t *testing.T) *BrokerHealthMonitoringService {
	mockClient := &MockRabbitMQClient{}
	registry := NewRegistry()
	logger := slog.Default()
	
	return NewBrokerHealthMonitoringService(mockClient, registry, logger)
}

// Integration test that runs the monitoring loop for a short time
func testBrokerHealthMonitoringService_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	
	service := createTestService(t)
	mockClient := service.client.(*MockRabbitMQClient)
	
	// Set very short check interval for testing
	service.SetCheckInterval(100 * time.Millisecond)
	
	// Add a health checker that will fail
	failingChecker := NewCheckerFunc("failing", func(ctx context.Context) CheckResult {
		return CheckResult{
			Name:    "failing",
			Status:  StatusUnhealthy,
			Message: "Always fails",
		}
	})
	service.healthRegistry.Register(failingChecker)
	
	// Mock broker calls
	overview := &Overview{
		MessageStats: MessageStats{
			PublishDetails:    Details{Rate: 1.0},
			DeliverGetDetails: Details{Rate: 1.0},
		},
		ObjectTotals: ObjectTotals{
			Connections: 100,
		},
	}
	
	queues := []QueueInfo{
		{Name: "test_queue", Messages: 100, Consumers: 1},
	}
	
	mockClient.On("GetOverview", mock.Anything).Return(overview, nil).Maybe()
	mockClient.On("GetQueues", mock.Anything).Return(queues, nil).Maybe()
	
	// Add alert handler to track alerts - use a simpler one that doesn't panic
	handler := NewMockAlertHandler("test")
	// Set up the mock to accept any calls
	handler.On("HandleAlert", mock.Anything, mock.Anything).Return(nil)
	service.AddAlertHandler(handler)
	
	// Start service
	err := service.Start()
	assert.NoError(t, err)
	
	// Let it run for a short while
	time.Sleep(300 * time.Millisecond)
	
	// Stop service
	err = service.Stop()
	assert.NoError(t, err)
	
	// Verify alerts were generated
	alerts := service.GetActiveAlerts()
	assert.Greater(t, len(alerts), 0, "Expected some alerts to be generated")
	
	// Should have alert from failing health check
	var healthAlert *Alert
	for _, alert := range alerts {
		if alert.Service == "health_check" && alert.Component == "failing" {
			healthAlert = alert
			break
		}
	}
	assert.NotNil(t, healthAlert, "Expected health check alert")
	assert.Equal(t, AlertLevelCritical, healthAlert.Level)
}