package monitor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAdvancedMetricsCollector(t *testing.T) {
	t.Run("AdvancedMetricsCollector interface compliance", func(t *testing.T) {
		collector := NewSimpleMetricsCollector()
		
		// Verify it implements both interfaces
		var basicCollector interface{} = collector
		var advancedCollector interface{} = collector
		
		assert.Implements(t, (*AdvancedMetricsCollector)(nil), basicCollector)
		assert.NotNil(t, advancedCollector)
	})
	
	t.Run("GetLatencyPercentiles with sample data", func(t *testing.T) {
		collector := NewSimpleMetricsCollector()
		
		// Add some processing time samples
		collector.RecordProcessingTime("test.message", 50*time.Millisecond)
		collector.RecordProcessingTime("test.message", 100*time.Millisecond)
		collector.RecordProcessingTime("test.message", 150*time.Millisecond)
		collector.RecordProcessingTime("test.message", 200*time.Millisecond)
		collector.RecordProcessingTime("test.message", 250*time.Millisecond)
		
		latencyStats := collector.GetLatencyPercentiles()
		
		assert.Greater(t, latencyStats.P50, time.Duration(0))
		assert.GreaterOrEqual(t, latencyStats.P95, latencyStats.P50)
		assert.GreaterOrEqual(t, latencyStats.P99, latencyStats.P95)
		assert.Equal(t, 50*time.Millisecond, latencyStats.Min)
		assert.Equal(t, 250*time.Millisecond, latencyStats.Max)
		assert.Equal(t, 150*time.Millisecond, latencyStats.Mean)
	})
	
	t.Run("GetThroughputStats", func(t *testing.T) {
		collector := NewSimpleMetricsCollector()
		
		// Simulate message processing
		collector.IncrementMessageCount("user.created")
		collector.IncrementMessageCount("user.created")
		collector.IncrementMessageCount("user.updated")
		
		throughputStats := collector.GetThroughputStats()
		
		assert.Greater(t, throughputStats.Current1Min, 0.0)
		assert.Contains(t, throughputStats.ByMessageType, "user.created")
		assert.Contains(t, throughputStats.ByMessageType, "user.updated")
		assert.Equal(t, float64(2), throughputStats.ByMessageType["user.created"])
		assert.Equal(t, float64(1), throughputStats.ByMessageType["user.updated"])
	})
	
	t.Run("GetErrorAnalysis", func(t *testing.T) {
		collector := NewSimpleMetricsCollector()
		
		// Simulate messages and errors
		collector.IncrementMessageCount("user.created")
		collector.IncrementMessageCount("user.created")
		collector.IncrementMessageCount("user.created")
		collector.IncrementErrorCount("user.created", "validation_error")
		collector.IncrementErrorCount("user.created", "timeout_error")
		collector.IncrementErrorCount("user.created", "validation_error")
		
		errorAnalysis := collector.GetErrorAnalysis()
		
		assert.Equal(t, int64(3), errorAnalysis.TotalErrors)
		assert.Equal(t, 1.0, errorAnalysis.ErrorRate) // 3 errors / 3 messages
		assert.Len(t, errorAnalysis.TopErrorTypes, 2)
		assert.Contains(t, errorAnalysis.ErrorsByMessageType, "user.created")
		assert.Equal(t, int64(3), errorAnalysis.ErrorsByMessageType["user.created"])
	})
}

func TestServiceMonitorConsumerStats(t *testing.T) {
	t.Run("ConsumerStats structure", func(t *testing.T) {
		// Test ConsumerStats structure
		stats := &ConsumerStats{
			ServiceName:     "user-service",
			ConsumerDetails: make([]ConsumerInfo, 0),
		}
		
		assert.Equal(t, "user-service", stats.ServiceName)
		assert.Equal(t, 0, stats.TotalConsumers)
		assert.NotNil(t, stats.ConsumerDetails)
	})
}