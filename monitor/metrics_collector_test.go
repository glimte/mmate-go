package monitor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSimpleMetricsCollector(t *testing.T) {
	t.Run("NewSimpleMetricsCollector creates collector", func(t *testing.T) {
		collector := NewSimpleMetricsCollector()
		assert.NotNil(t, collector)
		
		summary := collector.GetMetricsSummary()
		assert.NotNil(t, summary.MessageCounts)
		assert.NotNil(t, summary.ErrorCounts)
		assert.NotNil(t, summary.ProcessingStats)
		assert.Empty(t, summary.MessageCounts)
		assert.Empty(t, summary.ErrorCounts)
		assert.Empty(t, summary.ProcessingStats)
	})
	
	t.Run("IncrementMessageCount tracks messages", func(t *testing.T) {
		collector := NewSimpleMetricsCollector()
		
		collector.IncrementMessageCount("test.message")
		collector.IncrementMessageCount("test.message")
		collector.IncrementMessageCount("other.message")
		
		summary := collector.GetMetricsSummary()
		assert.Equal(t, int64(2), summary.MessageCounts["test.message"])
		assert.Equal(t, int64(1), summary.MessageCounts["other.message"])
	})
	
	t.Run("RecordProcessingTime tracks timing", func(t *testing.T) {
		collector := NewSimpleMetricsCollector()
		
		collector.RecordProcessingTime("test.message", 100*time.Millisecond)
		collector.RecordProcessingTime("test.message", 200*time.Millisecond)
		collector.RecordProcessingTime("test.message", 150*time.Millisecond)
		
		summary := collector.GetMetricsSummary()
		stats := summary.ProcessingStats["test.message"]
		
		assert.Equal(t, int64(3), stats.Count)
		assert.Equal(t, int64(150), stats.AvgMs) // (100+200+150)/3 = 150
		assert.Equal(t, int64(100), stats.MinMs)
		assert.Equal(t, int64(200), stats.MaxMs)
	})
	
	t.Run("IncrementErrorCount tracks errors", func(t *testing.T) {
		collector := NewSimpleMetricsCollector()
		
		collector.IncrementErrorCount("test.message", "validation_error")
		collector.IncrementErrorCount("test.message", "validation_error")
		collector.IncrementErrorCount("test.message", "timeout_error")
		collector.IncrementErrorCount("other.message", "network_error")
		
		summary := collector.GetMetricsSummary()
		
		assert.Equal(t, int64(2), summary.ErrorCounts["test.message"]["validation_error"])
		assert.Equal(t, int64(1), summary.ErrorCounts["test.message"]["timeout_error"])
		assert.Equal(t, int64(1), summary.ErrorCounts["other.message"]["network_error"])
	})
	
	t.Run("Percentile calculations work correctly", func(t *testing.T) {
		collector := NewSimpleMetricsCollector()
		
		// Add samples: 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 ms
		for i := 1; i <= 10; i++ {
			collector.RecordProcessingTime("test.message", time.Duration(i*10)*time.Millisecond)
		}
		
		summary := collector.GetMetricsSummary()
		stats := summary.ProcessingStats["test.message"]
		
		// P50 should be around 50ms (middle value)
		// P95 should be around 90ms (95th percentile)
		// P99 should be around 100ms (99th percentile)
		assert.Greater(t, stats.P50Ms, int64(40))
		assert.Less(t, stats.P50Ms, int64(60))
		assert.Greater(t, stats.P95Ms, int64(80))
		assert.Greater(t, stats.P99Ms, int64(80))
	})
	
	t.Run("Reset clears all metrics", func(t *testing.T) {
		collector := NewSimpleMetricsCollector()
		
		// Add some data
		collector.IncrementMessageCount("test.message")
		collector.RecordProcessingTime("test.message", 100*time.Millisecond)
		collector.IncrementErrorCount("test.message", "error")
		
		// Verify data exists
		summary := collector.GetMetricsSummary()
		assert.Len(t, summary.MessageCounts, 1)
		assert.Len(t, summary.ProcessingStats, 1)
		assert.Len(t, summary.ErrorCounts, 1)
		
		// Reset and verify it's cleared
		collector.Reset()
		summary = collector.GetMetricsSummary()
		assert.Empty(t, summary.MessageCounts)
		assert.Empty(t, summary.ProcessingStats)
		assert.Empty(t, summary.ErrorCounts)
	})
	
	t.Run("Thread safety with concurrent access", func(t *testing.T) {
		collector := NewSimpleMetricsCollector()
		
		// Run concurrent operations
		done := make(chan bool, 3)
		
		// Goroutine 1: Increment message counts
		go func() {
			for i := 0; i < 100; i++ {
				collector.IncrementMessageCount("test.message")
			}
			done <- true
		}()
		
		// Goroutine 2: Record processing times
		go func() {
			for i := 0; i < 100; i++ {
				collector.RecordProcessingTime("test.message", time.Duration(i)*time.Millisecond)
			}
			done <- true
		}()
		
		// Goroutine 3: Increment error counts
		go func() {
			for i := 0; i < 100; i++ {
				collector.IncrementErrorCount("test.message", "error")
			}
			done <- true
		}()
		
		// Wait for all goroutines to complete
		for i := 0; i < 3; i++ {
			<-done
		}
		
		// Verify final counts
		summary := collector.GetMetricsSummary()
		assert.Equal(t, int64(100), summary.MessageCounts["test.message"])
		assert.Equal(t, int64(100), summary.ProcessingStats["test.message"].Count)
		assert.Equal(t, int64(100), summary.ErrorCounts["test.message"]["error"])
	})
}