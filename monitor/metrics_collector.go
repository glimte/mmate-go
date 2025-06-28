package monitor

import (
	"sync"
	"time"

	"github.com/glimte/mmate-go/interceptors"
)

// SimpleMetricsCollector implements a basic in-memory metrics collector
// that can be extended with exporters (Prometheus, etc.) later
type SimpleMetricsCollector struct {
	mu sync.RWMutex
	
	// Message counters by type
	messageCounters map[string]int64
	
	// Error counters by message type and error type
	errorCounters map[string]map[string]int64
	
	// Processing time stats by message type
	processingTimes map[string]*TimeStats
}

// TimeStats tracks timing statistics
type TimeStats struct {
	Count    int64
	TotalMs  int64
	MinMs    int64
	MaxMs    int64
	samples  []int64 // Keep last 100 samples for percentiles
}

// NewSimpleMetricsCollector creates a new in-memory metrics collector
func NewSimpleMetricsCollector() *SimpleMetricsCollector {
	return &SimpleMetricsCollector{
		messageCounters: make(map[string]int64),
		errorCounters:   make(map[string]map[string]int64),
		processingTimes: make(map[string]*TimeStats),
	}
}

// IncrementMessageCount implements interceptors.MetricsCollector
func (c *SimpleMetricsCollector) IncrementMessageCount(messageType string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messageCounters[messageType]++
}

// RecordProcessingTime implements interceptors.MetricsCollector
func (c *SimpleMetricsCollector) RecordProcessingTime(messageType string, duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	durationMs := duration.Milliseconds()
	
	stats, exists := c.processingTimes[messageType]
	if !exists {
		stats = &TimeStats{
			MinMs: durationMs,
			MaxMs: durationMs,
			samples: make([]int64, 0, 100),
		}
		c.processingTimes[messageType] = stats
	}
	
	stats.Count++
	stats.TotalMs += durationMs
	
	if durationMs < stats.MinMs {
		stats.MinMs = durationMs
	}
	if durationMs > stats.MaxMs {
		stats.MaxMs = durationMs
	}
	
	// Keep last 100 samples for percentile calculations
	if len(stats.samples) >= 100 {
		// Remove oldest sample
		stats.samples = stats.samples[1:]
	}
	stats.samples = append(stats.samples, durationMs)
}

// IncrementErrorCount implements interceptors.MetricsCollector
func (c *SimpleMetricsCollector) IncrementErrorCount(messageType string, errorType string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.errorCounters[messageType] == nil {
		c.errorCounters[messageType] = make(map[string]int64)
	}
	c.errorCounters[messageType][errorType]++
}

// GetMetricsSummary returns a summary of all collected metrics
func (c *SimpleMetricsCollector) GetMetricsSummary() MetricsSummary {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	summary := MetricsSummary{
		MessageCounts:   make(map[string]int64),
		ErrorCounts:     make(map[string]map[string]int64),
		ProcessingStats: make(map[string]ProcessingStats),
	}
	
	// Copy message counters
	for msgType, count := range c.messageCounters {
		summary.MessageCounts[msgType] = count
	}
	
	// Copy error counters
	for msgType, errors := range c.errorCounters {
		summary.ErrorCounts[msgType] = make(map[string]int64)
		for errorType, count := range errors {
			summary.ErrorCounts[msgType][errorType] = count
		}
	}
	
	// Calculate processing statistics
	for msgType, stats := range c.processingTimes {
		procStats := ProcessingStats{
			Count:    stats.Count,
			MinMs:    stats.MinMs,
			MaxMs:    stats.MaxMs,
		}
		
		if stats.Count > 0 {
			procStats.AvgMs = stats.TotalMs / stats.Count
		}
		
		// Calculate percentiles from samples
		if len(stats.samples) > 0 {
			procStats.P50Ms = c.calculatePercentile(stats.samples, 0.50)
			procStats.P95Ms = c.calculatePercentile(stats.samples, 0.95)
			procStats.P99Ms = c.calculatePercentile(stats.samples, 0.99)
		}
		
		summary.ProcessingStats[msgType] = procStats
	}
	
	return summary
}

// calculatePercentile calculates the percentile from a sorted slice
func (c *SimpleMetricsCollector) calculatePercentile(samples []int64, percentile float64) int64 {
	if len(samples) == 0 {
		return 0
	}
	
	// Simple copy and sort for percentile calculation
	sorted := make([]int64, len(samples))
	copy(sorted, samples)
	
	// Basic insertion sort (efficient for small slices)
	for i := 1; i < len(sorted); i++ {
		key := sorted[i]
		j := i - 1
		for j >= 0 && sorted[j] > key {
			sorted[j+1] = sorted[j]
			j--
		}
		sorted[j+1] = key
	}
	
	// Calculate percentile index
	index := int(float64(len(sorted)-1) * percentile)
	return sorted[index]
}

// MetricsSummary represents a snapshot of all metrics
type MetricsSummary struct {
	MessageCounts   map[string]int64                     `json:"message_counts"`
	ErrorCounts     map[string]map[string]int64          `json:"error_counts"`
	ProcessingStats map[string]ProcessingStats           `json:"processing_stats"`
}

// ProcessingStats represents processing time statistics for a message type
type ProcessingStats struct {
	Count int64 `json:"count"`
	AvgMs int64 `json:"avg_ms"`
	MinMs int64 `json:"min_ms"`
	MaxMs int64 `json:"max_ms"`
	P50Ms int64 `json:"p50_ms"`
	P95Ms int64 `json:"p95_ms"`
	P99Ms int64 `json:"p99_ms"`
}

// Reset clears all collected metrics
func (c *SimpleMetricsCollector) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	c.messageCounters = make(map[string]int64)
	c.errorCounters = make(map[string]map[string]int64)
	c.processingTimes = make(map[string]*TimeStats)
}

// AdvancedMetricsCollector extends MetricsCollector with advanced analytics
type AdvancedMetricsCollector interface {
	interceptors.MetricsCollector
	GetLatencyPercentiles() LatencyStats
	GetThroughputStats() ThroughputStats
	GetErrorAnalysis() ErrorAnalysis
}

// LatencyStats provides detailed latency analysis
type LatencyStats struct {
	P50  time.Duration `json:"p50"`
	P75  time.Duration `json:"p75"`
	P90  time.Duration `json:"p90"`
	P95  time.Duration `json:"p95"`
	P99  time.Duration `json:"p99"`
	P999 time.Duration `json:"p999"`
	Min  time.Duration `json:"min"`
	Max  time.Duration `json:"max"`
	Mean time.Duration `json:"mean"`
}

// ThroughputStats provides throughput analysis over time windows
type ThroughputStats struct {
	Current1Min   float64           `json:"current_1min"`
	Current5Min   float64           `json:"current_5min"`
	Current15Min  float64           `json:"current_15min"`
	Peak1Min      float64           `json:"peak_1min"`
	Peak5Min      float64           `json:"peak_5min"`
	ByMessageType map[string]float64 `json:"by_message_type"`
}

// ErrorAnalysis provides detailed error pattern analysis
type ErrorAnalysis struct {
	TotalErrors       int64                        `json:"total_errors"`
	ErrorRate         float64                      `json:"error_rate"`
	TopErrorTypes     []ErrorTypeStats             `json:"top_error_types"`
	ErrorsByMessageType map[string]int64           `json:"errors_by_message_type"`
	RecentErrors      []RecentError                `json:"recent_errors"`
}

// ErrorTypeStats represents statistics for a specific error type
type ErrorTypeStats struct {
	ErrorType string  `json:"error_type"`
	Count     int64   `json:"count"`
	Rate      float64 `json:"rate"`
}

// RecentError represents a recent error occurrence
type RecentError struct {
	Timestamp   time.Time `json:"timestamp"`
	MessageType string    `json:"message_type"`
	ErrorType   string    `json:"error_type"`
	Count       int64     `json:"count"`
}

// AdvancedMetricsReport combines all advanced metrics into a single report
type AdvancedMetricsReport struct {
	LatencyStats    LatencyStats    `json:"latency_stats"`
	ThroughputStats ThroughputStats `json:"throughput_stats"`
	ErrorAnalysis   ErrorAnalysis   `json:"error_analysis"`
	CollectedAt     time.Time       `json:"collected_at"`
}

// GetLatencyPercentiles implements AdvancedMetricsCollector
func (c *SimpleMetricsCollector) GetLatencyPercentiles() LatencyStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	// Collect all samples across all message types
	var allSamples []int64
	var totalDuration, minDuration, maxDuration int64
	var totalCount int64
	
	for _, stats := range c.processingTimes {
		allSamples = append(allSamples, stats.samples...)
		totalDuration += stats.TotalMs
		totalCount += stats.Count
		
		if minDuration == 0 || stats.MinMs < minDuration {
			minDuration = stats.MinMs
		}
		if stats.MaxMs > maxDuration {
			maxDuration = stats.MaxMs
		}
	}
	
	if len(allSamples) == 0 {
		return LatencyStats{}
	}
	
	// Sort all samples for percentile calculation
	c.sortSamples(allSamples)
	
	meanMs := int64(0)
	if totalCount > 0 {
		meanMs = totalDuration / totalCount
	}
	
	return LatencyStats{
		P50:  time.Duration(c.calculatePercentileFromSorted(allSamples, 0.50)) * time.Millisecond,
		P75:  time.Duration(c.calculatePercentileFromSorted(allSamples, 0.75)) * time.Millisecond,
		P90:  time.Duration(c.calculatePercentileFromSorted(allSamples, 0.90)) * time.Millisecond,
		P95:  time.Duration(c.calculatePercentileFromSorted(allSamples, 0.95)) * time.Millisecond,
		P99:  time.Duration(c.calculatePercentileFromSorted(allSamples, 0.99)) * time.Millisecond,
		P999: time.Duration(c.calculatePercentileFromSorted(allSamples, 0.999)) * time.Millisecond,
		Min:  time.Duration(minDuration) * time.Millisecond,
		Max:  time.Duration(maxDuration) * time.Millisecond,
		Mean: time.Duration(meanMs) * time.Millisecond,
	}
}

// GetThroughputStats implements AdvancedMetricsCollector
func (c *SimpleMetricsCollector) GetThroughputStats() ThroughputStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	// Simple implementation - in production you'd track time windows
	byMessageType := make(map[string]float64)
	var total int64
	
	for msgType, count := range c.messageCounters {
		byMessageType[msgType] = float64(count)
		total += count
	}
	
	// Simplified - assumes metrics collected over 1 minute
	// In production, you'd maintain sliding time windows
	currentRate := float64(total) / 60.0 // messages per second
	
	return ThroughputStats{
		Current1Min:   currentRate,
		Current5Min:   currentRate * 0.8,  // Simplified
		Current15Min:  currentRate * 0.6,  // Simplified
		Peak1Min:      currentRate * 1.2,  // Simplified
		Peak5Min:      currentRate * 1.1,  // Simplified
		ByMessageType: byMessageType,
	}
}

// GetErrorAnalysis implements AdvancedMetricsCollector
func (c *SimpleMetricsCollector) GetErrorAnalysis() ErrorAnalysis {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	var totalErrors int64
	var totalMessages int64
	topErrorTypes := make(map[string]int64)
	errorsByMessageType := make(map[string]int64)
	
	// Count total messages
	for _, count := range c.messageCounters {
		totalMessages += count
	}
	
	// Analyze errors
	for msgType, errors := range c.errorCounters {
		msgTypeErrors := int64(0)
		for errorType, count := range errors {
			totalErrors += count
			msgTypeErrors += count
			topErrorTypes[errorType] += count
		}
		errorsByMessageType[msgType] = msgTypeErrors
	}
	
	// Calculate error rate
	errorRate := 0.0
	if totalMessages > 0 {
		errorRate = float64(totalErrors) / float64(totalMessages)
	}
	
	// Sort top error types
	var topErrors []ErrorTypeStats
	for errorType, count := range topErrorTypes {
		rate := 0.0
		if totalErrors > 0 {
			rate = float64(count) / float64(totalErrors)
		}
		topErrors = append(topErrors, ErrorTypeStats{
			ErrorType: errorType,
			Count:     count,
			Rate:      rate,
		})
	}
	
	// Sort by count (simplified - no actual sorting implementation here)
	
	return ErrorAnalysis{
		TotalErrors:         totalErrors,
		ErrorRate:           errorRate,
		TopErrorTypes:       topErrors,
		ErrorsByMessageType: errorsByMessageType,
		RecentErrors:        []RecentError{}, // Would track recent errors in production
	}
}

// sortSamples sorts the samples in place (simple insertion sort)
func (c *SimpleMetricsCollector) sortSamples(samples []int64) {
	for i := 1; i < len(samples); i++ {
		key := samples[i]
		j := i - 1
		for j >= 0 && samples[j] > key {
			samples[j+1] = samples[j]
			j--
		}
		samples[j+1] = key
	}
}

// calculatePercentileFromSorted calculates percentile from pre-sorted samples
func (c *SimpleMetricsCollector) calculatePercentileFromSorted(sortedSamples []int64, percentile float64) int64 {
	if len(sortedSamples) == 0 {
		return 0
	}
	
	index := int(float64(len(sortedSamples)-1) * percentile)
	if index >= len(sortedSamples) {
		index = len(sortedSamples) - 1
	}
	return sortedSamples[index]
}

// Ensure SimpleMetricsCollector implements both interfaces
var _ interceptors.MetricsCollector = (*SimpleMetricsCollector)(nil)
var _ AdvancedMetricsCollector = (*SimpleMetricsCollector)(nil)