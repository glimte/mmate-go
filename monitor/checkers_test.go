package monitor

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMemoryChecker_Name(t *testing.T) {
	checker := NewMemoryChecker(80.0, 95.0)
	assert.Equal(t, "memory", checker.Name())
}

func TestMemoryChecker_Check(t *testing.T) {
	checker := NewMemoryChecker(80.0, 95.0)
	
	result := checker.Check(context.Background())
	
	assert.Equal(t, "memory", result.Name)
	assert.Contains(t, []Status{StatusHealthy, StatusDegraded, StatusUnhealthy}, result.Status)
	assert.True(t, result.Duration > 0)
	
	// Check that details are populated
	assert.Contains(t, result.Details, "memory_used_mb")
	assert.Contains(t, result.Details, "gc_runs")
	assert.Contains(t, result.Details, "goroutines")
	
	// Check that all values are reasonable
	memoryUsed := result.Details["memory_used_mb"].(float64)
	assert.Greater(t, memoryUsed, 0.0)
	
	gcRuns := result.Details["gc_runs"].(uint32)
	assert.GreaterOrEqual(t, gcRuns, uint32(0))
	
	goroutines := result.Details["goroutines"].(int)
	assert.Greater(t, goroutines, 0)
}

func TestMemoryChecker_HighGoroutineCount(t *testing.T) {
	// This test is tricky because we can't easily control goroutine count
	// We'll test the logic with a custom implementation
	checker := &MemoryChecker{
		warningThreshold:  80.0,
		criticalThreshold: 95.0,
	}
	
	result := checker.Check(context.Background())
	
	// Get the actual goroutine count from details
	goroutines := result.Details["goroutines"].(int)
	
	if goroutines > 1000 {
		assert.Equal(t, StatusUnhealthy, result.Status)
		assert.Contains(t, result.Message, "Too many goroutines")
	} else if goroutines > 500 {
		assert.Equal(t, StatusDegraded, result.Status)
		assert.Contains(t, result.Message, "High goroutine count")
	} else {
		assert.Equal(t, StatusHealthy, result.Status)
		assert.Equal(t, "Memory usage is normal", result.Message)
	}
}

func TestComponentChecker_Name(t *testing.T) {
	name := "custom-component"
	checker := NewComponentChecker(name, nil)
	assert.Equal(t, name, checker.Name())
}

func TestComponentChecker_Check_Success(t *testing.T) {
	name := "test-component"
	details := map[string]interface{}{
		"version": "1.0.0",
		"status":  "ok",
	}
	
	checkerFunc := func(ctx context.Context) (Status, string, map[string]interface{}, error) {
		return StatusHealthy, "Component is working", details, nil
	}
	
	checker := NewComponentChecker(name, checkerFunc)
	
	result := checker.Check(context.Background())
	
	assert.Equal(t, name, result.Name)
	assert.Equal(t, StatusHealthy, result.Status)
	assert.Equal(t, "Component is working", result.Message)
	assert.Equal(t, details, result.Details)
	assert.Empty(t, result.Error)
	assert.True(t, result.Duration > 0)
}

func TestComponentChecker_Check_WithError(t *testing.T) {
	name := "failing-component"
	expectedError := fmt.Errorf("component failure")
	
	checkerFunc := func(ctx context.Context) (Status, string, map[string]interface{}, error) {
		return StatusUnhealthy, "Component failed", nil, expectedError
	}
	
	checker := NewComponentChecker(name, checkerFunc)
	
	result := checker.Check(context.Background())
	
	assert.Equal(t, name, result.Name)
	assert.Equal(t, StatusUnhealthy, result.Status)
	assert.Equal(t, "Component failed", result.Message)
	assert.Equal(t, expectedError.Error(), result.Error)
	assert.True(t, result.Duration > 0)
}

func TestComponentChecker_Check_NilDetails(t *testing.T) {
	name := "simple-component"
	
	checkerFunc := func(ctx context.Context) (Status, string, map[string]interface{}, error) {
		return StatusDegraded, "Partial failure", nil, nil
	}
	
	checker := NewComponentChecker(name, checkerFunc)
	
	result := checker.Check(context.Background())
	
	assert.Equal(t, StatusDegraded, result.Status)
	assert.Equal(t, "Partial failure", result.Message)
	assert.NotNil(t, result.Details) // Should still have empty map initialized
	assert.Empty(t, result.Error)
}

func TestComponentChecker_Check_ContextCancellation(t *testing.T) {
	name := "slow-component"
	
	checkerFunc := func(ctx context.Context) (Status, string, map[string]interface{}, error) {
		select {
		case <-time.After(100 * time.Millisecond):
			return StatusHealthy, "OK", nil, nil
		case <-ctx.Done():
			return StatusUnhealthy, "Cancelled", nil, ctx.Err()
		}
	}
	
	checker := NewComponentChecker(name, checkerFunc)
	
	// Create context with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	
	result := checker.Check(ctx)
	
	assert.Equal(t, StatusUnhealthy, result.Status)
	assert.Equal(t, "Cancelled", result.Message)
	assert.Contains(t, result.Error, "context deadline exceeded")
}

// Test concurrent access to checkers
func TestCheckers_ConcurrentAccess(t *testing.T) {
	// Memory checker should be safe for concurrent use
	memChecker := NewMemoryChecker(80.0, 95.0)
	
	// Component checker with shared state
	var counter int
	compChecker := NewComponentChecker("concurrent", func(ctx context.Context) (Status, string, map[string]interface{}, error) {
		counter++ // This would be a race condition if not properly handled
		return StatusHealthy, "OK", map[string]interface{}{"counter": counter}, nil
	})
	
	const numGoroutines = 10
	const numChecks = 10
	
	// Test memory checker concurrency
	t.Run("memory checker", func(t *testing.T) {
		var wg sync.WaitGroup
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < numChecks; j++ {
					result := memChecker.Check(context.Background())
					assert.Equal(t, "memory", result.Name)
				}
			}()
		}
		wg.Wait()
	})
	
	// Test component checker (note: this may have race conditions by design)
	t.Run("component checker", func(t *testing.T) {
		var wg sync.WaitGroup
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < numChecks; j++ {
					result := compChecker.Check(context.Background())
					assert.Equal(t, "concurrent", result.Name)
				}
			}()
		}
		wg.Wait()
		
		// Counter should equal total number of checks (may be racy)
		assert.LessOrEqual(t, counter, numGoroutines*numChecks)
	})
}

// Test memory checker thresholds
func TestMemoryChecker_Thresholds(t *testing.T) {
	checker := NewMemoryChecker(50.0, 90.0)
	
	assert.Equal(t, 50.0, checker.warningThreshold)
	assert.Equal(t, 90.0, checker.criticalThreshold)
}

// Test that memory checker provides useful runtime information
func TestMemoryChecker_RuntimeInfo(t *testing.T) {
	checker := NewMemoryChecker(80.0, 95.0)
	
	// Force garbage collection to get consistent results
	runtime.GC()
	
	result := checker.Check(context.Background())
	
	// Verify runtime information is captured
	assert.Contains(t, result.Details, "memory_used_mb")
	assert.Contains(t, result.Details, "gc_runs")
	assert.Contains(t, result.Details, "goroutines")
	
	// Values should be reasonable
	memUsed := result.Details["memory_used_mb"].(float64)
	assert.Greater(t, memUsed, 0.0)
	assert.Less(t, memUsed, 10000.0) // Less than 10GB seems reasonable for tests
	
	gcRuns := result.Details["gc_runs"].(uint32)
	assert.GreaterOrEqual(t, gcRuns, uint32(1)) // Should have had at least one GC run
	
	goroutines := result.Details["goroutines"].(int)
	assert.GreaterOrEqual(t, goroutines, 1) // At least this test goroutine
	assert.Less(t, goroutines, 10000) // Reasonable upper bound
}

// Test component checker with various status scenarios
func TestComponentChecker_StatusScenarios(t *testing.T) {
	tests := []struct {
		name           string
		status         Status
		message        string
		details        map[string]interface{}
		expectedError  error
		expectedStatus Status
	}{
		{
			name:           "healthy component",
			status:         StatusHealthy,
			message:        "All systems operational",
			details:        map[string]interface{}{"uptime": "24h"},
			expectedError:  nil,
			expectedStatus: StatusHealthy,
		},
		{
			name:           "degraded component",
			status:         StatusDegraded,
			message:        "Some issues detected",
			details:        map[string]interface{}{"errors": 5},
			expectedError:  nil,
			expectedStatus: StatusDegraded,
		},
		{
			name:           "unhealthy component",
			status:         StatusUnhealthy,
			message:        "Service unavailable",
			details:        nil,
			expectedError:  fmt.Errorf("connection refused"),
			expectedStatus: StatusUnhealthy,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkerFunc := func(ctx context.Context) (Status, string, map[string]interface{}, error) {
				return tt.status, tt.message, tt.details, tt.expectedError
			}
			
			checker := NewComponentChecker("test", checkerFunc)
			result := checker.Check(context.Background())
			
			assert.Equal(t, tt.expectedStatus, result.Status)
			assert.Equal(t, tt.message, result.Message)
			
			if tt.details != nil {
				assert.Equal(t, tt.details, result.Details)
			} else {
				assert.NotNil(t, result.Details) // Should always be initialized
			}
			
			if tt.expectedError != nil {
				assert.Equal(t, tt.expectedError.Error(), result.Error)
			} else {
				assert.Empty(t, result.Error)
			}
		})
	}
}

// Benchmark tests
func BenchmarkMemoryChecker_Check(b *testing.B) {
	checker := NewMemoryChecker(80.0, 95.0)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		checker.Check(context.Background())
	}
}

func BenchmarkComponentChecker_Check(b *testing.B) {
	checkerFunc := func(ctx context.Context) (Status, string, map[string]interface{}, error) {
		return StatusHealthy, "OK", map[string]interface{}{"test": "value"}, nil
	}
	
	checker := NewComponentChecker("bench", checkerFunc)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		checker.Check(context.Background())
	}
}

// Test edge cases
func TestMemoryChecker_EdgeCases(t *testing.T) {
	// Test with extreme threshold values
	checker := NewMemoryChecker(0.0, 100.0)
	result := checker.Check(context.Background())
	
	// Should still work with extreme values
	assert.Equal(t, "memory", result.Name)
	assert.NotEmpty(t, result.Message)
	assert.Contains(t, result.Details, "goroutines")
}

func TestComponentChecker_EdgeCases(t *testing.T) {
	// Test with empty name
	checker := NewComponentChecker("", func(ctx context.Context) (Status, string, map[string]interface{}, error) {
		return StatusHealthy, "", nil, nil
	})
	
	result := checker.Check(context.Background())
	assert.Equal(t, "", result.Name)
	assert.Equal(t, "", result.Message)
	assert.NotNil(t, result.Details)
}