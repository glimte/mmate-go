package monitor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStatus_String(t *testing.T) {
	tests := []struct {
		name   string
		status Status
		want   string
	}{
		{"healthy", StatusHealthy, "healthy"},
		{"degraded", StatusDegraded, "degraded"},
		{"unhealthy", StatusUnhealthy, "unhealthy"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, string(tt.status))
		})
	}
}

func TestNewCheckerFunc(t *testing.T) {
	name := "test-checker"
	fn := func(ctx context.Context) CheckResult {
		return CheckResult{
			Name:      name,
			Status:    StatusHealthy,
			Message:   "test message",
			Timestamp: time.Now(),
		}
	}

	checker := NewCheckerFunc(name, fn)
	
	assert.Equal(t, name, checker.Name())
	
	result := checker.Check(context.Background())
	assert.Equal(t, name, result.Name)
	assert.Equal(t, StatusHealthy, result.Status)
	assert.Equal(t, "test message", result.Message)
}

func TestNewRegistry(t *testing.T) {
	registry := NewRegistry()
	
	assert.NotNil(t, registry)
	assert.NotNil(t, registry.checkers)
	assert.NotNil(t, registry.metadata)
	assert.Equal(t, 0, len(registry.checkers))
	assert.Equal(t, 0, len(registry.metadata))
}

func TestRegistry_Register(t *testing.T) {
	registry := NewRegistry()
	
	checker1 := NewCheckerFunc("checker1", func(ctx context.Context) CheckResult {
		return CheckResult{Name: "checker1", Status: StatusHealthy}
	})
	
	checker2 := NewCheckerFunc("checker2", func(ctx context.Context) CheckResult {
		return CheckResult{Name: "checker2", Status: StatusDegraded}
	})
	
	// Register checkers
	registry.Register(checker1)
	registry.Register(checker2)
	
	// Verify they were registered
	registry.mu.RLock()
	assert.Equal(t, 2, len(registry.checkers))
	assert.Contains(t, registry.checkers, "checker1")
	assert.Contains(t, registry.checkers, "checker2")
	registry.mu.RUnlock()
	
	// Test replacing existing checker
	newChecker1 := NewCheckerFunc("checker1", func(ctx context.Context) CheckResult {
		return CheckResult{Name: "checker1", Status: StatusUnhealthy}
	})
	registry.Register(newChecker1)
	
	registry.mu.RLock()
	assert.Equal(t, 2, len(registry.checkers))
	registry.mu.RUnlock()
}

func TestRegistry_Unregister(t *testing.T) {
	registry := NewRegistry()
	
	checker := NewCheckerFunc("test-checker", func(ctx context.Context) CheckResult {
		return CheckResult{Name: "test-checker", Status: StatusHealthy}
	})
	
	registry.Register(checker)
	
	registry.mu.RLock()
	assert.Equal(t, 1, len(registry.checkers))
	registry.mu.RUnlock()
	
	// Unregister existing checker
	registry.Unregister("test-checker")
	
	registry.mu.RLock()
	assert.Equal(t, 0, len(registry.checkers))
	registry.mu.RUnlock()
	
	// Unregister non-existing checker (should not panic)
	registry.Unregister("non-existing")
	
	registry.mu.RLock()
	assert.Equal(t, 0, len(registry.checkers))
	registry.mu.RUnlock()
}

func TestRegistry_SetMetadata(t *testing.T) {
	registry := NewRegistry()
	
	registry.SetMetadata("version", "1.0.0")
	registry.SetMetadata("service", "test-service")
	registry.SetMetadata("environment", "test")
	
	registry.mu.RLock()
	assert.Equal(t, 3, len(registry.metadata))
	assert.Equal(t, "1.0.0", registry.metadata["version"])
	assert.Equal(t, "test-service", registry.metadata["service"])
	assert.Equal(t, "test", registry.metadata["environment"])
	registry.mu.RUnlock()
	
	// Test overwriting metadata
	registry.SetMetadata("version", "2.0.0")
	
	registry.mu.RLock()
	assert.Equal(t, 3, len(registry.metadata))
	assert.Equal(t, "2.0.0", registry.metadata["version"])
	registry.mu.RUnlock()
}

func TestRegistry_Check_SingleHealthyChecker(t *testing.T) {
	registry := NewRegistry()
	
	checker := NewCheckerFunc("healthy-checker", func(ctx context.Context) CheckResult {
		return CheckResult{
			Name:      "healthy-checker",
			Status:    StatusHealthy,
			Message:   "All good",
			Duration:  10 * time.Millisecond,
			Timestamp: time.Now(),
		}
	})
	
	registry.Register(checker)
	registry.SetMetadata("service", "test")
	
	health := registry.Check(context.Background())
	
	assert.Equal(t, StatusHealthy, health.Status)
	assert.Equal(t, 1, len(health.Checks))
	assert.Contains(t, health.Checks, "healthy-checker")
	assert.Equal(t, StatusHealthy, health.Checks["healthy-checker"].Status)
	assert.Equal(t, "All good", health.Checks["healthy-checker"].Message)
	assert.Equal(t, "test", health.Metadata["service"])
	assert.True(t, health.Duration > 0)
}

func TestRegistry_Check_MultipleCheckers(t *testing.T) {
	registry := NewRegistry()
	
	healthyChecker := NewCheckerFunc("healthy", func(ctx context.Context) CheckResult {
		return CheckResult{Name: "healthy", Status: StatusHealthy, Message: "OK"}
	})
	
	degradedChecker := NewCheckerFunc("degraded", func(ctx context.Context) CheckResult {
		return CheckResult{Name: "degraded", Status: StatusDegraded, Message: "Warning"}
	})
	
	unhealthyChecker := NewCheckerFunc("unhealthy", func(ctx context.Context) CheckResult {
		return CheckResult{Name: "unhealthy", Status: StatusUnhealthy, Message: "Error"}
	})
	
	// Test with only healthy and degraded (should result in degraded overall)
	registry.Register(healthyChecker)
	registry.Register(degradedChecker)
	
	health := registry.Check(context.Background())
	assert.Equal(t, StatusDegraded, health.Status)
	assert.Equal(t, 2, len(health.Checks))
	
	// Add unhealthy checker (should result in unhealthy overall)
	registry.Register(unhealthyChecker)
	
	health = registry.Check(context.Background())
	assert.Equal(t, StatusUnhealthy, health.Status)
	assert.Equal(t, 3, len(health.Checks))
}

func TestRegistry_Check_Timeout(t *testing.T) {
	registry := NewRegistry()
	
	slowChecker := NewCheckerFunc("slow", func(ctx context.Context) CheckResult {
		select {
		case <-time.After(100 * time.Millisecond):
			return CheckResult{Name: "slow", Status: StatusHealthy}
		case <-ctx.Done():
			return CheckResult{Name: "slow", Status: StatusUnhealthy, Error: ctx.Err().Error()}
		}
	})
	
	registry.Register(slowChecker)
	
	// Create context with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	
	health := registry.Check(ctx)
	
	assert.Equal(t, StatusUnhealthy, health.Status)
	assert.Equal(t, 1, len(health.Checks))
	assert.Equal(t, StatusUnhealthy, health.Checks["slow"].Status)
	assert.Contains(t, health.Checks["slow"].Message, "timed out")
}

func TestRegistry_Check_ConcurrentExecution(t *testing.T) {
	registry := NewRegistry()
	
	// Add multiple checkers that take some time
	for i := 0; i < 5; i++ {
		name := "checker-" + string(rune('1'+i))
		checker := NewCheckerFunc(name, func(ctx context.Context) CheckResult {
			time.Sleep(20 * time.Millisecond) // Simulate work
			return CheckResult{
				Name:      name,
				Status:    StatusHealthy,
				Duration:  20 * time.Millisecond,
				Timestamp: time.Now(),
			}
		})
		registry.Register(checker)
	}
	
	start := time.Now()
	health := registry.Check(context.Background())
	duration := time.Since(start)
	
	// Should complete in much less time than sequential execution (5 * 20ms = 100ms)
	// Allow some margin for overhead
	assert.Less(t, duration, 80*time.Millisecond)
	assert.Equal(t, StatusHealthy, health.Status)
	assert.Equal(t, 5, len(health.Checks))
}

func TestNewHandler(t *testing.T) {
	registry := NewRegistry()
	timeout := 30 * time.Second
	
	handler := NewHandler(registry, timeout)
	
	assert.NotNil(t, handler)
	assert.Equal(t, registry, handler.registry)
	assert.Equal(t, timeout, handler.timeout)
}

func TestHandler_ServeHTTP_MethodNotAllowed(t *testing.T) {
	registry := NewRegistry()
	handler := NewHandler(registry, 30*time.Second)
	
	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	w := httptest.NewRecorder()
	
	handler.ServeHTTP(w, req)
	
	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestHandler_ServeHTTP_Healthy(t *testing.T) {
	registry := NewRegistry()
	
	checker := NewCheckerFunc("test", func(ctx context.Context) CheckResult {
		return CheckResult{
			Name:      "test",
			Status:    StatusHealthy,
			Message:   "OK",
			Timestamp: time.Now(),
		}
	})
	registry.Register(checker)
	
	handler := NewHandler(registry, 30*time.Second)
	
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	
	handler.ServeHTTP(w, req)
	
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	
	// Verify response contains valid JSON
	body := w.Body.String()
	assert.Contains(t, body, "\"status\": \"healthy\"")
	assert.Contains(t, body, "\"test\"")
}

func TestHandler_ServeHTTP_Degraded(t *testing.T) {
	registry := NewRegistry()
	
	checker := NewCheckerFunc("test", func(ctx context.Context) CheckResult {
		return CheckResult{
			Name:      "test",
			Status:    StatusDegraded,
			Message:   "Warning",
			Timestamp: time.Now(),
		}
	})
	registry.Register(checker)
	
	handler := NewHandler(registry, 30*time.Second)
	
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	
	handler.ServeHTTP(w, req)
	
	assert.Equal(t, http.StatusOK, w.Code) // Degraded still returns 200
	assert.Contains(t, w.Body.String(), "\"status\": \"degraded\"")
}

func TestHandler_ServeHTTP_Unhealthy(t *testing.T) {
	registry := NewRegistry()
	
	checker := NewCheckerFunc("test", func(ctx context.Context) CheckResult {
		return CheckResult{
			Name:      "test",
			Status:    StatusUnhealthy,
			Message:   "Error",
			Timestamp: time.Now(),
		}
	})
	registry.Register(checker)
	
	handler := NewHandler(registry, 30*time.Second)
	
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	
	handler.ServeHTTP(w, req)
	
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Contains(t, w.Body.String(), "\"status\": \"unhealthy\"")
}

func TestReadinessHandler(t *testing.T) {
	registry := NewRegistry()
	
	t.Run("ready", func(t *testing.T) {
		healthyChecker := NewCheckerFunc("test", func(ctx context.Context) CheckResult {
			return CheckResult{Name: "test", Status: StatusHealthy}
		})
		registry.Register(healthyChecker)
		
		handler := ReadinessHandler(registry)
		req := httptest.NewRequest(http.MethodGet, "/ready", nil)
		w := httptest.NewRecorder()
		
		handler(w, req)
		
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "ready", w.Body.String())
	})
	
	t.Run("not ready", func(t *testing.T) {
		registry := NewRegistry() // Fresh registry
		unhealthyChecker := NewCheckerFunc("test", func(ctx context.Context) CheckResult {
			return CheckResult{Name: "test", Status: StatusUnhealthy}
		})
		registry.Register(unhealthyChecker)
		
		handler := ReadinessHandler(registry)
		req := httptest.NewRequest(http.MethodGet, "/ready", nil)
		w := httptest.NewRecorder()
		
		handler(w, req)
		
		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
		assert.Equal(t, "not ready", w.Body.String())
	})
	
	t.Run("degraded is ready", func(t *testing.T) {
		registry := NewRegistry() // Fresh registry
		degradedChecker := NewCheckerFunc("test", func(ctx context.Context) CheckResult {
			return CheckResult{Name: "test", Status: StatusDegraded}
		})
		registry.Register(degradedChecker)
		
		handler := ReadinessHandler(registry)
		req := httptest.NewRequest(http.MethodGet, "/ready", nil)
		w := httptest.NewRecorder()
		
		handler(w, req)
		
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "ready", w.Body.String())
	})
}

func TestLivenessHandler(t *testing.T) {
	handler := LivenessHandler()
	req := httptest.NewRequest(http.MethodGet, "/alive", nil)
	w := httptest.NewRecorder()
	
	handler(w, req)
	
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "alive", w.Body.String())
}

func TestCheckResult_Fields(t *testing.T) {
	now := time.Now()
	duration := 100 * time.Millisecond
	details := map[string]interface{}{
		"key1": "value1",
		"key2": 42,
	}
	
	result := CheckResult{
		Name:      "test-check",
		Status:    StatusHealthy,
		Message:   "test message",
		Duration:  duration,
		Details:   details,
		Timestamp: now,
		Error:     "test error",
	}
	
	assert.Equal(t, "test-check", result.Name)
	assert.Equal(t, StatusHealthy, result.Status)
	assert.Equal(t, "test message", result.Message)
	assert.Equal(t, duration, result.Duration)
	assert.Equal(t, details, result.Details)
	assert.Equal(t, now, result.Timestamp)
	assert.Equal(t, "test error", result.Error)
}

func TestOverallHealth_Fields(t *testing.T) {
	now := time.Now()
	duration := 200 * time.Millisecond
	checks := map[string]CheckResult{
		"check1": {Name: "check1", Status: StatusHealthy},
		"check2": {Name: "check2", Status: StatusDegraded},
	}
	metadata := map[string]interface{}{
		"version": "1.0.0",
		"service": "test",
	}
	
	health := OverallHealth{
		Status:    StatusDegraded,
		Timestamp: now,
		Duration:  duration,
		Checks:    checks,
		Metadata:  metadata,
	}
	
	assert.Equal(t, StatusDegraded, health.Status)
	assert.Equal(t, now, health.Timestamp)
	assert.Equal(t, duration, health.Duration)
	assert.Equal(t, checks, health.Checks)
	assert.Equal(t, metadata, health.Metadata)
}

// Benchmark tests
func BenchmarkRegistry_Check_SingleChecker(b *testing.B) {
	registry := NewRegistry()
	checker := NewCheckerFunc("bench", func(ctx context.Context) CheckResult {
		return CheckResult{Name: "bench", Status: StatusHealthy}
	})
	registry.Register(checker)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		registry.Check(context.Background())
	}
}

func BenchmarkRegistry_Check_MultipleCheckers(b *testing.B) {
	registry := NewRegistry()
	
	for i := 0; i < 10; i++ {
		name := "bench-" + string(rune('0'+i))
		checker := NewCheckerFunc(name, func(ctx context.Context) CheckResult {
			return CheckResult{Name: name, Status: StatusHealthy}
		})
		registry.Register(checker)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		registry.Check(context.Background())
	}
}