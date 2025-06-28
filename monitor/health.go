package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Status represents the health status
type Status string

const (
	StatusHealthy   Status = "healthy"
	StatusDegraded  Status = "degraded"
	StatusUnhealthy Status = "unhealthy"
)

// CheckResult represents the result of a health check
type CheckResult struct {
	Name      string                 `json:"name"`
	Status    Status                 `json:"status"`
	Message   string                 `json:"message,omitempty"`
	Duration  time.Duration          `json:"duration"`
	Details   map[string]interface{} `json:"details,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	Error     string                 `json:"error,omitempty"`
}

// OverallHealth represents the overall system health
type OverallHealth struct {
	Status    Status                 `json:"status"`
	Timestamp time.Time              `json:"timestamp"`
	Duration  time.Duration          `json:"duration"`
	Checks    map[string]CheckResult `json:"checks"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// Checker defines the interface for health checks
type Checker interface {
	Check(ctx context.Context) CheckResult
	Name() string
}

// CheckerFunc is a function adapter for Checker
type CheckerFunc struct {
	name string
	fn   func(ctx context.Context) CheckResult
}

func NewCheckerFunc(name string, fn func(ctx context.Context) CheckResult) *CheckerFunc {
	return &CheckerFunc{name: name, fn: fn}
}

func (c *CheckerFunc) Check(ctx context.Context) CheckResult {
	return c.fn(ctx)
}

func (c *CheckerFunc) Name() string {
	return c.name
}

// Registry manages health checks
type Registry struct {
	checkers map[string]Checker
	metadata map[string]interface{}
	mu       sync.RWMutex
}

// NewRegistry creates a new health check registry
func NewRegistry() *Registry {
	return &Registry{
		checkers: make(map[string]Checker),
		metadata: make(map[string]interface{}),
	}
}

// Register adds a health checker
func (r *Registry) Register(checker Checker) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checkers[checker.Name()] = checker
}

// Unregister removes a health checker
func (r *Registry) Unregister(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.checkers, name)
}

// SetMetadata sets global metadata
func (r *Registry) SetMetadata(key string, value interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metadata[key] = value
}

// Check executes all registered health checks
func (r *Registry) Check(ctx context.Context) OverallHealth {
	start := time.Now()
	
	r.mu.RLock()
	checkers := make(map[string]Checker, len(r.checkers))
	for k, v := range r.checkers {
		checkers[k] = v
	}
	metadata := make(map[string]interface{}, len(r.metadata))
	for k, v := range r.metadata {
		metadata[k] = v
	}
	r.mu.RUnlock()

	checks := make(map[string]CheckResult)
	overallStatus := StatusHealthy

	// Execute checks concurrently
	type checkResult struct {
		name   string
		result CheckResult
	}
	
	resultChan := make(chan checkResult, len(checkers))
	
	for name, checker := range checkers {
		go func(name string, checker Checker) {
			result := checker.Check(ctx)
			resultChan <- checkResult{name: name, result: result}
		}(name, checker)
	}

	// Collect results
collectLoop:
	for i := 0; i < len(checkers); i++ {
		select {
		case result := <-resultChan:
			checks[result.name] = result.result
			
			// Determine overall status
			switch result.result.Status {
			case StatusUnhealthy:
				overallStatus = StatusUnhealthy
			case StatusDegraded:
				if overallStatus == StatusHealthy {
					overallStatus = StatusDegraded
				}
			}
		case <-ctx.Done():
			// Timeout or cancellation
			for name := range checkers {
				if _, exists := checks[name]; !exists {
					checks[name] = CheckResult{
						Name:      name,
						Status:    StatusUnhealthy,
						Message:   "Check timed out",
						Duration:  time.Since(start),
						Timestamp: time.Now(),
						Error:     ctx.Err().Error(),
					}
				}
			}
			overallStatus = StatusUnhealthy
			break collectLoop
		}
	}

	return OverallHealth{
		Status:    overallStatus,
		Timestamp: time.Now(),
		Duration:  time.Since(start),
		Checks:    checks,
		Metadata:  metadata,
	}
}

// Handler provides HTTP endpoint for health checks
type Handler struct {
	registry *Registry
	timeout  time.Duration
}

// NewHandler creates a new health check HTTP handler
func NewHandler(registry *Registry, timeout time.Duration) *Handler {
	return &Handler{
		registry: registry,
		timeout:  timeout,
	}
}

// ServeHTTP implements http.Handler
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	health := h.registry.Check(ctx)

	// Set status code based on health
	statusCode := http.StatusOK
	switch health.Status {
	case StatusDegraded:
		statusCode = http.StatusOK // Still OK, but with warnings
	case StatusUnhealthy:
		statusCode = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(health); err != nil {
		http.Error(w, "Failed to encode health response", http.StatusInternalServerError)
	}
}

// ReadinessHandler provides a simple readiness check
func ReadinessHandler(registry *Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		health := registry.Check(ctx)
		
		if health.Status == StatusUnhealthy {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("not ready"))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready"))
	}
}

// LivenessHandler provides a simple liveness check
func LivenessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("alive"))
	}
}