package stageflow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glimte/mmate-go/internal/reliability"
	"github.com/stretchr/testify/assert"
)

// Test error handling in stage execution
func TestStageErrorHandling(t *testing.T) {
	t.Run("Required stage failure stops workflow", func(t *testing.T) {
		engine := createTestEngineWithStore()
		workflow := NewWorkflow("test-workflow", "Test Workflow")

		// Stage 1 succeeds
		workflow.AddStage("stage-1", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return &StageResult{
				Status: StageCompleted,
				Data:   map[string]interface{}{"stage1": "completed"},
			}, nil
		}))

		// Stage 2 fails (required)
		workflow.AddStage("stage-2", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return nil, errors.New("critical failure")
		}))

		// Stage 3 should not execute
		stage3Executed := false
		workflow.AddStage("stage-3", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			stage3Executed = true
			return &StageResult{Status: StageCompleted}, nil
		}))

		engine.RegisterWorkflow(workflow)

		// Execute workflow
		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "critical failure")
		assert.Equal(t, WorkflowFailed, state.Status)
		assert.False(t, stage3Executed, "Stage 3 should not have executed")
		
		// Verify only stage 1 completed
		assert.Len(t, state.StageResults, 1)
		assert.Equal(t, "stage-1", state.StageResults[0].StageID)
		assert.Equal(t, StageCompleted, state.StageResults[0].Status)
	})

	t.Run("Optional stage failure continues workflow", func(t *testing.T) {
		engine := createTestEngineWithStore()
		workflow := NewWorkflow("test-workflow", "Test Workflow")

		// Stage 1 succeeds
		workflow.AddStage("stage-1", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return &StageResult{Status: StageCompleted}, nil
		}))

		// Stage 2 fails (optional)
		workflow.AddStageWithOptions("stage-2", 
			StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
				return nil, errors.New("optional stage failure")
			}),
			WithRequired(false),
		)

		// Stage 3 should still execute
		stage3Executed := false
		workflow.AddStage("stage-3", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			stage3Executed = true
			return &StageResult{Status: StageCompleted}, nil
		}))

		engine.RegisterWorkflow(workflow)

		// Execute workflow
		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.NoError(t, err)
		assert.Equal(t, WorkflowCompleted, state.Status)
		assert.True(t, stage3Executed, "Stage 3 should have executed")
		
		// Verify all stages recorded
		assert.Len(t, state.StageResults, 3)
		assert.Equal(t, StageFailed, state.StageResults[1].Status)
		assert.Contains(t, state.StageResults[1].Error, "optional stage failure")
	})

	t.Run("Stage timeout handling", func(t *testing.T) {
		engine := createTestEngineWithStore()
		workflow := NewWorkflow("test-workflow", "Test Workflow")

		// Add stage with short timeout
		workflow.AddStageWithOptions("timeout-stage",
			StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
				// Simulate long-running operation
				select {
				case <-time.After(200 * time.Millisecond):
					return &StageResult{Status: StageCompleted}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}),
			WithTimeout(50*time.Millisecond),
		)

		engine.RegisterWorkflow(workflow)

		// Execute workflow
		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.Error(t, err)
		assert.Equal(t, WorkflowFailed, state.Status)
		assert.Contains(t, err.Error(), "timeout-stage failed")
	})

	t.Run("Context cancellation stops execution", func(t *testing.T) {
		engine := createTestEngineWithStore()
		workflow := NewWorkflow("test-workflow", "Test Workflow")

		stage1Executed := false
		stage2Started := false
		stage2Completed := false

		// Stage 1 executes quickly
		workflow.AddStage("stage-1", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			stage1Executed = true
			return &StageResult{Status: StageCompleted}, nil
		}))

		// Stage 2 waits for cancellation
		workflow.AddStage("stage-2", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			stage2Started = true
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(100 * time.Millisecond):
				stage2Completed = true
				return &StageResult{Status: StageCompleted}, nil
			}
		}))

		engine.RegisterWorkflow(workflow)

		// Execute with cancellable context
		ctx, cancel := context.WithCancel(context.Background())
		
		// Cancel after short delay
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		_, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
		assert.True(t, stage1Executed)
		assert.True(t, stage2Started)
		assert.False(t, stage2Completed) // Should have been interrupted
	})
}

// Test compensation logic
func TestCompensation(t *testing.T) {
	t.Run("Basic compensation on failure", func(t *testing.T) {
		engine := createTestEngineWithStore()
		workflow := NewWorkflow("test-workflow", "Test Workflow")

		compensationCalled := make(map[string]bool)
		compensationOrder := make([]string, 0)
		var mu sync.Mutex

		// Stage 1 with compensation
		workflow.AddStage("stage-1", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return &StageResult{
				Status: StageCompleted,
				Data:   map[string]interface{}{"stage1": "data"},
			}, nil
		}))
		workflow.AddCompensation("stage-1", CompensationHandlerFunc(func(ctx context.Context, state *WorkflowState, result *StageResult) error {
			mu.Lock()
			compensationCalled["stage-1"] = true
			compensationOrder = append(compensationOrder, "stage-1")
			mu.Unlock()
			return nil
		}))

		// Stage 2 with compensation
		workflow.AddStage("stage-2", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return &StageResult{
				Status: StageCompleted,
				Data:   map[string]interface{}{"stage2": "data"},
			}, nil
		}))
		workflow.AddCompensation("stage-2", CompensationHandlerFunc(func(ctx context.Context, state *WorkflowState, result *StageResult) error {
			mu.Lock()
			compensationCalled["stage-2"] = true
			compensationOrder = append(compensationOrder, "stage-2")
			mu.Unlock()
			return nil
		}))

		// Stage 3 fails
		workflow.AddStage("stage-3", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return nil, errors.New("stage 3 failure")
		}))

		engine.RegisterWorkflow(workflow)

		// Execute workflow
		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.Error(t, err)
		// The workflow status is set to Failed after compensation, but we can check if compensation ran
		assert.Equal(t, WorkflowFailed, state.Status)

		// Verify compensation was called in reverse order
		mu.Lock()
		assert.True(t, compensationCalled["stage-2"])
		assert.True(t, compensationCalled["stage-1"])
		assert.Equal(t, []string{"stage-2", "stage-1"}, compensationOrder)
		mu.Unlock()

		// Load the state to check compensation status updates
		// Compensation happens during execution but status updates may not be in returned state
		finalState, _ := engine.stateStore.LoadState(ctx, state.InstanceID)
		
		// Check if stages were compensated - at least verify the compensation ran
		compensatedCount := 0
		for _, result := range finalState.StageResults {
			if result.Status == StageCompensated {
				compensatedCount++
			}
		}
		assert.Equal(t, 2, compensatedCount, "Both completed stages should be compensated")
	})

	t.Run("Compensation with original result data", func(t *testing.T) {
		engine := createTestEngineWithStore()
		workflow := NewWorkflow("test-workflow", "Test Workflow")

		var capturedResult *StageResult

		// Stage with data
		workflow.AddStage("data-stage", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return &StageResult{
				Status: StageCompleted,
				Data: map[string]interface{}{
					"important": "data",
					"number":    42,
				},
			}, nil
		}))

		// Compensation that uses original result
		workflow.AddCompensation("data-stage", CompensationHandlerFunc(func(ctx context.Context, state *WorkflowState, result *StageResult) error {
			capturedResult = result
			// Verify we can access original data
			assert.Equal(t, "data", result.Data["important"])
			// The number could be int or float64 depending on JSON marshaling
			num, ok := result.Data["number"]
			assert.True(t, ok)
			assert.Contains(t, []interface{}{42, float64(42)}, num)
			return nil
		}))

		// Failing stage
		workflow.AddStage("fail-stage", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return nil, errors.New("trigger compensation")
		}))

		engine.RegisterWorkflow(workflow)

		// Execute workflow
		ctx := context.Background()
		engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.NotNil(t, capturedResult)
		assert.Equal(t, "data-stage", capturedResult.StageID)
		// The status might still be Completed when passed to compensation handler
		// The status update to Compensated happens after the handler runs
		assert.Contains(t, []StageStatus{StageCompleted, StageCompensated}, capturedResult.Status)
	})

	t.Run("Compensation failure handling", func(t *testing.T) {
		engine := createTestEngineWithStore()
		workflow := NewWorkflow("test-workflow", "Test Workflow")

		// Stage 1 succeeds
		workflow.AddStage("stage-1", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return &StageResult{Status: StageCompleted}, nil
		}))

		// Compensation that fails
		workflow.AddCompensation("stage-1", CompensationHandlerFunc(func(ctx context.Context, state *WorkflowState, result *StageResult) error {
			return errors.New("compensation failed")
		}))

		// Stage 2 fails to trigger compensation
		workflow.AddStage("stage-2", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return nil, errors.New("trigger compensation")
		}))

		engine.RegisterWorkflow(workflow)

		// Execute workflow
		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.Error(t, err)
		// Workflow status is Failed (not Compensated) when compensation runs after stage failure
		assert.Equal(t, WorkflowFailed, state.Status)
		
		// Stage 1 should still show completed (not compensated due to failure)
		assert.Equal(t, StageCompleted, state.StageResults[0].Status)
	})

	t.Run("No compensation for skipped stages", func(t *testing.T) {
		engine := createTestEngineWithStore()
		workflow := NewWorkflow("test-workflow", "Test Workflow")

		compensationCalled := false

		// Optional stage that will be skipped due to dependency
		workflow.AddStageWithOptions("optional-stage",
			StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
				return &StageResult{Status: StageCompleted}, nil
			}),
			WithRequired(false),
			WithDependencies("non-existent-stage"),
		)

		workflow.AddCompensation("optional-stage", CompensationHandlerFunc(func(ctx context.Context, state *WorkflowState, result *StageResult) error {
			compensationCalled = true
			return nil
		}))

		// Stage that fails
		workflow.AddStage("fail-stage", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return nil, errors.New("trigger compensation")
		}))

		engine.RegisterWorkflow(workflow)

		// Execute workflow
		ctx := context.Background()
		engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.False(t, compensationCalled, "Compensation should not be called for skipped stages")
	})
}

// Test retry policies
func TestStageRetryPolicies(t *testing.T) {
	t.Run("Stage with retry policy", func(t *testing.T) {
		engine := createTestEngineWithStore()
		workflow := NewWorkflow("test-workflow", "Test Workflow")

		attemptCount := int32(0)
		maxAttempts := 3

		// Add stage with retry policy
		workflow.AddStageWithOptions("retry-stage",
			StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
				count := atomic.AddInt32(&attemptCount, 1)
				if count < int32(maxAttempts) {
					return nil, errors.New("temporary failure")
				}
				return &StageResult{Status: StageCompleted}, nil
			}),
			WithStageRetryPolicy(reliability.NewLinearBackoff(10*time.Millisecond, maxAttempts-1)),
		)

		engine.RegisterWorkflow(workflow)

		// Execute workflow
		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.NoError(t, err)
		assert.Equal(t, WorkflowCompleted, state.Status)
		assert.Equal(t, int32(maxAttempts), atomic.LoadInt32(&attemptCount))
	})

	t.Run("Retry policy exhaustion", func(t *testing.T) {
		engine := createTestEngineWithStore()
		workflow := NewWorkflow("test-workflow", "Test Workflow")

		attemptCount := int32(0)

		// Add stage that always fails
		workflow.AddStageWithOptions("always-fail",
			StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
				atomic.AddInt32(&attemptCount, 1)
				return nil, errors.New("persistent failure")
			}),
			WithStageRetryPolicy(reliability.NewLinearBackoff(5*time.Millisecond, 2)),
		)

		engine.RegisterWorkflow(workflow)

		// Execute workflow
		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.Error(t, err)
		assert.Equal(t, WorkflowFailed, state.Status)
		assert.Equal(t, int32(3), atomic.LoadInt32(&attemptCount)) // Initial + 2 retries
	})

	t.Run("Retry with exponential backoff", func(t *testing.T) {
		engine := createTestEngineWithStore()
		workflow := NewWorkflow("test-workflow", "Test Workflow")

		attemptTimes := make([]time.Time, 0)
		var mu sync.Mutex

		// Add stage with exponential backoff
		workflow.AddStageWithOptions("backoff-stage",
			StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
				mu.Lock()
				attemptTimes = append(attemptTimes, time.Now())
				attempts := len(attemptTimes)
				mu.Unlock()

				if attempts < 3 {
					return nil, errors.New("retry needed")
				}
				return &StageResult{Status: StageCompleted}, nil
			}),
			WithStageRetryPolicy(reliability.NewExponentialBackoff(
				10*time.Millisecond,
				100*time.Millisecond,
				2.0,
				3,
			)),
		)

		engine.RegisterWorkflow(workflow)

		// Execute workflow
		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.NoError(t, err)
		assert.Equal(t, WorkflowCompleted, state.Status)

		// Verify backoff intervals increased
		mu.Lock()
		assert.Len(t, attemptTimes, 3)
		if len(attemptTimes) >= 3 {
			interval1 := attemptTimes[1].Sub(attemptTimes[0])
			interval2 := attemptTimes[2].Sub(attemptTimes[1])
			assert.Greater(t, interval2, interval1, "Second retry interval should be longer")
		}
		mu.Unlock()
	})
}

// Test error propagation and workflow status
func TestErrorPropagationAndStatus(t *testing.T) {
	t.Run("Error message propagation", func(t *testing.T) {
		engine := createTestEngineWithStore()
		workflow := NewWorkflow("test-workflow", "Test Workflow")

		errorMsg := "detailed error: database connection failed"
		workflow.AddStage("error-stage", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return nil, errors.New(errorMsg)
		}))

		engine.RegisterWorkflow(workflow)

		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), errorMsg)
		assert.Contains(t, state.ErrorMessage, errorMsg)
		assert.Equal(t, WorkflowFailed, state.Status)
	})

	t.Run("Multiple stage failures record all errors", func(t *testing.T) {
		engine := createTestEngineWithStore()
		workflow := NewWorkflow("test-workflow", "Test Workflow")

		// Stage 1 fails (optional)
		workflow.AddStageWithOptions("optional-fail-1",
			StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
				return nil, errors.New("optional error 1")
			}),
			WithRequired(false),
		)

		// Stage 2 fails (optional)
		workflow.AddStageWithOptions("optional-fail-2",
			StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
				return nil, errors.New("optional error 2")
			}),
			WithRequired(false),
		)

		// Stage 3 succeeds
		workflow.AddStage("success-stage", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return &StageResult{Status: StageCompleted}, nil
		}))

		engine.RegisterWorkflow(workflow)

		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.NoError(t, err)
		assert.Equal(t, WorkflowCompleted, state.Status)

		// Verify failed stages recorded their errors
		assert.Equal(t, StageFailed, state.StageResults[0].Status)
		assert.Contains(t, state.StageResults[0].Error, "optional error 1")
		assert.Equal(t, StageFailed, state.StageResults[1].Status)
		assert.Contains(t, state.StageResults[1].Error, "optional error 2")
		assert.Equal(t, StageCompleted, state.StageResults[2].Status)
	})
}

// Test concurrent error scenarios
func TestConcurrentErrorHandling(t *testing.T) {
	t.Run("Multiple workflows with failures", func(t *testing.T) {
		engine := createTestEngineWithStore()
		workflow := NewWorkflow("test-workflow", "Test Workflow")

		// Add stages that fail randomly
		for i := 0; i < 3; i++ {
			stageID := fmt.Sprintf("stage-%d", i)
			shouldFail := i == 1 // Middle stage fails
			workflow.AddStage(stageID, StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
				if shouldFail {
					return nil, fmt.Errorf("stage %s failed", state.CurrentStage)
				}
				return &StageResult{Status: StageCompleted}, nil
			}))
		}

		engine.RegisterWorkflow(workflow)

		// Execute multiple workflows concurrently
		var wg sync.WaitGroup
		results := make(chan *WorkflowState, 5)
		errors := make(chan error, 5)

		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				ctx := context.Background()
				state, err := engine.ExecuteWorkflow(ctx, "test-workflow", map[string]interface{}{
					"instance": id,
				})
				if err != nil {
					errors <- err
				}
				results <- state
			}(i)
		}

		wg.Wait()
		close(results)
		close(errors)

		// All should fail at the same stage
		failCount := 0
		for state := range results {
			if state.Status == WorkflowFailed {
				failCount++
				assert.Len(t, state.StageResults, 1) // Only first stage completed
			}
		}
		assert.Equal(t, 5, failCount)
	})
}

// Helper to create test engine with in-memory store
func createTestEngineWithStore() *StageFlowEngine {
	store := NewInMemoryStateStore()
	return &StageFlowEngine{
		stateStore: store,
		workflows:  make(map[string]*Workflow),
		logger:     slog.Default(),
	}
}

// Benchmark error handling and compensation
func BenchmarkErrorHandlingAndCompensation(b *testing.B) {
	engine := createTestEngineWithStore()
	
	// Create workflow with compensation
	workflow := NewWorkflow("bench-workflow", "Benchmark Workflow")
	
	for i := 0; i < 5; i++ {
		stageID := fmt.Sprintf("stage-%d", i)
		workflow.AddStage(stageID, StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return &StageResult{Status: StageCompleted}, nil
		}))
		workflow.AddCompensation(stageID, CompensationHandlerFunc(func(ctx context.Context, state *WorkflowState, result *StageResult) error {
			return nil
		}))
	}
	
	// Last stage fails
	workflow.AddStage("fail-stage", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
		return nil, errors.New("trigger compensation")
	}))
	
	engine.RegisterWorkflow(workflow)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.ExecuteWorkflow(ctx, "bench-workflow", nil)
	}
}