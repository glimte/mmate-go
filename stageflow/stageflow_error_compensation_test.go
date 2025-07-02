package stageflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/glimte/mmate-go/internal/reliability"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Test error handling in stage execution
func TestStageErrorHandling(t *testing.T) {
	t.Run("Queue-based workflow execution publishes to first stage", func(t *testing.T) {
		engine := createTestEngineForErrorCompensation()
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

		// Execute workflow - in queue-based mode, this publishes first message
		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		// Queue-based execution: Execute() succeeds if message is published
		assert.NoError(t, err, "Execute should succeed - publishes to first stage queue")
		assert.Equal(t, WorkflowRunning, state.Status)
		assert.False(t, stage3Executed, "Stage 3 should not have executed yet")
		
		// Initial state should have no completed stages (execution is async)
		assert.Len(t, state.StageResults, 0, "No stages completed yet - execution is asynchronous")
	})

	t.Run("Optional stage failure - workflow starts correctly", func(t *testing.T) {
		engine := createTestEngineForErrorCompensation()
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

		// Stage 3 should execute
		workflow.AddStage("stage-3", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return &StageResult{Status: StageCompleted}, nil
		}))

		engine.RegisterWorkflow(workflow)

		// Execute workflow - queue-based execution only starts workflow
		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.NoError(t, err)
		assert.Equal(t, WorkflowRunning, state.Status) // Queue-based starts as running
		
		// Initial state should have no completed stages (execution is async)
		assert.Len(t, state.StageResults, 0, "No stages completed yet - execution is asynchronous")
	})

	t.Run("Stage timeout handling - workflow starts correctly", func(t *testing.T) {
		engine := createTestEngineForErrorCompensation()
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

		// Execute workflow - queue-based execution only starts workflow
		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.NoError(t, err) // Starting the workflow should succeed
		assert.Equal(t, WorkflowRunning, state.Status) // Queue-based starts as running
	})

	t.Run("Context cancellation - workflow starts correctly", func(t *testing.T) {
		engine := createTestEngineForErrorCompensation()
		workflow := NewWorkflow("test-workflow", "Test Workflow")

		// Stage 1 executes quickly
		workflow.AddStage("stage-1", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return &StageResult{Status: StageCompleted}, nil
		}))

		// Stage 2 waits for cancellation
		workflow.AddStage("stage-2", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(100 * time.Millisecond):
				return &StageResult{Status: StageCompleted}, nil
			}
		}))

		engine.RegisterWorkflow(workflow)

		// Execute with normal context - queue-based execution only starts workflow
		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.NoError(t, err) // Starting the workflow should succeed
		assert.Equal(t, WorkflowRunning, state.Status) // Queue-based starts as running
	})
}

// Test compensation logic
func TestCompensation(t *testing.T) {
	t.Run("Basic compensation setup - workflow starts correctly", func(t *testing.T) {
		engine := createTestEngineForErrorCompensation()
		workflow := NewWorkflow("test-workflow", "Test Workflow")

		// Stage 1 with compensation
		workflow.AddStage("stage-1", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return &StageResult{
				Status: StageCompleted,
				Data:   map[string]interface{}{"stage1": "data"},
			}, nil
		}))
		workflow.AddCompensation("stage-1", CompensationHandlerFunc(func(ctx context.Context, state *WorkflowState, result *StageResult) error {
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
			return nil
		}))

		// Stage 3 fails
		workflow.AddStage("stage-3", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return nil, errors.New("stage 3 failure")
		}))

		engine.RegisterWorkflow(workflow)

		// Execute workflow - queue-based execution only starts workflow
		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.NoError(t, err) // Starting the workflow should succeed
		assert.Equal(t, WorkflowRunning, state.Status) // Queue-based starts as running
		
		// Initial state should have no completed stages (execution is async)
		assert.Len(t, state.StageResults, 0, "No stages completed yet - execution is asynchronous")
	})

	t.Run("Compensation with original result data - workflow starts correctly", func(t *testing.T) {
		engine := createTestEngineForErrorCompensation()
		workflow := NewWorkflow("test-workflow", "Test Workflow")

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
			return nil
		}))

		// Failing stage
		workflow.AddStage("fail-stage", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return nil, errors.New("trigger compensation")
		}))

		engine.RegisterWorkflow(workflow)

		// Execute workflow - queue-based execution only starts workflow
		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.NoError(t, err) // Starting the workflow should succeed
		assert.Equal(t, WorkflowRunning, state.Status) // Queue-based starts as running
	})

	t.Run("Compensation failure handling - workflow starts correctly", func(t *testing.T) {
		engine := createTestEngineForErrorCompensation()
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

		// Execute workflow - queue-based execution only starts workflow
		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.NoError(t, err) // Starting the workflow should succeed
		assert.Equal(t, WorkflowRunning, state.Status) // Queue-based starts as running
	})

	t.Run("No compensation for skipped stages - workflow starts correctly", func(t *testing.T) {
		engine := createTestEngineForErrorCompensation()
		workflow := NewWorkflow("test-workflow", "Test Workflow")

		// Optional stage that will be skipped due to dependency
		workflow.AddStageWithOptions("optional-stage",
			StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
				return &StageResult{Status: StageCompleted}, nil
			}),
			WithRequired(false),
			WithDependencies("non-existent-stage"),
		)

		workflow.AddCompensation("optional-stage", CompensationHandlerFunc(func(ctx context.Context, state *WorkflowState, result *StageResult) error {
			return nil
		}))

		// Stage that fails
		workflow.AddStage("fail-stage", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return nil, errors.New("trigger compensation")
		}))

		engine.RegisterWorkflow(workflow)

		// Execute workflow - queue-based execution only starts workflow
		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.NoError(t, err) // Starting the workflow should succeed
		assert.Equal(t, WorkflowRunning, state.Status) // Queue-based starts as running
	})
}

// Test retry policies
func TestStageRetryPolicies(t *testing.T) {
	t.Run("Stage with retry policy - workflow starts correctly", func(t *testing.T) {
		engine := createTestEngineForErrorCompensation()
		workflow := NewWorkflow("test-workflow", "Test Workflow")

		maxAttempts := 3

		// Add stage with retry policy
		workflow.AddStageWithOptions("retry-stage",
			StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
				return &StageResult{Status: StageCompleted}, nil
			}),
			WithStageRetryPolicy(reliability.NewLinearBackoff(10*time.Millisecond, maxAttempts-1)),
		)

		engine.RegisterWorkflow(workflow)

		// Execute workflow - queue-based execution only starts workflow
		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.NoError(t, err) // Starting the workflow should succeed
		assert.Equal(t, WorkflowRunning, state.Status) // Queue-based starts as running
	})

	t.Run("Retry policy exhaustion - workflow starts correctly", func(t *testing.T) {
		engine := createTestEngineForErrorCompensation()
		workflow := NewWorkflow("test-workflow", "Test Workflow")

		// Add stage that always fails
		workflow.AddStageWithOptions("always-fail",
			StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
				return nil, errors.New("persistent failure")
			}),
			WithStageRetryPolicy(reliability.NewLinearBackoff(5*time.Millisecond, 2)),
		)

		engine.RegisterWorkflow(workflow)

		// Execute workflow - queue-based execution only starts workflow
		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.NoError(t, err) // Starting the workflow should succeed
		assert.Equal(t, WorkflowRunning, state.Status) // Queue-based starts as running
	})

	t.Run("Retry with exponential backoff - workflow starts correctly", func(t *testing.T) {
		engine := createTestEngineForErrorCompensation()
		workflow := NewWorkflow("test-workflow", "Test Workflow")

		// Add stage with exponential backoff
		workflow.AddStageWithOptions("backoff-stage",
			StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
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

		// Execute workflow - queue-based execution only starts workflow
		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.NoError(t, err) // Starting the workflow should succeed
		assert.Equal(t, WorkflowRunning, state.Status) // Queue-based starts as running
	})
}

// Test error propagation and workflow status
func TestErrorPropagationAndStatus(t *testing.T) {
	t.Run("Error message propagation - workflow starts correctly", func(t *testing.T) {
		engine := createTestEngineForErrorCompensation()
		workflow := NewWorkflow("test-workflow", "Test Workflow")

		errorMsg := "detailed error: database connection failed"
		workflow.AddStage("error-stage", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return nil, errors.New(errorMsg)
		}))

		engine.RegisterWorkflow(workflow)

		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)

		assert.NoError(t, err) // Starting the workflow should succeed
		assert.Equal(t, WorkflowRunning, state.Status) // Queue-based starts as running
	})

	t.Run("Multiple stage failures setup - workflow starts correctly", func(t *testing.T) {
		engine := createTestEngineForErrorCompensation()
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

		assert.NoError(t, err) // Starting the workflow should succeed
		assert.Equal(t, WorkflowRunning, state.Status) // Queue-based starts as running
		
		// Initial state should have no completed stages (execution is async)
		assert.Len(t, state.StageResults, 0, "No stages completed yet - execution is asynchronous")
	})
}

// Test concurrent error scenarios
func TestConcurrentErrorHandling(t *testing.T) {
	t.Run("Multiple workflows with failures - workflows start correctly", func(t *testing.T) {
		engine := createTestEngineForErrorCompensation()
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

		// All should start as running (queue-based execution)
		runningCount := 0
		for state := range results {
			if state.Status == WorkflowRunning {
				runningCount++
				assert.Len(t, state.StageResults, 0) // No stages completed yet - async execution
			}
		}
		assert.Equal(t, 5, runningCount)
	})
}

// Helper to create test engine with in-memory store for error compensation tests
func createTestEngineForErrorCompensation() *StageFlowEngine {
	store := NewInMemoryStateStore()
	// Create mock publisher and subscriber for testing
	publisher := &mockPublisher{}
	subscriber := &mockSubscriber{}
	transport := &mockTransport{}
	
	// Setup mock expectations
	publisher.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	subscriber.On("Subscribe", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	transport.On("CreateQueue", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil).Maybe()
	
	engine := NewStageFlowEngine(publisher, subscriber, transport, WithStateStore(store))
	return engine
}

// Benchmark error handling and compensation
func BenchmarkErrorHandlingAndCompensation(b *testing.B) {
	engine := createTestEngineForErrorCompensation()
	
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