package stageflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// MockStateStore is a mock implementation of StateStore for testing
type MockStateStore struct {
	mu             sync.RWMutex
	states         map[string]*WorkflowState
	saveCallCount  int
	loadCallCount  int
	saveErrors     map[string]error
	loadErrors     map[string]error
	saveDelay      time.Duration
	loadDelay      time.Duration
	failAfterCount int
}

func NewMockStateStore() *MockStateStore {
	return &MockStateStore{
		states:     make(map[string]*WorkflowState),
		saveErrors: make(map[string]error),
		loadErrors: make(map[string]error),
	}
}

func (m *MockStateStore) SaveState(ctx context.Context, state *WorkflowState) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.saveCallCount++

	// Simulate failures after certain count
	if m.failAfterCount > 0 && m.saveCallCount > m.failAfterCount {
		return errors.New("save failed after count limit")
	}

	// Simulate delay
	if m.saveDelay > 0 {
		select {
		case <-time.After(m.saveDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Check for configured errors
	if err, exists := m.saveErrors[state.InstanceID]; exists {
		return err
	}

	// Deep copy state
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	var stateCopy WorkflowState
	err = json.Unmarshal(data, &stateCopy)
	if err != nil {
		return err
	}

	m.states[state.InstanceID] = &stateCopy
	return nil
}

func (m *MockStateStore) LoadState(ctx context.Context, instanceID string) (*WorkflowState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.loadCallCount++

	// Simulate delay
	if m.loadDelay > 0 {
		select {
		case <-time.After(m.loadDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// Check for configured errors
	if err, exists := m.loadErrors[instanceID]; exists {
		return nil, err
	}

	state, exists := m.states[instanceID]
	if !exists {
		return nil, fmt.Errorf("workflow state not found: %s", instanceID)
	}

	// Deep copy state
	data, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}

	var stateCopy WorkflowState
	err = json.Unmarshal(data, &stateCopy)
	if err != nil {
		return nil, err
	}

	return &stateCopy, nil
}

func (m *MockStateStore) DeleteState(ctx context.Context, instanceID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.states, instanceID)
	return nil
}

func (m *MockStateStore) ListActiveWorkflows(ctx context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var activeWorkflows []string
	for instanceID, state := range m.states {
		if state.Status == WorkflowRunning || state.Status == WorkflowPending {
			activeWorkflows = append(activeWorkflows, instanceID)
		}
	}

	return activeWorkflows, nil
}

// Test InMemoryStateStore implementation
func TestInMemoryStateStorePersistence(t *testing.T) {
	t.Run("Basic save and load", func(t *testing.T) {
		store := NewInMemoryStateStore()
		ctx := context.Background()

		state := &WorkflowState{
			WorkflowID:   "test-workflow",
			InstanceID:   "instance-123",
			Status:       WorkflowRunning,
			CurrentStage: "stage-1",
			GlobalData: map[string]interface{}{
				"key1": "value1",
				"key2": 42,
			},
			StartTime:    time.Now(),
			LastModified: time.Now(),
			Version:      1,
		}

		// Save state
		err := store.SaveState(ctx, state)
		assert.NoError(t, err)

		// Load state
		loaded, err := store.LoadState(ctx, "instance-123")
		assert.NoError(t, err)
		assert.Equal(t, state.WorkflowID, loaded.WorkflowID)
		assert.Equal(t, state.InstanceID, loaded.InstanceID)
		assert.Equal(t, state.Status, loaded.Status)
		assert.Equal(t, state.GlobalData["key1"], loaded.GlobalData["key1"])
		assert.Equal(t, float64(42), loaded.GlobalData["key2"]) // JSON unmarshals numbers as float64

		// Verify deep copy - modifying loaded state shouldn't affect stored state
		loaded.Status = WorkflowCompleted
		loaded.GlobalData["key3"] = "new value"

		reloaded, err := store.LoadState(ctx, "instance-123")
		assert.NoError(t, err)
		assert.Equal(t, WorkflowRunning, reloaded.Status)
		assert.Nil(t, reloaded.GlobalData["key3"])
	})

	t.Run("Load non-existent state", func(t *testing.T) {
		store := NewInMemoryStateStore()
		ctx := context.Background()

		_, err := store.LoadState(ctx, "non-existent")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("Delete state", func(t *testing.T) {
		store := NewInMemoryStateStore()
		ctx := context.Background()

		state := &WorkflowState{
			InstanceID: "instance-123",
			Status:     WorkflowRunning,
		}

		// Save and verify exists
		err := store.SaveState(ctx, state)
		assert.NoError(t, err)

		loaded, err := store.LoadState(ctx, "instance-123")
		assert.NoError(t, err)
		assert.NotNil(t, loaded)

		// Delete and verify doesn't exist
		err = store.DeleteState(ctx, "instance-123")
		assert.NoError(t, err)

		_, err = store.LoadState(ctx, "instance-123")
		assert.Error(t, err)
	})

	t.Run("List active workflows", func(t *testing.T) {
		store := NewInMemoryStateStore()
		ctx := context.Background()

		states := []*WorkflowState{
			{InstanceID: "running-1", Status: WorkflowRunning},
			{InstanceID: "pending-1", Status: WorkflowPending},
			{InstanceID: "completed-1", Status: WorkflowCompleted},
			{InstanceID: "failed-1", Status: WorkflowFailed},
			{InstanceID: "running-2", Status: WorkflowRunning},
		}

		for _, state := range states {
			err := store.SaveState(ctx, state)
			assert.NoError(t, err)
		}

		active, err := store.ListActiveWorkflows(ctx)
		assert.NoError(t, err)
		assert.Len(t, active, 3)
		assert.Contains(t, active, "running-1")
		assert.Contains(t, active, "running-2")
		assert.Contains(t, active, "pending-1")
	})

	t.Run("Save nil state", func(t *testing.T) {
		store := NewInMemoryStateStore()
		ctx := context.Background()

		err := store.SaveState(ctx, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "nil")
	})
}

// Test state persistence during workflow execution
func TestWorkflowStatePersistence(t *testing.T) {
	t.Run("State saved after each stage", func(t *testing.T) {
		mockStore := NewMockStateStore()
		engine := createTestEngine(mockStore)

		workflow := NewWorkflow("test-workflow", "Test Workflow")
		
		// Add stages that track execution
		executedStages := &sync.Map{}
		
		for i := 0; i < 3; i++ {
			stageID := fmt.Sprintf("stage-%d", i)
			workflow.AddStage(stageID, StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
				executedStages.Store(state.CurrentStage, true)
				return &StageResult{
					Status: StageCompleted,
					Data: map[string]interface{}{
						"result": fmt.Sprintf("data-%s", state.CurrentStage),
					},
				}, nil
			}))
		}

		engine.RegisterWorkflow(workflow)

		// Execute workflow
		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", map[string]interface{}{
			"initial": "data",
		})

		assert.NoError(t, err)
		assert.NotNil(t, state)
		assert.Equal(t, WorkflowCompleted, state.Status)

		// Verify save was called: initial + after each stage + final
		assert.Equal(t, 5, mockStore.saveCallCount)

		// Verify all stages were executed
		for i := 0; i < 3; i++ {
			stageID := fmt.Sprintf("stage-%d", i)
			_, exists := executedStages.Load(stageID)
			assert.True(t, exists, "Stage %s should have been executed", stageID)
		}

		// Verify final state contains all stage results
		assert.Len(t, state.StageResults, 3)
		for i, result := range state.StageResults {
			assert.Equal(t, fmt.Sprintf("stage-%d", i), result.StageID)
			assert.Equal(t, StageCompleted, result.Status)
		}
	})

	t.Run("State recovery after failure", func(t *testing.T) {
		mockStore := NewMockStateStore()
		engine := createTestEngine(mockStore)

		workflow := NewWorkflow("test-workflow", "Test Workflow")
		
		// Stage 1 succeeds
		workflow.AddStage("stage-1", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return &StageResult{
				Status: StageCompleted,
				Data:   map[string]interface{}{"stage1": "completed"},
			}, nil
		}))

		// Stage 2 fails
		workflow.AddStage("stage-2", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return nil, errors.New("stage 2 failed")
		}))

		engine.RegisterWorkflow(workflow)

		// Execute workflow (will fail)
		ctx := context.Background()
		_, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)
		assert.Error(t, err)

		// Load state from store
		savedStates := mockStore.states
		assert.Len(t, savedStates, 1)

		var instanceID string
		var savedState *WorkflowState
		for id, state := range savedStates {
			instanceID = id
			savedState = state
			break
		}

		assert.NotEmpty(t, instanceID)
		assert.Equal(t, WorkflowFailed, savedState.Status)
		assert.Contains(t, savedState.ErrorMessage, "stage 2 failed")
		
		// Verify stage 1 was completed
		assert.Len(t, savedState.StageResults, 1)
		assert.Equal(t, "stage-1", savedState.StageResults[0].StageID)
		assert.Equal(t, StageCompleted, savedState.StageResults[0].Status)
	})

	t.Run("State persistence with save errors", func(t *testing.T) {
		mockStore := NewMockStateStore()
		engine := createTestEngine(mockStore)

		workflow := NewWorkflow("test-workflow", "Test Workflow")
		workflow.AddStage("stage-1", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return &StageResult{Status: StageCompleted}, nil
		}))

		engine.RegisterWorkflow(workflow)

		// Execute workflow first to get instance ID
		ctx := context.Background()
		state, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)
		assert.NoError(t, err)
		assert.NotNil(t, state)
		
		// Now test save failure on subsequent execution
		mockStore.failAfterCount = mockStore.saveCallCount // Fail on next save
		
		// Try another execution
		_, err = engine.ExecuteWorkflow(ctx, "test-workflow", nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "save")
	})
}

// Test concurrent state access
func TestConcurrentStateAccess(t *testing.T) {
	t.Run("Concurrent save operations", func(t *testing.T) {
		store := NewInMemoryStateStore()
		ctx := context.Background()

		var wg sync.WaitGroup
		errors := make(chan error, 10)

		// Simulate 10 concurrent save operations
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				
				state := &WorkflowState{
					InstanceID: fmt.Sprintf("instance-%d", id),
					Status:     WorkflowRunning,
					Version:    1,
					GlobalData: map[string]interface{}{
						"thread": id,
						"data":   fmt.Sprintf("data-%d", id),
					},
				}

				err := store.SaveState(ctx, state)
				if err != nil {
					errors <- err
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		// Check for errors
		for err := range errors {
			t.Errorf("Concurrent save error: %v", err)
		}

		// Verify all states were saved
		for i := 0; i < 10; i++ {
			instanceID := fmt.Sprintf("instance-%d", i)
			state, err := store.LoadState(ctx, instanceID)
			assert.NoError(t, err)
			assert.Equal(t, float64(i), state.GlobalData["thread"]) // JSON unmarshals as float64
		}
	})

	t.Run("Concurrent read/write operations", func(t *testing.T) {
		store := NewInMemoryStateStore()
		ctx := context.Background()
		instanceID := "shared-instance"

		// Initial state
		initialState := &WorkflowState{
			InstanceID: instanceID,
			Status:     WorkflowRunning,
			Version:    1,
			GlobalData: map[string]interface{}{
				"counter": 0,
			},
		}
		err := store.SaveState(ctx, initialState)
		assert.NoError(t, err)

		var wg sync.WaitGroup
		readCount := int32(0)
		writeCount := int32(0)

		// 5 readers
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					state, err := store.LoadState(ctx, instanceID)
					if err == nil && state != nil {
						atomic.AddInt32(&readCount, 1)
					}
					time.Sleep(time.Microsecond)
				}
			}()
		}

		// 2 writers
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(writerID int) {
				defer wg.Done()
				for j := 0; j < 50; j++ {
					state, err := store.LoadState(ctx, instanceID)
					if err == nil {
						state.Version++
						state.GlobalData[fmt.Sprintf("writer-%d", writerID)] = j
						store.SaveState(ctx, state)
						atomic.AddInt32(&writeCount, 1)
					}
					time.Sleep(time.Microsecond * 2)
				}
			}(i)
		}

		wg.Wait()

		assert.Greater(t, atomic.LoadInt32(&readCount), int32(100))
		assert.Greater(t, atomic.LoadInt32(&writeCount), int32(50))
	})
}

// Test state versioning and consistency
func TestStateVersioning(t *testing.T) {
	t.Run("Version increments on updates", func(t *testing.T) {
		mockStore := NewMockStateStore()
		engine := createTestEngine(mockStore)

		workflow := NewWorkflow("test-workflow", "Test Workflow")
		
		// Add multiple stages
		for i := 0; i < 5; i++ {
			stageID := fmt.Sprintf("stage-%d", i)
			workflow.AddStage(stageID, StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
				return &StageResult{Status: StageCompleted}, nil
			}))
		}

		engine.RegisterWorkflow(workflow)

		ctx := context.Background()
		finalState, err := engine.ExecuteWorkflow(ctx, "test-workflow", nil)
		assert.NoError(t, err)

		// Version should increment with each update
		assert.Greater(t, finalState.Version, 5)
		
		// Load from store and verify version
		stored, err := mockStore.LoadState(ctx, finalState.InstanceID)
		assert.NoError(t, err)
		assert.Equal(t, finalState.Version, stored.Version)
	})

	t.Run("State consistency across saves", func(t *testing.T) {
		store := NewInMemoryStateStore()
		ctx := context.Background()

		originalState := &WorkflowState{
			WorkflowID: "test-workflow",
			InstanceID: "instance-123",
			Status:     WorkflowRunning,
			StageResults: []StageResult{
				{StageID: "stage-1", Status: StageCompleted},
				{StageID: "stage-2", Status: StageRunning},
			},
			GlobalData: map[string]interface{}{
				"nested": map[string]interface{}{
					"key": "value",
					"num": 123,
				},
			},
			StartTime:    time.Now(),
			LastModified: time.Now(),
			Version:      1,
		}

		// Save state
		err := store.SaveState(ctx, originalState)
		assert.NoError(t, err)

		// Load and verify deep equality
		loaded, err := store.LoadState(ctx, "instance-123")
		assert.NoError(t, err)

		assert.Equal(t, originalState.WorkflowID, loaded.WorkflowID)
		assert.Equal(t, originalState.InstanceID, loaded.InstanceID)
		assert.Equal(t, originalState.Status, loaded.Status)
		assert.Len(t, loaded.StageResults, 2)
		assert.Equal(t, originalState.StageResults[0].StageID, loaded.StageResults[0].StageID)
		assert.Equal(t, originalState.StageResults[1].Status, loaded.StageResults[1].Status)
		
		// Verify nested data
		nestedData := loaded.GlobalData["nested"].(map[string]interface{})
		assert.Equal(t, "value", nestedData["key"])
		assert.Equal(t, float64(123), nestedData["num"]) // JSON unmarshals numbers as float64
	})
}

// Test state store performance
func TestStateStorePerformance(t *testing.T) {
	t.Run("Save performance with delay", func(t *testing.T) {
		mockStore := NewMockStateStore()
		mockStore.saveDelay = 50 * time.Millisecond
		
		ctx := context.Background()
		start := time.Now()

		state := &WorkflowState{
			InstanceID: "perf-test",
			Status:     WorkflowRunning,
		}

		err := mockStore.SaveState(ctx, state)
		assert.NoError(t, err)

		duration := time.Since(start)
		assert.GreaterOrEqual(t, duration, 50*time.Millisecond)
	})

	t.Run("Context cancellation during save", func(t *testing.T) {
		mockStore := NewMockStateStore()
		mockStore.saveDelay = 100 * time.Millisecond

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		state := &WorkflowState{
			InstanceID: "timeout-test",
			Status:     WorkflowRunning,
		}

		err := mockStore.SaveState(ctx, state)
		assert.Equal(t, context.DeadlineExceeded, err)
	})
}

// Helper function to create test engine with mock store
func createTestEngine(store StateStore) *StageFlowEngine {
	// Mock publisher and subscriber (not used in these tests)
	return &StageFlowEngine{
		stateStore: store,
		workflows:  make(map[string]*Workflow),
		logger:     slog.Default(),
	}
}

// Benchmark tests
func BenchmarkStateStore(b *testing.B) {
	store := NewInMemoryStateStore()
	ctx := context.Background()

	state := &WorkflowState{
		WorkflowID: "bench-workflow",
		InstanceID: "bench-instance",
		Status:     WorkflowRunning,
		GlobalData: map[string]interface{}{
			"key1": "value1",
			"key2": 42,
			"key3": []string{"a", "b", "c"},
		},
		StageResults: []StageResult{
			{StageID: "stage-1", Status: StageCompleted},
			{StageID: "stage-2", Status: StageCompleted},
		},
		Version: 1,
	}

	b.Run("SaveState", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			state.InstanceID = fmt.Sprintf("instance-%d", i)
			store.SaveState(ctx, state)
		}
	})

	b.Run("LoadState", func(b *testing.B) {
		// Pre-populate
		for i := 0; i < 1000; i++ {
			state.InstanceID = fmt.Sprintf("instance-%d", i)
			store.SaveState(ctx, state)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			instanceID := fmt.Sprintf("instance-%d", i%1000)
			store.LoadState(ctx, instanceID)
		}
	})

	b.Run("ListActiveWorkflows", func(b *testing.B) {
		// Pre-populate with mix of active and inactive
		for i := 0; i < 1000; i++ {
			state.InstanceID = fmt.Sprintf("instance-%d", i)
			if i%3 == 0 {
				state.Status = WorkflowRunning
			} else {
				state.Status = WorkflowCompleted
			}
			store.SaveState(ctx, state)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			store.ListActiveWorkflows(ctx)
		}
	})
}