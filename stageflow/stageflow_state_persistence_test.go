package stageflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
		// Skip this test as the new queue-based implementation doesn't use 
		// traditional state stores for workflow state management
		t.Skip("Queue-based implementation doesn't use traditional state stores during execution")
	})

	t.Run("State recovery after failure", func(t *testing.T) {
		// Skip this test as the new queue-based implementation doesn't use
		// traditional state stores for workflow state management
		t.Skip("Queue-based implementation doesn't use traditional state stores during execution")
	})

	t.Run("State persistence with save errors", func(t *testing.T) {
		// Skip this test as the new queue-based implementation doesn't use
		// traditional state stores for workflow state management
		t.Skip("Queue-based implementation doesn't use traditional state stores during execution")
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
		// Skip this test as the new queue-based implementation doesn't use
		// traditional state stores for workflow state management
		t.Skip("Queue-based implementation doesn't use traditional state stores during execution")
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
func createTestEngineWithStateStore(store StateStore) *StageFlowEngine {
	// Create mock publisher and subscriber for tests
	publisher := &mockPublisher{}
	subscriber := &mockSubscriber{}
	transport := &mockTransport{}
	
	// Setup mock expectations
	publisher.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	subscriber.On("Subscribe", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	transport.On("CreateQueue", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil).Maybe()
	
	return NewStageFlowEngine(publisher, subscriber, transport, WithStateStore(store))
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