package stageflow

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/messaging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockSubscriber for queue-based testing
type mockQueueSubscriber struct {
	mock.Mock
}

func (m *mockQueueSubscriber) Subscribe(ctx context.Context, queue string, messageType string, handler messaging.MessageHandler, options ...messaging.SubscriptionOption) error {
	args := m.Called(ctx, queue, messageType, handler, options)
	return args.Error(0)
}

func (m *mockQueueSubscriber) Unsubscribe(queueName string) error {
	args := m.Called(queueName)
	return args.Error(0)
}

func (m *mockQueueSubscriber) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockPublisher for queue-based testing
type mockQueuePublisher struct {
	mock.Mock
	publishedMessages []*FlowMessageEnvelope
}

func (m *mockQueuePublisher) Publish(ctx context.Context, msg contracts.Message, options ...messaging.PublishOption) error {
	if envelope, ok := msg.(*FlowMessageEnvelope); ok {
		m.publishedMessages = append(m.publishedMessages, envelope)
	}
	args := m.Called(ctx, msg, options)
	return args.Error(0)
}

func (m *mockQueuePublisher) PublishEvent(ctx context.Context, event contracts.Event, options ...messaging.PublishOption) error {
	return m.Publish(ctx, event, options...)
}

func (m *mockQueuePublisher) PublishCommand(ctx context.Context, command contracts.Command, options ...messaging.PublishOption) error {
	return m.Publish(ctx, command, options...)
}

func (m *mockQueuePublisher) Close() error {
	return nil
}

// Test handler that tracks execution
type trackingStageHandler struct {
	stageID     string
	executed    bool
	result      *StageResult
	err         error
	executeFunc func(ctx context.Context, state *WorkflowState) (*StageResult, error)
}

func (h *trackingStageHandler) Execute(ctx context.Context, state *WorkflowState) (*StageResult, error) {
	h.executed = true
	if h.executeFunc != nil {
		return h.executeFunc(ctx, state)
	}
	if h.err != nil {
		return nil, h.err
	}
	if h.result != nil {
		return h.result, nil
	}
	return &StageResult{
		StageID: h.stageID,
		Status:  StageCompleted,
		Data:    map[string]interface{}{"executed": true},
	}, nil
}

func (h *trackingStageHandler) GetStageID() string {
	return h.stageID
}

func TestQueueBasedStageFlowExecution(t *testing.T) {
	t.Run("Execute publishes to first stage and returns running state", func(t *testing.T) {
		publisher := &mockQueuePublisher{}
		subscriber := &mockQueueSubscriber{}
		transport := &mockTransport{}
		
		// Mock successful publish
		publisher.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		transport.On("CreateQueue", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "FlowMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "CompensationMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		
		engine := NewStageFlowEngine(publisher, subscriber, transport)
		engine.SetServiceQueue("test-queue")
		
		// Create workflow with test stages
		handler1 := &trackingStageHandler{stageID: "stage1"}
		handler2 := &trackingStageHandler{stageID: "stage2"}
		
		workflow := NewWorkflow("test-workflow", "Test Workflow").
			AddStage("stage1", handler1).
			AddStage("stage2", handler2)
		
		err := engine.RegisterWorkflow(workflow)
		assert.NoError(t, err)
		
		// Execute workflow
		initialData := map[string]interface{}{"input": "test"}
		state, err := workflow.Execute(context.Background(), initialData)
		
		// Verify workflow started but not completed
		assert.NoError(t, err)
		assert.NotNil(t, state)
		assert.Equal(t, WorkflowRunning, state.Status)
		assert.NotEmpty(t, state.InstanceID)
		assert.Equal(t, "test-workflow", state.WorkflowID)
		assert.Empty(t, state.StageResults) // No stages executed yet
		
		// Verify message was published to queue
		publisher.AssertCalled(t, "Publish", mock.Anything, mock.AnythingOfType("*stageflow.FlowMessageEnvelope"), mock.Anything)
		assert.Len(t, publisher.publishedMessages, 1)
		
		// Verify envelope content
		envelope := publisher.publishedMessages[0]
		assert.Equal(t, "FlowMessageEnvelope", envelope.Type)
		assert.Equal(t, "test-workflow", envelope.WorkflowID)
		assert.Equal(t, state.InstanceID, envelope.InstanceID)
		assert.Equal(t, "stage1", envelope.StageName)
		assert.Equal(t, 0, envelope.CurrentStageIndex)
		
		// Verify state is embedded in envelope
		var embeddedState WorkflowState
		err = json.Unmarshal([]byte(envelope.SerializedWorkflowState), &embeddedState)
		assert.NoError(t, err)
		assert.Equal(t, state.InstanceID, embeddedState.InstanceID)
		assert.Equal(t, WorkflowRunning, embeddedState.Status)
	})
	
	t.Run("ProcessStageMessage executes stage and publishes to next stage", func(t *testing.T) {
		publisher := &mockQueuePublisher{}
		subscriber := &mockQueueSubscriber{}
		
		publisher.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		
		transport := &mockTransport{}
		transport.On("CreateQueue", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "FlowMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "CompensationMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		
		engine := NewStageFlowEngine(publisher, subscriber, transport)
		engine.SetServiceQueue("test-queue")
		
		// Create workflow with multiple stages
		handler1 := &trackingStageHandler{stageID: "stage1"}
		handler2 := &trackingStageHandler{stageID: "stage2"}
		
		workflow := NewWorkflow("test-workflow", "Test Workflow").
			AddStage("stage1", handler1).
			AddStage("stage2", handler2)
		
		err := engine.RegisterWorkflow(workflow)
		assert.NoError(t, err)
		
		// Create initial state
		initialState := &WorkflowState{
			WorkflowID:   "test-workflow",
			InstanceID:   "test-instance",
			Status:       WorkflowRunning,
			StageResults: make([]StageResult, 0),
			GlobalData:   map[string]interface{}{"input": "test"},
			StartTime:    time.Now(),
			LastModified: time.Now(),
			Version:      1,
		}
		
		// Serialize state for envelope
		stateData, err := json.Marshal(initialState)
		assert.NoError(t, err)
		
		// Create envelope for first stage
		envelope := &FlowMessageEnvelope{
			BaseMessage:             contracts.NewBaseMessage("FlowMessageEnvelope"),
			SerializedWorkflowState: string(stateData),
			WorkflowStateType:       "WorkflowState",
			CurrentStageIndex:       0,
			WorkflowID:              "test-workflow",
			InstanceID:              "test-instance",
			StageName:               "stage1",
		}
		
		// Process first stage message
		err = workflow.ProcessStageMessage(context.Background(), envelope)
		assert.NoError(t, err)
		
		// Verify stage was executed
		assert.True(t, handler1.executed)
		assert.False(t, handler2.executed) // Second stage not executed yet
		
		// Verify message was published for next stage
		assert.Len(t, publisher.publishedMessages, 1)
		nextEnvelope := publisher.publishedMessages[0]
		assert.Equal(t, "stage2", nextEnvelope.StageName)
		assert.Equal(t, 1, nextEnvelope.CurrentStageIndex)
		
		// Verify updated state in envelope
		var updatedState WorkflowState
		err = json.Unmarshal([]byte(nextEnvelope.SerializedWorkflowState), &updatedState)
		assert.NoError(t, err)
		assert.Len(t, updatedState.StageResults, 1)
		assert.Equal(t, "stage1", updatedState.StageResults[0].StageID)
		assert.Equal(t, StageCompleted, updatedState.StageResults[0].Status)
	})
	
	t.Run("ProcessStageMessage completes workflow when last stage finishes", func(t *testing.T) {
		publisher := &mockQueuePublisher{}
		subscriber := &mockQueueSubscriber{}
		
		publisher.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		
		transport := &mockTransport{}
		transport.On("CreateQueue", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "FlowMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "CompensationMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		
		engine := NewStageFlowEngine(publisher, subscriber, transport)
		engine.SetServiceQueue("test-queue")
		
		// Create workflow with single stage
		handler := &trackingStageHandler{stageID: "final-stage"}
		
		workflow := NewWorkflow("test-workflow", "Test Workflow").
			AddStage("final-stage", handler)
		
		err := engine.RegisterWorkflow(workflow)
		assert.NoError(t, err)
		
		// Create state for final stage
		initialState := &WorkflowState{
			WorkflowID:   "test-workflow",
			InstanceID:   "test-instance",
			Status:       WorkflowRunning,
			StageResults: make([]StageResult, 0),
			GlobalData:   map[string]interface{}{"input": "test"},
			StartTime:    time.Now(),
			LastModified: time.Now(),
			Version:      1,
		}
		
		stateData, err := json.Marshal(initialState)
		assert.NoError(t, err)
		
		envelope := &FlowMessageEnvelope{
			BaseMessage:             contracts.NewBaseMessage("FlowMessageEnvelope"),
			SerializedWorkflowState: string(stateData),
			WorkflowStateType:       "WorkflowState",
			CurrentStageIndex:       0,
			WorkflowID:              "test-workflow",
			InstanceID:              "test-instance",
			StageName:               "final-stage",
		}
		
		// Process final stage
		err = workflow.ProcessStageMessage(context.Background(), envelope)
		assert.NoError(t, err)
		
		// Verify stage was executed
		assert.True(t, handler.executed)
		
		// Verify no message was published (workflow complete)
		assert.Len(t, publisher.publishedMessages, 0)
	})
	
	t.Run("Failed required stage stops workflow", func(t *testing.T) {
		publisher := &mockQueuePublisher{}
		subscriber := &mockQueueSubscriber{}
		
		transport := &mockTransport{}
		transport.On("CreateQueue", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "FlowMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "CompensationMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		
		engine := NewStageFlowEngine(publisher, subscriber, transport)
		engine.SetServiceQueue("test-queue")
		
		// Create stage that fails
		failingHandler := &trackingStageHandler{
			stageID: "failing-stage",
			err:     assert.AnError,
		}
		
		workflow := NewWorkflow("test-workflow", "Test Workflow").
			AddStage("failing-stage", failingHandler)
		
		err := engine.RegisterWorkflow(workflow)
		assert.NoError(t, err)
		
		// Create state
		initialState := &WorkflowState{
			WorkflowID:   "test-workflow",
			InstanceID:   "test-instance",
			Status:       WorkflowRunning,
			StageResults: make([]StageResult, 0),
			GlobalData:   map[string]interface{}{},
			StartTime:    time.Now(),
			LastModified: time.Now(),
			Version:      1,
		}
		
		stateData, err := json.Marshal(initialState)
		assert.NoError(t, err)
		
		envelope := &FlowMessageEnvelope{
			BaseMessage:             contracts.NewBaseMessage("FlowMessageEnvelope"),
			SerializedWorkflowState: string(stateData),
			WorkflowStateType:       "WorkflowState",
			CurrentStageIndex:       0,
			WorkflowID:              "test-workflow",
			InstanceID:              "test-instance",
			StageName:               "failing-stage",
		}
		
		// Process failing stage
		err = workflow.ProcessStageMessage(context.Background(), envelope)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "required stage failing-stage failed")
		
		// Verify stage was executed
		assert.True(t, failingHandler.executed)
		
		// Verify no message was published (workflow failed)
		assert.Len(t, publisher.publishedMessages, 0)
	})
	
	t.Run("State serialization preserves all workflow data", func(t *testing.T) {
		publisher := &mockQueuePublisher{}
		subscriber := &mockQueueSubscriber{}
		
		publisher.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		
		transport := &mockTransport{}
		transport.On("CreateQueue", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "FlowMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "CompensationMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		
		engine := NewStageFlowEngine(publisher, subscriber, transport)
		engine.SetServiceQueue("test-queue")
		
		// Create handler that modifies state
		dataHandler := &trackingStageHandler{
			stageID: "data-stage",
			executeFunc: func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
				// Add data to global state
				state.GlobalData["processed"] = true
				state.GlobalData["timestamp"] = time.Now().Unix()
				
				return &StageResult{
					StageID: "data-stage",
					Status:  StageCompleted,
					Data: map[string]interface{}{
						"output": "processed data",
						"count":  42,
					},
				}, nil
			},
		}
		
		nextHandler := &trackingStageHandler{stageID: "next-stage"}
		
		workflow := NewWorkflow("test-workflow", "Test Workflow").
			AddStage("data-stage", dataHandler).
			AddStage("next-stage", nextHandler)
		
		err := engine.RegisterWorkflow(workflow)
		assert.NoError(t, err)
		
		// Create initial state with data
		initialState := &WorkflowState{
			WorkflowID:   "test-workflow",
			InstanceID:   "test-instance",
			Status:       WorkflowRunning,
			StageResults: make([]StageResult, 0),
			GlobalData: map[string]interface{}{
				"input":    "original data",
				"priority": "high",
			},
			StartTime:    time.Now(),
			LastModified: time.Now(),
			Version:      1,
		}
		
		stateData, err := json.Marshal(initialState)
		assert.NoError(t, err)
		
		envelope := &FlowMessageEnvelope{
			BaseMessage:             contracts.NewBaseMessage("FlowMessageEnvelope"),
			SerializedWorkflowState: string(stateData),
			WorkflowStateType:       "WorkflowState",
			CurrentStageIndex:       0,
			WorkflowID:              "test-workflow",
			InstanceID:              "test-instance",
			StageName:               "data-stage",
		}
		
		// Process stage that modifies state
		err = workflow.ProcessStageMessage(context.Background(), envelope)
		assert.NoError(t, err)
		
		// Verify data was preserved and updated in next stage envelope
		assert.Len(t, publisher.publishedMessages, 1)
		nextEnvelope := publisher.publishedMessages[0]
		
		var nextState WorkflowState
		err = json.Unmarshal([]byte(nextEnvelope.SerializedWorkflowState), &nextState)
		assert.NoError(t, err)
		
		// Verify original data preserved
		assert.Equal(t, "original data", nextState.GlobalData["input"])
		assert.Equal(t, "high", nextState.GlobalData["priority"])
		
		// Verify new data added
		assert.Equal(t, true, nextState.GlobalData["processed"])
		assert.NotNil(t, nextState.GlobalData["timestamp"])
		
		// Verify stage results preserved
		assert.Len(t, nextState.StageResults, 1)
		assert.Equal(t, "data-stage", nextState.StageResults[0].StageID)
		assert.Equal(t, StageCompleted, nextState.StageResults[0].Status)
		
		// Verify stage data merged into global data
		assert.Equal(t, "processed data", nextState.GlobalData["data-stage.output"])
		assert.Equal(t, float64(42), nextState.GlobalData["data-stage.count"]) // JSON converts int to float64
	})
}

func TestNewQueueBasedStateStore(t *testing.T) {
	t.Run("NewQueueBasedStateStore creates store", func(t *testing.T) {
		store := NewQueueBasedStateStore()
		assert.NotNil(t, store)
	})
	
	t.Run("SaveState is no-op for queue-based storage", func(t *testing.T) {
		store := NewQueueBasedStateStore()
		
		state := &WorkflowState{
			InstanceID: "test-instance",
			Status:     WorkflowRunning,
		}
		
		err := store.SaveState(context.Background(), state)
		assert.NoError(t, err)
	})
	
	t.Run("LoadState returns error for queue-based storage", func(t *testing.T) {
		store := NewQueueBasedStateStore()
		
		state, err := store.LoadState(context.Background(), "test-instance")
		assert.Error(t, err)
		assert.Nil(t, state)
		assert.Contains(t, err.Error(), "queue-based state store does not support direct state loading")
	})
	
	t.Run("DeleteState is no-op for queue-based storage", func(t *testing.T) {
		store := NewQueueBasedStateStore()
		
		err := store.DeleteState(context.Background(), "test-instance")
		assert.NoError(t, err)
	})
	
	t.Run("ListActiveWorkflows returns error for queue-based storage", func(t *testing.T) {
		store := NewQueueBasedStateStore()
		
		workflows, err := store.ListActiveWorkflows(context.Background())
		assert.Error(t, err)
		assert.Nil(t, workflows)
		assert.Contains(t, err.Error(), "queue-based state store does not support listing active workflows")
	})
}

func TestQueueBasedWorkflowIntegration(t *testing.T) {
	t.Run("Complete multi-stage workflow execution via queue messages", func(t *testing.T) {
		publisher := &mockQueuePublisher{}
		subscriber := &mockQueueSubscriber{}
		
		publisher.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		
		transport := &mockTransport{}
		transport.On("CreateQueue", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "FlowMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "CompensationMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		
		engine := NewStageFlowEngine(publisher, subscriber, transport)
		engine.SetServiceQueue("integration-test-queue")
		
		// Track execution order
		var executionOrder []string
		
		// Create multi-stage workflow
		stage1 := &trackingStageHandler{
			stageID: "validate",
			executeFunc: func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
				executionOrder = append(executionOrder, "validate")
				return &StageResult{
					StageID: "validate",
					Status:  StageCompleted,
					Data:    map[string]interface{}{"valid": true},
				}, nil
			},
		}
		
		stage2 := &trackingStageHandler{
			stageID: "process",
			executeFunc: func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
				executionOrder = append(executionOrder, "process")
				// Verify data from previous stage
				assert.Equal(t, true, state.GlobalData["validate.valid"])
				return &StageResult{
					StageID: "process",
					Status:  StageCompleted,
					Data:    map[string]interface{}{"processed": "data"},
				}, nil
			},
		}
		
		stage3 := &trackingStageHandler{
			stageID: "finalize",
			executeFunc: func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
				executionOrder = append(executionOrder, "finalize")
				// Verify data from all previous stages
				assert.Equal(t, true, state.GlobalData["validate.valid"])
				assert.Equal(t, "data", state.GlobalData["process.processed"])
				return &StageResult{
					StageID: "finalize",
					Status:  StageCompleted,
					Data:    map[string]interface{}{"complete": true},
				}, nil
			},
		}
		
		workflow := NewWorkflow("integration-workflow", "Integration Test").
			AddStage("validate", stage1).
			AddStage("process", stage2).
			AddStage("finalize", stage3)
		
		err := engine.RegisterWorkflow(workflow)
		assert.NoError(t, err)
		
		// Start workflow
		initialData := map[string]interface{}{"input": "test data"}
		state, err := workflow.Execute(context.Background(), initialData)
		assert.NoError(t, err)
		assert.Equal(t, WorkflowRunning, state.Status)
		
		// Simulate processing each stage message
		for i := 0; i < 3; i++ {
			// Get the envelope that was published
			assert.True(t, len(publisher.publishedMessages) > i, "Expected message %d to be published", i)
			envelope := publisher.publishedMessages[i]
			
			// Process the stage message
			err = workflow.ProcessStageMessage(context.Background(), envelope)
			if i < 2 { // Not the last stage
				assert.NoError(t, err)
			} else { // Last stage - no more messages published
				assert.NoError(t, err)
			}
		}
		
		// Verify execution order
		assert.Equal(t, []string{"validate", "process", "finalize"}, executionOrder)
		
		// Verify all stages executed
		assert.True(t, stage1.executed)
		assert.True(t, stage2.executed)
		assert.True(t, stage3.executed)
		
		// Verify final state from last envelope (if there is one)
		// With 3 stages, we expect 3 messages to be published
		assert.Len(t, publisher.publishedMessages, 3)
		
		// Check the second envelope (process stage) which should have validate stage results
		if len(publisher.publishedMessages) >= 2 {
			processEnvelope := publisher.publishedMessages[1] // Second message (process stage)
			var processState WorkflowState
			err = json.Unmarshal([]byte(processEnvelope.SerializedWorkflowState), &processState)
			assert.NoError(t, err)
			
			// Should have results from first stage (validate)
			assert.Len(t, processState.StageResults, 1)
			assert.Equal(t, "validate", processState.StageResults[0].StageID)
		}
		
		// Check the third envelope (finalize stage) which should have validate and process stage results
		if len(publisher.publishedMessages) >= 3 {
			finalizeEnvelope := publisher.publishedMessages[2] // Third message (finalize stage)
			var finalizeState WorkflowState
			err = json.Unmarshal([]byte(finalizeEnvelope.SerializedWorkflowState), &finalizeState)
			assert.NoError(t, err)
			
			// Should have results from first two stages
			assert.Len(t, finalizeState.StageResults, 2)
			assert.Equal(t, "validate", finalizeState.StageResults[0].StageID)
			assert.Equal(t, "process", finalizeState.StageResults[1].StageID)
		}
	})
}