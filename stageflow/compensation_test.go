package stageflow

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Mock compensation handler for testing
type mockCompensationHandler struct {
	stageID     string
	executed    bool
	err         error
	compensateFunc func(ctx context.Context, state *WorkflowState, originalResult *StageResult) error
}

func (h *mockCompensationHandler) Compensate(ctx context.Context, state *WorkflowState, originalResult *StageResult) error {
	h.executed = true
	if h.compensateFunc != nil {
		return h.compensateFunc(ctx, state, originalResult)
	}
	return h.err
}

func (h *mockCompensationHandler) GetStageID() string {
	return h.stageID
}

func TestQueueBasedCompensation(t *testing.T) {
	t.Run("Failed required stage triggers queue-based compensation", func(t *testing.T) {
		publisher := &mockQueuePublisher{}
		subscriber := &mockQueueSubscriber{}
		transport := &mockTransport{}
		
		// Mock all necessary calls
		publisher.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		transport.On("CreateQueue", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "FlowMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "CompensationMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		
		engine := NewStageFlowEngine(publisher, subscriber, transport)
		engine.SetServiceQueue("test-queue")
		
		// Create workflow with compensation handlers
		comp1 := &mockCompensationHandler{stageID: "stage1"}
		comp2 := &mockCompensationHandler{stageID: "stage2"}
		
		handler1 := &trackingStageHandler{stageID: "stage1"}
		handler2 := &trackingStageHandler{stageID: "stage2"}
		failingHandler := &trackingStageHandler{stageID: "stage3", err: assert.AnError}
		
		workflow := NewWorkflow("test-workflow", "Test Workflow").
			AddStage("stage1", handler1).
			AddStage("stage2", handler2).
			AddStage("stage3", failingHandler)
		
		workflow.AddCompensation("stage1", comp1)
		workflow.AddCompensation("stage2", comp2)
		
		err := engine.RegisterWorkflow(workflow)
		assert.NoError(t, err)
		
		// Create state with completed stages before failure
		state := &WorkflowState{
			WorkflowID:   "test-workflow",
			InstanceID:   "test-instance",
			Status:       WorkflowRunning,
			StageResults: []StageResult{
				{StageID: "stage1", Status: StageCompleted, StartTime: time.Now(), Duration: time.Second},
				{StageID: "stage2", Status: StageCompleted, StartTime: time.Now(), Duration: time.Second},
			},
			GlobalData:   map[string]interface{}{},
			StartTime:    time.Now(),
			LastModified: time.Now(),
			Version:      3,
		}
		
		stateData, err := json.Marshal(state)
		assert.NoError(t, err)
		
		envelope := &FlowMessageEnvelope{
			BaseMessage:             contracts.NewBaseMessage("FlowMessageEnvelope"),
			SerializedWorkflowState: string(stateData),
			WorkflowStateType:       "WorkflowState",
			CurrentStageIndex:       2, // Failing on third stage
			WorkflowID:              "test-workflow",
			InstanceID:              "test-instance",
			StageName:               "stage3",
		}
		
		// Process failing stage
		err = workflow.ProcessStageMessage(context.Background(), envelope)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "required stage stage3 failed")
		
		// Verify compensation message was published
		publisher.AssertCalled(t, "Publish", mock.Anything, mock.AnythingOfType("*stageflow.CompensationMessageEnvelope"), mock.Anything)
		
		// Find the compensation message
		var compensationEnvelope *CompensationMessageEnvelope
		for _, call := range publisher.Calls {
			if call.Method == "Publish" && len(call.Arguments) > 1 {
				if env, ok := call.Arguments[1].(*CompensationMessageEnvelope); ok {
					compensationEnvelope = env
					break
				}
			}
		}
		
		assert.NotNil(t, compensationEnvelope)
		assert.Equal(t, "test-workflow", compensationEnvelope.WorkflowID)
		assert.Equal(t, "test-instance", compensationEnvelope.InstanceID)
		assert.Equal(t, 2, compensationEnvelope.FailedStageIndex)
		assert.Equal(t, 2, compensationEnvelope.TotalCompensations) // Two completed stages to compensate
		assert.Equal(t, 1, compensationEnvelope.CompensationIndex) // Start with stage2 (reverse order)
		assert.Equal(t, "stage2", compensationEnvelope.StageToCompensate)
	})
	
	t.Run("Compensation message processing executes compensation handler", func(t *testing.T) {
		publisher := &mockQueuePublisher{}
		subscriber := &mockQueueSubscriber{}
		transport := &mockTransport{}
		
		publisher.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		transport.On("CreateQueue", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "FlowMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "CompensationMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		
		engine := NewStageFlowEngine(publisher, subscriber, transport)
		
		// Create compensation handler that tracks execution
		var compensatedState *WorkflowState
		var compensatedResult *StageResult
		comp1 := &mockCompensationHandler{
			stageID: "stage1",
			compensateFunc: func(ctx context.Context, state *WorkflowState, originalResult *StageResult) error {
				compensatedState = state
				compensatedResult = originalResult
				return nil
			},
		}
		
		handler1 := &trackingStageHandler{stageID: "stage1"}
		workflow := NewWorkflow("test-workflow", "Test Workflow").
			AddStage("stage1", handler1)
		workflow.AddCompensation("stage1", comp1)
		
		err := engine.RegisterWorkflow(workflow)
		assert.NoError(t, err)
		
		// Create workflow state
		state := &WorkflowState{
			WorkflowID:   "test-workflow",
			InstanceID:   "test-instance",
			Status:       WorkflowFailed,
			StageResults: []StageResult{
				{StageID: "stage1", Status: StageCompleted, Data: map[string]interface{}{"result": "data"}},
			},
			GlobalData: map[string]interface{}{},
			StartTime:  time.Now(),
			Version:    2,
		}
		
		stateData, err := json.Marshal(state)
		assert.NoError(t, err)
		
		originalResult := state.StageResults[0]
		resultData, err := json.Marshal(originalResult)
		assert.NoError(t, err)
		
		// Create compensation envelope
		envelope := &CompensationMessageEnvelope{
			BaseMessage:              contracts.NewBaseMessage("CompensationMessageEnvelope"),
			WorkflowID:               "test-workflow",
			InstanceID:               "test-instance",
			FailedStageIndex:         1,
			CompensationIndex:        0, // Last (and only) compensation
			StageToCompensate:        "stage1",
			TotalCompensations:       1,
			SerializedWorkflowState:  string(stateData),
			WorkflowStateType:        "WorkflowState",
			SerializedOriginalResult: string(resultData),
			OriginalResultType:       "StageResult",
			OriginalError:            "test error",
			FailureTimestamp:         time.Now(),
			CompensationResults:      []CompensationResult{},
		}
		
		// Process compensation message
		err = workflow.processCompensationMessage(context.Background(), envelope)
		assert.NoError(t, err)
		
		// Verify compensation was executed
		assert.True(t, comp1.executed)
		assert.NotNil(t, compensatedState)
		assert.NotNil(t, compensatedResult)
		assert.Equal(t, "stage1", compensatedResult.StageID)
		assert.Equal(t, "data", compensatedResult.Data["result"])
		
		// Verify compensation result was added to envelope
		assert.Len(t, envelope.CompensationResults, 1)
		compResult := envelope.CompensationResults[0]
		assert.Equal(t, "stage1", compResult.StageID)
		assert.Equal(t, CompensationCompleted, compResult.Status)
		assert.Equal(t, 0, compResult.CompensationIndex)
	})
	
	t.Run("Multi-stage compensation processes in reverse order", func(t *testing.T) {
		publisher := &mockQueuePublisher{}
		subscriber := &mockQueueSubscriber{}
		transport := &mockTransport{}
		
		publisher.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		transport.On("CreateQueue", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "FlowMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "CompensationMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		
		engine := NewStageFlowEngine(publisher, subscriber, transport)
		
		// Track compensation order
		var compensationOrder []string
		
		comp1 := &mockCompensationHandler{
			stageID: "stage1",
			compensateFunc: func(ctx context.Context, state *WorkflowState, originalResult *StageResult) error {
				compensationOrder = append(compensationOrder, "stage1")
				return nil
			},
		}
		
		comp2 := &mockCompensationHandler{
			stageID: "stage2",
			compensateFunc: func(ctx context.Context, state *WorkflowState, originalResult *StageResult) error {
				compensationOrder = append(compensationOrder, "stage2")
				return nil
			},
		}
		
		comp3 := &mockCompensationHandler{
			stageID: "stage3",
			compensateFunc: func(ctx context.Context, state *WorkflowState, originalResult *StageResult) error {
				compensationOrder = append(compensationOrder, "stage3")
				return nil
			},
		}
		
		workflow := NewWorkflow("test-workflow", "Test Workflow").
			AddStage("stage1", &trackingStageHandler{stageID: "stage1"}).
			AddStage("stage2", &trackingStageHandler{stageID: "stage2"}).
			AddStage("stage3", &trackingStageHandler{stageID: "stage3"}).
			AddStage("stage4", &trackingStageHandler{stageID: "stage4"})
		
		workflow.AddCompensation("stage1", comp1)
		workflow.AddCompensation("stage2", comp2)
		workflow.AddCompensation("stage3", comp3)
		
		err := engine.RegisterWorkflow(workflow)
		assert.NoError(t, err)
		
		// Create state with 3 completed stages before failure
		state := &WorkflowState{
			WorkflowID:   "test-workflow",
			InstanceID:   "test-instance",
			Status:       WorkflowFailed,
			StageResults: []StageResult{
				{StageID: "stage1", Status: StageCompleted},
				{StageID: "stage2", Status: StageCompleted},
				{StageID: "stage3", Status: StageCompleted},
			},
			GlobalData: map[string]interface{}{},
			StartTime:  time.Now(),
			Version:    4,
		}
		
		stateData, err := json.Marshal(state)
		assert.NoError(t, err)
		
		// Start with stage3 (last completed stage)
		stage3Result := state.StageResults[2]
		resultData, err := json.Marshal(stage3Result)
		assert.NoError(t, err)
		
		envelope1 := &CompensationMessageEnvelope{
			BaseMessage:              contracts.NewBaseMessage("CompensationMessageEnvelope"),
			WorkflowID:               "test-workflow",
			InstanceID:               "test-instance",
			FailedStageIndex:         3,
			CompensationIndex:        2, // Start with stage3 (index 2 in reverse)
			StageToCompensate:        "stage3",
			TotalCompensations:       3,
			SerializedWorkflowState:  string(stateData),
			WorkflowStateType:        "WorkflowState",
			SerializedOriginalResult: string(resultData),
			OriginalResultType:       "StageResult",
			OriginalError:            "stage4 failed",
			FailureTimestamp:         time.Now(),
			CompensationResults:      []CompensationResult{},
		}
		
		// Process first compensation (stage3)
		err = workflow.processCompensationMessage(context.Background(), envelope1)
		assert.NoError(t, err)
		
		// Should publish next compensation message (stage2)
		publisher.AssertCalled(t, "Publish", mock.Anything, mock.AnythingOfType("*stageflow.CompensationMessageEnvelope"), mock.Anything)
		
		// Verify stage3 was compensated and next message was published
		assert.Equal(t, []string{"stage3"}, compensationOrder)
		
		// Simulate receiving the next compensation message (stage2)
		stage2Result := state.StageResults[1]
		resultData2, err := json.Marshal(stage2Result)
		assert.NoError(t, err)
		
		envelope2 := &CompensationMessageEnvelope{
			BaseMessage:              contracts.NewBaseMessage("CompensationMessageEnvelope"),
			WorkflowID:               "test-workflow",
			InstanceID:               "test-instance",
			FailedStageIndex:         3,
			CompensationIndex:        1, // Now compensating stage2
			StageToCompensate:        "stage2",
			TotalCompensations:       3,
			SerializedWorkflowState:  string(stateData),
			WorkflowStateType:        "WorkflowState",
			SerializedOriginalResult: string(resultData2),
			OriginalResultType:       "StageResult",
			OriginalError:            "stage4 failed",
			FailureTimestamp:         time.Now(),
			CompensationResults: []CompensationResult{
				{StageID: "stage3", Status: CompensationCompleted, CompensationIndex: 2},
			},
		}
		
		err = workflow.processCompensationMessage(context.Background(), envelope2)
		assert.NoError(t, err)
		
		// Finally, process stage1 compensation
		stage1Result := state.StageResults[0]
		resultData1, err := json.Marshal(stage1Result)
		assert.NoError(t, err)
		
		envelope3 := &CompensationMessageEnvelope{
			BaseMessage:              contracts.NewBaseMessage("CompensationMessageEnvelope"),
			WorkflowID:               "test-workflow",
			InstanceID:               "test-instance",
			FailedStageIndex:         3,
			CompensationIndex:        0, // Final compensation (stage1)
			StageToCompensate:        "stage1",
			TotalCompensations:       3,
			SerializedWorkflowState:  string(stateData),
			WorkflowStateType:        "WorkflowState",
			SerializedOriginalResult: string(resultData1),
			OriginalResultType:       "StageResult",
			OriginalError:            "stage4 failed",
			FailureTimestamp:         time.Now(),
			CompensationResults: []CompensationResult{
				{StageID: "stage3", Status: CompensationCompleted, CompensationIndex: 2},
				{StageID: "stage2", Status: CompensationCompleted, CompensationIndex: 1},
			},
		}
		
		err = workflow.processCompensationMessage(context.Background(), envelope3)
		assert.NoError(t, err)
		
		// Verify all compensations executed in reverse order
		assert.Equal(t, []string{"stage3", "stage2", "stage1"}, compensationOrder)
	})
	
	t.Run("Compensation handles stage without compensation handler", func(t *testing.T) {
		publisher := &mockQueuePublisher{}
		subscriber := &mockQueueSubscriber{}
		transport := &mockTransport{}
		
		publisher.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		transport.On("CreateQueue", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "FlowMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "CompensationMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		
		engine := NewStageFlowEngine(publisher, subscriber, transport)
		
		// Create workflow without compensation handler for stage1
		workflow := NewWorkflow("test-workflow", "Test Workflow").
			AddStage("stage1", &trackingStageHandler{stageID: "stage1"})
		
		err := engine.RegisterWorkflow(workflow)
		assert.NoError(t, err)
		
		state := &WorkflowState{
			WorkflowID:   "test-workflow",
			InstanceID:   "test-instance",
			Status:       WorkflowFailed,
			StageResults: []StageResult{
				{StageID: "stage1", Status: StageCompleted},
			},
			GlobalData: map[string]interface{}{},
			StartTime:  time.Now(),
			Version:    2,
		}
		
		stateData, err := json.Marshal(state)
		assert.NoError(t, err)
		
		originalResult := state.StageResults[0]
		resultData, err := json.Marshal(originalResult)
		assert.NoError(t, err)
		
		envelope := &CompensationMessageEnvelope{
			BaseMessage:              contracts.NewBaseMessage("CompensationMessageEnvelope"),
			WorkflowID:               "test-workflow",
			InstanceID:               "test-instance",
			FailedStageIndex:         1,
			CompensationIndex:        0, // Final compensation
			StageToCompensate:        "stage1",
			TotalCompensations:       1,
			SerializedWorkflowState:  string(stateData),
			WorkflowStateType:        "WorkflowState",
			SerializedOriginalResult: string(resultData),
			OriginalResultType:       "StageResult",
			OriginalError:            "test error",
			FailureTimestamp:         time.Now(),
			CompensationResults:      []CompensationResult{},
		}
		
		// Process compensation message
		err = workflow.processCompensationMessage(context.Background(), envelope)
		assert.NoError(t, err)
		
		// Verify compensation was skipped (no handler)
		assert.Len(t, envelope.CompensationResults, 1)
		compResult := envelope.CompensationResults[0]
		assert.Equal(t, "stage1", compResult.StageID)
		assert.Equal(t, CompensationSkipped, compResult.Status)
	})
	
	t.Run("No compensation needed when no completed stages", func(t *testing.T) {
		publisher := &mockQueuePublisher{}
		subscriber := &mockQueueSubscriber{}
		transport := &mockTransport{}
		
		// Don't expect compensation message to be published
		publisher.On("Publish", mock.Anything, mock.AnythingOfType("*stageflow.FlowMessageEnvelope"), mock.Anything).Return(nil)
		transport.On("CreateQueue", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "FlowMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "CompensationMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		
		engine := NewStageFlowEngine(publisher, subscriber, transport)
		
		failingHandler := &trackingStageHandler{stageID: "stage1", err: assert.AnError}
		workflow := NewWorkflow("test-workflow", "Test Workflow").
			AddStage("stage1", failingHandler)
		
		err := engine.RegisterWorkflow(workflow)
		assert.NoError(t, err)
		
		// Create state with no completed stages (failure on first stage)
		state := &WorkflowState{
			WorkflowID:   "test-workflow",
			InstanceID:   "test-instance",
			Status:       WorkflowRunning,
			StageResults: []StageResult{}, // No completed stages
			GlobalData:   map[string]interface{}{},
			StartTime:    time.Now(),
			LastModified: time.Now(),
			Version:      1,
		}
		
		stateData, err := json.Marshal(state)
		assert.NoError(t, err)
		
		envelope := &FlowMessageEnvelope{
			BaseMessage:             contracts.NewBaseMessage("FlowMessageEnvelope"),
			SerializedWorkflowState: string(stateData),
			WorkflowStateType:       "WorkflowState",
			CurrentStageIndex:       0, // Failing on first stage
			WorkflowID:              "test-workflow",
			InstanceID:              "test-instance",
			StageName:               "stage1",
		}
		
		// Process failing stage
		err = workflow.ProcessStageMessage(context.Background(), envelope)
		assert.Error(t, err)
		
		// Verify NO compensation message was published (no completed stages to compensate)
		publisher.AssertNotCalled(t, "Publish", mock.Anything, mock.AnythingOfType("*stageflow.CompensationMessageEnvelope"), mock.Anything)
	})
}

func TestCompensationEnvelope(t *testing.T) {
	t.Run("NewCompensationMessageEnvelope creates correct envelope", func(t *testing.T) {
		state := &WorkflowState{
			WorkflowID:   "test-workflow",
			InstanceID:   "test-instance",
			Status:       WorkflowFailed,
			StageResults: []StageResult{
				{StageID: "stage1", Status: StageCompleted},
				{StageID: "stage2", Status: StageCompleted},
				{StageID: "stage3", Status: StageFailed}, // This is the failed stage
			},
			GlobalData: map[string]interface{}{},
			StartTime:  time.Now(),
			Version:    3,
		}
		
		envelope, err := NewCompensationMessageEnvelope("test-workflow", "test-instance", 2, state, "stage3 failed")
		assert.NoError(t, err)
		assert.NotNil(t, envelope)
		
		assert.Equal(t, "CompensationMessageEnvelope", envelope.GetType())
		assert.Equal(t, "test-workflow", envelope.WorkflowID)
		assert.Equal(t, "test-instance", envelope.InstanceID)
		assert.Equal(t, 2, envelope.FailedStageIndex)
		assert.Equal(t, 2, envelope.TotalCompensations) // Two completed stages
		assert.Equal(t, 1, envelope.CompensationIndex)  // Start with stage2 (last completed)
		assert.Equal(t, "stage2", envelope.StageToCompensate)
		assert.Equal(t, "stage3 failed", envelope.OriginalError)
		assert.NotEmpty(t, envelope.SerializedWorkflowState)
		assert.NotEmpty(t, envelope.SerializedOriginalResult)
	})
	
	t.Run("NewCompensationMessageEnvelope handles no completed stages", func(t *testing.T) {
		state := &WorkflowState{
			WorkflowID:   "test-workflow",
			InstanceID:   "test-instance",
			Status:       WorkflowFailed,
			StageResults: []StageResult{}, // No completed stages
			GlobalData:   map[string]interface{}{},
			StartTime:    time.Now(),
			Version:      1,
		}
		
		envelope, err := NewCompensationMessageEnvelope("test-workflow", "test-instance", 0, state, "first stage failed")
		assert.NoError(t, err)
		assert.NotNil(t, envelope)
		
		assert.Equal(t, 0, envelope.TotalCompensations)
		assert.Equal(t, -1, envelope.CompensationIndex) // No compensations needed
		assert.Equal(t, "", envelope.StageToCompensate)
		assert.Equal(t, "", envelope.SerializedOriginalResult)
	})
}