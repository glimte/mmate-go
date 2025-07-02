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

// Mock publisher that captures published events
type mockEventPublisher struct {
	mock.Mock
	publishedEvents []contracts.Event
}

func (m *mockEventPublisher) Publish(ctx context.Context, msg contracts.Message, options ...messaging.PublishOption) error {
	args := m.Called(ctx, msg, options)
	return args.Error(0)
}

func (m *mockEventPublisher) PublishEvent(ctx context.Context, event contracts.Event, options ...messaging.PublishOption) error {
	m.publishedEvents = append(m.publishedEvents, event)
	args := m.Called(ctx, event, options)
	return args.Error(0)
}

func (m *mockEventPublisher) PublishCommand(ctx context.Context, command contracts.Command, options ...messaging.PublishOption) error {
	return m.Publish(ctx, command, options...)
}

func (m *mockEventPublisher) Close() error {
	return nil
}

func TestWorkflowCompletionEvents(t *testing.T) {
	t.Run("Workflow completion publishes event when enabled", func(t *testing.T) {
		publisher := &mockEventPublisher{}
		subscriber := &mockQueueSubscriber{}
		transport := &mockTransport{}
		
		// Mock successful event publish (both stage and workflow completion)
		publisher.On("PublishEvent", mock.Anything, mock.AnythingOfType("*stageflow.StageCompletedEvent"), mock.Anything).Return(nil)
		publisher.On("PublishEvent", mock.Anything, mock.AnythingOfType("*stageflow.WorkflowCompletedEvent"), mock.Anything).Return(nil)
		publisher.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		transport.On("CreateQueue", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "FlowMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		
		// Create engine with completion events enabled
		engine := NewStageFlowEngine(publisher, subscriber, transport, 
			WithCompletionEvents(true, messaging.WithExchange("events")))
		
		// Create simple workflow
		handler := &trackingStageHandler{
			stageID: "final-stage",
			result: &StageResult{
				StageID: "final-stage",
				Status:  StageCompleted,
				Data:    map[string]interface{}{"result": "success"},
			},
		}
		
		workflow := NewWorkflow("test-workflow", "Test Workflow").
			AddStage("final-stage", handler)
		
		err := engine.RegisterWorkflow(workflow)
		assert.NoError(t, err)
		
		// Create state for final stage
		state := &WorkflowState{
			WorkflowID:   "test-workflow",
			InstanceID:   "test-instance",
			Status:       WorkflowRunning,
			StageResults: []StageResult{},
			GlobalData:   map[string]interface{}{"input": "test"},
			StartTime:    time.Now().Add(-1 * time.Minute),
			LastModified: time.Now(),
			Version:      1,
		}
		
		stateData, err := json.Marshal(state)
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
		
		// Process the final stage
		err = workflow.ProcessStageMessage(context.Background(), envelope)
		assert.NoError(t, err)
		
		// Verify both stage and workflow completion events were published
		publisher.AssertCalled(t, "PublishEvent", mock.Anything, mock.AnythingOfType("*stageflow.StageCompletedEvent"), mock.Anything)
		publisher.AssertCalled(t, "PublishEvent", mock.Anything, mock.AnythingOfType("*stageflow.WorkflowCompletedEvent"), mock.Anything)
		assert.Len(t, publisher.publishedEvents, 2) // Stage + Workflow events
		
		// Verify event content - find the workflow completed event
		var completedEvent *WorkflowCompletedEvent
		for _, event := range publisher.publishedEvents {
			if wce, ok := event.(*WorkflowCompletedEvent); ok {
				completedEvent = wce
				break
			}
		}
		assert.NotNil(t, completedEvent)
		assert.Equal(t, "test-workflow", completedEvent.WorkflowID)
		assert.Equal(t, "Test Workflow", completedEvent.WorkflowName)
		assert.Equal(t, "test-instance", completedEvent.InstanceID)
		assert.Equal(t, WorkflowCompleted, completedEvent.Status)
		assert.Equal(t, 1, completedEvent.TotalStages)
		assert.Equal(t, 1, completedEvent.CompletedStages)
		assert.Equal(t, 0, completedEvent.FailedStages)
	})
	
	t.Run("Workflow failure publishes event when enabled", func(t *testing.T) {
		publisher := &mockEventPublisher{}
		subscriber := &mockQueueSubscriber{}
		transport := &mockTransport{}
		
		// Mock successful event publish
		publisher.On("PublishEvent", mock.Anything, mock.AnythingOfType("*stageflow.WorkflowFailedEvent"), mock.Anything).Return(nil)
		transport.On("CreateQueue", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "FlowMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		
		// Create engine with completion events enabled
		engine := NewStageFlowEngine(publisher, subscriber, transport, 
			WithCompletionEvents(true))
		
		// Create workflow with failing stage
		failingHandler := &trackingStageHandler{
			stageID: "failing-stage",
			err:     assert.AnError,
		}
		
		workflow := NewWorkflow("test-workflow", "Test Workflow").
			AddStage("failing-stage", failingHandler)
		
		err := engine.RegisterWorkflow(workflow)
		assert.NoError(t, err)
		
		// Create state
		state := &WorkflowState{
			WorkflowID:   "test-workflow",
			InstanceID:   "test-instance",
			Status:       WorkflowRunning,
			StageResults: []StageResult{},
			GlobalData:   map[string]interface{}{},
			StartTime:    time.Now().Add(-30 * time.Second),
			LastModified: time.Now(),
			Version:      1,
		}
		
		stateData, err := json.Marshal(state)
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
		
		// Process the failing stage
		err = workflow.ProcessStageMessage(context.Background(), envelope)
		assert.Error(t, err)
		
		// Verify failed event was published
		publisher.AssertCalled(t, "PublishEvent", mock.Anything, mock.AnythingOfType("*stageflow.WorkflowFailedEvent"), mock.Anything)
		assert.Len(t, publisher.publishedEvents, 1)
		
		// Verify event content
		failedEvent, ok := publisher.publishedEvents[0].(*WorkflowFailedEvent)
		assert.True(t, ok)
		assert.Equal(t, "test-workflow", failedEvent.WorkflowID)
		assert.Equal(t, "Test Workflow", failedEvent.WorkflowName)
		assert.Equal(t, "test-instance", failedEvent.InstanceID)
		assert.Equal(t, "failing-stage", failedEvent.FailedStageID)
		assert.Equal(t, 0, failedEvent.FailedStageIndex)
		assert.NotEmpty(t, failedEvent.Error)
	})
	
	t.Run("Stage completion publishes event when enabled", func(t *testing.T) {
		publisher := &mockEventPublisher{}
		subscriber := &mockQueueSubscriber{}
		transport := &mockTransport{}
		
		// Mock successful event and message publish
		publisher.On("PublishEvent", mock.Anything, mock.AnythingOfType("*stageflow.StageCompletedEvent"), mock.Anything).Return(nil)
		publisher.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		transport.On("CreateQueue", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "FlowMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		
		// Create engine with completion events enabled
		engine := NewStageFlowEngine(publisher, subscriber, transport, 
			WithCompletionEvents(true))
		
		// Create workflow with multiple stages
		handler1 := &trackingStageHandler{
			stageID: "stage1",
			result: &StageResult{
				StageID: "stage1",
				Status:  StageCompleted,
				Data:    map[string]interface{}{"stage1": "data"},
			},
		}
		handler2 := &trackingStageHandler{stageID: "stage2"}
		
		workflow := NewWorkflow("test-workflow", "Test Workflow").
			AddStage("stage1", handler1).
			AddStage("stage2", handler2)
		
		err := engine.RegisterWorkflow(workflow)
		assert.NoError(t, err)
		
		// Create state
		state := &WorkflowState{
			WorkflowID:   "test-workflow",
			InstanceID:   "test-instance",
			Status:       WorkflowRunning,
			StageResults: []StageResult{},
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
			CurrentStageIndex:       0,
			WorkflowID:              "test-workflow",
			InstanceID:              "test-instance",
			StageName:               "stage1",
		}
		
		// Process first stage
		err = workflow.ProcessStageMessage(context.Background(), envelope)
		assert.NoError(t, err)
		
		// Verify stage event was published
		publisher.AssertCalled(t, "PublishEvent", mock.Anything, mock.AnythingOfType("*stageflow.StageCompletedEvent"), mock.Anything)
		assert.Len(t, publisher.publishedEvents, 1)
		
		// Verify event content
		stageEvent, ok := publisher.publishedEvents[0].(*StageCompletedEvent)
		assert.True(t, ok)
		assert.Equal(t, "test-workflow", stageEvent.WorkflowID)
		assert.Equal(t, "test-instance", stageEvent.InstanceID)
		assert.Equal(t, "stage1", stageEvent.StageID)
		assert.Equal(t, 0, stageEvent.StageIndex)
		assert.Equal(t, StageCompleted, stageEvent.Status)
		assert.Equal(t, 1, stageEvent.CurrentStage)
		assert.Equal(t, 2, stageEvent.TotalStages)
	})
	
	t.Run("Events not published when disabled", func(t *testing.T) {
		publisher := &mockEventPublisher{}
		subscriber := &mockQueueSubscriber{}
		transport := &mockTransport{}
		
		// Mock message publish but NOT event publish (shouldn't be called)
		publisher.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		transport.On("CreateQueue", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "FlowMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		
		// Create engine with completion events disabled (default)
		engine := NewStageFlowEngine(publisher, subscriber, transport)
		
		// Create simple workflow
		handler := &trackingStageHandler{
			stageID: "stage1",
			result: &StageResult{
				StageID: "stage1",
				Status:  StageCompleted,
			},
		}
		
		workflow := NewWorkflow("test-workflow", "Test Workflow").
			AddStage("stage1", handler)
		
		err := engine.RegisterWorkflow(workflow)
		assert.NoError(t, err)
		
		// Create state
		state := &WorkflowState{
			WorkflowID:   "test-workflow",
			InstanceID:   "test-instance",
			Status:       WorkflowRunning,
			StageResults: []StageResult{},
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
			CurrentStageIndex:       0,
			WorkflowID:              "test-workflow",
			InstanceID:              "test-instance",
			StageName:               "stage1",
		}
		
		// Process stage
		err = workflow.ProcessStageMessage(context.Background(), envelope)
		assert.NoError(t, err)
		
		// Verify NO events were published
		publisher.AssertNotCalled(t, "PublishEvent", mock.Anything, mock.Anything, mock.Anything)
		assert.Len(t, publisher.publishedEvents, 0)
	})
}

func TestEventCreation(t *testing.T) {
	t.Run("NewWorkflowCompletedEvent creates correct event", func(t *testing.T) {
		workflow := &Workflow{
			ID:   "test-workflow",
			Name: "Test Workflow",
			Stages: []*Stage{
				{ID: "stage1"},
				{ID: "stage2"},
				{ID: "stage3"},
			},
		}
		
		endTime := time.Now()
		state := &WorkflowState{
			WorkflowID: "test-workflow",
			InstanceID: "test-instance",
			Status:     WorkflowCompleted,
			StartTime:  endTime.Add(-5 * time.Minute),
			EndTime:    &endTime,
			Version:    3,
			GlobalData: map[string]interface{}{"final": "data"},
			StageResults: []StageResult{
				{StageID: "stage1", Status: StageCompleted},
				{StageID: "stage2", Status: StageCompleted},
				{StageID: "stage3", Status: StageCompleted},
			},
		}
		
		event := NewWorkflowCompletedEvent(workflow, state)
		
		assert.NotNil(t, event)
		assert.Equal(t, "WorkflowCompletedEvent", event.GetType())
		assert.Equal(t, "test-workflow", event.WorkflowID)
		assert.Equal(t, "Test Workflow", event.WorkflowName)
		assert.Equal(t, "test-instance", event.InstanceID)
		assert.Equal(t, WorkflowCompleted, event.Status)
		assert.Equal(t, 3, event.TotalStages)
		assert.Equal(t, 3, event.CompletedStages)
		assert.Equal(t, 0, event.FailedStages)
		assert.Equal(t, 5*time.Minute, event.Duration)
		assert.Equal(t, state.GlobalData, event.FinalData)
		assert.Equal(t, "test-instance", event.GetAggregateID())
		assert.Equal(t, int64(3), event.GetSequence())
	})
	
	t.Run("NewWorkflowFailedEvent creates correct event", func(t *testing.T) {
		workflow := &Workflow{
			ID:   "test-workflow",
			Name: "Test Workflow",
			Stages: []*Stage{
				{ID: "stage1"},
				{ID: "stage2"},
				{ID: "stage3"},
			},
		}
		
		state := &WorkflowState{
			WorkflowID: "test-workflow",
			InstanceID: "test-instance",
			Status:     WorkflowFailed,
			StartTime:  time.Now().Add(-2 * time.Minute),
			Version:    2,
			GlobalData: map[string]interface{}{"partial": "data"},
			StageResults: []StageResult{
				{StageID: "stage1", Status: StageCompleted},
			},
		}
		
		stageErr := assert.AnError
		event := NewWorkflowFailedEvent(workflow, state, stageErr, 1)
		
		assert.NotNil(t, event)
		assert.Equal(t, "WorkflowFailedEvent", event.GetType())
		assert.Equal(t, "test-workflow", event.WorkflowID)
		assert.Equal(t, "Test Workflow", event.WorkflowName)
		assert.Equal(t, "test-instance", event.InstanceID)
		assert.Equal(t, "stage2", event.FailedStageID)
		assert.Equal(t, 1, event.FailedStageIndex)
		assert.Equal(t, stageErr.Error(), event.Error)
		assert.Equal(t, 1, event.CompletedStages)
		assert.Equal(t, 3, event.TotalStages)
		assert.Equal(t, state.GlobalData, event.StateData)
	})
	
	t.Run("NewStageCompletedEvent creates correct event", func(t *testing.T) {
		workflow := &Workflow{
			ID: "test-workflow",
			Stages: []*Stage{
				{ID: "stage1"},
				{ID: "stage2"},
			},
		}
		
		state := &WorkflowState{
			WorkflowID: "test-workflow",
			InstanceID: "test-instance",
			Version:    2,
		}
		
		stage := &Stage{ID: "stage1"}
		
		endTime := time.Now()
		result := &StageResult{
			StageID:   "stage1",
			Status:    StageCompleted,
			StartTime: endTime.Add(-30 * time.Second),
			EndTime:   &endTime,
			Duration:  30 * time.Second,
			Data:      map[string]interface{}{"output": "value"},
		}
		
		event := NewStageCompletedEvent(workflow, state, stage, 0, result)
		
		assert.NotNil(t, event)
		assert.Equal(t, "StageCompletedEvent", event.GetType())
		assert.Equal(t, "test-workflow", event.WorkflowID)
		assert.Equal(t, "test-instance", event.InstanceID)
		assert.Equal(t, "stage1", event.StageID)
		assert.Equal(t, 0, event.StageIndex)
		assert.Equal(t, StageCompleted, event.Status)
		assert.Equal(t, 1, event.CurrentStage)
		assert.Equal(t, 2, event.TotalStages)
		assert.Equal(t, 30*time.Second, event.Duration)
		assert.Equal(t, result.Data, event.Data)
	})
}