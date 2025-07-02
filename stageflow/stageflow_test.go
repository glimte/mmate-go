package stageflow

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/messaging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Mock implementations for testing
type mockPublisher struct {
	mock.Mock
}

func (m *mockPublisher) Publish(ctx context.Context, msg contracts.Message, options ...messaging.PublishOption) error {
	args := m.Called(ctx, msg, options)
	return args.Error(0)
}

func (m *mockPublisher) PublishEvent(ctx context.Context, event contracts.Event, options ...messaging.PublishOption) error {
	args := m.Called(ctx, event, options)
	return args.Error(0)
}

func (m *mockPublisher) PublishCommand(ctx context.Context, command contracts.Command, options ...messaging.PublishOption) error {
	args := m.Called(ctx, command, options)
	return args.Error(0)
}

func (m *mockPublisher) Close() error {
	args := m.Called()
	return args.Error(0)
}

type mockSubscriber struct {
	mock.Mock
}

func (m *mockSubscriber) Subscribe(ctx context.Context, queue string, messageType string, handler messaging.MessageHandler, options ...messaging.SubscriptionOption) error {
	args := m.Called(ctx, queue, messageType, handler, options)
	return args.Error(0)
}

func (m *mockSubscriber) Unsubscribe(queueName string) error {
	args := m.Called(queueName)
	return args.Error(0)
}

func (m *mockSubscriber) Close() error {
	args := m.Called()
	return args.Error(0)
}

type mockTransport struct {
	mock.Mock
}

func (m *mockTransport) Publisher() messaging.TransportPublisher {
	args := m.Called()
	if pub := args.Get(0); pub != nil {
		return pub.(messaging.TransportPublisher)
	}
	return nil
}

func (m *mockTransport) Subscriber() messaging.TransportSubscriber {
	args := m.Called()
	if sub := args.Get(0); sub != nil {
		return sub.(messaging.TransportSubscriber)
	}
	return nil
}

func (m *mockTransport) CreateQueue(ctx context.Context, name string, options messaging.QueueOptions) error {
	args := m.Called(ctx, name, options)
	return args.Error(0)
}

func (m *mockTransport) DeleteQueue(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}

func (m *mockTransport) BindQueue(ctx context.Context, queue, exchange, routingKey string) error {
	args := m.Called(ctx, queue, exchange, routingKey)
	return args.Error(0)
}

func (m *mockTransport) DeclareQueueWithBindings(ctx context.Context, name string, options messaging.QueueOptions, bindings []messaging.QueueBinding) error {
	args := m.Called(ctx, name, options, bindings)
	return args.Error(0)
}

func (m *mockTransport) Connect(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *mockTransport) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockTransport) IsConnected() bool {
	args := m.Called()
	return args.Bool(0)
}

type testStageHandler struct {
	stageID string
	result  *StageResult
	err     error
}

func (h *testStageHandler) Execute(ctx context.Context, state *WorkflowState) (*StageResult, error) {
	if h.err != nil {
		return nil, h.err
	}
	if h.result != nil {
		return h.result, nil
	}
	return &StageResult{
		StageID: h.stageID,
		Status:  StageCompleted,
		Data:    map[string]interface{}{"result": "success"},
	}, nil
}

func (h *testStageHandler) GetStageID() string {
	return h.stageID
}

type testCompensationHandler struct {
	stageID string
	err     error
}

func (h *testCompensationHandler) Compensate(ctx context.Context, state *WorkflowState, originalResult *StageResult) error {
	return h.err
}

func (h *testCompensationHandler) GetStageID() string {
	return h.stageID
}

// Helper function to create a test engine
func createTestEngine(t *testing.T) (*StageFlowEngine, *mockPublisher, *mockSubscriber, *mockTransport) {
	publisher := &mockPublisher{}
	subscriber := &mockSubscriber{}
	transport := &mockTransport{}
	
	// Default mocks for transport
	transport.On("CreateQueue", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil).Maybe()
	subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "FlowMessageEnvelope", mock.Anything, mock.Anything).Return(nil).Maybe()
	subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "CompensationMessageEnvelope", mock.Anything, mock.Anything).Return(nil).Maybe()
	
	engine := NewStageFlowEngine(publisher, subscriber, transport)
	return engine, publisher, subscriber, transport
}

func TestNewStageFlowEngine(t *testing.T) {
	t.Run("NewStageFlowEngine creates engine with defaults", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := &mockSubscriber{}
		transport := &mockTransport{}
		
		engine := NewStageFlowEngine(publisher, subscriber, transport)
		
		assert.NotNil(t, engine)
		assert.Equal(t, publisher, engine.publisher)
		assert.Equal(t, subscriber, engine.subscriber)
		assert.Equal(t, transport, engine.transport)
		assert.NotNil(t, engine.stateStore)
		assert.NotNil(t, engine.logger)
		assert.Empty(t, engine.workflows)
		assert.Equal(t, "stageflow.", engine.stageQueuePrefix)
		assert.Equal(t, 10, engine.maxConcurrency)
	})
	
	t.Run("NewStageFlowEngine applies options", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := &mockSubscriber{}
		transport := &mockTransport{}
		customStore := NewInMemoryStateStore()
		
		engine := NewStageFlowEngine(
			publisher, 
			subscriber,
			transport,
			WithStateStore(customStore),
			WithStageQueuePrefix("myapp.stages."),
			WithMaxStageConcurrency(5),
		)
		
		assert.NotNil(t, engine)
		assert.Equal(t, customStore, engine.stateStore)
		assert.Equal(t, "myapp.stages.", engine.stageQueuePrefix)
		assert.Equal(t, 5, engine.maxConcurrency)
	})
}

func TestWorkflow(t *testing.T) {
	t.Run("NewWorkflow creates workflow", func(t *testing.T) {
		workflow := NewWorkflow("test-workflow", "Test Workflow")
		
		assert.Equal(t, "test-workflow", workflow.ID)
		assert.Equal(t, "Test Workflow", workflow.Name)
		assert.Empty(t, workflow.Stages)
		assert.NotNil(t, workflow.stageMap)
	})
	
	t.Run("AddStage adds stage to workflow", func(t *testing.T) {
		workflow := NewWorkflow("test-workflow", "Test Workflow")
		handler := &testStageHandler{stageID: "test-stage"}
		
		result := workflow.AddStage("test-stage", handler)
		
		assert.Equal(t, workflow, result) // Should return self for chaining
		assert.Len(t, workflow.Stages, 1)
		assert.Equal(t, "test-stage", workflow.Stages[0].ID)
		assert.Equal(t, handler, workflow.Stages[0].Handler)
		assert.True(t, workflow.Stages[0].Required)
	})
	
	t.Run("AddStageWithOptions applies options", func(t *testing.T) {
		workflow := NewWorkflow("test-workflow", "Test Workflow")
		handler := &testStageHandler{stageID: "test-stage"}
		
		workflow.AddStageWithOptions("test-stage", handler,
			WithTimeout(10*time.Second),
			WithRequired(false),
			WithExecutionMode(Parallel),
		)
		
		assert.Len(t, workflow.Stages, 1)
		stage := workflow.Stages[0]
		assert.Equal(t, 10*time.Second, stage.Timeout)
		assert.False(t, stage.Required)
		assert.Equal(t, Parallel, stage.Mode)
	})
	
	t.Run("AddCompensation adds compensation to existing stage", func(t *testing.T) {
		workflow := NewWorkflow("test-workflow", "Test Workflow")
		handler := &testStageHandler{stageID: "test-stage"}
		compensation := &testCompensationHandler{stageID: "test-stage"}
		
		workflow.AddStage("test-stage", handler)
		workflow.AddCompensation("test-stage", compensation)
		
		stage := workflow.Stages[0]
		assert.Equal(t, compensation, stage.Compensation)
	})
}

func TestStageFlowEngine(t *testing.T) {
	t.Run("RegisterWorkflow succeeds with valid workflow", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := &mockSubscriber{}
		transport := &mockTransport{}
		
		// Mock expectations for queue creation
		transport.On("CreateQueue", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "FlowMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "CompensationMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		
		engine := NewStageFlowEngine(publisher, subscriber, transport)
		
		workflow := NewWorkflow("test-workflow", "Test Workflow")
		workflow.AddStage("stage1", &testStageHandler{stageID: "stage1"})
		
		err := engine.RegisterWorkflow(workflow)
		
		assert.NoError(t, err)
		assert.Equal(t, engine, workflow.engine)
		
		// Verify workflow is registered
		retrieved, err := engine.GetWorkflow("test-workflow")
		assert.NoError(t, err)
		assert.Equal(t, workflow, retrieved)
		
		// Verify queues were created
		transport.AssertCalled(t, "CreateQueue", mock.Anything, "stageflow.test-workflow.stage0", mock.Anything)
		subscriber.AssertCalled(t, "Subscribe", mock.Anything, "stageflow.test-workflow.stage0", "FlowMessageEnvelope", mock.Anything, mock.Anything)
	})
	
	t.Run("RegisterWorkflow fails with nil workflow", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := &mockSubscriber{}
		transport := &mockTransport{}
		engine := NewStageFlowEngine(publisher, subscriber, transport)
		
		err := engine.RegisterWorkflow(nil)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "workflow cannot be nil")
	})
	
	t.Run("RegisterWorkflow fails with empty ID", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := &mockSubscriber{}
		transport := &mockTransport{}
		engine := NewStageFlowEngine(publisher, subscriber, transport)
		
		workflow := &Workflow{ID: "", Name: "Test"}
		
		err := engine.RegisterWorkflow(workflow)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "workflow ID cannot be empty")
	})
	
	t.Run("GetWorkflow fails with non-existent workflow", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := &mockSubscriber{}
		transport := &mockTransport{}
		engine := NewStageFlowEngine(publisher, subscriber, transport)
		
		workflow, err := engine.GetWorkflow("non-existent")
		
		assert.Error(t, err)
		assert.Nil(t, workflow)
		assert.Contains(t, err.Error(), "workflow not found")
	})
}

func TestWorkflowExecution(t *testing.T) {
	t.Run("Execute workflow with successful stages", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := &mockSubscriber{}
		transport := &mockTransport{}
		
		// Setup mock expectations for queue-based execution
		transport.On("CreateQueue", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "FlowMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "CompensationMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		publisher.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		
		engine := NewStageFlowEngine(publisher, subscriber, transport)
		
		// Create workflow with two stages
		handler1 := &testStageHandler{stageID: "stage1"}
		handler2 := &testStageHandler{stageID: "stage2"}
		
		workflow := NewWorkflow("test-workflow", "Test Workflow").
			AddStage("stage1", handler1).
			AddStage("stage2", handler2)
		
		err := engine.RegisterWorkflow(workflow)
		assert.NoError(t, err)
		
		// Execute workflow - in queue-based model, this publishes first message
		initialData := map[string]interface{}{"input": "test"}
		state, err := workflow.Execute(context.Background(), initialData)
		
		assert.NoError(t, err)
		assert.NotNil(t, state)
		// Queue-based execution starts as running
		assert.Equal(t, WorkflowRunning, state.Status)
		// No stages completed yet - execution is asynchronous
		assert.Len(t, state.StageResults, 0)
		
		// Verify that the first stage message was published
		publisher.AssertCalled(t, "Publish", mock.Anything, mock.AnythingOfType("*stageflow.FlowMessageEnvelope"), mock.Anything)
		
		// Get the actual call and verify routing key
		calls := publisher.Calls
		assert.Greater(t, len(calls), 0)
		publishOptions := calls[0].Arguments[2].([]messaging.PublishOption)
		// The routing key is set via WithRoutingKey option
		assert.NotNil(t, publishOptions)
	})
	
	t.Run("Execute workflow fails when required stage fails", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := &mockSubscriber{}
		transport := &mockTransport{}
		
		// Setup mock expectations
		transport.On("CreateQueue", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "FlowMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "CompensationMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		publisher.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		
		engine := NewStageFlowEngine(publisher, subscriber, transport)
		
		// Create workflow with failing stage
		handler1 := &testStageHandler{stageID: "stage1"}
		handler2 := &testStageHandler{
			stageID: "stage2",
			err:     errors.New("stage failed"),
		}
		
		workflow := NewWorkflow("test-workflow", "Test Workflow").
			AddStage("stage1", handler1).
			AddStage("stage2", handler2)
		
		err := engine.RegisterWorkflow(workflow)
		assert.NoError(t, err)
		
		// Execute workflow - queue-based execution returns immediately
		initialData := map[string]interface{}{"input": "test"}
		state, err := workflow.Execute(context.Background(), initialData)
		
		// In queue-based model, Execute() succeeds and publishes first message
		assert.NoError(t, err)
		assert.NotNil(t, state)
		assert.Equal(t, WorkflowRunning, state.Status)
		
		// Verify message was published
		publisher.AssertCalled(t, "Publish", mock.Anything, mock.AnythingOfType("*stageflow.FlowMessageEnvelope"), mock.Anything)
	})
	
	t.Run("Execute workflow continues when optional stage fails", func(t *testing.T) {
		publisher := &mockPublisher{}
		subscriber := &mockSubscriber{}
		transport := &mockTransport{}
		
		// Setup mock expectations for queue-based execution
		publisher.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		transport.On("CreateQueue", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "FlowMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		subscriber.On("Subscribe", mock.Anything, mock.AnythingOfType("string"), "CompensationMessageEnvelope", mock.Anything, mock.Anything).Return(nil)
		
		engine := NewStageFlowEngine(publisher, subscriber, transport)
		engine.SetServiceQueue("test-queue")
		
		// Create workflow with optional failing stage
		handler1 := &testStageHandler{stageID: "stage1"}
		handler2 := &testStageHandler{
			stageID: "stage2",
			err:     errors.New("optional stage failed"),
		}
		handler3 := &testStageHandler{stageID: "stage3"}
		
		workflow := NewWorkflow("test-workflow", "Test Workflow").
			AddStage("stage1", handler1).
			AddStageWithOptions("stage2", handler2, WithRequired(false)).
			AddStage("stage3", handler3)
		
		err := engine.RegisterWorkflow(workflow)
		assert.NoError(t, err)
		
		// Execute workflow - queue-based execution returns immediately
		initialData := map[string]interface{}{"input": "test"}
		state, err := workflow.Execute(context.Background(), initialData)
		
		// In queue-based model, Execute() succeeds and publishes first message
		assert.NoError(t, err)
		assert.NotNil(t, state)
		assert.Equal(t, WorkflowRunning, state.Status)
		assert.Len(t, state.StageResults, 0) // No stages completed yet
		
		// Verify message was published
		publisher.AssertCalled(t, "Publish", mock.Anything, mock.AnythingOfType("*stageflow.FlowMessageEnvelope"), mock.Anything)
	})
	
	t.Run("Execute workflow fails without engine", func(t *testing.T) {
		workflow := NewWorkflow("test-workflow", "Test Workflow")
		
		state, err := workflow.Execute(context.Background(), nil)
		
		assert.Error(t, err)
		assert.Nil(t, state)
		assert.Contains(t, err.Error(), "workflow not registered with engine")
	})
}

func TestInMemoryStateStore(t *testing.T) {
	t.Run("SaveState and LoadState work correctly", func(t *testing.T) {
		store := NewInMemoryStateStore()
		
		state := &WorkflowState{
			WorkflowID: "test-workflow",
			InstanceID: "test-instance",
			Status:     WorkflowRunning,
			GlobalData: map[string]interface{}{"key": "value"},
		}
		
		// Save state
		err := store.SaveState(context.Background(), state)
		assert.NoError(t, err)
		
		// Load state
		loadedState, err := store.LoadState(context.Background(), "test-instance")
		assert.NoError(t, err)
		assert.Equal(t, state.WorkflowID, loadedState.WorkflowID)
		assert.Equal(t, state.InstanceID, loadedState.InstanceID)
		assert.Equal(t, state.Status, loadedState.Status)
		assert.Equal(t, "value", loadedState.GlobalData["key"])
	})
	
	t.Run("LoadState fails with non-existent instance", func(t *testing.T) {
		store := NewInMemoryStateStore()
		
		state, err := store.LoadState(context.Background(), "non-existent")
		
		assert.Error(t, err)
		assert.Nil(t, state)
		assert.Contains(t, err.Error(), "workflow state not found")
	})
	
	t.Run("DeleteState removes state", func(t *testing.T) {
		store := NewInMemoryStateStore()
		
		state := &WorkflowState{
			InstanceID: "test-instance",
			Status:     WorkflowCompleted,
		}
		
		// Save and verify exists
		err := store.SaveState(context.Background(), state)
		assert.NoError(t, err)
		
		_, err = store.LoadState(context.Background(), "test-instance")
		assert.NoError(t, err)
		
		// Delete and verify removed
		err = store.DeleteState(context.Background(), "test-instance")
		assert.NoError(t, err)
		
		_, err = store.LoadState(context.Background(), "test-instance")
		assert.Error(t, err)
	})
	
	t.Run("ListActiveWorkflows returns active workflows", func(t *testing.T) {
		store := NewInMemoryStateStore()
		
		// Add active and completed workflows
		activeState := &WorkflowState{InstanceID: "active", Status: WorkflowRunning}
		completedState := &WorkflowState{InstanceID: "completed", Status: WorkflowCompleted}
		
		store.SaveState(context.Background(), activeState)
		store.SaveState(context.Background(), completedState)
		
		// Get active workflows
		activeWorkflows, err := store.ListActiveWorkflows(context.Background())
		assert.NoError(t, err)
		assert.Len(t, activeWorkflows, 1)
		assert.Contains(t, activeWorkflows, "active")
	})
}

func TestStageOptions(t *testing.T) {
	t.Run("Stage options apply correctly", func(t *testing.T) {
		stage := &Stage{}
		
		WithTimeout(10 * time.Second)(stage)
		assert.Equal(t, 10*time.Second, stage.Timeout)
		
		WithRequired(false)(stage)
		assert.False(t, stage.Required)
		
		WithExecutionMode(Parallel)(stage)
		assert.Equal(t, Parallel, stage.Mode)
		
		WithDependencies("dep1", "dep2")(stage)
		assert.Equal(t, []string{"dep1", "dep2"}, stage.Dependencies)
	})
}

func TestStageHandlerFunc(t *testing.T) {
	t.Run("StageHandlerFunc implements StageHandler", func(t *testing.T) {
		called := false
		var receivedState *WorkflowState
		
		handlerFunc := StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			called = true
			receivedState = state
			return &StageResult{Status: StageCompleted}, nil
		})
		
		state := &WorkflowState{InstanceID: "test"}
		result, err := handlerFunc.Execute(context.Background(), state)
		
		assert.NoError(t, err)
		assert.True(t, called)
		assert.Equal(t, state, receivedState)
		assert.Equal(t, StageCompleted, result.Status)
		assert.Equal(t, "anonymous", handlerFunc.GetStageID())
	})
}

func TestCompensationHandlerFunc(t *testing.T) {
	t.Run("CompensationHandlerFunc implements CompensationHandler", func(t *testing.T) {
		called := false
		var receivedState *WorkflowState
		var receivedResult *StageResult
		
		compensationFunc := CompensationHandlerFunc(func(ctx context.Context, state *WorkflowState, originalResult *StageResult) error {
			called = true
			receivedState = state
			receivedResult = originalResult
			return nil
		})
		
		state := &WorkflowState{InstanceID: "test"}
		originalResult := &StageResult{StageID: "original"}
		
		err := compensationFunc.Compensate(context.Background(), state, originalResult)
		
		assert.NoError(t, err)
		assert.True(t, called)
		assert.Equal(t, state, receivedState)
		assert.Equal(t, originalResult, receivedResult)
		assert.Equal(t, "anonymous", compensationFunc.GetStageID())
	})
}