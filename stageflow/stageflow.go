package stageflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/internal/reliability"
	"github.com/glimte/mmate-go/messaging"
	"github.com/google/uuid"
)

// StageStatus represents the execution status of a stage
type StageStatus string

const (
	StagePending     StageStatus = "pending"
	StageRunning     StageStatus = "running"
	StageCompleted   StageStatus = "completed"
	StageFailed      StageStatus = "failed"
	StageCompensated StageStatus = "compensated"
	StageSkipped     StageStatus = "skipped"
)

// WorkflowStatus represents the overall workflow status
type WorkflowStatus string

const (
	WorkflowPending     WorkflowStatus = "pending"
	WorkflowRunning     WorkflowStatus = "running"
	WorkflowCompleted   WorkflowStatus = "completed"
	WorkflowFailed      WorkflowStatus = "failed"
	WorkflowCompensated WorkflowStatus = "compensated"
	WorkflowCancelled   WorkflowStatus = "cancelled"
)

// ExecutionMode defines how stages are executed
type ExecutionMode string

const (
	Sequential ExecutionMode = "sequential"
	Parallel   ExecutionMode = "parallel"
)

// StageResult contains the result of stage execution
type StageResult struct {
	StageID   string                 `json:"stageId"`
	Status    StageStatus            `json:"status"`
	Data      map[string]interface{} `json:"data,omitempty"`
	Error     string                 `json:"error,omitempty"`
	StartTime time.Time              `json:"startTime"`
	EndTime   *time.Time             `json:"endTime,omitempty"`
	Duration  time.Duration          `json:"duration"`
}

// WorkflowState represents the current state of a workflow execution
type WorkflowState struct {
	WorkflowID   string                 `json:"workflowId"`
	InstanceID   string                 `json:"instanceId"`
	Status       WorkflowStatus         `json:"status"`
	CurrentStage string                 `json:"currentStage,omitempty"`
	StageResults []StageResult          `json:"stageResults"`
	GlobalData   map[string]interface{} `json:"globalData"`
	StartTime    time.Time              `json:"startTime"`
	EndTime      *time.Time             `json:"endTime,omitempty"`
	LastModified time.Time              `json:"lastModified"`
	ErrorMessage string                 `json:"errorMessage,omitempty"`
	Version      int                    `json:"version"`
}

// StageHandler defines the interface for stage execution
type StageHandler interface {
	Execute(ctx context.Context, state *WorkflowState) (*StageResult, error)
	GetStageID() string
}

// CompensationHandler defines the interface for compensation logic
type CompensationHandler interface {
	Compensate(ctx context.Context, state *WorkflowState, originalResult *StageResult) error
	GetStageID() string
}

// StateStore defines the interface for workflow state persistence
type StateStore interface {
	SaveState(ctx context.Context, state *WorkflowState) error
	LoadState(ctx context.Context, instanceID string) (*WorkflowState, error)
	DeleteState(ctx context.Context, instanceID string) error
	ListActiveWorkflows(ctx context.Context) ([]string, error)
}

// StageHandlerFunc is a function adapter for StageHandler
type StageHandlerFunc func(ctx context.Context, state *WorkflowState) (*StageResult, error)

func (f StageHandlerFunc) Execute(ctx context.Context, state *WorkflowState) (*StageResult, error) {
	return f(ctx, state)
}

func (f StageHandlerFunc) GetStageID() string {
	return "anonymous"
}

// CompensationHandlerFunc is a function adapter for CompensationHandler
type CompensationHandlerFunc func(ctx context.Context, state *WorkflowState, originalResult *StageResult) error

func (f CompensationHandlerFunc) Compensate(ctx context.Context, state *WorkflowState, originalResult *StageResult) error {
	return f(ctx, state, originalResult)
}

func (f CompensationHandlerFunc) GetStageID() string {
	return "anonymous"
}

// Stage represents a single stage in a workflow
type Stage struct {
	ID           string
	Handler      StageHandler
	Compensation CompensationHandler
	Timeout      time.Duration
	RetryPolicy  reliability.RetryPolicy
	Dependencies []string
	Mode         ExecutionMode
	Required     bool
}

// Workflow represents a complete workflow definition
type Workflow struct {
	ID          string
	Name        string
	Description string
	Stages      []*Stage
	stageMap    map[string]*Stage
	engine      *StageFlowEngine
	timeout     time.Duration
	retryPolicy reliability.RetryPolicy
}

// StageFlowEngine manages workflow execution
type StageFlowEngine struct {
	publisher         messaging.Publisher
	subscriber        messaging.Subscriber
	transport         messaging.Transport
	stateStore        StateStore
	workflows         map[string]*Workflow
	serviceQueue      string
	stageQueuePrefix  string
	maxConcurrency    int
	mu                sync.RWMutex
	logger            *slog.Logger
	contractExtractor messaging.ContractExtractor
	stageHandlers     map[string]func(context.Context, *FlowMessageEnvelope) error
	enableCompletionEvents bool
	completionEventOptions []messaging.PublishOption
}

// EngineOption configures the stage flow engine
type EngineOption func(*EngineConfig)

// EngineConfig holds configuration for the engine
type EngineConfig struct {
	StateStore StateStore
	Logger     *slog.Logger
	StageQueuePrefix string
	MaxStageConcurrency int
	EnableCompletionEvents bool
	CompletionEventOptions []messaging.PublishOption
}

// WithStateStore sets the state store for workflow persistence
func WithStateStore(store StateStore) EngineOption {
	return func(c *EngineConfig) {
		c.StateStore = store
	}
}

// WithStageFlowLogger sets the logger for the engine
func WithStageFlowLogger(logger *slog.Logger) EngineOption {
	return func(c *EngineConfig) {
		c.Logger = logger
	}
}

// WithStageQueuePrefix sets the prefix for stage queue names
func WithStageQueuePrefix(prefix string) EngineOption {
	return func(c *EngineConfig) {
		c.StageQueuePrefix = prefix
	}
}

// WithMaxStageConcurrency sets the max concurrent messages per stage
func WithMaxStageConcurrency(max int) EngineOption {
	return func(c *EngineConfig) {
		c.MaxStageConcurrency = max
	}
}

// WithCompletionEvents enables publishing of workflow completion events
func WithCompletionEvents(enabled bool, options ...messaging.PublishOption) EngineOption {
	return func(c *EngineConfig) {
		c.EnableCompletionEvents = enabled
		c.CompletionEventOptions = options
	}
}

// NewStageFlowEngine creates a new stage flow engine
func NewStageFlowEngine(publisher messaging.Publisher, subscriber messaging.Subscriber, transport messaging.Transport, opts ...EngineOption) *StageFlowEngine {
	config := &EngineConfig{
		StateStore: NewQueueBasedStateStore(), // Use queue-based state store
		Logger:     slog.Default(),
		StageQueuePrefix: "stageflow.",
		MaxStageConcurrency: 10,
	}

	for _, opt := range opts {
		opt(config)
	}

	engine := &StageFlowEngine{
		publisher:  publisher,
		subscriber: subscriber,
		transport:  transport,
		stateStore: config.StateStore,
		workflows:  make(map[string]*Workflow),
		stageQueuePrefix: config.StageQueuePrefix,
		maxConcurrency: config.MaxStageConcurrency,
		logger:     config.Logger,
		stageHandlers: make(map[string]func(context.Context, *FlowMessageEnvelope) error),
		enableCompletionEvents: config.EnableCompletionEvents,
		completionEventOptions: config.CompletionEventOptions,
	}

	// Register message types
	messaging.Register("FlowMessageEnvelope", func() contracts.Message { return &FlowMessageEnvelope{} })
	messaging.Register("WorkflowCompletedEvent", func() contracts.Message { return &WorkflowCompletedEvent{} })
	messaging.Register("WorkflowFailedEvent", func() contracts.Message { return &WorkflowFailedEvent{} })
	messaging.Register("StageCompletedEvent", func() contracts.Message { return &StageCompletedEvent{} })

	return engine
}

// proceedToNextStage determines next action after stage completion
func (w *Workflow) proceedToNextStage(ctx context.Context, envelope *FlowMessageEnvelope, state *WorkflowState) error {
	nextStageIndex := envelope.CurrentStageIndex + 1
	
	// Check if workflow is complete
	if nextStageIndex >= len(w.Stages) {
		// Workflow completed
		state.Status = WorkflowCompleted
		endTime := time.Now()
		state.EndTime = &endTime
		state.LastModified = time.Now()
		state.Version++
		
		w.engine.logger.Info("workflow execution completed",
			"workflowId", envelope.WorkflowID,
			"instanceId", envelope.InstanceID,
			"duration", endTime.Sub(state.StartTime))
		
		// Publish completion event if enabled
		if w.engine.enableCompletionEvents {
			event := NewWorkflowCompletedEvent(w, state)
			options := append([]messaging.PublishOption{messaging.WithPersistent(true)}, w.engine.completionEventOptions...)
			if err := w.engine.publisher.PublishEvent(ctx, event, options...); err != nil {
				w.engine.logger.Error("failed to publish workflow completion event",
					"error", err,
					"workflowId", envelope.WorkflowID,
					"instanceId", envelope.InstanceID)
				// Don't fail the workflow if event publishing fails
			}
		}
		
		return nil
	}
	
	// Publish to next stage with updated state
	return w.publishToStage(ctx, nextStageIndex, state, envelope.Payload)
}

// ProcessStageMessage processes a stage message - exposed for external dispatcher integration
func (w *Workflow) ProcessStageMessage(ctx context.Context, envelope *FlowMessageEnvelope) error {
	// Verify this message is for this workflow
	if envelope.WorkflowID != w.ID {
		return fmt.Errorf("message workflow ID %s does not match %s", envelope.WorkflowID, w.ID)
	}
	
	w.engine.logger.Info("processing stage message",
		"workflowId", envelope.WorkflowID,
		"instanceId", envelope.InstanceID,
		"stageIndex", envelope.CurrentStageIndex,
		"stageName", envelope.StageName)
	
	// Process the stage
	return w.processStageMessage(ctx, envelope)
}

// RegisterWorkflow registers a workflow with the engine
func (e *StageFlowEngine) RegisterWorkflow(workflow *Workflow) error {
	ctx := context.Background()
	
	if workflow == nil {
		return fmt.Errorf("workflow cannot be nil")
	}
	if workflow.ID == "" {
		return fmt.Errorf("workflow ID cannot be empty")
	}

	workflow.engine = e

	// Create stage queues
	if err := e.createStageQueues(ctx, workflow); err != nil {
		return fmt.Errorf("failed to create stage queues: %w", err)
	}
	
	// Subscribe to stage queues
	if err := e.subscribeToStageQueues(ctx, workflow); err != nil {
		return fmt.Errorf("failed to subscribe to stage queues: %w", err)
	}

	e.mu.Lock()
	e.workflows[workflow.ID] = workflow
	e.mu.Unlock()

	e.logger.Info("registered workflow", "workflowId", workflow.ID, "name", workflow.Name, "stages", len(workflow.Stages))
	
	// Extract and publish contract if extractor is configured
	// For now, we pass nil as input type since workflows don't have explicit input message types
	// In the future, we could enhance workflows to declare their trigger message type
	if e.contractExtractor != nil {
		if err := e.contractExtractor.ExtractFromWorkflow(ctx, workflow.ID, workflow.Name, nil); err != nil {
			e.logger.Warn("failed to extract workflow contract", "error", err, "workflowId", workflow.ID)
		}
	}
	
	return nil
}

// GetWorkflow retrieves a registered workflow by ID
func (e *StageFlowEngine) GetWorkflow(workflowID string) (*Workflow, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	workflow, exists := e.workflows[workflowID]
	if !exists {
		return nil, fmt.Errorf("workflow not found: %s", workflowID)
	}

	return workflow, nil
}

// SetServiceQueue sets the service queue name for workflow message routing
func (e *StageFlowEngine) SetServiceQueue(queueName string) {
	e.serviceQueue = queueName
	e.logger.Info("service queue set for stageflow", "queue", queueName)
}

// SetContractExtractor sets the contract extractor for workflow registration
func (e *StageFlowEngine) SetContractExtractor(extractor messaging.ContractExtractor) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.contractExtractor = extractor
}

// getStageQueueName returns the queue name for a specific stage
func (e *StageFlowEngine) getStageQueueName(workflowID string, stageIndex int) string {
	return fmt.Sprintf("%s%s.stage%d", e.stageQueuePrefix, workflowID, stageIndex)
}

// createStageQueues creates all queues for a workflow
func (e *StageFlowEngine) createStageQueues(ctx context.Context, workflow *Workflow) error {
	for i := range workflow.Stages {
		queueName := e.getStageQueueName(workflow.ID, i)
		
		// Create queue with DLQ settings
		queueOptions := messaging.QueueOptions{
			Durable:     true,
			AutoDelete:  false,
			Args: map[string]interface{}{
				"x-message-ttl": int32(3600000), // 1 hour TTL
				"x-dead-letter-exchange": "",
				"x-dead-letter-routing-key": fmt.Sprintf("dlq.%s", queueName),
			},
		}
		
		if err := e.transport.CreateQueue(ctx, queueName, queueOptions); err != nil {
			return fmt.Errorf("failed to create queue %s: %w", queueName, err)
		}
		
		e.logger.Info("created stage queue", "queue", queueName, "workflow", workflow.ID, "stage", i)
	}
	
	return nil
}

// subscribeToStageQueues subscribes to all stage queues for a workflow
func (e *StageFlowEngine) subscribeToStageQueues(ctx context.Context, workflow *Workflow) error {
	for i, stage := range workflow.Stages {
		queueName := e.getStageQueueName(workflow.ID, i)
		stageIndex := i
		currentStage := stage
		
		// Create handler for this stage
		handler := func(ctx context.Context, envelope *FlowMessageEnvelope) error {
			envelope.CurrentStageIndex = stageIndex
			envelope.StageName = currentStage.ID
			return workflow.processStageMessage(ctx, envelope)
		}
		
		// Store handler
		e.stageHandlers[queueName] = handler
		
		// Subscribe to queue
		messageHandler := messaging.MessageHandlerFunc(func(ctx context.Context, msg contracts.Message) error {
			envelope, ok := msg.(*FlowMessageEnvelope)
			if !ok {
				return fmt.Errorf("expected FlowMessageEnvelope, got %T", msg)
			}
			return handler(ctx, envelope)
		})
		
		// Subscribe with prefetch for concurrency control
		if err := e.subscriber.Subscribe(ctx, queueName, "FlowMessageEnvelope", 
			messageHandler, 
			messaging.WithPrefetchCount(e.maxConcurrency)); err != nil {
			return fmt.Errorf("failed to subscribe to queue %s: %w", queueName, err)
		}
		
		e.logger.Info("subscribed to stage queue", "queue", queueName, "workflow", workflow.ID, "stage", i)
	}
	
	return nil
}

// ExecuteWorkflow starts execution of a workflow
func (e *StageFlowEngine) ExecuteWorkflow(ctx context.Context, workflowID string, initialData map[string]interface{}) (*WorkflowState, error) {
	workflow, err := e.GetWorkflow(workflowID)
	if err != nil {
		return nil, err
	}

	return workflow.Execute(ctx, initialData)
}

// NewWorkflow creates a new workflow definition
func NewWorkflow(id, name string) *Workflow {
	return &Workflow{
		ID:          id,
		Name:        name,
		Stages:      make([]*Stage, 0),
		stageMap:    make(map[string]*Stage),
		timeout:     30 * time.Minute,
		retryPolicy: reliability.NewExponentialBackoff(100*time.Millisecond, 5*time.Second, 2.0, 3),
	}
}

// AddStage adds a stage to the workflow
func (w *Workflow) AddStage(stageID string, handler StageHandler) *Workflow {
	stage := &Stage{
		ID:       stageID,
		Handler:  handler,
		Mode:     Sequential,
		Required: true,
		Timeout:  5 * time.Minute,
	}

	w.Stages = append(w.Stages, stage)
	w.stageMap[stageID] = stage

	return w
}

// AddStageWithOptions adds a stage with custom options
func (w *Workflow) AddStageWithOptions(stageID string, handler StageHandler, opts ...StageOption) *Workflow {
	stage := &Stage{
		ID:       stageID,
		Handler:  handler,
		Mode:     Sequential,
		Required: true,
		Timeout:  5 * time.Minute,
	}

	for _, opt := range opts {
		opt(stage)
	}

	w.Stages = append(w.Stages, stage)
	w.stageMap[stageID] = stage

	return w
}

// AddCompensation adds compensation logic for a stage
func (w *Workflow) AddCompensation(stageID string, compensation CompensationHandler) *Workflow {
	if stage, exists := w.stageMap[stageID]; exists {
		stage.Compensation = compensation
	}
	return w
}

// StageOption configures a stage
type StageOption func(*Stage)

// WithTimeout sets the timeout for a stage
func WithTimeout(timeout time.Duration) StageOption {
	return func(s *Stage) {
		s.Timeout = timeout
	}
}

// WithStageRetryPolicy sets the retry policy for a stage
func WithStageRetryPolicy(policy reliability.RetryPolicy) StageOption {
	return func(s *Stage) {
		s.RetryPolicy = policy
	}
}

// WithDependencies sets the dependencies for a stage
func WithDependencies(deps ...string) StageOption {
	return func(s *Stage) {
		s.Dependencies = deps
	}
}

// WithExecutionMode sets the execution mode for a stage
func WithExecutionMode(mode ExecutionMode) StageOption {
	return func(s *Stage) {
		s.Mode = mode
	}
}

// WithRequired sets whether a stage is required
func WithRequired(required bool) StageOption {
	return func(s *Stage) {
		s.Required = required
	}
}

// Execute starts the workflow execution by publishing to first stage queue
func (w *Workflow) Execute(ctx context.Context, initialData map[string]interface{}) (*WorkflowState, error) {
	if w.engine == nil {
		return nil, fmt.Errorf("workflow not registered with engine")
	}

	if len(w.Stages) == 0 {
		return nil, fmt.Errorf("workflow has no stages")
	}

	// Create workflow instance
	instanceID := uuid.New().String()
	state := &WorkflowState{
		WorkflowID:   w.ID,
		InstanceID:   instanceID,
		Status:       WorkflowRunning,
		StageResults: make([]StageResult, 0),
		GlobalData:   initialData,
		StartTime:    time.Now(),
		LastModified: time.Now(),
		Version:      1,
	}

	if state.GlobalData == nil {
		state.GlobalData = make(map[string]interface{})
	}

	w.engine.logger.Info("starting workflow execution",
		"workflowId", w.ID,
		"instanceId", instanceID,
		"stageCount", len(w.Stages))

	// Publish to first stage queue with state embedded
	err := w.publishToStage(ctx, 0, state, initialData)
	if err != nil {
		return nil, fmt.Errorf("failed to start workflow: %w", err)
	}

	return state, nil
}

// publishToStage publishes a message to the specific stage queue with state embedded
func (w *Workflow) publishToStage(ctx context.Context, stageIndex int, state *WorkflowState, payload interface{}) error {
	if stageIndex >= len(w.Stages) {
		return fmt.Errorf("stage index %d out of bounds", stageIndex)
	}

	stage := w.Stages[stageIndex]

	// Serialize workflow state
	stateData, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to serialize workflow state: %w", err)
	}

	// Serialize payload
	payloadData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to serialize payload: %w", err)
	}

	// Create flow message envelope with state embedded
	envelope := &FlowMessageEnvelope{}
	envelope.Type = "FlowMessageEnvelope"
	envelope.ID = uuid.New().String()
	envelope.Timestamp = time.Now()
	envelope.Payload = payloadData
	envelope.PayloadType = fmt.Sprintf("%T", payload)
	envelope.SerializedWorkflowState = string(stateData)
	envelope.WorkflowStateType = "WorkflowState"
	envelope.CurrentStageIndex = stageIndex
	envelope.WorkflowID = w.ID
	envelope.InstanceID = state.InstanceID
	envelope.StageName = stage.ID

	// Set next stage queue if not last stage
	if stageIndex+1 < len(w.Stages) {
		envelope.NextStageQueue = w.engine.getStageQueueName(w.ID, stageIndex+1)
	}

	// Get the queue name for this stage
	queueName := w.engine.getStageQueueName(w.ID, stageIndex)

	w.engine.logger.Info("publishing message to stage queue",
		"workflowId", w.ID,
		"instanceId", state.InstanceID,
		"stageId", stage.ID,
		"stageIndex", stageIndex,
		"queue", queueName)

	// Publish to specific stage queue
	return w.engine.publisher.Publish(ctx, envelope,
		messaging.WithExchange(""), // Direct queue delivery
		messaging.WithRoutingKey(queueName),
		messaging.WithPersistent(true), // Ensure message persistence
	)
}

// processStageMessage processes a single stage from a queue message
func (w *Workflow) processStageMessage(ctx context.Context, envelope *FlowMessageEnvelope) error {
	// Deserialize workflow state from envelope
	var state WorkflowState
	err := json.Unmarshal([]byte(envelope.SerializedWorkflowState), &state)
	if err != nil {
		return fmt.Errorf("failed to deserialize workflow state: %w", err)
	}

	// Find the stage to execute
	if envelope.CurrentStageIndex >= len(w.Stages) {
		return fmt.Errorf("stage index %d out of bounds", envelope.CurrentStageIndex)
	}

	stage := w.Stages[envelope.CurrentStageIndex]
	if stage.ID != envelope.StageName {
		return fmt.Errorf("stage ID mismatch: expected %s, got %s", stage.ID, envelope.StageName)
	}

	w.engine.logger.Info("processing stage from queue",
		"workflowId", envelope.WorkflowID,
		"instanceId", envelope.InstanceID,
		"stageId", stage.ID,
		"stageIndex", envelope.CurrentStageIndex)

	startTime := time.Now()
	state.CurrentStage = stage.ID
	state.LastModified = time.Now()

	// Check dependencies
	if !w.dependenciesSatisfied(stage, &state) {
		if stage.Required {
			return fmt.Errorf("dependencies not satisfied for required stage: %s", stage.ID)
		}
		// Skip optional stage
		result := &StageResult{
			StageID:   stage.ID,
			Status:    StageSkipped,
			StartTime: startTime,
			Duration:  0,
		}
		w.addStageResult(&state, result)
		return w.proceedToNextStage(ctx, envelope, &state)
	}

	// Execute stage with timeout and retry
	var result *StageResult
	executeFunc := func() error {
		stageCtx, cancel := context.WithTimeout(ctx, stage.Timeout)
		defer cancel()
		
		var execErr error
		result, execErr = stage.Handler.Execute(stageCtx, &state)
		return execErr
	}

	var execErr error
	if stage.RetryPolicy != nil {
		execErr = reliability.Retry(ctx, stage.RetryPolicy, executeFunc)
	} else {
		execErr = executeFunc()
	}

	duration := time.Since(startTime)

	if execErr != nil {
		w.engine.logger.Error("stage execution failed",
			"stageId", stage.ID,
			"workflowId", envelope.WorkflowID,
			"instanceId", envelope.InstanceID,
			"duration", duration,
			"error", execErr.Error())

		if stage.Required {
			// Mark workflow as failed and start compensation
			state.Status = WorkflowFailed
			state.ErrorMessage = execErr.Error()
			endTime := time.Now()
			state.EndTime = &endTime
			state.LastModified = time.Now()
			state.Version++

			// Publish workflow failed event if enabled
			if w.engine.enableCompletionEvents {
				event := NewWorkflowFailedEvent(w, &state, execErr, envelope.CurrentStageIndex)
				options := append([]messaging.PublishOption{messaging.WithPersistent(true)}, w.engine.completionEventOptions...)
				if err := w.engine.publisher.PublishEvent(ctx, event, options...); err != nil {
					w.engine.logger.Error("failed to publish workflow failed event",
						"error", err,
						"workflowId", envelope.WorkflowID,
						"instanceId", envelope.InstanceID)
				}
			}

			// TODO: Implement compensation via queue messages
			return fmt.Errorf("required stage %s failed: %w", stage.ID, execErr)
		}

		// Mark as failed but continue
		result = &StageResult{
			StageID:   stage.ID,
			Status:    StageFailed,
			Error:     execErr.Error(),
			StartTime: startTime,
			Duration:  duration,
		}
	} else {
		// Ensure result is properly populated
		if result == nil {
			result = &StageResult{
				StageID:  stage.ID,
				Status:   StageCompleted,
				Data:     make(map[string]interface{}),
				Duration: duration,
			}
		}

		result.StageID = stage.ID
		result.StartTime = startTime
		result.Duration = duration
		endTime := startTime.Add(duration)
		result.EndTime = &endTime

		if result.Status == "" {
			result.Status = StageCompleted
		}

		w.engine.logger.Info("stage execution completed",
			"stageId", stage.ID,
			"workflowId", envelope.WorkflowID,
			"instanceId", envelope.InstanceID,
			"duration", duration,
			"status", result.Status)
	}

	// Add result to state
	w.addStageResult(&state, result)
	state.CurrentStage = ""

	// Publish stage completion event if enabled
	if w.engine.enableCompletionEvents && result.Status == StageCompleted {
		event := NewStageCompletedEvent(w, &state, stage, envelope.CurrentStageIndex, result)
		options := append([]messaging.PublishOption{messaging.WithPersistent(true)}, w.engine.completionEventOptions...)
		if err := w.engine.publisher.PublishEvent(ctx, event, options...); err != nil {
			w.engine.logger.Error("failed to publish stage completion event",
				"error", err,
				"stageId", stage.ID,
				"workflowId", envelope.WorkflowID,
				"instanceId", envelope.InstanceID)
			// Don't fail the stage if event publishing fails
		}
	}

	// Proceed to next stage or complete workflow
	return w.proceedToNextStage(ctx, envelope, &state)
}

// dependenciesSatisfied checks if all dependencies for a stage are satisfied
func (w *Workflow) dependenciesSatisfied(stage *Stage, state *WorkflowState) bool {
	if len(stage.Dependencies) == 0 {
		return true
	}

	for _, dep := range stage.Dependencies {
		found := false
		for _, result := range state.StageResults {
			if result.StageID == dep && result.Status == StageCompleted {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

// addStageResult adds a stage result to the workflow state
func (w *Workflow) addStageResult(state *WorkflowState, result *StageResult) {
	state.StageResults = append(state.StageResults, *result)
	state.LastModified = time.Now()
	state.Version++

	// Merge stage data into global data if present
	if result.Data != nil {
		for key, value := range result.Data {
			state.GlobalData[fmt.Sprintf("%s.%s", result.StageID, key)] = value
		}
	}
}

// compensateStages runs compensation logic for all completed stages in reverse order
func (w *Workflow) compensateStages(ctx context.Context, state *WorkflowState) {
	w.engine.logger.Info("starting compensation", "workflowId", state.WorkflowID, "instanceId", state.InstanceID)

	// Process in reverse order
	for i := len(state.StageResults) - 1; i >= 0; i-- {
		result := &state.StageResults[i]
		if result.Status != StageCompleted {
			continue
		}

		stage, exists := w.stageMap[result.StageID]
		if !exists || stage.Compensation == nil {
			continue
		}

		w.engine.logger.Info("compensating stage", "stageId", result.StageID)

		err := stage.Compensation.Compensate(ctx, state, result)
		if err != nil {
			w.engine.logger.Error("compensation failed",
				"stageId", result.StageID,
				"error", err.Error())
		} else {
			result.Status = StageCompensated
			w.engine.logger.Info("stage compensated", "stageId", result.StageID)
		}
	}

	state.Status = WorkflowCompensated
	state.LastModified = time.Now()
	state.Version++
}

// InMemoryStateStore provides an in-memory implementation of StateStore
type InMemoryStateStore struct {
	states map[string]*WorkflowState
	mu     sync.RWMutex
}

// NewInMemoryStateStore creates a new in-memory state store
func NewInMemoryStateStore() *InMemoryStateStore {
	return &InMemoryStateStore{
		states: make(map[string]*WorkflowState),
	}
}

// SaveState saves workflow state to memory
func (s *InMemoryStateStore) SaveState(ctx context.Context, state *WorkflowState) error {
	if state == nil {
		return fmt.Errorf("state cannot be nil")
	}

	// Deep copy state to avoid mutations
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	var stateCopy WorkflowState
	err = json.Unmarshal(data, &stateCopy)
	if err != nil {
		return fmt.Errorf("failed to unmarshal state: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.states[state.InstanceID] = &stateCopy
	return nil
}

// LoadState loads workflow state from memory
func (s *InMemoryStateStore) LoadState(ctx context.Context, instanceID string) (*WorkflowState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	state, exists := s.states[instanceID]
	if !exists {
		return nil, fmt.Errorf("workflow state not found: %s", instanceID)
	}

	// Deep copy state to avoid mutations
	data, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal state: %w", err)
	}

	var stateCopy WorkflowState
	err = json.Unmarshal(data, &stateCopy)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal state: %w", err)
	}

	return &stateCopy, nil
}

// DeleteState removes workflow state from memory
func (s *InMemoryStateStore) DeleteState(ctx context.Context, instanceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.states, instanceID)
	return nil
}

// ListActiveWorkflows returns list of active workflow instance IDs
func (s *InMemoryStateStore) ListActiveWorkflows(ctx context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var activeWorkflows []string
	for instanceID, state := range s.states {
		if state.Status == WorkflowRunning || state.Status == WorkflowPending {
			activeWorkflows = append(activeWorkflows, instanceID)
		}
	}

	return activeWorkflows, nil
}
