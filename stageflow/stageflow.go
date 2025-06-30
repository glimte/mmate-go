package stageflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

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
	publisher  messaging.Publisher
	subscriber messaging.Subscriber
	stateStore StateStore
	workflows  map[string]*Workflow
	mu         sync.RWMutex
	logger     *slog.Logger
}

// EngineOption configures the stage flow engine
type EngineOption func(*EngineConfig)

// EngineConfig holds configuration for the engine
type EngineConfig struct {
	StateStore StateStore
	Logger     *slog.Logger
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

// NewStageFlowEngine creates a new stage flow engine
func NewStageFlowEngine(publisher messaging.Publisher, subscriber messaging.Subscriber, opts ...EngineOption) *StageFlowEngine {
	config := &EngineConfig{
		StateStore: NewInMemoryStateStore(),
		Logger:     slog.Default(),
	}

	for _, opt := range opts {
		opt(config)
	}

	return &StageFlowEngine{
		publisher:  publisher,
		subscriber: subscriber,
		stateStore: config.StateStore,
		workflows:  make(map[string]*Workflow),
		logger:     config.Logger,
	}
}

// RegisterWorkflow registers a workflow with the engine
func (e *StageFlowEngine) RegisterWorkflow(workflow *Workflow) error {
	if workflow == nil {
		return fmt.Errorf("workflow cannot be nil")
	}
	if workflow.ID == "" {
		return fmt.Errorf("workflow ID cannot be empty")
	}

	workflow.engine = e

	e.mu.Lock()
	defer e.mu.Unlock()

	e.workflows[workflow.ID] = workflow
	e.logger.Info("registered workflow", "workflowId", workflow.ID, "name", workflow.Name)

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

// Execute starts the workflow execution
func (w *Workflow) Execute(ctx context.Context, initialData map[string]interface{}) (*WorkflowState, error) {
	if w.engine == nil {
		return nil, fmt.Errorf("workflow not registered with engine")
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

	// Save initial state
	err := w.engine.stateStore.SaveState(ctx, state)
	if err != nil {
		return nil, fmt.Errorf("failed to save initial state: %w", err)
	}

	w.engine.logger.Info("starting workflow execution",
		"workflowId", w.ID,
		"instanceId", instanceID,
		"stageCount", len(w.Stages))

	// Execute stages
	err = w.executeStages(ctx, state)
	if err != nil {
		state.Status = WorkflowFailed
		state.ErrorMessage = err.Error()
		endTime := time.Now()
		state.EndTime = &endTime
		state.LastModified = time.Now()
		state.Version++

		// Save failed state
		w.engine.stateStore.SaveState(ctx, state)

		w.engine.logger.Error("workflow execution failed",
			"workflowId", w.ID,
			"instanceId", instanceID,
			"error", err.Error())

		return state, err
	}

	// Mark as completed
	state.Status = WorkflowCompleted
	endTime := time.Now()
	state.EndTime = &endTime
	state.LastModified = time.Now()
	state.Version++

	err = w.engine.stateStore.SaveState(ctx, state)
	if err != nil {
		w.engine.logger.Warn("failed to save final state", "error", err.Error())
	}

	w.engine.logger.Info("workflow execution completed",
		"workflowId", w.ID,
		"instanceId", instanceID,
		"duration", endTime.Sub(state.StartTime))

	return state, nil
}

// executeStages executes all stages in the workflow
func (w *Workflow) executeStages(ctx context.Context, state *WorkflowState) error {
	for _, stage := range w.Stages {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Check dependencies
		if !w.dependenciesSatisfied(stage, state) {
			if stage.Required {
				return fmt.Errorf("dependencies not satisfied for required stage: %s", stage.ID)
			}
			// Skip optional stage
			w.addStageResult(state, &StageResult{
				StageID:   stage.ID,
				Status:    StageSkipped,
				StartTime: time.Now(),
				Duration:  0,
			})
			continue
		}

		// Execute stage
		state.CurrentStage = stage.ID
		state.LastModified = time.Now()

		result, err := w.executeStage(ctx, stage, state)
		if err != nil {
			if stage.Required {
				// Start compensation
				w.compensateStages(ctx, state)
				return fmt.Errorf("required stage %s failed: %w", stage.ID, err)
			}
			// Mark as failed but continue
			result = &StageResult{
				StageID:   stage.ID,
				Status:    StageFailed,
				Error:     err.Error(),
				StartTime: time.Now(),
				Duration:  0,
			}
		}

		w.addStageResult(state, result)

		// Save state after each stage
		err = w.engine.stateStore.SaveState(ctx, state)
		if err != nil {
			w.engine.logger.Warn("failed to save state after stage", "stageId", stage.ID, "error", err.Error())
		}
	}

	state.CurrentStage = ""
	return nil
}

// executeStage executes a single stage
func (w *Workflow) executeStage(ctx context.Context, stage *Stage, state *WorkflowState) (*StageResult, error) {
	w.engine.logger.Info("executing stage", "stageId", stage.ID, "workflowId", state.WorkflowID, "instanceId", state.InstanceID)

	startTime := time.Now()

	var result *StageResult
	var err error

	// Execute with retry if policy is configured
	executeFunc := func() error {
		// Create a new timeout context for each retry attempt
		stageCtx, cancel := context.WithTimeout(ctx, stage.Timeout)
		defer cancel()
		
		var execErr error
		result, execErr = stage.Handler.Execute(stageCtx, state)
		return execErr
	}

	if stage.RetryPolicy != nil {
		err = reliability.Retry(ctx, stage.RetryPolicy, executeFunc)
	} else {
		err = executeFunc()
	}

	duration := time.Since(startTime)

	if err != nil {
		w.engine.logger.Error("stage execution failed",
			"stageId", stage.ID,
			"workflowId", state.WorkflowID,
			"instanceId", state.InstanceID,
			"duration", duration,
			"error", err.Error())

		return &StageResult{
			StageID:   stage.ID,
			Status:    StageFailed,
			Error:     err.Error(),
			StartTime: startTime,
			Duration:  duration,
		}, err
	}

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
		"workflowId", state.WorkflowID,
		"instanceId", state.InstanceID,
		"duration", duration,
		"status", result.Status)

	return result, nil
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
