package stageflow

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/glimte/mmate-go/contracts"
)

// FlowMessageEnvelope wraps a message with workflow state for queue-based stage processing
type FlowMessageEnvelope struct {
	contracts.BaseMessage
	
	// Message payload
	Payload json.RawMessage `json:"payload"`
	PayloadType string `json:"payloadType"`
	
	// Serialized workflow state
	SerializedWorkflowState string `json:"serializedWorkflowState"`
	WorkflowStateType string `json:"workflowStateType"`
	
	// Processing checkpoint
	Checkpoint ProcessingCheckpoint `json:"checkpoint"`
	
	// Stage routing information
	CurrentStageIndex int `json:"currentStageIndex"`
	NextStageQueue string `json:"nextStageQueue,omitempty"`
	
	// Workflow metadata
	WorkflowID string `json:"workflowId"`
	InstanceID string `json:"instanceId"`
	StageName string `json:"stageName"`
}

// ProcessingCheckpoint tracks fine-grained progress within a stage
type ProcessingCheckpoint struct {
	StageID string `json:"stageId"`
	StepName string `json:"stepName,omitempty"`
	StepCompleted bool `json:"stepCompleted"`
	StepResults map[string]interface{} `json:"stepResults,omitempty"`
	LastUpdate time.Time `json:"lastUpdate"`
	RetryCount int `json:"retryCount"`
}

// StageContext provides context for stage execution with queue-based state
type StageContext struct {
	State *WorkflowState
	Envelope *FlowMessageEnvelope
	StageIndex int
	checkpoints map[string]*ProcessingCheckpoint
}

// IsStepCompleted checks if a step within a stage has been completed
func (ctx *StageContext) IsStepCompleted(stepName string) bool {
	if checkpoint, exists := ctx.checkpoints[stepName]; exists {
		return checkpoint.StepCompleted
	}
	return false
}

// GetStepResult retrieves the result of a completed step
func (ctx *StageContext) GetStepResult(stepName string) (interface{}, bool) {
	if checkpoint, exists := ctx.checkpoints[stepName]; exists {
		if result, found := checkpoint.StepResults[stepName]; found {
			return result, true
		}
	}
	return nil, false
}

// SaveStepResult stores the result of a step for recovery
func (ctx *StageContext) SaveStepResult(stepName string, result interface{}) {
	if ctx.checkpoints == nil {
		ctx.checkpoints = make(map[string]*ProcessingCheckpoint)
	}
	
	checkpoint, exists := ctx.checkpoints[stepName]
	if !exists {
		checkpoint = &ProcessingCheckpoint{
			StageID: ctx.Envelope.StageName,
			StepName: stepName,
			StepResults: make(map[string]interface{}),
		}
		ctx.checkpoints[stepName] = checkpoint
	}
	
	checkpoint.StepResults[stepName] = result
	checkpoint.StepCompleted = true
	checkpoint.LastUpdate = time.Now()
	
	// Update envelope checkpoint
	ctx.Envelope.Checkpoint = *checkpoint
}

// SaveStageResult saves a result that will be available to subsequent stages
func (ctx *StageContext) SaveStageResult(key string, value interface{}) {
	if ctx.State.StageResults == nil {
		ctx.State.StageResults = make([]StageResult, 0)
	}
	
	// Find or create stage result
	var stageResult *StageResult
	for i := range ctx.State.StageResults {
		if ctx.State.StageResults[i].StageID == ctx.Envelope.StageName {
			stageResult = &ctx.State.StageResults[i]
			break
		}
	}
	
	if stageResult == nil {
		ctx.State.StageResults = append(ctx.State.StageResults, StageResult{
			StageID: ctx.Envelope.StageName,
			Status: StageRunning,
			Data: make(map[string]interface{}),
			StartTime: time.Now(),
		})
		stageResult = &ctx.State.StageResults[len(ctx.State.StageResults)-1]
	}
	
	if stageResult.Data == nil {
		stageResult.Data = make(map[string]interface{})
	}
	stageResult.Data[key] = value
}

// QueueBasedStateStore implements StateStore using message queues for persistence
type QueueBasedStateStore struct {
	// State is persisted within the FlowMessageEnvelope in the queue
	// This store acts as a pass-through since state travels with messages
}

// NewQueueBasedStateStore creates a new queue-based state store
func NewQueueBasedStateStore() *QueueBasedStateStore {
	return &QueueBasedStateStore{}
}

// SaveState is a no-op for queue-based storage as state travels with messages
func (s *QueueBasedStateStore) SaveState(ctx context.Context, state *WorkflowState) error {
	// State is saved by including it in the FlowMessageEnvelope
	// when publishing to the next stage queue
	return nil
}

// LoadState is not applicable for queue-based storage
func (s *QueueBasedStateStore) LoadState(ctx context.Context, instanceID string) (*WorkflowState, error) {
	// State is loaded from the FlowMessageEnvelope when a message is received
	return nil, fmt.Errorf("queue-based state store does not support direct state loading")
}

// DeleteState is a no-op for queue-based storage
func (s *QueueBasedStateStore) DeleteState(ctx context.Context, instanceID string) error {
	// State is automatically removed when the message is acknowledged
	return nil
}

// ListActiveWorkflows is not applicable for queue-based storage
func (s *QueueBasedStateStore) ListActiveWorkflows(ctx context.Context) ([]string, error) {
	// Active workflows are those with messages in stage queues
	return nil, fmt.Errorf("queue-based state store does not support listing active workflows")
}