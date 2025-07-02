package stageflow

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/messaging"
)

// CompensationMessageEnvelope wraps compensation data for queue-based compensation processing
type CompensationMessageEnvelope struct {
	contracts.BaseMessage
	
	// Workflow identification
	WorkflowID string `json:"workflowId"`
	InstanceID string `json:"instanceId"`
	
	// Compensation details
	FailedStageIndex    int    `json:"failedStageIndex"`
	CompensationIndex   int    `json:"compensationIndex"`   // Which stage to compensate (counts down from failed stage)
	StageToCompensate   string `json:"stageToCompensate"`   // Stage ID being compensated
	TotalCompensations  int    `json:"totalCompensations"`  // Total number of stages needing compensation
	
	// Serialized workflow state at time of failure
	SerializedWorkflowState string `json:"serializedWorkflowState"`
	WorkflowStateType       string `json:"workflowStateType"`
	
	// Original stage result being compensated
	SerializedOriginalResult string `json:"serializedOriginalResult"`
	OriginalResultType       string `json:"originalResultType"`
	
	// Failure information
	OriginalError     string    `json:"originalError"`
	FailureTimestamp  time.Time `json:"failureTimestamp"`
	
	// Compensation tracking
	CompensationResults []CompensationResult `json:"compensationResults"`
	NextCompensationQueue string `json:"nextCompensationQueue,omitempty"`
}

// CompensationResult tracks the result of a compensation operation
type CompensationResult struct {
	StageID           string                 `json:"stageId"`
	CompensationIndex int                    `json:"compensationIndex"`
	Status            CompensationStatus     `json:"status"`
	StartTime         time.Time              `json:"startTime"`
	EndTime           *time.Time             `json:"endTime,omitempty"`
	Duration          time.Duration          `json:"duration"`
	Error             string                 `json:"error,omitempty"`
	Data              map[string]interface{} `json:"data,omitempty"`
}

// CompensationStatus represents the status of compensation execution
type CompensationStatus string

const (
	CompensationPending   CompensationStatus = "pending"
	CompensationRunning   CompensationStatus = "running"
	CompensationCompleted CompensationStatus = "completed"
	CompensationFailed    CompensationStatus = "failed"
	CompensationSkipped   CompensationStatus = "skipped"
)

// NewCompensationMessageEnvelope creates a new compensation message envelope
func NewCompensationMessageEnvelope(workflowID, instanceID string, failedStageIndex int, state *WorkflowState, originalError string) (*CompensationMessageEnvelope, error) {
	// Serialize workflow state
	stateData, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	
	// Count completed stages that need compensation (everything before the failed stage)
	completedStages := 0
	for i := 0; i < failedStageIndex && i < len(state.StageResults); i++ {
		if state.StageResults[i].Status == StageCompleted {
			completedStages++
		}
	}
	
	envelope := &CompensationMessageEnvelope{
		BaseMessage:             contracts.NewBaseMessage("CompensationMessageEnvelope"),
		WorkflowID:              workflowID,
		InstanceID:              instanceID,
		FailedStageIndex:        failedStageIndex,
		CompensationIndex:       completedStages - 1, // Start from last completed stage
		TotalCompensations:      completedStages,
		SerializedWorkflowState: string(stateData),
		WorkflowStateType:       "WorkflowState",
		OriginalError:           originalError,
		FailureTimestamp:        time.Now(),
		CompensationResults:     make([]CompensationResult, 0),
	}
	
	// Set stage to compensate if there are any to compensate
	if completedStages > 0 {
		// Find the stage at the compensation index (reverse order)
		compensationStageIndex := failedStageIndex - 1 - (completedStages - 1 - envelope.CompensationIndex)
		if compensationStageIndex >= 0 && compensationStageIndex < len(state.StageResults) {
			envelope.StageToCompensate = state.StageResults[compensationStageIndex].StageID
			
			// Serialize the original result being compensated
			originalResult := state.StageResults[compensationStageIndex]
			resultData, err := json.Marshal(originalResult)
			if err != nil {
				return nil, err
			}
			envelope.SerializedOriginalResult = string(resultData)
			envelope.OriginalResultType = "StageResult"
		}
	}
	
	return envelope, nil
}

// GetCompensationQueueName returns the queue name for compensation processing
func (e *StageFlowEngine) getCompensationQueueName(workflowID string) string {
	return fmt.Sprintf("%scompensation.%s", e.stageQueuePrefix, workflowID)
}

// createCompensationQueue creates the compensation queue for a workflow
func (e *StageFlowEngine) createCompensationQueue(ctx context.Context, workflowID string) error {
	queueName := e.getCompensationQueueName(workflowID)
	
	// Create queue with DLQ settings for failed compensations
	queueOptions := messaging.QueueOptions{
		Durable:     true,
		AutoDelete:  false,
		Args: map[string]interface{}{
			"x-message-ttl": int32(7200000), // 2 hour TTL for compensation messages
			"x-dead-letter-exchange": "",
			"x-dead-letter-routing-key": fmt.Sprintf("dlq.%s", queueName),
		},
	}
	
	if err := e.transport.CreateQueue(ctx, queueName, queueOptions); err != nil {
		return fmt.Errorf("failed to create compensation queue %s: %w", queueName, err)
	}
	
	e.logger.Info("created compensation queue", "queue", queueName, "workflow", workflowID)
	return nil
}

// subscribeToCompensationQueue subscribes to the compensation queue for a workflow
func (e *StageFlowEngine) subscribeToCompensationQueue(ctx context.Context, workflow *Workflow) error {
	queueName := e.getCompensationQueueName(workflow.ID)
	
	// Create compensation handler
	handler := func(ctx context.Context, envelope *CompensationMessageEnvelope) error {
		return workflow.processCompensationMessage(ctx, envelope)
	}
	
	// Subscribe to compensation queue
	messageHandler := messaging.MessageHandlerFunc(func(ctx context.Context, msg contracts.Message) error {
		envelope, ok := msg.(*CompensationMessageEnvelope)
		if !ok {
			return fmt.Errorf("expected CompensationMessageEnvelope, got %T", msg)
		}
		return handler(ctx, envelope)
	})
	
	// Subscribe with prefetch for compensation processing
	if err := e.subscriber.Subscribe(ctx, queueName, "CompensationMessageEnvelope", 
		messageHandler, 
		messaging.WithPrefetchCount(e.maxConcurrency)); err != nil {
		return fmt.Errorf("failed to subscribe to compensation queue %s: %w", queueName, err)
	}
	
	e.logger.Info("subscribed to compensation queue", "queue", queueName, "workflow", workflow.ID)
	return nil
}