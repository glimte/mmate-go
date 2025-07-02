package stageflow

import (
	"time"

	"github.com/glimte/mmate-go/contracts"
)

// WorkflowCompletedEvent is published when a workflow completes successfully
type WorkflowCompletedEvent struct {
	contracts.BaseEvent
	
	// Workflow identification
	WorkflowID   string `json:"workflowId"`
	WorkflowName string `json:"workflowName"`
	InstanceID   string `json:"instanceId"`
	
	// Completion details
	Status       WorkflowStatus `json:"status"`
	StartTime    time.Time      `json:"startTime"`
	EndTime      time.Time      `json:"endTime"`
	Duration     time.Duration  `json:"duration"`
	
	// Stage results summary
	TotalStages      int              `json:"totalStages"`
	CompletedStages  int              `json:"completedStages"`
	FailedStages     int              `json:"failedStages"`
	SkippedStages    int              `json:"skippedStages"`
	StageResults     []StageResult    `json:"stageResults"`
	
	// Final workflow data
	FinalData        map[string]interface{} `json:"finalData"`
	
	// Error information (if failed)
	Error            string `json:"error,omitempty"`
	LastFailedStage  string `json:"lastFailedStage,omitempty"`
}

// WorkflowFailedEvent is published when a workflow fails
type WorkflowFailedEvent struct {
	contracts.BaseEvent
	
	// Workflow identification
	WorkflowID   string `json:"workflowId"`
	WorkflowName string `json:"workflowName"`
	InstanceID   string `json:"instanceId"`
	
	// Failure details
	FailedStageID    string         `json:"failedStageId"`
	FailedStageIndex int            `json:"failedStageIndex"`
	Error            string         `json:"error"`
	StartTime        time.Time      `json:"startTime"`
	FailureTime      time.Time      `json:"failureTime"`
	Duration         time.Duration  `json:"duration"`
	
	// Progress before failure
	CompletedStages  int            `json:"completedStages"`
	TotalStages      int            `json:"totalStages"`
	StageResults     []StageResult  `json:"stageResults"`
	
	// State at failure
	StateData        map[string]interface{} `json:"stateData"`
	
	// Compensation status
	CompensationExecuted bool      `json:"compensationExecuted"`
	CompensationResults  []string  `json:"compensationResults,omitempty"`
}

// StageCompletedEvent is published when an individual stage completes
type StageCompletedEvent struct {
	contracts.BaseEvent
	
	// Workflow identification
	WorkflowID   string `json:"workflowId"`
	InstanceID   string `json:"instanceId"`
	
	// Stage details
	StageID      string         `json:"stageId"`
	StageIndex   int            `json:"stageIndex"`
	StageName    string         `json:"stageName"`
	Status       StageStatus    `json:"status"`
	
	// Timing
	StartTime    time.Time      `json:"startTime"`
	EndTime      time.Time      `json:"endTime"`
	Duration     time.Duration  `json:"duration"`
	
	// Results
	Data         map[string]interface{} `json:"data,omitempty"`
	Error        string                 `json:"error,omitempty"`
	
	// Progress
	CurrentStage int `json:"currentStage"`
	TotalStages  int `json:"totalStages"`
}

// NewWorkflowCompletedEvent creates a new workflow completed event
func NewWorkflowCompletedEvent(workflow *Workflow, state *WorkflowState) *WorkflowCompletedEvent {
	event := &WorkflowCompletedEvent{
		BaseEvent: contracts.BaseEvent{
			BaseMessage: contracts.NewBaseMessage("WorkflowCompletedEvent"),
			AggregateID: state.InstanceID,
			Sequence:    int64(state.Version),
			Source:      "stageflow",
		},
		WorkflowID:   workflow.ID,
		WorkflowName: workflow.Name,
		InstanceID:   state.InstanceID,
		Status:       state.Status,
		StartTime:    state.StartTime,
		EndTime:      *state.EndTime,
		Duration:     state.EndTime.Sub(state.StartTime),
		TotalStages:  len(workflow.Stages),
		StageResults: state.StageResults,
		FinalData:    state.GlobalData,
	}
	
	// Count stage statuses
	for _, result := range state.StageResults {
		switch result.Status {
		case StageCompleted:
			event.CompletedStages++
		case StageFailed:
			event.FailedStages++
		case StageSkipped:
			event.SkippedStages++
		}
	}
	
	// Add error info if workflow failed
	if state.Status == WorkflowFailed && state.ErrorMessage != "" {
		event.Error = state.ErrorMessage
		// Find last failed stage
		for i := len(state.StageResults) - 1; i >= 0; i-- {
			if state.StageResults[i].Status == StageFailed {
				event.LastFailedStage = state.StageResults[i].StageID
				break
			}
		}
	}
	
	return event
}

// NewWorkflowFailedEvent creates a new workflow failed event
func NewWorkflowFailedEvent(workflow *Workflow, state *WorkflowState, stageErr error, stageIndex int) *WorkflowFailedEvent {
	event := &WorkflowFailedEvent{
		BaseEvent: contracts.BaseEvent{
			BaseMessage: contracts.NewBaseMessage("WorkflowFailedEvent"),
			AggregateID: state.InstanceID,
			Sequence:    int64(state.Version),
			Source:      "stageflow",
		},
		WorkflowID:       workflow.ID,
		WorkflowName:     workflow.Name,
		InstanceID:       state.InstanceID,
		Error:            stageErr.Error(),
		StartTime:        state.StartTime,
		FailureTime:      time.Now(),
		Duration:         time.Now().Sub(state.StartTime),
		CompletedStages:  len(state.StageResults),
		TotalStages:      len(workflow.Stages),
		StageResults:     state.StageResults,
		StateData:        state.GlobalData,
	}
	
	// Set failed stage info
	if stageIndex < len(workflow.Stages) {
		event.FailedStageID = workflow.Stages[stageIndex].ID
		event.FailedStageIndex = stageIndex
	}
	
	return event
}

// NewStageCompletedEvent creates a new stage completed event
func NewStageCompletedEvent(workflow *Workflow, state *WorkflowState, stage *Stage, stageIndex int, result *StageResult) *StageCompletedEvent {
	event := &StageCompletedEvent{
		BaseEvent: contracts.BaseEvent{
			BaseMessage: contracts.NewBaseMessage("StageCompletedEvent"),
			AggregateID: state.InstanceID,
			Sequence:    int64(state.Version),
			Source:      "stageflow",
		},
		WorkflowID:   workflow.ID,
		InstanceID:   state.InstanceID,
		StageID:      stage.ID,
		StageIndex:   stageIndex,
		StageName:    stage.ID,
		Status:       result.Status,
		StartTime:    result.StartTime,
		Duration:     result.Duration,
		Data:         result.Data,
		CurrentStage: stageIndex + 1,
		TotalStages:  len(workflow.Stages),
	}
	
	if result.EndTime != nil {
		event.EndTime = *result.EndTime
	} else {
		event.EndTime = result.StartTime.Add(result.Duration)
	}
	
	if result.Error != "" {
		event.Error = result.Error
	}
	
	return event
}

// WorkflowCompensatedEvent is published when a workflow completes compensation
type WorkflowCompensatedEvent struct {
	contracts.BaseEvent
	
	// Workflow identification
	WorkflowID   string `json:"workflowId"`
	WorkflowName string `json:"workflowName"`
	InstanceID   string `json:"instanceId"`
	
	// Compensation details
	OriginalError        string    `json:"originalError"`
	FailureTime          time.Time `json:"failureTime"`
	CompensationStartTime time.Time `json:"compensationStartTime"`
	CompensationEndTime   time.Time `json:"compensationEndTime"`
	CompensationDuration  time.Duration `json:"compensationDuration"`
	
	// Compensation results
	TotalCompensations      int                  `json:"totalCompensations"`
	SuccessfulCompensations int                  `json:"successfulCompensations"`
	FailedCompensations     int                  `json:"failedCompensations"`
	SkippedCompensations    int                  `json:"skippedCompensations"`
	CompensationResults     []CompensationResult `json:"compensationResults"`
	
	// Original workflow data
	FailedStageIndex int                    `json:"failedStageIndex"`
	WorkflowData     map[string]interface{} `json:"workflowData"`
}

// NewWorkflowCompensatedEvent creates a new workflow compensated event
func NewWorkflowCompensatedEvent(workflow *Workflow, state *WorkflowState, envelope *CompensationMessageEnvelope) *WorkflowCompensatedEvent {
	event := &WorkflowCompensatedEvent{
		BaseEvent: contracts.BaseEvent{
			BaseMessage: contracts.NewBaseMessage("WorkflowCompensatedEvent"),
			AggregateID: state.InstanceID,
			Sequence:    int64(state.Version),
			Source:      "stageflow",
		},
		WorkflowID:              workflow.ID,
		WorkflowName:            workflow.Name,
		InstanceID:              state.InstanceID,
		OriginalError:           envelope.OriginalError,
		FailureTime:             envelope.FailureTimestamp,
		CompensationStartTime:   envelope.GetTimestamp(),
		CompensationEndTime:     time.Now(),
		TotalCompensations:      envelope.TotalCompensations,
		CompensationResults:     envelope.CompensationResults,
		FailedStageIndex:        envelope.FailedStageIndex,
		WorkflowData:            state.GlobalData,
	}
	
	// Calculate compensation duration
	if len(envelope.CompensationResults) > 0 {
		firstResult := envelope.CompensationResults[0]
		lastResult := envelope.CompensationResults[len(envelope.CompensationResults)-1]
		if lastResult.EndTime != nil {
			event.CompensationDuration = lastResult.EndTime.Sub(firstResult.StartTime)
		}
	}
	
	// Count compensation statuses
	for _, result := range envelope.CompensationResults {
		switch result.Status {
		case CompensationCompleted:
			event.SuccessfulCompensations++
		case CompensationFailed:
			event.FailedCompensations++
		case CompensationSkipped:
			event.SkippedCompensations++
		}
	}
	
	return event
}