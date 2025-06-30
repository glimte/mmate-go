package stageflow

import (
	"context"
	"encoding/json"
	"fmt"
)

// TypedWorkflowContext is the interface that typed workflow contexts must implement
type TypedWorkflowContext interface {
	Validate() error
}

// TypedStageHandler is a stage handler that works with typed contexts
type TypedStageHandler[T TypedWorkflowContext] interface {
	Execute(ctx context.Context, data T) error
	GetStageID() string
}

// TypedStageAdapter adapts a typed stage handler to work with the untyped StageHandler interface
type TypedStageAdapter[T TypedWorkflowContext] struct {
	typed        TypedStageHandler[T]
	deserializer func(map[string]interface{}) (T, error)
	serializer   func(T) (map[string]interface{}, error)
}

// Execute implements StageHandler interface
func (a *TypedStageAdapter[T]) Execute(ctx context.Context, state *WorkflowState) (*StageResult, error) {
	// Deserialize the untyped data into typed context
	typedData, err := a.deserializer(state.GlobalData)
	if err != nil {
		return &StageResult{
			StageID: a.GetStageID(),
			Status:  StageFailed,
			Error:   fmt.Sprintf("Failed to deserialize data: %v", err),
		}, fmt.Errorf("deserialization failed: %w", err)
	}

	// Execute the typed handler
	err = a.typed.Execute(ctx, typedData)
	if err != nil {
		return &StageResult{
			StageID: a.GetStageID(),
			Status:  StageFailed,
			Error:   err.Error(),
		}, err
	}

	// Serialize the updated typed data back to untyped
	updatedData, err := a.serializer(typedData)
	if err != nil {
		return &StageResult{
			StageID: a.GetStageID(),
			Status:  StageFailed,
			Error:   fmt.Sprintf("Failed to serialize data: %v", err),
		}, fmt.Errorf("serialization failed: %w", err)
	}

	// Update the global data with the serialized result
	for k, v := range updatedData {
		state.GlobalData[k] = v
	}

	return &StageResult{
		StageID: a.GetStageID(),
		Status:  StageCompleted,
	}, nil
}

// GetStageID implements StageHandler interface
func (a *TypedStageAdapter[T]) GetStageID() string {
	return a.typed.GetStageID()
}

// JSONSerializer serializes typed data to map using JSON as intermediate format
func JSONSerializer[T any](data T) (map[string]interface{}, error) {
	bytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("json marshal failed: %w", err)
	}

	var result map[string]interface{}
	err = json.Unmarshal(bytes, &result)
	if err != nil {
		return nil, fmt.Errorf("json unmarshal failed: %w", err)
	}

	return result, nil
}

// JSONDeserializer deserializes map to typed data using JSON as intermediate format
func JSONDeserializer[T any](data map[string]interface{}) (T, error) {
	var result T
	
	bytes, err := json.Marshal(data)
	if err != nil {
		return result, fmt.Errorf("json marshal failed: %w", err)
	}

	err = json.Unmarshal(bytes, &result)
	if err != nil {
		return result, fmt.Errorf("json unmarshal failed: %w", err)
	}

	return result, nil
}

// TypedWorkflowBuilder helps build workflows with typed contexts
type TypedWorkflowBuilder[T TypedWorkflowContext] struct {
	workflow     *Workflow
	serializer   func(T) (map[string]interface{}, error)
	deserializer func(map[string]interface{}) (T, error)
}

// NewTypedWorkflow creates a new typed workflow builder
func NewTypedWorkflow[T TypedWorkflowContext](id, name string) *TypedWorkflowBuilder[T] {
	return &TypedWorkflowBuilder[T]{
		workflow:     NewWorkflow(id, name),
		serializer:   JSONSerializer[T],
		deserializer: JSONDeserializer[T],
	}
}

// AddTypedStage adds a typed stage to the workflow
func (b *TypedWorkflowBuilder[T]) AddTypedStage(id string, handler TypedStageHandler[T]) *TypedWorkflowBuilder[T] {
	adapter := &TypedStageAdapter[T]{
		typed:        handler,
		serializer:   b.serializer,
		deserializer: b.deserializer,
	}
	b.workflow.AddStage(id, adapter)
	return b
}

// WithTimeout sets the workflow timeout
func (b *TypedWorkflowBuilder[T]) WithTimeout(timeout int) *TypedWorkflowBuilder[T] {
	// Note: The existing Workflow doesn't expose timeout setting, 
	// but we can add it to maintain API consistency
	return b
}

// Build returns the configured workflow
func (b *TypedWorkflowBuilder[T]) Build() *Workflow {
	return b.workflow
}

// ExecuteTyped is a helper to execute a workflow with typed initial data
func ExecuteTyped[T TypedWorkflowContext](workflow *Workflow, ctx context.Context, initialData T) (*WorkflowState, error) {
	// Serialize typed data to map
	dataMap, err := JSONSerializer(initialData)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize initial data: %w", err)
	}

	// Execute with the serialized data
	return workflow.Execute(ctx, dataMap)
}