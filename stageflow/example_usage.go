package stageflow

import (
	"context"
	"log/slog"

	"github.com/glimte/mmate-go/messaging"
)

// Example showing how to use workflow completion events
func ExampleWorkflowCompletionEvents() {
	var publisher messaging.Publisher
	var subscriber messaging.Subscriber
	var transport messaging.Transport

	// Create engine with completion events enabled
	engine := NewStageFlowEngine(
		publisher, 
		subscriber, 
		transport,
		WithCompletionEvents(true, 
			messaging.WithExchange("workflow.events"),    // Publish to events exchange
			messaging.WithRoutingKey("workflow.completed"), // Custom routing key
		),
		WithStageFlowLogger(slog.Default()),
	)

	// Create a workflow
	workflow := NewWorkflow("order-processing", "Order Processing Workflow").
		AddStage("validate", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			// Validate order logic
			return &StageResult{
				StageID: "validate",
				Status:  StageCompleted,
				Data:    map[string]interface{}{"validated": true},
			}, nil
		})).
		AddStage("payment", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			// Process payment logic
			return &StageResult{
				StageID: "payment",
				Status:  StageCompleted,
				Data:    map[string]interface{}{"paymentId": "pay_123"},
			}, nil
		})).
		AddStage("fulfill", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			// Fulfillment logic
			return &StageResult{
				StageID: "fulfill",
				Status:  StageCompleted,
				Data:    map[string]interface{}{"trackingNumber": "TRK123"},
			}, nil
		}))

	// Register workflow
	err := engine.RegisterWorkflow(workflow)
	if err != nil {
		panic(err)
	}

	// When this workflow completes, the following events will be published:
	// 1. StageCompletedEvent for each stage completion (validate, payment, fulfill)
	// 2. WorkflowCompletedEvent when the entire workflow finishes
	//
	// These events can be consumed by external systems for:
	// - Audit logging
	// - Analytics and monitoring
	// - Triggering downstream processes
	// - Notifications
	// - Compensation workflows

	// Execute workflow
	state, err := workflow.Execute(context.Background(), map[string]interface{}{
		"orderId": "order_456",
		"amount":  99.99,
	})
	if err != nil {
		panic(err)
	}

	_ = state // Workflow will process asynchronously via queues
}

// Example showing how to disable events (default behavior)
func ExampleWorkflowWithoutEvents() {
	var publisher messaging.Publisher
	var subscriber messaging.Subscriber
	var transport messaging.Transport

	// Create engine WITHOUT completion events (default)
	engine := NewStageFlowEngine(publisher, subscriber, transport)

	// Or explicitly disable them
	engine2 := NewStageFlowEngine(
		publisher, 
		subscriber, 
		transport,
		WithCompletionEvents(false), // Explicitly disabled
	)

	_, _ = engine, engine2 // No events will be published
}