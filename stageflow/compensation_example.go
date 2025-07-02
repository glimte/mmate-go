package stageflow

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/glimte/mmate-go/messaging"
)

// Example showing how queue-based compensation works in StageFlow
func ExampleQueueBasedCompensation() {
	var publisher messaging.Publisher
	var subscriber messaging.Subscriber
	var transport messaging.Transport

	// Create engine - compensation queues are created automatically
	engine := NewStageFlowEngine(
		publisher, 
		subscriber, 
		transport,
		WithCompletionEvents(true), // Optional: enables compensation events
		WithStageFlowLogger(slog.Default()),
	)

	// Example: E-commerce order processing workflow with compensation
	workflow := NewWorkflow("order-processing", "Order Processing Workflow").
		AddStage("reserve-inventory", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			// Reserve inventory for the order
			orderID := state.GlobalData["orderId"].(string)
			fmt.Printf("Reserving inventory for order %s\n", orderID)
			
			return &StageResult{
				StageID: "reserve-inventory",
				Status:  StageCompleted,
				Data: map[string]interface{}{
					"reservationId": "res_123",
					"items": []string{"item1", "item2"},
				},
			}, nil
		})).
		AddStage("process-payment", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			// Process payment
			amount := state.GlobalData["amount"].(float64)
			fmt.Printf("Processing payment of $%.2f\n", amount)
			
			return &StageResult{
				StageID: "process-payment",
				Status:  StageCompleted,
				Data: map[string]interface{}{
					"paymentId": "pay_456",
					"chargeId":  "ch_789",
				},
			}, nil
		})).
		AddStage("ship-order", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			// This stage might fail (e.g., shipping service down)
			return nil, fmt.Errorf("shipping service unavailable")
		}))

	// Add compensation handlers for each stage
	workflow.AddCompensation("reserve-inventory", CompensationHandlerFunc(func(ctx context.Context, state *WorkflowState, originalResult *StageResult) error {
		// Release the reserved inventory
		reservationId := originalResult.Data["reservationId"].(string)
		fmt.Printf("Releasing inventory reservation %s\n", reservationId)
		
		// Call inventory service to release reservation
		// This runs asynchronously via queue when workflow fails
		return nil
	}))

	workflow.AddCompensation("process-payment", CompensationHandlerFunc(func(ctx context.Context, state *WorkflowState, originalResult *StageResult) error {
		// Refund the payment
		paymentId := originalResult.Data["paymentId"].(string)
		chargeId := originalResult.Data["chargeId"].(string)
		fmt.Printf("Refunding payment %s (charge %s)\n", paymentId, chargeId)
		
		// Call payment service to process refund
		// This runs asynchronously via queue when workflow fails
		return nil
	}))

	// Register workflow - this creates both stage queues AND compensation queue
	err := engine.RegisterWorkflow(workflow)
	if err != nil {
		panic(err)
	}

	// Execute workflow
	state, err := workflow.Execute(context.Background(), map[string]interface{}{
		"orderId": "order_123",
		"amount":  99.99,
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("Workflow started: %s\n", state.InstanceID)

	// What happens when the workflow runs:
	//
	// 1. reserve-inventory stage executes successfully
	//    └─ Message published to: stageflow.order-processing.stage1
	//
	// 2. process-payment stage executes successfully  
	//    └─ Message published to: stageflow.order-processing.stage2
	//
	// 3. ship-order stage fails
	//    └─ Compensation message published to: stageflow.compensation.order-processing
	//    └─ WorkflowFailedEvent published (if events enabled)
	//
	// 4. Compensation processing begins (via queue):
	//    a) process-payment compensation executes (refund)
	//       └─ Next compensation message published to compensation queue
	//    b) reserve-inventory compensation executes (release inventory)  
	//       └─ WorkflowCompensatedEvent published (if events enabled)
	//
	// 5. Final state: WorkflowCompensated
	//
	// Benefits of queue-based compensation:
	// ✅ Resilient: If compensation fails, message stays in queue for retry
	// ✅ Scalable: Compensation can be processed by different worker instances
	// ✅ Observable: Compensation progress tracked via queue metrics
	// ✅ Distributed: Each compensation can run on different services
	// ✅ Ordered: Compensation happens in reverse order (LIFO)
}

// Example showing compensation with different failure scenarios
func ExampleCompensationScenarios() {
	var publisher messaging.Publisher
	var subscriber messaging.Subscriber 
	var transport messaging.Transport

	engine := NewStageFlowEngine(publisher, subscriber, transport)

	// Scenario 1: No compensation needed (failure on first stage)
	workflow1 := NewWorkflow("fail-first", "Fail on First Stage").
		AddStage("validate", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return nil, fmt.Errorf("validation failed")
		})).
		AddStage("process", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return &StageResult{StageID: "process", Status: StageCompleted}, nil
		}))
	
	// No compensation handlers needed - nothing to compensate
	
	// Scenario 2: Partial compensation (failure in middle)
	workflow2 := NewWorkflow("fail-middle", "Fail in Middle").
		AddStage("step1", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return &StageResult{StageID: "step1", Status: StageCompleted}, nil
		})).
		AddStage("step2", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return nil, fmt.Errorf("step2 failed")
		})).
		AddStage("step3", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return &StageResult{StageID: "step3", Status: StageCompleted}, nil
		}))
	
	// Only step1 needs compensation (step2 failed, step3 never executed)
	workflow2.AddCompensation("step1", CompensationHandlerFunc(func(ctx context.Context, state *WorkflowState, originalResult *StageResult) error {
		fmt.Println("Compensating step1")
		return nil
	}))
	
	// Scenario 3: Compensation failure handling
	workflow3 := NewWorkflow("compensation-fails", "Compensation Can Fail").
		AddStage("create-resource", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return &StageResult{
				StageID: "create-resource",
				Status:  StageCompleted,
				Data:    map[string]interface{}{"resourceId": "res_123"},
			}, nil
		})).
		AddStage("use-resource", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return nil, fmt.Errorf("resource usage failed")
		}))
	
	workflow3.AddCompensation("create-resource", CompensationHandlerFunc(func(ctx context.Context, state *WorkflowState, originalResult *StageResult) error {
		// This compensation might fail (e.g., resource service down)
		return fmt.Errorf("failed to delete resource - service unavailable")
		// Message will remain in compensation queue for retry
	}))
	
	// Scenario 4: Optional stage compensation
	workflow4 := NewWorkflow("optional-stages", "Optional Stage Handling").
		AddStage("required-step", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return &StageResult{StageID: "required-step", Status: StageCompleted}, nil
		})).
		AddStageWithOptions("optional-step", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return nil, fmt.Errorf("optional step failed")
		}), WithRequired(false)). // Optional stage
		AddStage("final-step", StageHandlerFunc(func(ctx context.Context, state *WorkflowState) (*StageResult, error) {
			return nil, fmt.Errorf("final step failed")
		}))
	
	// Only required-step gets compensated (optional-step failure doesn't trigger compensation)
	workflow4.AddCompensation("required-step", CompensationHandlerFunc(func(ctx context.Context, state *WorkflowState, originalResult *StageResult) error {
		fmt.Println("Compensating required step")
		return nil
	}))

	// Register all workflows
	engine.RegisterWorkflow(workflow1)
	engine.RegisterWorkflow(workflow2) 
	engine.RegisterWorkflow(workflow3)
	engine.RegisterWorkflow(workflow4)
}