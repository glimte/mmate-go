// Package stageflow provides multi-stage workflow orchestration with state management.
//
// StageFlow enables complex business processes to be broken down into discrete stages
// that execute in sequence or parallel. Each stage can maintain state, handle errors,
// and coordinate with other stages through messaging.
//
// Key features:
//   - Multi-stage workflow execution with state persistence
//   - Error handling and compensation patterns
//   - Parallel and sequential stage execution
//   - State management with persistence backends
//   - Saga pattern implementation for distributed transactions
//   - Timeout handling and retry policies
//   - Workflow monitoring and observability
//
// Basic usage:
//
//	// Define workflow stages
//	stageflow := stageflow.NewStageFlowEngine(publisher, subscriber)
//	
//	workflow := stageflow.NewWorkflow("order-processing").
//	    AddStage("validate", validateStage).
//	    AddStage("reserve", reserveStage).
//	    AddStage("charge", chargeStage).
//	    AddCompensation("reserve", releaseReservation).
//	    AddCompensation("charge", refundCharge)
//	
//	// Execute workflow
//	result, err := workflow.Execute(ctx, orderData)
//
// The stageflow package supports:
//   - Sequential and parallel stage execution
//   - State persistence across stage boundaries
//   - Automatic compensation on failures
//   - Retry policies for transient failures
//   - Workflow versioning and migration
//   - Distributed execution across services
package stageflow