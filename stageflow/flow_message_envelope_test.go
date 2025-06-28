package stageflow

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/stretchr/testify/assert"
)

// Test data structures
type testFlowPayload struct {
	Data   string `json:"data"`
	Number int    `json:"number"`
}

func TestFlowMessageEnvelope(t *testing.T) {
	t.Run("creates envelope with payload", func(t *testing.T) {
		payload := testFlowPayload{
			Data:   "test data",
			Number: 42,
		}

		payloadBytes, err := json.Marshal(payload)
		assert.NoError(t, err)

		envelope := &FlowMessageEnvelope{
			BaseMessage:               contracts.NewBaseMessage("TestFlowMessage"),
			Payload:                   payloadBytes,
			PayloadType:               "testFlowPayload",
			SerializedWorkflowState:   `{"status":"running"}`,
			WorkflowStateType:         "WorkflowState",
			CurrentStageIndex:         1,
			NextStageQueue:            "stage2.queue",
			WorkflowID:                "workflow-123",
			InstanceID:                "instance-456",
			StageName:                 "stage1",
		}

		assert.Equal(t, "TestFlowMessage", envelope.Type)
		assert.Equal(t, "testFlowPayload", envelope.PayloadType)
		assert.Equal(t, 1, envelope.CurrentStageIndex)
		assert.Equal(t, "stage2.queue", envelope.NextStageQueue)
		assert.Equal(t, "workflow-123", envelope.WorkflowID)
		assert.Equal(t, "instance-456", envelope.InstanceID)
		assert.Equal(t, "stage1", envelope.StageName)

		// Verify payload can be unmarshaled
		var unmarshaledPayload testFlowPayload
		err = json.Unmarshal(envelope.Payload, &unmarshaledPayload)
		assert.NoError(t, err)
		assert.Equal(t, payload, unmarshaledPayload)
	})

	t.Run("serializes and deserializes correctly", func(t *testing.T) {
		now := time.Now().Truncate(time.Second) // Truncate for JSON comparison

		envelope := &FlowMessageEnvelope{
			BaseMessage:             contracts.NewBaseMessage("TestFlowMessage"),
			Payload:                 json.RawMessage(`{"test": "data"}`),
			PayloadType:             "testPayload",
			SerializedWorkflowState: `{"instanceId": "123"}`,
			WorkflowStateType:       "WorkflowState",
			Checkpoint: ProcessingCheckpoint{
				StageID:       "stage1",
				StepName:      "step1",
				StepCompleted: true,
				StepResults:   map[string]interface{}{"result": "success"},
				LastUpdate:    now,
				RetryCount:    2,
			},
			CurrentStageIndex: 0,
			NextStageQueue:    "next.queue",
			WorkflowID:        "workflow-123",
			InstanceID:        "instance-456",
			StageName:         "stage1",
		}

		// Serialize
		data, err := json.Marshal(envelope)
		assert.NoError(t, err)

		// Deserialize
		var deserialized FlowMessageEnvelope
		err = json.Unmarshal(data, &deserialized)
		assert.NoError(t, err)

		// Verify all fields
		assert.Equal(t, envelope.Type, deserialized.Type)
		assert.Equal(t, envelope.PayloadType, deserialized.PayloadType)
		assert.Equal(t, envelope.SerializedWorkflowState, deserialized.SerializedWorkflowState)
		assert.Equal(t, envelope.WorkflowStateType, deserialized.WorkflowStateType)
		assert.Equal(t, envelope.CurrentStageIndex, deserialized.CurrentStageIndex)
		assert.Equal(t, envelope.NextStageQueue, deserialized.NextStageQueue)
		assert.Equal(t, envelope.WorkflowID, deserialized.WorkflowID)
		assert.Equal(t, envelope.InstanceID, deserialized.InstanceID)
		assert.Equal(t, envelope.StageName, deserialized.StageName)

		// Verify checkpoint
		assert.Equal(t, envelope.Checkpoint.StageID, deserialized.Checkpoint.StageID)
		assert.Equal(t, envelope.Checkpoint.StepName, deserialized.Checkpoint.StepName)
		assert.Equal(t, envelope.Checkpoint.StepCompleted, deserialized.Checkpoint.StepCompleted)
		assert.Equal(t, envelope.Checkpoint.RetryCount, deserialized.Checkpoint.RetryCount)
		assert.Equal(t, envelope.Checkpoint.LastUpdate.Unix(), deserialized.Checkpoint.LastUpdate.Unix())
	})
}

func TestProcessingCheckpoint(t *testing.T) {
	t.Run("creates checkpoint with step results", func(t *testing.T) {
		now := time.Now()

		checkpoint := ProcessingCheckpoint{
			StageID:       "stage1",
			StepName:      "processData",
			StepCompleted: true,
			StepResults: map[string]interface{}{
				"processData": "processing complete",
				"count":       42,
				"success":     true,
			},
			LastUpdate: now,
			RetryCount:  1,
		}

		assert.Equal(t, "stage1", checkpoint.StageID)
		assert.Equal(t, "processData", checkpoint.StepName)
		assert.True(t, checkpoint.StepCompleted)
		assert.Equal(t, "processing complete", checkpoint.StepResults["processData"])
		assert.Equal(t, 42, checkpoint.StepResults["count"])
		assert.Equal(t, true, checkpoint.StepResults["success"])
		assert.Equal(t, now, checkpoint.LastUpdate)
		assert.Equal(t, 1, checkpoint.RetryCount)
	})

	t.Run("handles empty step results", func(t *testing.T) {
		checkpoint := ProcessingCheckpoint{
			StageID:       "stage1",
			StepName:      "step1",
			StepCompleted: false,
			StepResults:   nil,
			LastUpdate:    time.Now(),
			RetryCount:    0,
		}

		assert.False(t, checkpoint.StepCompleted)
		assert.Nil(t, checkpoint.StepResults)
		assert.Equal(t, 0, checkpoint.RetryCount)
	})
}

func TestStageContext(t *testing.T) {
	createTestContext := func() *StageContext {
		state := &WorkflowState{
			InstanceID:   "instance-123",
			Status:       WorkflowRunning,
			StageResults: make([]StageResult, 0),
		}

		envelope := &FlowMessageEnvelope{
			BaseMessage:       contracts.NewBaseMessage("TestFlow"),
			WorkflowID:        "workflow-123",
			InstanceID:        "instance-123",
			StageName:         "stage1",
			CurrentStageIndex: 0,
		}

		return &StageContext{
			State:       state,
			Envelope:    envelope,
			StageIndex:  0,
			checkpoints: make(map[string]*ProcessingCheckpoint),
		}
	}

	t.Run("IsStepCompleted returns false for non-existent step", func(t *testing.T) {
		ctx := createTestContext()

		completed := ctx.IsStepCompleted("nonexistent")

		assert.False(t, completed)
	})

	t.Run("IsStepCompleted returns correct status for existing step", func(t *testing.T) {
		ctx := createTestContext()

		// Add a completed checkpoint
		ctx.checkpoints["step1"] = &ProcessingCheckpoint{
			StageID:       "stage1",
			StepName:      "step1",
			StepCompleted: true,
		}

		// Add an incomplete checkpoint
		ctx.checkpoints["step2"] = &ProcessingCheckpoint{
			StageID:       "stage1",
			StepName:      "step2",
			StepCompleted: false,
		}

		assert.True(t, ctx.IsStepCompleted("step1"))
		assert.False(t, ctx.IsStepCompleted("step2"))
	})

	t.Run("GetStepResult returns result for existing step", func(t *testing.T) {
		ctx := createTestContext()

		ctx.checkpoints["step1"] = &ProcessingCheckpoint{
			StageID:       "stage1",
			StepName:      "step1",
			StepCompleted: true,
			StepResults: map[string]interface{}{
				"step1": "test result",
			},
		}

		result, found := ctx.GetStepResult("step1")

		assert.True(t, found)
		assert.Equal(t, "test result", result)
	})

	t.Run("GetStepResult returns false for non-existent step", func(t *testing.T) {
		ctx := createTestContext()

		result, found := ctx.GetStepResult("nonexistent")

		assert.False(t, found)
		assert.Nil(t, result)
	})

	t.Run("GetStepResult returns false for step without result", func(t *testing.T) {
		ctx := createTestContext()

		ctx.checkpoints["step1"] = &ProcessingCheckpoint{
			StageID:       "stage1",
			StepName:      "step1",
			StepCompleted: true,
			StepResults:   map[string]interface{}{},
		}

		result, found := ctx.GetStepResult("step1")

		assert.False(t, found)
		assert.Nil(t, result)
	})

	t.Run("SaveStepResult creates new checkpoint", func(t *testing.T) {
		ctx := createTestContext()
		ctx.checkpoints = nil // Test nil initialization

		result := map[string]interface{}{"key": "value"}

		ctx.SaveStepResult("step1", result)

		assert.NotNil(t, ctx.checkpoints)
		assert.Contains(t, ctx.checkpoints, "step1")

		checkpoint := ctx.checkpoints["step1"]
		assert.Equal(t, "stage1", checkpoint.StageID)
		assert.Equal(t, "step1", checkpoint.StepName)
		assert.True(t, checkpoint.StepCompleted)
		assert.Equal(t, result, checkpoint.StepResults["step1"])
		assert.False(t, checkpoint.LastUpdate.IsZero())

		// Verify envelope checkpoint is updated
		assert.Equal(t, *checkpoint, ctx.Envelope.Checkpoint)
	})

	t.Run("SaveStepResult updates existing checkpoint", func(t *testing.T) {
		ctx := createTestContext()
		initialTime := time.Now().Add(-time.Hour)

		// Create existing checkpoint
		ctx.checkpoints["step1"] = &ProcessingCheckpoint{
			StageID:       "stage1",
			StepName:      "step1",
			StepCompleted: false,
			StepResults:   make(map[string]interface{}),
			LastUpdate:    initialTime,
		}

		newResult := "updated result"
		ctx.SaveStepResult("step1", newResult)

		checkpoint := ctx.checkpoints["step1"]
		assert.True(t, checkpoint.StepCompleted)
		assert.Equal(t, newResult, checkpoint.StepResults["step1"])
		assert.True(t, checkpoint.LastUpdate.After(initialTime))
	})

	t.Run("SaveStageResult creates new stage result", func(t *testing.T) {
		ctx := createTestContext()
		ctx.State.StageResults = nil // Test nil initialization

		ctx.SaveStageResult("output", "stage output value")

		assert.NotNil(t, ctx.State.StageResults)
		assert.Len(t, ctx.State.StageResults, 1)

		stageResult := ctx.State.StageResults[0]
		assert.Equal(t, "stage1", stageResult.StageID)
		assert.Equal(t, StageRunning, stageResult.Status)
		assert.Equal(t, "stage output value", stageResult.Data["output"])
		assert.False(t, stageResult.StartTime.IsZero())
	})

	t.Run("SaveStageResult updates existing stage result", func(t *testing.T) {
		ctx := createTestContext()

		// Create existing stage result
		ctx.State.StageResults = []StageResult{
			{
				StageID:   "stage1",
				Status:    StageRunning,
				Data:      map[string]interface{}{"existing": "data"},
				StartTime: time.Now().Add(-time.Hour),
			},
		}

		ctx.SaveStageResult("new_key", "new_value")

		assert.Len(t, ctx.State.StageResults, 1)

		stageResult := ctx.State.StageResults[0]
		assert.Equal(t, "data", stageResult.Data["existing"])
		assert.Equal(t, "new_value", stageResult.Data["new_key"])
	})

	t.Run("SaveStageResult initializes Data map if nil", func(t *testing.T) {
		ctx := createTestContext()

		// Create stage result with nil Data
		ctx.State.StageResults = []StageResult{
			{
				StageID:   "stage1",
				Status:    StageRunning,
				Data:      nil,
				StartTime: time.Now(),
			},
		}

		ctx.SaveStageResult("test_key", "test_value")

		stageResult := ctx.State.StageResults[0]
		assert.NotNil(t, stageResult.Data)
		assert.Equal(t, "test_value", stageResult.Data["test_key"])
	})

	t.Run("SaveStageResult works with different stages", func(t *testing.T) {
		ctx := createTestContext()

		// Create results for different stages
		ctx.State.StageResults = []StageResult{
			{
				StageID:   "stage0",
				Status:    StageCompleted,
				Data:      map[string]interface{}{"stage0": "data"},
				StartTime: time.Now(),
			},
		}

		ctx.SaveStageResult("stage1_output", "stage1_data")

		assert.Len(t, ctx.State.StageResults, 2)

		// Verify stage0 result unchanged
		assert.Equal(t, "stage0", ctx.State.StageResults[0].StageID)
		assert.Equal(t, "data", ctx.State.StageResults[0].Data["stage0"])

		// Verify new stage1 result
		assert.Equal(t, "stage1", ctx.State.StageResults[1].StageID)
		assert.Equal(t, "stage1_data", ctx.State.StageResults[1].Data["stage1_output"])
	})
}

func TestQueueBasedStateStore(t *testing.T) {
	t.Run("creates new store", func(t *testing.T) {
		store := NewQueueBasedStateStore()

		assert.NotNil(t, store)
	})

	t.Run("SaveState is no-op", func(t *testing.T) {
		store := NewQueueBasedStateStore()
		state := &WorkflowState{
			InstanceID: "test-123",
			Status:     WorkflowRunning,
		}

		err := store.SaveState(context.Background(), state)

		assert.NoError(t, err)
	})

	t.Run("LoadState returns error", func(t *testing.T) {
		store := NewQueueBasedStateStore()

		state, err := store.LoadState(context.Background(), "test-123")

		assert.Error(t, err)
		assert.Nil(t, state)
		assert.Contains(t, err.Error(), "queue-based state store does not support direct state loading")
	})

	t.Run("DeleteState is no-op", func(t *testing.T) {
		store := NewQueueBasedStateStore()

		err := store.DeleteState(context.Background(), "test-123")

		assert.NoError(t, err)
	})

	t.Run("ListActiveWorkflows returns error", func(t *testing.T) {
		store := NewQueueBasedStateStore()

		workflows, err := store.ListActiveWorkflows(context.Background())

		assert.Error(t, err)
		assert.Nil(t, workflows)
		assert.Contains(t, err.Error(), "queue-based state store does not support listing active workflows")
	})
}

// Integration tests
func TestFlowMessageEnvelopeIntegration(t *testing.T) {
	t.Run("complete workflow with envelope and context", func(t *testing.T) {
		// Create initial workflow state
		state := &WorkflowState{
			InstanceID:   "integration-test-123",
			Status:       WorkflowRunning,
			StageResults: make([]StageResult, 0),
		}

		// Create flow envelope
		payload := testFlowPayload{Data: "integration test", Number: 100}
		payloadBytes, _ := json.Marshal(payload)

		envelope := &FlowMessageEnvelope{
			BaseMessage:             contracts.NewBaseMessage("IntegrationTestFlow"),
			Payload:                 payloadBytes,
			PayloadType:             "testFlowPayload",
			SerializedWorkflowState: `{"instanceId":"integration-test-123"}`,
			WorkflowStateType:       "WorkflowState",
			CurrentStageIndex:       0,
			NextStageQueue:          "stage1.queue",
			WorkflowID:              "integration-workflow",
			InstanceID:              "integration-test-123",
			StageName:               "stage0",
		}

		// Create stage context
		ctx := &StageContext{
			State:       state,
			Envelope:    envelope,
			StageIndex:  0,
			checkpoints: make(map[string]*ProcessingCheckpoint),
		}

		// Simulate stage processing with multiple steps
		assert.False(t, ctx.IsStepCompleted("validate"))
		ctx.SaveStepResult("validate", map[string]interface{}{
			"valid":     true,
			"timestamp": time.Now(),
		})
		assert.True(t, ctx.IsStepCompleted("validate"))

		result, found := ctx.GetStepResult("validate")
		assert.True(t, found)
		validateResult := result.(map[string]interface{})
		assert.True(t, validateResult["valid"].(bool))

		// Save stage output
		ctx.SaveStageResult("processed_data", "validation complete")
		ctx.SaveStageResult("count", 100)

		// Verify state contains stage results
		assert.Len(t, ctx.State.StageResults, 1)
		stageResult := ctx.State.StageResults[0]
		assert.Equal(t, "stage0", stageResult.StageID)
		assert.Equal(t, "validation complete", stageResult.Data["processed_data"])
		assert.Equal(t, 100, stageResult.Data["count"])

		// Verify envelope checkpoint is updated
		assert.Equal(t, "validate", envelope.Checkpoint.StepName)
		assert.True(t, envelope.Checkpoint.StepCompleted)

		// Simulate serializing for next stage
		envelopeBytes, err := json.Marshal(envelope)
		assert.NoError(t, err)

		// Deserialize for next stage
		var nextStageEnvelope FlowMessageEnvelope
		err = json.Unmarshal(envelopeBytes, &nextStageEnvelope)
		assert.NoError(t, err)

		// Verify envelope integrity
		assert.Equal(t, envelope.WorkflowID, nextStageEnvelope.WorkflowID)
		assert.Equal(t, envelope.InstanceID, nextStageEnvelope.InstanceID)
		assert.Equal(t, envelope.PayloadType, nextStageEnvelope.PayloadType)
		assert.Equal(t, envelope.Checkpoint.StepName, nextStageEnvelope.Checkpoint.StepName)
		assert.Equal(t, envelope.Checkpoint.StepCompleted, nextStageEnvelope.Checkpoint.StepCompleted)
	})
}

// Performance tests
func TestFlowMessageEnvelopePerformance(t *testing.T) {
	t.Run("serialization performance", func(t *testing.T) {
		// Create a large payload
		largePayload := make(map[string]interface{})
		for i := 0; i < 1000; i++ {
			largePayload[fmt.Sprintf("field_%d", i)] = fmt.Sprintf("value_%d", i)
		}

		payloadBytes, _ := json.Marshal(largePayload)

		envelope := &FlowMessageEnvelope{
			BaseMessage:             contracts.NewBaseMessage("PerformanceTest"),
			Payload:                 payloadBytes,
			PayloadType:             "largePayload",
			SerializedWorkflowState: `{"instanceId":"perf-test","status":"running"}`,
			WorkflowStateType:       "WorkflowState",
			CurrentStageIndex:       5,
			WorkflowID:              "perf-workflow",
			InstanceID:              "perf-test-123",
			StageName:               "stage5",
		}

		// Measure serialization time
		start := time.Now()
		data, err := json.Marshal(envelope)
		serializationTime := time.Since(start)

		assert.NoError(t, err)
		assert.NotEmpty(t, data)
		assert.Less(t, serializationTime, 100*time.Millisecond) // Should be fast

		// Measure deserialization time
		start = time.Now()
		var deserialized FlowMessageEnvelope
		err = json.Unmarshal(data, &deserialized)
		deserializationTime := time.Since(start)

		assert.NoError(t, err)
		assert.Less(t, deserializationTime, 100*time.Millisecond) // Should be fast

		// Verify payload integrity
		assert.Equal(t, envelope.PayloadType, deserialized.PayloadType)
		assert.Equal(t, len(envelope.Payload), len(deserialized.Payload))
	})
}