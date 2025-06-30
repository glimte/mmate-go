package schema

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/serialization"
)

// Mock message types for testing
type TestRequest struct {
	contracts.BaseMessage
	Name  string `json:"name"`
	Value int    `json:"value"`
}

func (r *TestRequest) GetType() string {
	return "TestRequest"
}

type TestResponse struct {
	contracts.BaseMessage
	Success bool   `json:"success"`
	Result  string `json:"result"`
}

func (r *TestResponse) GetType() string {
	return "TestResponse"
}

func setupTestRegistry(t *testing.T) {
	// Register test types
	if err := serialization.GetGlobalRegistry().Register("TestRequest", &TestRequest{}); err != nil {
		t.Fatalf("Failed to register TestRequest: %v", err)
	}

	if err := serialization.GetGlobalRegistry().Register("TestResponse", &TestResponse{}); err != nil {
		t.Fatalf("Failed to register TestResponse: %v", err)
	}
}

func TestContractValidator_ValidateContract(t *testing.T) {
	setupTestRegistry(t)
	
	validator := NewMessageValidator()
	cv := NewContractValidator(validator)
	ctx := context.Background()

	tests := []struct {
		name     string
		contract *contracts.EndpointContract
		wantErr  bool
		errMsg   string
	}{
		{
			name:     "nil contract",
			contract: nil,
			wantErr:  true,
			errMsg:   "contract cannot be nil",
		},
		{
			name: "valid contract",
			contract: &contracts.EndpointContract{
				EndpointID:  "test.endpoint",
				Queue:       "test.queue",
				InputType:   "TestRequest",
				OutputType:  "TestResponse",
				ServiceName: "test-service",
			},
			wantErr: false,
		},
		{
			name: "invalid contract - missing fields",
			contract: &contracts.EndpointContract{
				EndpointID: "test.endpoint",
				Queue:      "test.queue",
			},
			wantErr: true,
			errMsg:  "invalid contract",
		},
		{
			name: "unregistered input type",
			contract: &contracts.EndpointContract{
				EndpointID:  "test.endpoint",
				Queue:       "test.queue",
				InputType:   "UnregisteredType",
				OutputType:  "TestResponse",
				ServiceName: "test-service",
			},
			wantErr: true,
			errMsg:  "not registered",
		},
		{
			name: "unregistered output type",
			contract: &contracts.EndpointContract{
				EndpointID:  "test.endpoint",
				Queue:       "test.queue",
				InputType:   "TestRequest",
				OutputType:  "UnregisteredType",
				ServiceName: "test-service",
			},
			wantErr: true,
			errMsg:  "not registered",
		},
		{
			name: "invalid input schema JSON",
			contract: &contracts.EndpointContract{
				EndpointID:  "test.endpoint",
				Queue:       "test.queue",
				InputType:   "TestRequest",
				OutputType:  "TestResponse",
				ServiceName: "test-service",
				InputSchema: json.RawMessage(`{invalid json}`),
			},
			wantErr: true,
			errMsg:  "invalid JSON",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cv.ValidateContract(ctx, tt.contract)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateContract() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && tt.errMsg != "" && !contains(err.Error(), tt.errMsg) {
				t.Errorf("ValidateContract() error = %v, want error containing %v", err, tt.errMsg)
			}
		})
	}
}

func TestContractValidator_EnrichContractWithSchemas(t *testing.T) {
	setupTestRegistry(t)
	
	validator := NewMessageValidator()
	
	// Register a schema for TestRequest
	requestSchema := &Schema{
		Name:    "TestRequest",
		Version: "1.0.0",
		Type:    "object",
		Properties: map[string]*PropertyDef{
			"name": {
				Type:     "string",
				Required: []string{"name"},
			},
			"value": {
				Type: "number",
			},
		},
	}
	validator.RegisterSchema("TestRequest", requestSchema)
	
	cv := NewContractValidator(validator)

	contract := &contracts.EndpointContract{
		EndpointID:  "test.endpoint",
		Queue:       "test.queue",
		InputType:   "TestRequest",
		OutputType:  "TestResponse",
		ServiceName: "test-service",
	}

	err := cv.EnrichContractWithSchemas(contract)
	if err != nil {
		t.Fatalf("EnrichContractWithSchemas() error = %v", err)
	}

	// Check that input schema was added
	if len(contract.InputSchema) == 0 {
		t.Error("Expected input schema to be enriched")
	}

	// Verify the schema is valid JSON
	var enrichedSchema Schema
	if err := json.Unmarshal(contract.InputSchema, &enrichedSchema); err != nil {
		t.Errorf("Enriched schema is not valid JSON: %v", err)
	}

	if enrichedSchema.Name != "TestRequest" {
		t.Errorf("Enriched schema name = %v, want TestRequest", enrichedSchema.Name)
	}
}

func TestContractValidator_ValidateMessageAgainstContract(t *testing.T) {
	setupTestRegistry(t)
	
	validator := NewMessageValidator()
	cv := NewContractValidator(validator)
	ctx := context.Background()

	contract := &contracts.EndpointContract{
		EndpointID:  "test.endpoint",
		Queue:       "test.queue",
		InputType:   "TestRequest",
		OutputType:  "TestResponse",
		ServiceName: "test-service",
	}

	tests := []struct {
		name    string
		msg     contracts.Message
		isInput bool
		wantErr bool
		errMsg  string
	}{
		{
			name:    "nil message",
			msg:     nil,
			isInput: true,
			wantErr: true,
			errMsg:  "cannot be nil",
		},
		{
			name: "valid input message",
			msg: &TestRequest{
				BaseMessage: contracts.BaseMessage{
					ID: "test-123",
				},
				Name:  "test",
				Value: 42,
			},
			isInput: true,
			wantErr: false,
		},
		{
			name: "valid output message",
			msg: &TestResponse{
				BaseMessage: contracts.BaseMessage{
					ID: "test-456",
				},
				Success: true,
				Result:  "OK",
			},
			isInput: false,
			wantErr: false,
		},
		{
			name: "wrong message type for input",
			msg: &TestResponse{
				BaseMessage: contracts.BaseMessage{
					ID: "test-789",
				},
			},
			isInput: true,
			wantErr: true,
			errMsg:  "doesn't match expected",
		},
		{
			name: "wrong message type for output",
			msg: &TestRequest{
				BaseMessage: contracts.BaseMessage{
					ID: "test-999",
				},
			},
			isInput: false,
			wantErr: true,
			errMsg:  "doesn't match expected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cv.ValidateMessageAgainstContract(ctx, tt.msg, contract, tt.isInput)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateMessageAgainstContract() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && tt.errMsg != "" && !contains(err.Error(), tt.errMsg) {
				t.Errorf("ValidateMessageAgainstContract() error = %v, want error containing %v", err, tt.errMsg)
			}
		})
	}
}

func TestContractValidator_GenerateJSONSchema(t *testing.T) {
	setupTestRegistry(t)
	
	validator := NewMessageValidator()
	cv := NewContractValidator(validator)

	// Test with registered schema
	schema := &Schema{
		Name:    "TestRequest",
		Version: "1.0.0",
		Type:    "object",
	}
	validator.RegisterSchema("TestRequest", schema)

	jsonSchema, err := cv.GenerateJSONSchema("TestRequest")
	if err != nil {
		t.Fatalf("GenerateJSONSchema() error = %v", err)
	}

	if len(jsonSchema) == 0 {
		t.Error("Expected non-empty JSON schema")
	}

	// Test with unregistered type but registered in type registry
	jsonSchema2, err := cv.GenerateJSONSchema("TestResponse")
	if err != nil {
		t.Fatalf("GenerateJSONSchema() for type in registry error = %v", err)
	}

	if len(jsonSchema2) == 0 {
		t.Error("Expected non-empty JSON schema for type in registry")
	}

	// Test with completely unknown type
	_, err = cv.GenerateJSONSchema("UnknownType")
	if err == nil {
		t.Error("Expected error for unknown type")
	}
}

// Helper function
func contains(s, substr string) bool {
	return len(substr) > 0 && len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || len(s) > len(substr) && containsMiddle(s, substr)))
}

func containsMiddle(s, substr string) bool {
	for i := 1; i < len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}