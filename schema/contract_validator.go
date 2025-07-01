package schema

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/serialization"
)

// ContractValidator validates endpoint contracts with schema integration
type ContractValidator struct {
	validator       *MessageValidator
	schemaGenerator *JSONSchemaGenerator
}

// NewContractValidator creates a new contract validator
func NewContractValidator(validator *MessageValidator) *ContractValidator {
	return &ContractValidator{
		validator:       validator,
		schemaGenerator: NewJSONSchemaGenerator(),
	}
}

// ValidateContract validates an endpoint contract
func (cv *ContractValidator) ValidateContract(ctx context.Context, contract *contracts.EndpointContract) error {
	if contract == nil {
		return fmt.Errorf("contract cannot be nil")
	}

	// Basic validation
	if !contract.IsValid() {
		return fmt.Errorf("invalid contract: missing required fields")
	}

	// Check if message types are registered
	typeRegistry := serialization.GetGlobalRegistry()
	
	// Validate input type exists
	if _, err := typeRegistry.GetFactory(contract.InputType); err != nil {
		return fmt.Errorf("input type %s not registered in type registry", contract.InputType)
	}

	// Validate output type exists
	if _, err := typeRegistry.GetFactory(contract.OutputType); err != nil {
		return fmt.Errorf("output type %s not registered in type registry", contract.OutputType)
	}

	// Validate schemas if provided
	if len(contract.InputSchema) > 0 {
		if !json.Valid(contract.InputSchema) {
			return fmt.Errorf("invalid JSON in input schema")
		}
	}

	if len(contract.OutputSchema) > 0 {
		if !json.Valid(contract.OutputSchema) {
			return fmt.Errorf("invalid JSON in output schema")
		}
	}

	return nil
}

// EnrichContractWithSchemas adds JSON schemas to contract from registry
func (cv *ContractValidator) EnrichContractWithSchemas(contract *contracts.EndpointContract) error {
	if contract == nil {
		return fmt.Errorf("contract cannot be nil")
	}

	// Try to get schemas from validator's registry first
	if inputSchema, err := cv.validator.GetSchema(contract.InputType); err == nil {
		// Convert schema to JSON
		schemaJSON, err := json.Marshal(inputSchema)
		if err == nil {
			contract.InputSchema = schemaJSON
		}
	} else {
		// Fall back to generator
		if err := cv.schemaGenerator.GenerateForContract(contract); err != nil {
			// Log but don't fail - schemas are optional enrichment
			return nil
		}
	}

	// For output schema, use existing if available
	if outputSchema, err := cv.validator.GetSchema(contract.OutputType); err == nil {
		// Convert schema to JSON
		schemaJSON, err := json.Marshal(outputSchema)
		if err == nil {
			contract.OutputSchema = schemaJSON
		}
	} else if len(contract.InputSchema) == 0 {
		// Only generate if we haven't already done it
		cv.schemaGenerator.GenerateForContract(contract)
	}

	return nil
}

// ValidateMessageAgainstContract validates a message against contract's schema
func (cv *ContractValidator) ValidateMessageAgainstContract(ctx context.Context, msg contracts.Message, contract *contracts.EndpointContract, isInput bool) error {
	if msg == nil || contract == nil {
		return fmt.Errorf("message and contract cannot be nil")
	}

	// Determine which type to validate against
	expectedType := contract.InputType
	schemaData := contract.InputSchema
	if !isInput {
		expectedType = contract.OutputType
		schemaData = contract.OutputSchema
	}

	// Check message type matches
	if msg.GetType() != expectedType {
		return fmt.Errorf("message type %s doesn't match expected %s", msg.GetType(), expectedType)
	}

	// If we have a schema, validate against it
	if len(schemaData) > 0 {
		var schema Schema
		if err := json.Unmarshal(schemaData, &schema); err != nil {
			return fmt.Errorf("failed to unmarshal schema: %w", err)
		}

		result := cv.validator.ValidateWithSchema(ctx, msg, &schema)
		if !result.Valid {
			return fmt.Errorf("message validation failed: %v", result.Errors)
		}
	} else {
		// Fall back to validator's registered schemas
		if err := cv.validator.Validate(ctx, msg); err != nil {
			return err
		}
	}

	return nil
}

// GenerateJSONSchema generates a JSON Schema for a message type
func (cv *ContractValidator) GenerateJSONSchema(messageType string) (json.RawMessage, error) {
	// First try to get from validator's registry
	schema, err := cv.validator.GetSchema(messageType)
	if err == nil {
		return json.Marshal(schema)
	}

	// If no schema registered, try to generate from type
	factory, err := serialization.GetGlobalRegistry().GetFactory(messageType)
	if err != nil {
		// Fall back to basic generator
		return cv.schemaGenerator.GenerateFromType(messageType)
	}

	// Create instance and generate schema
	msg := factory()
	return cv.schemaGenerator.GenerateFromMessage(msg)
}

