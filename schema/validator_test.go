package schema

import (
	"context"
	"testing"

	"github.com/glimte/mmate-go/contracts"
	"github.com/stretchr/testify/assert"
)

// Test message types
type TestMessage struct {
	contracts.BaseMessage
	Name  string `json:"name"`
	Email string `json:"email"`
	Age   int    `json:"age"`
}

type TestComplexMessage struct {
	contracts.BaseMessage
	User    TestUser      `json:"user"`
	Tags    []string      `json:"tags"`
	Metadata map[string]interface{} `json:"metadata"`
}

type TestUser struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Email    string `json:"email"`
	Active   bool   `json:"active"`
}

func TestNewMessageValidator(t *testing.T) {
	t.Run("NewMessageValidator creates validator with defaults", func(t *testing.T) {
		validator := NewMessageValidator()
		
		assert.NotNil(t, validator)
		assert.NotNil(t, validator.schemas)
		assert.NotNil(t, validator.rules)
	})
	
	t.Run("NewMessageValidator applies options", func(t *testing.T) {
		validator := NewMessageValidator(WithStrictMode(true))
		
		assert.NotNil(t, validator)
	})
}

func TestRegisterSchema(t *testing.T) {
	t.Run("RegisterSchema succeeds with valid parameters", func(t *testing.T) {
		validator := NewMessageValidator()
		schema := &Schema{
			Name:    "TestMessage",
			Version: "1.0",
			Type:    "object",
		}
		
		err := validator.RegisterSchema("TestMessage", schema)
		
		assert.NoError(t, err)
		
		// Verify schema is registered
		retrievedSchema, err := validator.GetSchema("TestMessage")
		assert.NoError(t, err)
		assert.Equal(t, schema, retrievedSchema)
	})
	
	t.Run("RegisterSchema fails with empty message type", func(t *testing.T) {
		validator := NewMessageValidator()
		schema := &Schema{Name: "Test"}
		
		err := validator.RegisterSchema("", schema)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "message type cannot be empty")
	})
	
	t.Run("RegisterSchema fails with nil schema", func(t *testing.T) {
		validator := NewMessageValidator()
		
		err := validator.RegisterSchema("TestMessage", nil)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "schema cannot be nil")
	})
}

func TestValidation(t *testing.T) {
	t.Run("Validate passes when no schema is registered", func(t *testing.T) {
		validator := NewMessageValidator()
		msg := &TestMessage{
			BaseMessage: contracts.NewBaseMessage("TestMessage"),
			Name:        "John Doe",
			Email:       "john@example.com",
			Age:         30,
		}
		
		err := validator.Validate(context.Background(), msg)
		
		assert.NoError(t, err)
	})
	
	t.Run("Validate succeeds with valid message", func(t *testing.T) {
		validator := NewMessageValidator()
		
		// Register schema
		schema := &Schema{
			Name:    "TestMessage",
			Version: "1.0",
			Type:    "object",
			Properties: map[string]*PropertyDef{
				"name": {
					Type:      "string",
					MinLength: &[]int{1}[0],
					MaxLength: &[]int{100}[0],
				},
				"email": {
					Type:   "string",
					Format: "email",
				},
				"age": {
					Type:    "number",
					Minimum: &[]float64{0}[0],
					Maximum: &[]float64{150}[0],
				},
			},
			Required: []string{"name", "email"},
		}
		
		err := validator.RegisterSchema("TestMessage", schema)
		assert.NoError(t, err)
		
		// Create valid message
		msg := &TestMessage{
			BaseMessage: contracts.NewBaseMessage("TestMessage"),
			Name:        "John Doe",
			Email:       "john@example.com",
			Age:         30,
		}
		
		err = validator.Validate(context.Background(), msg)
		assert.NoError(t, err)
	})
	
	t.Run("Validate fails with invalid message", func(t *testing.T) {
		validator := NewMessageValidator()
		
		// Register schema with strict requirements
		schema := &Schema{
			Name:    "TestMessage",
			Version: "1.0",
			Type:    "object",
			Properties: map[string]*PropertyDef{
				"name": {
					Type:      "string",
					MinLength: &[]int{2}[0],
				},
				"email": {
					Type:   "string",
					Format: "email",
				},
			},
			Required: []string{"name", "email"},
		}
		
		err := validator.RegisterSchema("TestMessage", schema)
		assert.NoError(t, err)
		
		// Create invalid message (missing required field, invalid email)
		msg := &TestMessage{
			BaseMessage: contracts.NewBaseMessage("TestMessage"),
			Name:        "", // Too short
			Email:       "invalid-email",
			Age:         30,
		}
		
		err = validator.Validate(context.Background(), msg)
		assert.Error(t, err)
	})
}

func TestValidateWithSchema(t *testing.T) {
	t.Run("ValidateWithSchema returns detailed validation results", func(t *testing.T) {
		validator := NewMessageValidator()
		
		schema := &Schema{
			Name:    "TestMessage",
			Version: "1.0",
			Type:    "object",
			Properties: map[string]*PropertyDef{
				"name": {
					Type:      "string",
					MinLength: &[]int{2}[0],
					MaxLength: &[]int{50}[0],
				},
				"email": {
					Type:   "string",
					Format: "email",
				},
				"age": {
					Type:    "number",
					Minimum: &[]float64{0}[0],
					Maximum: &[]float64{120}[0],
				},
			},
			Required: []string{"name", "email"},
		}
		
		// Test valid message
		validMsg := &TestMessage{
			BaseMessage: contracts.NewBaseMessage("TestMessage"),
			Name:        "John Doe",
			Email:       "john@example.com",
			Age:         30,
		}
		
		result := validator.ValidateWithSchema(context.Background(), validMsg, schema)
		assert.True(t, result.Valid)
		assert.Empty(t, result.Errors)
		
		// Test invalid message
		invalidMsg := &TestMessage{
			BaseMessage: contracts.NewBaseMessage("TestMessage"),
			Name:        "J", // Too short
			Email:       "invalid-email",
			Age:         200, // Too high
		}
		
		result = validator.ValidateWithSchema(context.Background(), invalidMsg, schema)
		assert.False(t, result.Valid)
		assert.NotEmpty(t, result.Errors)
		
		// Check specific errors
		var foundLengthError, foundFormatError, foundRangeError bool
		for _, err := range result.Errors {
			switch err.Code {
			case "MIN_LENGTH_VIOLATION":
				foundLengthError = true
			case "FORMAT_VIOLATION":
				foundFormatError = true
			case "MAXIMUM_VIOLATION":
				foundRangeError = true
			}
		}
		
		assert.True(t, foundLengthError)
		assert.True(t, foundFormatError)
		assert.True(t, foundRangeError)
	})
}

func TestPropertyValidation(t *testing.T) {
	t.Run("String validation works correctly", func(t *testing.T) {
		validator := NewMessageValidator()
		result := &ValidationResult{Valid: true, Errors: make([]ValidationError, 0)}
		
		propDef := &PropertyDef{
			Type:      "string",
			MinLength: &[]int{5}[0],
			MaxLength: &[]int{10}[0],
			Pattern:   "^[A-Z][a-z]+$",
		}
		
		// Valid string
		validator.validateProperty(context.Background(), "test", "Hello", propDef, result)
		assert.True(t, result.Valid)
		
		// Too short
		result = &ValidationResult{Valid: true, Errors: make([]ValidationError, 0)}
		validator.validateProperty(context.Background(), "test", "Hi", propDef, result)
		assert.False(t, result.Valid)
		
		// Too long
		result = &ValidationResult{Valid: true, Errors: make([]ValidationError, 0)}
		validator.validateProperty(context.Background(), "test", "This is too long", propDef, result)
		assert.False(t, result.Valid)
		
		// Invalid pattern
		result = &ValidationResult{Valid: true, Errors: make([]ValidationError, 0)}
		validator.validateProperty(context.Background(), "test", "hello", propDef, result)
		assert.False(t, result.Valid)
	})
	
	t.Run("Number validation works correctly", func(t *testing.T) {
		validator := NewMessageValidator()
		result := &ValidationResult{Valid: true, Errors: make([]ValidationError, 0)}
		
		propDef := &PropertyDef{
			Type:    "number",
			Minimum: &[]float64{0}[0],
			Maximum: &[]float64{100}[0],
		}
		
		// Valid number
		validator.validateProperty(context.Background(), "test", 50.0, propDef, result)
		assert.True(t, result.Valid)
		
		// Too small
		result = &ValidationResult{Valid: true, Errors: make([]ValidationError, 0)}
		validator.validateProperty(context.Background(), "test", -5.0, propDef, result)
		assert.False(t, result.Valid)
		
		// Too large
		result = &ValidationResult{Valid: true, Errors: make([]ValidationError, 0)}
		validator.validateProperty(context.Background(), "test", 150.0, propDef, result)
		assert.False(t, result.Valid)
	})
	
	t.Run("Enum validation works correctly", func(t *testing.T) {
		validator := NewMessageValidator()
		result := &ValidationResult{Valid: true, Errors: make([]ValidationError, 0)}
		
		propDef := &PropertyDef{
			Type: "string",
			Enum: []interface{}{"red", "green", "blue"},
		}
		
		// Valid enum value
		validator.validateProperty(context.Background(), "test", "red", propDef, result)
		assert.True(t, result.Valid)
		
		// Invalid enum value
		result = &ValidationResult{Valid: true, Errors: make([]ValidationError, 0)}
		validator.validateProperty(context.Background(), "test", "yellow", propDef, result)
		assert.False(t, result.Valid)
	})
	
	t.Run("Array validation works correctly", func(t *testing.T) {
		validator := NewMessageValidator()
		result := &ValidationResult{Valid: true, Errors: make([]ValidationError, 0)}
		
		propDef := &PropertyDef{
			Type: "array",
			Items: &PropertyDef{
				Type:      "string",
				MinLength: &[]int{1}[0],
			},
		}
		
		// Valid array
		arr := []interface{}{"item1", "item2", "item3"}
		validator.validateProperty(context.Background(), "test", arr, propDef, result)
		assert.True(t, result.Valid)
		
		// Array with invalid items
		result = &ValidationResult{Valid: true, Errors: make([]ValidationError, 0)}
		invalidArr := []interface{}{"item1", "", "item3"} // Empty string violates minLength
		validator.validateProperty(context.Background(), "test", invalidArr, propDef, result)
		assert.False(t, result.Valid)
	})
}

func TestFormatValidation(t *testing.T) {
	validator := NewMessageValidator()
	
	t.Run("Email format validation", func(t *testing.T) {
		valid, _ := validator.validateEmail("test@example.com")
		assert.True(t, valid)
		
		valid, _ = validator.validateEmail("invalid-email")
		assert.False(t, valid)
	})
	
	t.Run("UUID format validation", func(t *testing.T) {
		valid, _ := validator.validateUUID("123e4567-e89b-12d3-a456-426614174000")
		assert.True(t, valid)
		
		valid, _ = validator.validateUUID("invalid-uuid")
		assert.False(t, valid)
	})
	
	t.Run("Date format validation", func(t *testing.T) {
		valid, _ := validator.validateDate("2023-12-25")
		assert.True(t, valid)
		
		valid, _ = validator.validateDate("2023/12/25")
		assert.False(t, valid)
	})
	
	t.Run("DateTime format validation", func(t *testing.T) {
		valid, _ := validator.validateDateTime("2023-12-25T10:30:00Z")
		assert.True(t, valid)
		
		valid, _ = validator.validateDateTime("2023-12-25 10:30:00")
		assert.False(t, valid)
	})
	
	t.Run("URI format validation", func(t *testing.T) {
		valid, _ := validator.validateURI("https://example.com")
		assert.True(t, valid)
		
		valid, _ = validator.validateURI("not-a-uri")
		assert.False(t, valid)
	})
}

func TestCustomValidationRules(t *testing.T) {
	t.Run("RegisterRule and custom validation", func(t *testing.T) {
		validator := NewMessageValidator()
		
		// Register custom rule that checks the message's Name field
		customRule := ValidationRuleFunc(func(ctx context.Context, field string, value interface{}) *ValidationError {
			if msg, ok := value.(*TestMessage); ok {
				if msg.Name == "forbidden" {
					return &ValidationError{
						Field:   "name",
						Message: "forbidden value",
						Code:    "FORBIDDEN_VALUE",
						Value:   msg.Name,
					}
				}
			}
			return nil
		})
		
		err := validator.RegisterRule(customRule)
		assert.NoError(t, err)
		
		// Test with schema that includes custom rule
		schema := &Schema{
			Name:    "TestMessage",
			Version: "1.0",
			Type:    "object",
			Rules:   []ValidationRule{customRule},
		}
		
		// Test with forbidden value
		msg := &TestMessage{
			BaseMessage: contracts.NewBaseMessage("TestMessage"),
			Name:        "forbidden",
		}
		
		result := validator.ValidateWithSchema(context.Background(), msg, schema)
		assert.False(t, result.Valid)
		assert.NotEmpty(t, result.Errors)
		
		// Find the forbidden value error
		var foundForbiddenError bool
		for _, err := range result.Errors {
			if err.Code == "FORBIDDEN_VALUE" {
				foundForbiddenError = true
				break
			}
		}
		assert.True(t, foundForbiddenError)
	})
	
	t.Run("RegisterRule fails with nil rule", func(t *testing.T) {
		validator := NewMessageValidator()
		
		err := validator.RegisterRule(nil)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "rule cannot be nil")
	})
}

func TestTypeValidation(t *testing.T) {
	validator := NewMessageValidator()
	
	t.Run("validateType checks types correctly", func(t *testing.T) {
		assert.True(t, validator.validateType("string", "string"))
		assert.True(t, validator.validateType(42.0, "number"))
		assert.True(t, validator.validateType(42, "number"))
		assert.True(t, validator.validateType(42, "integer"))
		assert.True(t, validator.validateType(42.0, "integer")) // 42.0 is integer
		assert.False(t, validator.validateType(42.5, "integer"))
		assert.True(t, validator.validateType(true, "boolean"))
		assert.True(t, validator.validateType([]interface{}{}, "array"))
		assert.True(t, validator.validateType(map[string]interface{}{}, "object"))
		
		// Unknown types should pass
		assert.True(t, validator.validateType("anything", "unknown"))
	})
}

func TestBuiltInRules(t *testing.T) {
	t.Run("Built-in rules are registered", func(t *testing.T) {
		validator := NewMessageValidator()
		
		// Check that built-in rules exist
		nonEmptyRule, exists := validator.rules["non-empty"]
		assert.True(t, exists)
		assert.NotNil(t, nonEmptyRule)
		
		positiveRule, exists := validator.rules["positive"]
		assert.True(t, exists)
		assert.NotNil(t, positiveRule)
		
		// Test non-empty rule
		err := nonEmptyRule.Validate(context.Background(), "test", "")
		assert.NotNil(t, err)
		assert.Equal(t, "NON_EMPTY_VIOLATION", err.Code)
		
		err = nonEmptyRule.Validate(context.Background(), "test", "valid")
		assert.Nil(t, err)
		
		// Test positive rule
		err = positiveRule.Validate(context.Background(), "test", -5.0)
		assert.NotNil(t, err)
		assert.Equal(t, "POSITIVE_VIOLATION", err.Code)
		
		err = positiveRule.Validate(context.Background(), "test", 5.0)
		assert.Nil(t, err)
	})
}

func TestValidationError(t *testing.T) {
	t.Run("ValidationError implements error interface", func(t *testing.T) {
		err := ValidationError{
			Field:   "test.field",
			Message: "validation failed",
			Code:    "TEST_ERROR",
			Value:   "invalid",
		}
		
		errorString := err.Error()
		assert.Contains(t, errorString, "test.field")
		assert.Contains(t, errorString, "validation failed")
	})
}

func TestComplexMessageValidation(t *testing.T) {
	t.Run("Nested object validation", func(t *testing.T) {
		validator := NewMessageValidator()
		
		// Define schema with nested object
		schema := &Schema{
			Name:    "TestComplexMessage",
			Version: "1.0",
			Type:    "object",
			Properties: map[string]*PropertyDef{
				"user": {
					Type: "object",
					Properties: map[string]*PropertyDef{
						"id": {
							Type:   "string",
							Format: "uuid",
						},
						"name": {
							Type:      "string",
							MinLength: &[]int{1}[0],
						},
						"email": {
							Type:   "string",
							Format: "email",
						},
						"active": {
							Type: "boolean",
						},
					},
					Required: []string{"id", "name", "email"},
				},
				"tags": {
					Type: "array",
					Items: &PropertyDef{
						Type:      "string",
						MinLength: &[]int{1}[0],
					},
				},
			},
			Required: []string{"user"},
		}
		
		err := validator.RegisterSchema("TestComplexMessage", schema)
		assert.NoError(t, err)
		
		// Valid complex message
		validMsg := &TestComplexMessage{
			BaseMessage: contracts.NewBaseMessage("TestComplexMessage"),
			User: TestUser{
				ID:     "123e4567-e89b-12d3-a456-426614174000",
				Name:   "John Doe",
				Email:  "john@example.com",
				Active: true,
			},
			Tags: []string{"tag1", "tag2"},
			Metadata: map[string]interface{}{
				"version": "1.0",
			},
		}
		
		err = validator.Validate(context.Background(), validMsg)
		assert.NoError(t, err)
		
		// Invalid complex message (invalid UUID, invalid email)
		invalidMsg := &TestComplexMessage{
			BaseMessage: contracts.NewBaseMessage("TestComplexMessage"),
			User: TestUser{
				ID:     "invalid-uuid",
				Name:   "John Doe",
				Email:  "invalid-email",
				Active: true,
			},
			Tags: []string{"", "tag2"}, // Empty tag violates minLength
		}
		
		result := validator.ValidateWithSchema(context.Background(), invalidMsg, schema)
		assert.False(t, result.Valid)
		assert.NotEmpty(t, result.Errors)
		
		// Should have errors for UUID, email, and array item
		var foundUUIDError, foundEmailError, foundArrayError bool
		for _, err := range result.Errors {
			if err.Field == "user.id" && err.Code == "FORMAT_VIOLATION" {
				foundUUIDError = true
			}
			if err.Field == "user.email" && err.Code == "FORMAT_VIOLATION" {
				foundEmailError = true
			}
			if err.Field == "tags[0]" && err.Code == "MIN_LENGTH_VIOLATION" {
				foundArrayError = true
			}
		}
		
		assert.True(t, foundUUIDError)
		assert.True(t, foundEmailError)
		assert.True(t, foundArrayError)
	})
}