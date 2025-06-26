package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"

	"github.com/glimte/mmate-go/contracts"
)

// ValidationResult represents the result of message validation
type ValidationResult struct {
	Valid  bool              `json:"valid"`
	Errors []ValidationError `json:"errors,omitempty"`
}

// ValidationError represents a single validation error
type ValidationError struct {
	Field   string      `json:"field"`
	Message string      `json:"message"`
	Code    string      `json:"code"`
	Value   interface{} `json:"value,omitempty"`
}

// Error implements the error interface for ValidationError
func (ve ValidationError) Error() string {
	return fmt.Sprintf("validation error in field '%s': %s", ve.Field, ve.Message)
}

// ValidationRule defines a custom validation rule
type ValidationRule interface {
	Validate(ctx context.Context, field string, value interface{}) *ValidationError
	GetName() string
}

// ValidationRuleFunc is a function adapter for ValidationRule
type ValidationRuleFunc func(ctx context.Context, field string, value interface{}) *ValidationError

func (f ValidationRuleFunc) Validate(ctx context.Context, field string, value interface{}) *ValidationError {
	return f(ctx, field, value)
}

func (f ValidationRuleFunc) GetName() string {
	return "anonymous"
}

// Schema represents a message schema definition
type Schema struct {
	Name        string                  `json:"name"`
	Version     string                  `json:"version"`
	Type        string                  `json:"type"`
	Properties  map[string]*PropertyDef `json:"properties,omitempty"`
	Required    []string                `json:"required,omitempty"`
	Rules       []ValidationRule        `json:"-"`
	Constraints map[string]interface{}  `json:"constraints,omitempty"`
}

// PropertyDef defines validation rules for a message property
type PropertyDef struct {
	Type        string                  `json:"type"`
	Format      string                  `json:"format,omitempty"`
	Pattern     string                  `json:"pattern,omitempty"`
	MinLength   *int                    `json:"minLength,omitempty"`
	MaxLength   *int                    `json:"maxLength,omitempty"`
	Minimum     *float64                `json:"minimum,omitempty"`
	Maximum     *float64                `json:"maximum,omitempty"`
	Enum        []interface{}           `json:"enum,omitempty"`
	Default     interface{}             `json:"default,omitempty"`
	Description string                  `json:"description,omitempty"`
	Items       *PropertyDef            `json:"items,omitempty"`
	Properties  map[string]*PropertyDef `json:"properties,omitempty"`
	Required    []string                `json:"required,omitempty"`
}

// MessageValidator provides message validation capabilities
type MessageValidator struct {
	schemas map[string]*Schema
	rules   map[string]ValidationRule
	mu      sync.RWMutex
}

// ValidatorOption configures the message validator
type ValidatorOption func(*ValidatorConfig)

// ValidatorConfig holds configuration for the validator
type ValidatorConfig struct {
	StrictMode bool
}

// WithStrictMode enables strict validation mode
func WithStrictMode(strict bool) ValidatorOption {
	return func(c *ValidatorConfig) {
		c.StrictMode = strict
	}
}

// NewMessageValidator creates a new message validator
func NewMessageValidator(opts ...ValidatorOption) *MessageValidator {
	config := &ValidatorConfig{
		StrictMode: false,
	}

	for _, opt := range opts {
		opt(config)
	}

	validator := &MessageValidator{
		schemas: make(map[string]*Schema),
		rules:   make(map[string]ValidationRule),
	}

	// Register built-in rules
	validator.registerBuiltInRules()

	return validator
}

// RegisterSchema registers a schema for a message type
func (v *MessageValidator) RegisterSchema(messageType string, schema *Schema) error {
	if messageType == "" {
		return fmt.Errorf("message type cannot be empty")
	}
	if schema == nil {
		return fmt.Errorf("schema cannot be nil")
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	v.schemas[messageType] = schema
	return nil
}

// RegisterRule registers a custom validation rule
func (v *MessageValidator) RegisterRule(rule ValidationRule) error {
	if rule == nil {
		return fmt.Errorf("rule cannot be nil")
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	v.rules[rule.GetName()] = rule
	return nil
}

// GetSchema retrieves a schema by message type
func (v *MessageValidator) GetSchema(messageType string) (*Schema, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	schema, exists := v.schemas[messageType]
	if !exists {
		return nil, fmt.Errorf("schema not found for message type: %s", messageType)
	}

	return schema, nil
}

// Validate validates a message against its registered schema
func (v *MessageValidator) Validate(ctx context.Context, msg contracts.Message) error {
	if msg == nil {
		return fmt.Errorf("message cannot be nil")
	}

	messageType := msg.GetMessageType()
	schema, err := v.GetSchema(messageType)
	if err != nil {
		// If no schema is registered, validation passes (opt-in validation)
		return nil
	}

	result := v.ValidateWithSchema(ctx, msg, schema)
	if !result.Valid {
		return &ValidationError{
			Field:   "message",
			Message: fmt.Sprintf("validation failed with %d errors", len(result.Errors)),
			Code:    "VALIDATION_FAILED",
		}
	}

	return nil
}

// ValidateWithSchema validates a message against a specific schema
func (v *MessageValidator) ValidateWithSchema(ctx context.Context, msg contracts.Message, schema *Schema) *ValidationResult {
	result := &ValidationResult{
		Valid:  true,
		Errors: make([]ValidationError, 0),
	}

	// Convert message to map for validation
	msgData, err := v.messageToMap(msg)
	if err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, ValidationError{
			Field:   "message",
			Message: fmt.Sprintf("failed to convert message to map: %v", err),
			Code:    "CONVERSION_ERROR",
		})
		return result
	}

	// Validate against schema
	v.validateObject(ctx, "", msgData, schema, result)

	// Run custom rules
	for _, rule := range schema.Rules {
		if validationErr := rule.Validate(ctx, "message", msg); validationErr != nil {
			result.Valid = false
			result.Errors = append(result.Errors, *validationErr)
		}
	}

	return result
}

// validateObject validates an object against schema properties
func (v *MessageValidator) validateObject(ctx context.Context, fieldPath string, data map[string]interface{}, schema *Schema, result *ValidationResult) {
	// Check required fields
	for _, required := range schema.Required {
		if _, exists := data[required]; !exists {
			result.Valid = false
			result.Errors = append(result.Errors, ValidationError{
				Field:   v.buildFieldPath(fieldPath, required),
				Message: "required field is missing",
				Code:    "REQUIRED_FIELD_MISSING",
			})
		}
	}

	// Validate properties
	for fieldName, value := range data {
		currentPath := v.buildFieldPath(fieldPath, fieldName)

		if propDef, exists := schema.Properties[fieldName]; exists {
			v.validateProperty(ctx, currentPath, value, propDef, result)
		}
	}
}

// validateProperty validates a single property against its definition
func (v *MessageValidator) validateProperty(ctx context.Context, fieldPath string, value interface{}, propDef *PropertyDef, result *ValidationResult) {
	if value == nil {
		return
	}

	// Type validation
	if propDef.Type != "" && !v.validateType(value, propDef.Type) {
		result.Valid = false
		result.Errors = append(result.Errors, ValidationError{
			Field:   fieldPath,
			Message: fmt.Sprintf("expected type %s, got %T", propDef.Type, value),
			Code:    "TYPE_MISMATCH",
			Value:   value,
		})
		return
	}

	// String validations
	if str, ok := value.(string); ok {
		v.validateString(fieldPath, str, propDef, result)
	}

	// Number validations
	if num, ok := value.(float64); ok {
		v.validateNumber(fieldPath, num, propDef, result)
	}

	// Array validations
	if arr, ok := value.([]interface{}); ok {
		v.validateArray(ctx, fieldPath, arr, propDef, result)
	}

	// Object validations
	if obj, ok := value.(map[string]interface{}); ok && propDef.Properties != nil {
		objSchema := &Schema{
			Properties: propDef.Properties,
			Required:   propDef.Required,
		}
		v.validateObject(ctx, fieldPath, obj, objSchema, result)
	}

	// Enum validation
	if len(propDef.Enum) > 0 {
		v.validateEnum(fieldPath, value, propDef.Enum, result)
	}

	// Format validation
	if propDef.Format != "" {
		v.validateFormat(fieldPath, value, propDef.Format, result)
	}

	// Pattern validation
	if propDef.Pattern != "" {
		v.validatePattern(fieldPath, value, propDef.Pattern, result)
	}
}

// validateType checks if value matches expected type
func (v *MessageValidator) validateType(value interface{}, expectedType string) bool {
	switch expectedType {
	case "string":
		_, ok := value.(string)
		return ok
	case "number":
		_, ok := value.(float64)
		if !ok {
			_, ok = value.(int)
		}
		return ok
	case "integer":
		_, ok := value.(int)
		if !ok {
			if f, ok := value.(float64); ok {
				return f == float64(int(f))
			}
		}
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "array":
		_, ok := value.([]interface{})
		return ok
	case "object":
		_, ok := value.(map[string]interface{})
		return ok
	default:
		return true // Unknown types pass validation
	}
}

// validateString validates string properties
func (v *MessageValidator) validateString(fieldPath, value string, propDef *PropertyDef, result *ValidationResult) {
	// Length validations
	if propDef.MinLength != nil && len(value) < *propDef.MinLength {
		result.Valid = false
		result.Errors = append(result.Errors, ValidationError{
			Field:   fieldPath,
			Message: fmt.Sprintf("string length %d is less than minimum %d", len(value), *propDef.MinLength),
			Code:    "MIN_LENGTH_VIOLATION",
			Value:   value,
		})
	}

	if propDef.MaxLength != nil && len(value) > *propDef.MaxLength {
		result.Valid = false
		result.Errors = append(result.Errors, ValidationError{
			Field:   fieldPath,
			Message: fmt.Sprintf("string length %d exceeds maximum %d", len(value), *propDef.MaxLength),
			Code:    "MAX_LENGTH_VIOLATION",
			Value:   value,
		})
	}
}

// validateNumber validates numeric properties
func (v *MessageValidator) validateNumber(fieldPath string, value float64, propDef *PropertyDef, result *ValidationResult) {
	if propDef.Minimum != nil && value < *propDef.Minimum {
		result.Valid = false
		result.Errors = append(result.Errors, ValidationError{
			Field:   fieldPath,
			Message: fmt.Sprintf("value %f is less than minimum %f", value, *propDef.Minimum),
			Code:    "MINIMUM_VIOLATION",
			Value:   value,
		})
	}

	if propDef.Maximum != nil && value > *propDef.Maximum {
		result.Valid = false
		result.Errors = append(result.Errors, ValidationError{
			Field:   fieldPath,
			Message: fmt.Sprintf("value %f exceeds maximum %f", value, *propDef.Maximum),
			Code:    "MAXIMUM_VIOLATION",
			Value:   value,
		})
	}
}

// validateArray validates array properties
func (v *MessageValidator) validateArray(ctx context.Context, fieldPath string, value []interface{}, propDef *PropertyDef, result *ValidationResult) {
	if propDef.Items != nil {
		for i, item := range value {
			itemPath := fmt.Sprintf("%s[%d]", fieldPath, i)
			v.validateProperty(ctx, itemPath, item, propDef.Items, result)
		}
	}
}

// validateEnum validates enum constraints
func (v *MessageValidator) validateEnum(fieldPath string, value interface{}, enum []interface{}, result *ValidationResult) {
	for _, enumValue := range enum {
		if reflect.DeepEqual(value, enumValue) {
			return
		}
	}

	result.Valid = false
	result.Errors = append(result.Errors, ValidationError{
		Field:   fieldPath,
		Message: fmt.Sprintf("value is not in allowed enum values: %v", enum),
		Code:    "ENUM_VIOLATION",
		Value:   value,
	})
}

// validateFormat validates format constraints
func (v *MessageValidator) validateFormat(fieldPath string, value interface{}, format string, result *ValidationResult) {
	str, ok := value.(string)
	if !ok {
		return
	}

	var isValid bool
	var errorMsg string

	switch format {
	case "email":
		isValid, errorMsg = v.validateEmail(str)
	case "uri":
		isValid, errorMsg = v.validateURI(str)
	case "uuid":
		isValid, errorMsg = v.validateUUID(str)
	case "date":
		isValid, errorMsg = v.validateDate(str)
	case "date-time":
		isValid, errorMsg = v.validateDateTime(str)
	default:
		return // Unknown format, skip validation
	}

	if !isValid {
		result.Valid = false
		result.Errors = append(result.Errors, ValidationError{
			Field:   fieldPath,
			Message: errorMsg,
			Code:    "FORMAT_VIOLATION",
			Value:   value,
		})
	}
}

// validatePattern validates regex pattern constraints
func (v *MessageValidator) validatePattern(fieldPath string, value interface{}, pattern string, result *ValidationResult) {
	str, ok := value.(string)
	if !ok {
		return
	}

	regex, err := regexp.Compile(pattern)
	if err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, ValidationError{
			Field:   fieldPath,
			Message: fmt.Sprintf("invalid regex pattern: %s", pattern),
			Code:    "INVALID_PATTERN",
			Value:   value,
		})
		return
	}

	if !regex.MatchString(str) {
		result.Valid = false
		result.Errors = append(result.Errors, ValidationError{
			Field:   fieldPath,
			Message: fmt.Sprintf("value does not match pattern: %s", pattern),
			Code:    "PATTERN_VIOLATION",
			Value:   value,
		})
	}
}

// Format validation functions
func (v *MessageValidator) validateEmail(value string) (bool, string) {
	emailRegex := regexp.MustCompile(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$`)
	if !emailRegex.MatchString(value) {
		return false, "invalid email format"
	}
	return true, ""
}

func (v *MessageValidator) validateURI(value string) (bool, string) {
	if !strings.Contains(value, "://") {
		return false, "invalid URI format"
	}
	return true, ""
}

func (v *MessageValidator) validateUUID(value string) (bool, string) {
	uuidRegex := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	if !uuidRegex.MatchString(strings.ToLower(value)) {
		return false, "invalid UUID format"
	}
	return true, ""
}

func (v *MessageValidator) validateDate(value string) (bool, string) {
	dateRegex := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	if !dateRegex.MatchString(value) {
		return false, "invalid date format (expected YYYY-MM-DD)"
	}
	return true, ""
}

func (v *MessageValidator) validateDateTime(value string) (bool, string) {
	// ISO 8601 format
	dateTimeRegex := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z?$`)
	if !dateTimeRegex.MatchString(value) {
		return false, "invalid date-time format (expected ISO 8601)"
	}
	return true, ""
}

// buildFieldPath constructs a field path for error reporting
func (v *MessageValidator) buildFieldPath(parent, field string) string {
	if parent == "" {
		return field
	}
	return fmt.Sprintf("%s.%s", parent, field)
}

// messageToMap converts a message to a map for validation
func (v *MessageValidator) messageToMap(msg contracts.Message) (map[string]interface{}, error) {
	data, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}

	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return result, nil
}

// registerBuiltInRules registers built-in validation rules
func (v *MessageValidator) registerBuiltInRules() {
	// Non-empty string rule
	v.rules["non-empty"] = ValidationRuleFunc(func(ctx context.Context, field string, value interface{}) *ValidationError {
		if str, ok := value.(string); ok && strings.TrimSpace(str) == "" {
			return &ValidationError{
				Field:   field,
				Message: "value cannot be empty",
				Code:    "NON_EMPTY_VIOLATION",
				Value:   value,
			}
		}
		return nil
	})

	// Positive number rule
	v.rules["positive"] = ValidationRuleFunc(func(ctx context.Context, field string, value interface{}) *ValidationError {
		if num, ok := value.(float64); ok && num <= 0 {
			return &ValidationError{
				Field:   field,
				Message: "value must be positive",
				Code:    "POSITIVE_VIOLATION",
				Value:   value,
			}
		}
		return nil
	})
}
