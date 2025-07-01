package schema

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/serialization"
)

// JSONSchemaGenerator generates JSON schemas from Go types
type JSONSchemaGenerator struct {
	// Track types we've already seen to avoid infinite recursion
	seen map[reflect.Type]bool
}

// NewJSONSchemaGenerator creates a new JSON schema generator
func NewJSONSchemaGenerator() *JSONSchemaGenerator {
	return &JSONSchemaGenerator{
		seen: make(map[reflect.Type]bool),
	}
}

// GenerateFromMessage generates a JSON schema from a message instance
func (g *JSONSchemaGenerator) GenerateFromMessage(msg contracts.Message) (json.RawMessage, error) {
	g.seen = make(map[reflect.Type]bool) // Reset for each generation
	
	msgType := reflect.TypeOf(msg)
	if msgType.Kind() == reflect.Ptr {
		msgType = msgType.Elem()
	}
	
	schema := g.generateSchema(msgType, msgType.Name())
	
	// Add metadata
	schema["$schema"] = "http://json-schema.org/draft-07/schema#"
	schema["title"] = msgType.Name()
	schema["description"] = fmt.Sprintf("Schema for %s message", msgType.Name())
	
	return json.Marshal(schema)
}

// GenerateFromType generates a JSON schema from a type name
func (g *JSONSchemaGenerator) GenerateFromType(typeName string) (json.RawMessage, error) {
	// Try to get type from registry
	typeRegistry := serialization.GetGlobalRegistry()
	factory, err := typeRegistry.GetFactory(typeName)
	if err != nil {
		// Return a basic schema for unknown types
		schema := map[string]interface{}{
			"$schema":     "http://json-schema.org/draft-07/schema#",
			"title":       typeName,
			"description": fmt.Sprintf("Schema for %s", typeName),
			"type":        "object",
			"properties": map[string]interface{}{
				"id": map[string]interface{}{
					"type":        "string",
					"description": "Unique message identifier",
				},
				"type": map[string]interface{}{
					"type":        "string",
					"description": "Message type name",
				},
				"timestamp": map[string]interface{}{
					"type":        "string",
					"format":      "date-time",
					"description": "Message timestamp",
				},
			},
			"required": []string{"id", "type", "timestamp"},
		}
		return json.Marshal(schema)
	}
	
	// Create instance and generate schema
	msg := factory()
	return g.GenerateFromMessage(msg)
}

// generateSchema recursively generates a JSON schema for a type
func (g *JSONSchemaGenerator) generateSchema(t reflect.Type, fieldName string) map[string]interface{} {
	// Handle pointers
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	
	// Check if we've seen this type before (avoid infinite recursion)
	if _, seen := g.seen[t]; seen && t.Kind() == reflect.Struct {
		return map[string]interface{}{
			"$ref": fmt.Sprintf("#/definitions/%s", t.Name()),
		}
	}
	
	switch t.Kind() {
	case reflect.String:
		return map[string]interface{}{"type": "string"}
		
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return map[string]interface{}{"type": "integer"}
		
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]interface{}{"type": "integer", "minimum": 0}
		
	case reflect.Float32, reflect.Float64:
		return map[string]interface{}{"type": "number"}
		
	case reflect.Bool:
		return map[string]interface{}{"type": "boolean"}
		
	case reflect.Slice, reflect.Array:
		itemSchema := g.generateSchema(t.Elem(), "item")
		return map[string]interface{}{
			"type":  "array",
			"items": itemSchema,
		}
		
	case reflect.Map:
		valueSchema := g.generateSchema(t.Elem(), "value")
		return map[string]interface{}{
			"type": "object",
			"additionalProperties": valueSchema,
		}
		
	case reflect.Struct:
		// Handle special types
		if t == reflect.TypeOf(time.Time{}) {
			return map[string]interface{}{
				"type":   "string",
				"format": "date-time",
			}
		}
		
		// Mark this type as seen
		g.seen[t] = true
		
		properties := make(map[string]interface{})
		required := make([]string, 0)
		
		// Iterate through struct fields
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			
			// Skip unexported fields
			if field.PkgPath != "" {
				continue
			}
			
			// Get JSON tag
			jsonTag := field.Tag.Get("json")
			if jsonTag == "-" {
				continue
			}
			
			fieldName := field.Name
			omitempty := false
			
			if jsonTag != "" {
				parts := strings.Split(jsonTag, ",")
				if parts[0] != "" {
					fieldName = parts[0]
				}
				for _, part := range parts[1:] {
					if part == "omitempty" {
						omitempty = true
					}
				}
			} else {
				// Convert to lowercase for JSON
				fieldName = strings.ToLower(fieldName[:1]) + fieldName[1:]
			}
			
			// Generate schema for this field
			fieldSchema := g.generateSchema(field.Type, field.Name)
			
			// Add description from struct tag if available
			if desc := field.Tag.Get("description"); desc != "" {
				// fieldSchema is already a map[string]interface{}
				fieldSchema["description"] = desc
			}
			
			properties[fieldName] = fieldSchema
			
			// Add to required fields if not omitempty
			if !omitempty {
				required = append(required, fieldName)
			}
		}
		
		schema := map[string]interface{}{
			"type":       "object",
			"properties": properties,
		}
		
		if len(required) > 0 {
			schema["required"] = required
		}
		
		return schema
		
	case reflect.Interface:
		// For interfaces, we can't determine the concrete type
		return map[string]interface{}{
			"type":        "object",
			"description": "Interface type - actual schema depends on concrete implementation",
		}
		
	default:
		// Unknown type
		return map[string]interface{}{
			"type":        "string",
			"description": fmt.Sprintf("Unknown type: %v", t.Kind()),
		}
	}
}

// GenerateForContract generates schemas for a contract's input and output types
func (g *JSONSchemaGenerator) GenerateForContract(contract *contracts.EndpointContract) error {
	// Try to generate input schema
	if contract.InputType != "" && contract.InputType != "None" && contract.InputType != "WorkflowTrigger" {
		if schema, err := g.GenerateFromType(contract.InputType); err == nil {
			contract.InputSchema = schema
		}
	}
	
	// Try to generate output schema
	if contract.OutputType != "" && contract.OutputType != "None" && contract.OutputType != "WorkflowResult" {
		if schema, err := g.GenerateFromType(contract.OutputType); err == nil {
			contract.OutputSchema = schema
		}
	}
	
	return nil
}