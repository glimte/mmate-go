// Package schema provides message validation and schema management for the mmate messaging framework.
//
// The schema package enables strict validation of message structures to ensure
// data integrity and contract compliance across distributed services. It supports
// JSON Schema validation, custom validation rules, and schema evolution.
//
// Key features:
//   - JSON Schema validation for message structures
//   - Custom validation rules and constraints
//   - Schema versioning and evolution
//   - Message transformation and normalization
//   - Cross-language schema compatibility
//   - Runtime validation with detailed error reporting
//
// Basic usage:
//
//	// Define schema
//	validator := schema.NewMessageValidator()
//	
//	// Register message schema
//	err := validator.RegisterSchema("UserCreatedEvent", userSchema)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	
//	// Validate message
//	err = validator.Validate(ctx, message)
//	if err != nil {
//	    log.Printf("Validation failed: %v", err)
//	}
//
// The schema package supports:
//   - JSON Schema Draft 7 validation
//   - Custom validation functions
//   - Schema inheritance and composition
//   - Conditional validation rules
//   - Format validation (email, URI, etc.)
//   - Semantic versioning for schemas
//   - Migration between schema versions
package schema