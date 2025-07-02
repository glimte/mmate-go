package contracts

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEndpointContract_MatchesVersion(t *testing.T) {
	contract := &EndpointContract{
		Version: "1.2.3",
	}

	tests := []struct {
		name      string
		requested string
		expected  bool
	}{
		// Empty version
		{"empty version matches any", "", true},
		
		// Exact matches
		{"exact match", "1.2.3", true},
		{"exact mismatch", "1.2.4", false},
		
		// Wildcard patterns
		{"major wildcard", "1.x", true},
		{"major wildcard mismatch", "2.x", false},
		{"minor wildcard", "1.2.x", true},
		{"minor wildcard mismatch", "1.3.x", false},
		{"full wildcard", "x.x.x", true},
		
		// Semantic version constraints
		{"caret range match", "^1.2.0", true},
		{"caret range too high", "^1.3.0", false},
		{"caret range major mismatch", "^2.0.0", false},
		{"tilde range match", "~1.2.0", true},
		{"tilde range mismatch", "~1.3.0", false},
		{"greater than", ">1.2.0", true},
		{"greater than equal", ">=1.2.3", true},
		{"less than", "<2.0.0", true},
		{"less than mismatch", "<1.2.0", false},
		{"range", ">=1.0.0 <2.0.0", true},
		{"range mismatch", ">=2.0.0", false},
		
		// Invalid patterns
		{"invalid constraint", "invalid", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := contract.matchesVersion(tt.requested)
			assert.Equal(t, tt.expected, result, "Version %s should match=%v", tt.requested, tt.expected)
		})
	}
}

func TestEndpointContract_MatchesVersion_NonSemver(t *testing.T) {
	// Test with non-semver version
	contract := &EndpointContract{
		Version: "alpha",
	}

	tests := []struct {
		name      string
		requested string
		expected  bool
	}{
		{"empty matches", "", true},
		{"exact match", "alpha", true},
		{"exact mismatch", "beta", false},
		{"semver constraint fails", "^1.0.0", false},
		{"wildcard fails", "1.x", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := contract.matchesVersion(tt.requested)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEndpointContract_MatchesPattern(t *testing.T) {
	contract := &EndpointContract{
		EndpointID: "order.validate.items",
	}

	tests := []struct {
		name     string
		pattern  string
		expected bool
	}{
		// Empty and exact
		{"empty pattern matches", "", true},
		{"exact match", "order.validate.items", true},
		{"exact mismatch", "order.validate.item", false},
		
		// Trailing wildcards (original functionality)
		{"trailing wildcard", "order.*", true},
		{"trailing wildcard deeper", "order.validate.*", true},
		{"trailing wildcard mismatch", "inventory.*", false},
		
		// Leading wildcards
		{"leading wildcard", "*.items", true},
		{"leading wildcard deeper", "*.validate.items", true},
		{"leading wildcard mismatch", "*.products", false},
		
		// Middle wildcards
		{"middle wildcard", "order.*.items", true},
		{"middle wildcard mismatch", "order.*.products", false},
		
		// Multiple wildcards
		{"multiple wildcards", "*.validate.*", true},
		{"all wildcard", "*", true},
		{"complex pattern", "order.*.*", true},
		
		// No wildcards
		{"no wildcard mismatch", "order", false},
		{"partial match no wildcard", "order.validate", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := contract.matchesPattern(tt.pattern)
			assert.Equal(t, tt.expected, result, "Pattern %s should match=%v", tt.pattern, tt.expected)
		})
	}
}

func TestEndpointContract_Matches_Combined(t *testing.T) {
	contract := &EndpointContract{
		EndpointID: "order.validate",
		Version:    "2.1.0",
	}

	tests := []struct {
		name     string
		pattern  string
		version  string
		expected bool
	}{
		// Both match
		{"both match", "order.*", "^2.0.0", true},
		{"exact both", "order.validate", "2.1.0", true},
		
		// Pattern fails
		{"pattern fails", "inventory.*", "^2.0.0", false},
		
		// Version fails
		{"version fails", "order.*", "^3.0.0", false},
		
		// Both fail
		{"both fail", "inventory.*", "^3.0.0", false},
		
		// Empty values
		{"empty both", "", "", true},
		{"empty pattern", "", "^2.0.0", true},
		{"empty version", "order.*", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := contract.Matches(tt.pattern, tt.version)
			assert.Equal(t, tt.expected, result)
		})
	}
}