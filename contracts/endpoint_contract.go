package contracts

import (
	"encoding/json"
	"regexp"
	"strings"
	"time"
	
	"github.com/Masterminds/semver/v3"
)

// EndpointContract defines a discoverable service endpoint
type EndpointContract struct {
	// Identity
	EndpointID   string    `json:"endpointId"`   // e.g., "order.validate"
	Version      string    `json:"version"`      // e.g., "1.0.0"
	ServiceName  string    `json:"serviceName"`  // e.g., "order-service"
	Description  string    `json:"description"`
	
	// Queue configuration
	Queue        string    `json:"queue"`        // e.g., "order.validate"
	
	// Message types (must be registered in schema registry)
	InputType    string    `json:"inputType"`    // e.g., "ValidateOrderRequest"
	OutputType   string    `json:"outputType"`   // e.g., "ValidateOrderResponse"
	
	// Optional JSON Schema for validation
	InputSchema  json.RawMessage `json:"inputSchema,omitempty"`
	OutputSchema json.RawMessage `json:"outputSchema,omitempty"`
	
	// Configuration
	Timeout      string    `json:"timeout,omitempty"`      // e.g., "30s"
	MaxRetries   int       `json:"maxRetries,omitempty"`
	
	// Metadata
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
	TTL          string    `json:"ttl,omitempty"` // How long contract is valid
}

// ContractDiscoveryRequest is used to discover endpoints
type ContractDiscoveryRequest struct {
	BaseMessage
	Pattern string `json:"pattern"` // e.g., "order.*" or specific "order.validate"
	Version string `json:"version,omitempty"` // e.g., "1.x" or "1.0.0"
}

// ContractDiscoveryResponse contains discovered contracts
type ContractDiscoveryResponse struct {
	BaseMessage
	Contracts []EndpointContract `json:"contracts"`
}

// ContractAnnouncement is broadcast when endpoints are registered
type ContractAnnouncement struct {
	BaseMessage
	Contracts []EndpointContract `json:"contracts"`
	Action    string `json:"action"` // "registered" or "unregistered"
}

// GetType implements Message interface for discovery messages
func (r *ContractDiscoveryRequest) GetType() string {
	return "ContractDiscoveryRequest"
}

func (r *ContractDiscoveryResponse) GetType() string {
	return "ContractDiscoveryResponse"
}

func (a *ContractAnnouncement) GetType() string {
	return "ContractAnnouncement"
}

// IsValid checks if the endpoint contract is valid
func (c *EndpointContract) IsValid() bool {
	return c.EndpointID != "" && 
		c.Queue != "" && 
		c.InputType != "" && 
		c.OutputType != "" &&
		c.ServiceName != ""
}

// Matches checks if contract matches a pattern and version
func (c *EndpointContract) Matches(pattern string, version string) bool {
	// Pattern matching
	if pattern != "" && !c.matchesPattern(pattern) {
		return false
	}
	
	// Version matching
	if version != "" && !c.matchesVersion(version) {
		return false
	}
	
	return true
}

// matchesPattern checks if endpoint ID matches the given pattern
func (c *EndpointContract) matchesPattern(pattern string) bool {
	// Empty pattern matches everything
	if pattern == "" {
		return true
	}
	
	// Exact match
	if pattern == c.EndpointID {
		return true
	}
	
	// No wildcards means exact match required
	if !strings.Contains(pattern, "*") {
		return false
	}
	
	// Convert pattern to regex
	// Escape special regex characters except *
	regexPattern := regexp.QuoteMeta(pattern)
	// Replace escaped \* with regex .*
	regexPattern = strings.ReplaceAll(regexPattern, "\\*", ".*")
	// Anchor to match entire string
	regexPattern = "^" + regexPattern + "$"
	
	matched, err := regexp.MatchString(regexPattern, c.EndpointID)
	return err == nil && matched
}

// matchesVersion checks if contract version matches requested version
func (c *EndpointContract) matchesVersion(requestedVersion string) bool {
	// Empty version matches any version
	if requestedVersion == "" {
		return true
	}
	
	// Try exact match first
	if c.Version == requestedVersion {
		return true
	}
	
	// Handle simple wildcards like "1.x" or "2.x.x"
	if strings.Contains(requestedVersion, "x") {
		pattern := strings.ReplaceAll(requestedVersion, "x", "[0-9]+")
		pattern = strings.ReplaceAll(pattern, ".", "\\.")
		pattern = "^" + pattern + "$"
		matched, err := regexp.MatchString(pattern, c.Version)
		if err == nil && matched {
			return true
		}
	}
	
	// Try semantic version constraint matching
	constraint, err := semver.NewConstraint(requestedVersion)
	if err != nil {
		// If it's not a valid constraint, fall back to exact match
		return false
	}
	
	version, err := semver.NewVersion(c.Version)
	if err != nil {
		// If contract version is not valid semver, can't match
		return false
	}
	
	return constraint.Check(version)
}