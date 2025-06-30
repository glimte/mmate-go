package contracts

import (
	"encoding/json"
	"time"
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
	// Simple pattern matching for now
	// TODO: Implement proper pattern matching (e.g., "order.*")
	if pattern != "" && pattern != c.EndpointID {
		// Check if pattern ends with * for wildcard matching
		if len(pattern) > 1 && pattern[len(pattern)-1] == '*' {
			prefix := pattern[:len(pattern)-1]
			if len(c.EndpointID) < len(prefix) || c.EndpointID[:len(prefix)] != prefix {
				return false
			}
		} else {
			return false
		}
	}
	
	// Version matching
	if version != "" && !c.matchesVersion(version) {
		return false
	}
	
	return true
}

// matchesVersion checks if contract version matches requested version
func (c *EndpointContract) matchesVersion(requestedVersion string) bool {
	// Simple version matching for now
	// TODO: Implement semantic version matching (e.g., "1.x", "^1.0.0")
	return c.Version == requestedVersion || requestedVersion == ""
}