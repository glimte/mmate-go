package contracts

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEndpointContract_IsValid(t *testing.T) {
	tests := []struct {
		name     string
		contract EndpointContract
		want     bool
	}{
		{
			name: "valid contract",
			contract: EndpointContract{
				EndpointID:  "order.validate",
				Queue:       "order.validate",
				InputType:   "ValidateOrderRequest",
				OutputType:  "ValidateOrderResponse",
				ServiceName: "order-service",
			},
			want: true,
		},
		{
			name: "missing endpoint ID",
			contract: EndpointContract{
				Queue:       "order.validate",
				InputType:   "ValidateOrderRequest",
				OutputType:  "ValidateOrderResponse",
				ServiceName: "order-service",
			},
			want: false,
		},
		{
			name: "missing queue",
			contract: EndpointContract{
				EndpointID:  "order.validate",
				InputType:   "ValidateOrderRequest",
				OutputType:  "ValidateOrderResponse",
				ServiceName: "order-service",
			},
			want: false,
		},
		{
			name: "missing input type",
			contract: EndpointContract{
				EndpointID:  "order.validate",
				Queue:       "order.validate",
				OutputType:  "ValidateOrderResponse",
				ServiceName: "order-service",
			},
			want: false,
		},
		{
			name: "missing output type",
			contract: EndpointContract{
				EndpointID:  "order.validate",
				Queue:       "order.validate",
				InputType:   "ValidateOrderRequest",
				ServiceName: "order-service",
			},
			want: false,
		},
		{
			name: "missing service name",
			contract: EndpointContract{
				EndpointID: "order.validate",
				Queue:      "order.validate",
				InputType:  "ValidateOrderRequest",
				OutputType: "ValidateOrderResponse",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.contract.IsValid(); got != tt.want {
				t.Errorf("EndpointContract.IsValid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEndpointContract_Matches(t *testing.T) {
	contract := EndpointContract{
		EndpointID: "order.validate",
		Version:    "1.0.0",
	}

	tests := []struct {
		name    string
		pattern string
		version string
		want    bool
	}{
		{
			name:    "exact match",
			pattern: "order.validate",
			version: "1.0.0",
			want:    true,
		},
		{
			name:    "pattern with wildcard",
			pattern: "order.*",
			version: "",
			want:    true,
		},
		{
			name:    "pattern mismatch",
			pattern: "inventory.*",
			version: "",
			want:    false,
		},
		{
			name:    "version mismatch",
			pattern: "order.validate",
			version: "2.0.0",
			want:    false,
		},
		{
			name:    "empty pattern matches",
			pattern: "",
			version: "",
			want:    true,
		},
		{
			name:    "wildcard at end only",
			pattern: "ord*",
			version: "",
			want:    true,
		},
		{
			name:    "wildcard too short",
			pattern: "order.val*",
			version: "",
			want:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := contract.Matches(tt.pattern, tt.version); got != tt.want {
				t.Errorf("EndpointContract.Matches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestContractDiscoveryRequest_GetType(t *testing.T) {
	req := &ContractDiscoveryRequest{}
	if got := req.GetType(); got != "ContractDiscoveryRequest" {
		t.Errorf("ContractDiscoveryRequest.GetType() = %v, want %v", got, "ContractDiscoveryRequest")
	}
}

func TestContractDiscoveryResponse_GetType(t *testing.T) {
	resp := &ContractDiscoveryResponse{}
	if got := resp.GetType(); got != "ContractDiscoveryResponse" {
		t.Errorf("ContractDiscoveryResponse.GetType() = %v, want %v", got, "ContractDiscoveryResponse")
	}
}

func TestContractAnnouncement_GetType(t *testing.T) {
	ann := &ContractAnnouncement{}
	if got := ann.GetType(); got != "ContractAnnouncement" {
		t.Errorf("ContractAnnouncement.GetType() = %v, want %v", got, "ContractAnnouncement")
	}
}

func TestEndpointContract_JSONSerialization(t *testing.T) {
	original := EndpointContract{
		EndpointID:   "order.validate",
		Version:      "1.0.0",
		ServiceName:  "order-service",
		Description:  "Validates order requests",
		Queue:        "order.validate",
		InputType:    "ValidateOrderRequest",
		OutputType:   "ValidateOrderResponse",
		InputSchema:  json.RawMessage(`{"type":"object"}`),
		OutputSchema: json.RawMessage(`{"type":"object"}`),
		Timeout:      "30s",
		MaxRetries:   3,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
		TTL:          "1h",
	}

	// Marshal to JSON
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Failed to marshal contract: %v", err)
	}

	// Unmarshal back
	var decoded EndpointContract
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal contract: %v", err)
	}

	// Compare fields
	if decoded.EndpointID != original.EndpointID {
		t.Errorf("EndpointID mismatch: got %v, want %v", decoded.EndpointID, original.EndpointID)
	}
	if decoded.Version != original.Version {
		t.Errorf("Version mismatch: got %v, want %v", decoded.Version, original.Version)
	}
	if decoded.ServiceName != original.ServiceName {
		t.Errorf("ServiceName mismatch: got %v, want %v", decoded.ServiceName, original.ServiceName)
	}
	if decoded.Queue != original.Queue {
		t.Errorf("Queue mismatch: got %v, want %v", decoded.Queue, original.Queue)
	}
	if decoded.InputType != original.InputType {
		t.Errorf("InputType mismatch: got %v, want %v", decoded.InputType, original.InputType)
	}
	if decoded.OutputType != original.OutputType {
		t.Errorf("OutputType mismatch: got %v, want %v", decoded.OutputType, original.OutputType)
	}
}