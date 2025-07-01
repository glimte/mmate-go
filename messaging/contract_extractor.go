package messaging

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/glimte/mmate-go/contracts"
)

// ContractExtractor extracts contract information from registered handlers
type ContractExtractor interface {
	// ExtractFromHandler extracts contract information when a handler is registered
	ExtractFromHandler(ctx context.Context, messageType contracts.Message, handler MessageHandler, options HandlerOptions) error
	
	// ExtractFromWorkflow extracts contract information when a workflow is registered
	ExtractFromWorkflow(ctx context.Context, workflowID string, workflowName string, inputType contracts.Message) error
}

// contractExtractorImpl implements ContractExtractor
type contractExtractorImpl struct {
	contractDiscovery *ContractDiscovery
	serviceName       string
}

// NewContractExtractor creates a new contract extractor
func NewContractExtractor(contractDiscovery *ContractDiscovery, serviceName string) ContractExtractor {
	return &contractExtractorImpl{
		contractDiscovery: contractDiscovery,
		serviceName:       serviceName,
	}
}

// ExtractFromHandler extracts and publishes contract information from a handler registration
func (ce *contractExtractorImpl) ExtractFromHandler(ctx context.Context, messageType contracts.Message, handler MessageHandler, options HandlerOptions) error {
	if ce.contractDiscovery == nil {
		return nil // Contract discovery not enabled
	}

	msgType := reflect.TypeOf(messageType)
	if msgType.Kind() == reflect.Ptr {
		msgType = msgType.Elem()
	}

	typeName := msgType.Name()
	if typeName == "" {
		return fmt.Errorf("message type must have a name")
	}

	// Determine if this handler produces a reply
	outputType := ""
	if isCommand(messageType) || isQuery(messageType) {
		// Commands and queries typically have replies
		outputType = inferReplyType(typeName)
	}
	
	// For handlers without output type, use a placeholder
	if outputType == "" {
		outputType = "None"
	}

	// Create endpoint contract
	contract := &contracts.EndpointContract{
		EndpointID:   fmt.Sprintf("%s.%s", ce.serviceName, typeName),
		Version:      "1.0.0",
		ServiceName:  ce.serviceName,
		Description:  fmt.Sprintf("Handles %s messages", typeName),
		Queue:        options.Queue,
		InputType:    typeName,
		OutputType:   outputType,
		Timeout:      "30s", // Default timeout
		MaxRetries:   3,     // Default retries
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	// Register the contract
	return ce.contractDiscovery.RegisterEndpoint(ctx, contract)
}

// ExtractFromWorkflow extracts and publishes contract information from a workflow registration
func (ce *contractExtractorImpl) ExtractFromWorkflow(ctx context.Context, workflowID string, workflowName string, inputType contracts.Message) error {
	if ce.contractDiscovery == nil {
		return nil // Contract discovery not enabled
	}

	inputTypeName := ""
	outputTypeName := ""
	
	if inputType != nil {
		msgType := reflect.TypeOf(inputType)
		if msgType.Kind() == reflect.Ptr {
			msgType = msgType.Elem()
		}
		inputTypeName = msgType.Name()
		
		// Infer output type for workflows that process commands
		outputTypeName = inferReplyType(inputTypeName)
	}

	// For workflows without explicit input type, use a placeholder
	if inputTypeName == "" {
		inputTypeName = "WorkflowTrigger"
	}
	if outputTypeName == "" {
		outputTypeName = "WorkflowResult"
	}

	// Create workflow contract
	contract := &contracts.EndpointContract{
		EndpointID:   fmt.Sprintf("workflow.%s", workflowID),
		Version:      "1.0.0",
		ServiceName:  ce.serviceName,
		Description:  fmt.Sprintf("%s - StageFlow workflow", workflowName),
		Queue:        fmt.Sprintf("%s-queue", ce.serviceName), // Workflows use service queue
		InputType:    inputTypeName,
		OutputType:   outputTypeName, // Workflows may have replies for commands
		Timeout:      "120s", // Longer timeout for workflows
		MaxRetries:   1,      // Workflows handle their own retries
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	// Register the contract
	return ce.contractDiscovery.RegisterEndpoint(ctx, contract)
}

// isCommand checks if a message is a command
func isCommand(msg contracts.Message) bool {
	_, ok := msg.(contracts.Command)
	return ok
}

// isQuery checks if a message is a query
func isQuery(msg contracts.Message) bool {
	_, ok := msg.(contracts.Query)
	return ok
}

// inferReplyType attempts to infer the reply type from the message type name
func inferReplyType(typeName string) string {
	// Common patterns:
	// ProcessOrderCommand -> OrderProcessedReply
	// GetOrderQuery -> OrderDetailsReply
	
	if len(typeName) > 7 && typeName[len(typeName)-7:] == "Command" {
		// Remove "Command" suffix and add "Reply"
		base := typeName[:len(typeName)-7]
		// Try to make it past tense if it's a verb
		if len(base) > 7 && base[:7] == "Process" {
			return base[7:] + "ProcessedReply"
		}
		return base + "Reply"
	}
	
	if len(typeName) > 5 && typeName[len(typeName)-5:] == "Query" {
		// Remove "Query" suffix and add "Reply"
		base := typeName[:len(typeName)-5]
		if len(base) > 3 && base[:3] == "Get" {
			return base[3:] + "Reply"
		}
		return base + "Reply"
	}
	
	return "" // No reply type for events or other message types
}