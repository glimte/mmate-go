package interceptors

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/glimte/mmate-go/contracts"
)

// Test message implementation
type TestPipelineMessage struct {
	contracts.BaseMessage
	Content string `json:"content"`
}

func NewTestPipelineMessage(content string) *TestPipelineMessage {
	msg := &TestPipelineMessage{
		BaseMessage: contracts.NewBaseMessage("TestPipelineMessage"),
		Content:     content,
	}
	return msg
}

// Test interceptor that records execution
type TestPipelineInterceptor struct {
	name string
	log  *[]string
	mu   *sync.Mutex
}

func NewTestPipelineInterceptor(name string, log *[]string, mu *sync.Mutex) *TestPipelineInterceptor {
	return &TestPipelineInterceptor{
		name: name,
		log:  log,
		mu:   mu,
	}
}

func (i *TestPipelineInterceptor) Intercept(ctx context.Context, msg contracts.Message, next MessageHandler) error {
	i.mu.Lock()
	*i.log = append(*i.log, fmt.Sprintf("before:%s", i.name))
	i.mu.Unlock()

	err := next.Handle(ctx, msg)

	i.mu.Lock()
	*i.log = append(*i.log, fmt.Sprintf("after:%s", i.name))
	i.mu.Unlock()

	return err
}

func (i *TestPipelineInterceptor) Name() string {
	return i.name
}

func TestPipeline_NewPipeline(t *testing.T) {
	// Test creating pipeline with interceptors
	interceptor1 := NewTestPipelineInterceptor("test1", nil, nil)
	interceptor2 := NewTestPipelineInterceptor("test2", nil, nil)
	
	pipeline := NewPipeline(interceptor1, interceptor2)
	
	if len(pipeline.interceptors) != 2 {
		t.Errorf("Expected 2 interceptors, got %d", len(pipeline.interceptors))
	}
	
	if pipeline.logger == nil {
		t.Error("Expected logger to be set")
	}
}

func TestPipeline_Use(t *testing.T) {
	pipeline := NewPipeline()
	
	interceptor1 := NewTestPipelineInterceptor("test1", nil, nil)
	interceptor2 := NewTestPipelineInterceptor("test2", nil, nil)
	
	// Test fluent interface
	result := pipeline.Use(interceptor1).Use(interceptor2)
	
	if result != pipeline {
		t.Error("Use should return the same pipeline for fluent interface")
	}
	
	if len(pipeline.interceptors) != 2 {
		t.Errorf("Expected 2 interceptors, got %d", len(pipeline.interceptors))
	}
}

func TestPipeline_ExecuteWithoutInterceptors(t *testing.T) {
	pipeline := NewPipeline()
	
	var executionLog []string
	var mu sync.Mutex
	
	handler := MessageHandlerFunc(func(ctx context.Context, msg contracts.Message) error {
		mu.Lock()
		executionLog = append(executionLog, "handler")
		mu.Unlock()
		return nil
	})
	
	msg := NewTestPipelineMessage("test")
	err := pipeline.Execute(context.Background(), msg, handler)
	
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	
	mu.Lock()
	if len(executionLog) != 1 || executionLog[0] != "handler" {
		t.Errorf("Expected ['handler'], got %v", executionLog)
	}
	mu.Unlock()
}

func TestPipeline_ExecuteWithInterceptors(t *testing.T) {
	var executionLog []string
	var mu sync.Mutex
	
	// Create pipeline with interceptors
	pipeline := NewPipeline()
	pipeline.Use(NewTestPipelineInterceptor("First", &executionLog, &mu))
	pipeline.Use(NewTestPipelineInterceptor("Second", &executionLog, &mu))
	pipeline.Use(NewTestPipelineInterceptor("Third", &executionLog, &mu))
	
	// Create handler that logs execution
	handler := MessageHandlerFunc(func(ctx context.Context, msg contracts.Message) error {
		mu.Lock()
		executionLog = append(executionLog, "handler")
		mu.Unlock()
		return nil
	})
	
	// Execute pipeline
	msg := NewTestPipelineMessage("test")
	err := pipeline.Execute(context.Background(), msg, handler)
	
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	
	// Verify execution order
	expected := []string{
		"before:First",
		"before:Second", 
		"before:Third",
		"handler",
		"after:Third",
		"after:Second",
		"after:First",
	}
	
	mu.Lock()
	actual := make([]string, len(executionLog))
	copy(actual, executionLog)
	mu.Unlock()
	
	if len(actual) != len(expected) {
		t.Errorf("Expected %d log entries, got %d\nExpected: %v\nActual: %v", 
			len(expected), len(actual), expected, actual)
	}
	
	for i, expectedEntry := range expected {
		if i >= len(actual) || actual[i] != expectedEntry {
			t.Errorf("Execution order mismatch at index %d: expected %s, got %s", 
				i, expectedEntry, actual[i])
		}
	}
}

func TestPipeline_ExecuteWithError(t *testing.T) {
	var executionLog []string
	var mu sync.Mutex
	
	// Create pipeline with interceptors
	pipeline := NewPipeline()
	pipeline.Use(NewTestPipelineInterceptor("First", &executionLog, &mu))
	pipeline.Use(NewTestPipelineInterceptor("Second", &executionLog, &mu))
	
	// Create handler that returns error
	expectedError := fmt.Errorf("handler error")
	handler := MessageHandlerFunc(func(ctx context.Context, msg contracts.Message) error {
		mu.Lock()
		executionLog = append(executionLog, "handler")
		mu.Unlock()
		return expectedError
	})
	
	// Execute pipeline
	msg := NewTestPipelineMessage("test")
	err := pipeline.Execute(context.Background(), msg, handler)
	
	if err != expectedError {
		t.Errorf("Expected error %v, got %v", expectedError, err)
	}
	
	// Verify interceptors still executed in proper order
	expected := []string{
		"before:First",
		"before:Second",
		"handler",
		"after:Second",
		"after:First",
	}
	
	mu.Lock()
	actual := make([]string, len(executionLog))
	copy(actual, executionLog)
	mu.Unlock()
	
	if len(actual) != len(expected) {
		t.Errorf("Expected %d log entries, got %d\nExpected: %v\nActual: %v", 
			len(expected), len(actual), expected, actual)
	}
}

func TestPipeline_BuilderPattern(t *testing.T) {
	// Test the documented pattern from mmate-docs
	var executionLog []string
	var mu sync.Mutex
	
	// Create pipeline following documented pattern
	pipeline := NewPipeline()
	pipeline.Use(NewTestPipelineInterceptor("Security", &executionLog, &mu))
	pipeline.Use(NewTestPipelineInterceptor("Validation", &executionLog, &mu))
	pipeline.Use(NewTestPipelineInterceptor("Logging", &executionLog, &mu))
	
	// Test that we can also use the constructor pattern
	pipeline2 := NewPipeline(
		NewTestPipelineInterceptor("Metrics", &executionLog, &mu),
		NewTestPipelineInterceptor("Tracing", &executionLog, &mu),
	)
	
	if len(pipeline.interceptors) != 3 {
		t.Errorf("Pipeline 1: expected 3 interceptors, got %d", len(pipeline.interceptors))
	}
	
	if len(pipeline2.interceptors) != 2 {
		t.Errorf("Pipeline 2: expected 2 interceptors, got %d", len(pipeline2.interceptors))
	}
	
	// Test execution
	handler := MessageHandlerFunc(func(ctx context.Context, msg contracts.Message) error {
		mu.Lock()
		executionLog = append(executionLog, "handler")
		mu.Unlock()
		return nil
	})
	
	msg := NewTestPipelineMessage("test")
	err := pipeline.Execute(context.Background(), msg, handler)
	
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	
	// Should execute in order: Security -> Validation -> Logging -> handler -> Logging -> Validation -> Security
	expected := []string{
		"before:Security",
		"before:Validation",
		"before:Logging",
		"handler",
		"after:Logging",
		"after:Validation",
		"after:Security",
	}
	
	mu.Lock()
	if len(executionLog) != len(expected) {
		t.Errorf("Expected %d entries, got %d\nExpected: %v\nActual: %v",
			len(expected), len(executionLog), expected, executionLog)
	}
	mu.Unlock()
}