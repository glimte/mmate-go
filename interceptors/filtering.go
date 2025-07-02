package interceptors

import (
	"context"
	"fmt"
	
	"github.com/glimte/mmate-go/contracts"
)

// MessageFilter defines the interface for message filtering
type MessageFilter interface {
	// ShouldProcess returns true if the message should be processed
	ShouldProcess(ctx context.Context, msg contracts.Message) (bool, error)
}

// MessageFilterFunc is a function adapter for MessageFilter
type MessageFilterFunc func(ctx context.Context, msg contracts.Message) (bool, error)

// ShouldProcess implements MessageFilter
func (f MessageFilterFunc) ShouldProcess(ctx context.Context, msg contracts.Message) (bool, error) {
	return f(ctx, msg)
}

// FilteringInterceptor filters messages based on conditions
type FilteringInterceptor struct {
	filter       MessageFilter
	skipBehavior SkipBehavior
}

// SkipBehavior defines what happens when a message is filtered out
type SkipBehavior int

const (
	// SkipSilently skips the message without error
	SkipSilently SkipBehavior = iota
	// SkipWithError returns an error when message is filtered
	SkipWithError
	// SkipWithLog logs that the message was skipped
	SkipWithLog
)

// NewFilteringInterceptor creates a new filtering interceptor
func NewFilteringInterceptor(filter MessageFilter, skipBehavior SkipBehavior) *FilteringInterceptor {
	return &FilteringInterceptor{
		filter:       filter,
		skipBehavior: skipBehavior,
	}
}

// Intercept implements Interceptor
func (i *FilteringInterceptor) Intercept(ctx context.Context, msg contracts.Message, next MessageHandler) error {
	shouldProcess, err := i.filter.ShouldProcess(ctx, msg)
	if err != nil {
		return fmt.Errorf("filter error: %w", err)
	}
	
	if !shouldProcess {
		switch i.skipBehavior {
		case SkipWithError:
			return fmt.Errorf("message filtered: type=%s, id=%s", msg.GetType(), msg.GetID())
		case SkipWithLog:
			// In real implementation, you'd log here
			return nil
		default: // SkipSilently
			return nil
		}
	}
	
	return next.Handle(ctx, msg)
}

// Name implements Interceptor
func (i *FilteringInterceptor) Name() string {
	return "FilteringInterceptor"
}

// CompositeFilter combines multiple filters with AND logic
type CompositeFilter struct {
	filters []MessageFilter
}

// NewCompositeFilter creates a new composite filter
func NewCompositeFilter(filters ...MessageFilter) *CompositeFilter {
	return &CompositeFilter{filters: filters}
}

// ShouldProcess implements MessageFilter - all filters must return true
func (f *CompositeFilter) ShouldProcess(ctx context.Context, msg contracts.Message) (bool, error) {
	for _, filter := range f.filters {
		shouldProcess, err := filter.ShouldProcess(ctx, msg)
		if err != nil {
			return false, err
		}
		if !shouldProcess {
			return false, nil
		}
	}
	return true, nil
}

// OrFilter combines multiple filters with OR logic
type OrFilter struct {
	filters []MessageFilter
}

// NewOrFilter creates a new OR filter
func NewOrFilter(filters ...MessageFilter) *OrFilter {
	return &OrFilter{filters: filters}
}

// ShouldProcess implements MessageFilter - at least one filter must return true
func (f *OrFilter) ShouldProcess(ctx context.Context, msg contracts.Message) (bool, error) {
	for _, filter := range f.filters {
		shouldProcess, err := filter.ShouldProcess(ctx, msg)
		if err != nil {
			return false, err
		}
		if shouldProcess {
			return true, nil
		}
	}
	return false, nil
}

// MessageTypeFilter filters messages by type
type MessageTypeFilter struct {
	allowedTypes map[string]bool
}

// NewMessageTypeFilter creates a filter that only allows specific message types
func NewMessageTypeFilter(allowedTypes ...string) *MessageTypeFilter {
	typeMap := make(map[string]bool)
	for _, t := range allowedTypes {
		typeMap[t] = true
	}
	return &MessageTypeFilter{allowedTypes: typeMap}
}

// ShouldProcess implements MessageFilter
func (f *MessageTypeFilter) ShouldProcess(ctx context.Context, msg contracts.Message) (bool, error) {
	return f.allowedTypes[msg.GetType()], nil
}

// ConditionalInterceptor executes an interceptor only if a condition is met
type ConditionalInterceptor struct {
	condition   MessageFilter
	interceptor Interceptor
}

// NewConditionalInterceptor creates a new conditional interceptor
func NewConditionalInterceptor(condition MessageFilter, interceptor Interceptor) *ConditionalInterceptor {
	return &ConditionalInterceptor{
		condition:   condition,
		interceptor: interceptor,
	}
}

// Intercept implements Interceptor
func (i *ConditionalInterceptor) Intercept(ctx context.Context, msg contracts.Message, next MessageHandler) error {
	shouldExecute, err := i.condition.ShouldProcess(ctx, msg)
	if err != nil {
		return err
	}
	
	if shouldExecute {
		return i.interceptor.Intercept(ctx, msg, next)
	}
	
	return next.Handle(ctx, msg)
}

// Name implements Interceptor
func (i *ConditionalInterceptor) Name() string {
	return fmt.Sprintf("ConditionalInterceptor[%s]", i.interceptor.Name())
}

// ContextBasedFilter filters based on values in the interceptor context
type ContextBasedFilter struct {
	contextKey    string
	expectedValue interface{}
}

// NewContextBasedFilter creates a filter that checks context values
func NewContextBasedFilter(contextKey string, expectedValue interface{}) *ContextBasedFilter {
	return &ContextBasedFilter{
		contextKey:    contextKey,
		expectedValue: expectedValue,
	}
}

// ShouldProcess implements MessageFilter
func (f *ContextBasedFilter) ShouldProcess(ctx context.Context, msg contracts.Message) (bool, error) {
	ic, exists := GetInterceptorContext(ctx)
	if !exists {
		return false, nil
	}
	
	value, exists := ic.Get(f.contextKey)
	if !exists {
		return false, nil
	}
	
	return value == f.expectedValue, nil
}