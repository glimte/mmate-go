package interceptors

import (
	"context"
	"sync"
	
	"github.com/glimte/mmate-go/contracts"
)

// contextKey is a type for context keys to avoid collisions
type contextKey string

const (
	// InterceptorContextKey is the key for storing interceptor context
	InterceptorContextKey contextKey = "mmate:interceptor:context"
)

// InterceptorContext holds shared data between interceptors
type InterceptorContext struct {
	values map[string]interface{}
	mu     sync.RWMutex
}

// NewInterceptorContext creates a new interceptor context
func NewInterceptorContext() *InterceptorContext {
	return &InterceptorContext{
		values: make(map[string]interface{}),
	}
}

// Set stores a value in the interceptor context
func (ic *InterceptorContext) Set(key string, value interface{}) {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	ic.values[key] = value
}

// Get retrieves a value from the interceptor context
func (ic *InterceptorContext) Get(key string) (interface{}, bool) {
	ic.mu.RLock()
	defer ic.mu.RUnlock()
	value, exists := ic.values[key]
	return value, exists
}

// GetString retrieves a string value from the interceptor context
func (ic *InterceptorContext) GetString(key string) (string, bool) {
	value, exists := ic.Get(key)
	if !exists {
		return "", false
	}
	str, ok := value.(string)
	return str, ok
}

// GetInt retrieves an int value from the interceptor context
func (ic *InterceptorContext) GetInt(key string) (int, bool) {
	value, exists := ic.Get(key)
	if !exists {
		return 0, false
	}
	i, ok := value.(int)
	return i, ok
}

// Delete removes a value from the interceptor context
func (ic *InterceptorContext) Delete(key string) {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	delete(ic.values, key)
}

// Clear removes all values from the interceptor context
func (ic *InterceptorContext) Clear() {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	ic.values = make(map[string]interface{})
}

// Copy creates a copy of the interceptor context
func (ic *InterceptorContext) Copy() *InterceptorContext {
	ic.mu.RLock()
	defer ic.mu.RUnlock()
	
	newContext := NewInterceptorContext()
	for k, v := range ic.values {
		newContext.values[k] = v
	}
	return newContext
}

// GetInterceptorContext retrieves the interceptor context from the context
func GetInterceptorContext(ctx context.Context) (*InterceptorContext, bool) {
	value := ctx.Value(InterceptorContextKey)
	if value == nil {
		return nil, false
	}
	ic, ok := value.(*InterceptorContext)
	return ic, ok
}

// WithInterceptorContext adds the interceptor context to the context
func WithInterceptorContext(ctx context.Context, ic *InterceptorContext) context.Context {
	return context.WithValue(ctx, InterceptorContextKey, ic)
}

// EnsureInterceptorContext ensures an interceptor context exists in the context
func EnsureInterceptorContext(ctx context.Context) (context.Context, *InterceptorContext) {
	ic, exists := GetInterceptorContext(ctx)
	if !exists {
		ic = NewInterceptorContext()
		ctx = WithInterceptorContext(ctx, ic)
	}
	return ctx, ic
}

// ContextEnrichmentInterceptor enriches the context with message metadata
type ContextEnrichmentInterceptor struct {
	enricher ContextEnricher
}

// ContextEnricher defines the interface for context enrichment
type ContextEnricher interface {
	Enrich(ctx context.Context, ic *InterceptorContext, msg contracts.Message) error
}

// NewContextEnrichmentInterceptor creates a new context enrichment interceptor
func NewContextEnrichmentInterceptor(enricher ContextEnricher) *ContextEnrichmentInterceptor {
	return &ContextEnrichmentInterceptor{enricher: enricher}
}

// Intercept implements Interceptor
func (i *ContextEnrichmentInterceptor) Intercept(ctx context.Context, msg contracts.Message, next MessageHandler) error {
	// Ensure we have an interceptor context
	ctx, ic := EnsureInterceptorContext(ctx)
	
	// Enrich the context
	if err := i.enricher.Enrich(ctx, ic, msg); err != nil {
		return err
	}
	
	return next.Handle(ctx, msg)
}

// Name implements Interceptor
func (i *ContextEnrichmentInterceptor) Name() string {
	return "ContextEnrichmentInterceptor"
}