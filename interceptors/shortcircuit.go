package interceptors

import (
	"context"
	"errors"
	
	"github.com/glimte/mmate-go/contracts"
)

// ErrShortCircuit is returned when an interceptor wants to short-circuit the chain
var ErrShortCircuit = errors.New("interceptor chain short-circuited")

// ShortCircuitResult contains the result of a short-circuited operation
type ShortCircuitResult struct {
	Result interface{}
	Reason string
}

// ShortCircuitError represents a short-circuit with additional information
type ShortCircuitError struct {
	Result *ShortCircuitResult
}

// Error implements the error interface
func (e *ShortCircuitError) Error() string {
	if e.Result != nil && e.Result.Reason != "" {
		return e.Result.Reason
	}
	return "interceptor chain short-circuited"
}

// IsShortCircuit checks if an error is a short-circuit error
func IsShortCircuit(err error) bool {
	if err == nil {
		return false
	}
	var scErr *ShortCircuitError
	return errors.As(err, &scErr) || errors.Is(err, ErrShortCircuit)
}

// GetShortCircuitResult extracts the short-circuit result from an error
func GetShortCircuitResult(err error) (*ShortCircuitResult, bool) {
	var scErr *ShortCircuitError
	if errors.As(err, &scErr) && scErr.Result != nil {
		return scErr.Result, true
	}
	return nil, false
}

// ShortCircuitInterceptor can short-circuit the interceptor chain based on conditions
type ShortCircuitInterceptor struct {
	evaluator ShortCircuitEvaluator
}

// ShortCircuitEvaluator determines if the chain should be short-circuited
type ShortCircuitEvaluator interface {
	// ShouldShortCircuit returns true if the chain should be short-circuited
	// It can also return a result that will be available to the caller
	ShouldShortCircuit(ctx context.Context, msg contracts.Message) (bool, *ShortCircuitResult, error)
}

// NewShortCircuitInterceptor creates a new short-circuit interceptor
func NewShortCircuitInterceptor(evaluator ShortCircuitEvaluator) *ShortCircuitInterceptor {
	return &ShortCircuitInterceptor{evaluator: evaluator}
}

// Intercept implements Interceptor
func (i *ShortCircuitInterceptor) Intercept(ctx context.Context, msg contracts.Message, next MessageHandler) error {
	shouldShortCircuit, result, err := i.evaluator.ShouldShortCircuit(ctx, msg)
	if err != nil {
		return err
	}
	
	if shouldShortCircuit {
		return &ShortCircuitError{Result: result}
	}
	
	return next.Handle(ctx, msg)
}

// Name implements Interceptor
func (i *ShortCircuitInterceptor) Name() string {
	return "ShortCircuitInterceptor"
}

// CachingInterceptor provides caching with short-circuit on cache hit
type CachingInterceptor struct {
	cache MessageCache
}

// MessageCache defines the interface for message caching
type MessageCache interface {
	Get(ctx context.Context, key string) (interface{}, bool, error)
	Set(ctx context.Context, key string, value interface{}) error
}

// NewCachingInterceptor creates a new caching interceptor
func NewCachingInterceptor(cache MessageCache) *CachingInterceptor {
	return &CachingInterceptor{cache: cache}
}

// Intercept implements Interceptor
func (i *CachingInterceptor) Intercept(ctx context.Context, msg contracts.Message, next MessageHandler) error {
	// Use message ID as cache key
	cacheKey := msg.GetID()
	
	// Check cache
	cached, found, err := i.cache.Get(ctx, cacheKey)
	if err != nil {
		return err
	}
	
	if found {
		// Short-circuit with cached result
		return &ShortCircuitError{
			Result: &ShortCircuitResult{
				Result: cached,
				Reason: "cache hit",
			},
		}
	}
	
	// Process normally
	err = next.Handle(ctx, msg)
	if err != nil {
		return err
	}
	
	// Cache the successful result (in a real implementation, you'd need to capture the result)
	// For now, we'll just cache that it was processed
	_ = i.cache.Set(ctx, cacheKey, true)
	
	return nil
}

// Name implements Interceptor
func (i *CachingInterceptor) Name() string {
	return "CachingInterceptor"
}

// DuplicateDetectionInterceptor prevents duplicate message processing
type DuplicateDetectionInterceptor struct {
	detector DuplicateDetector
}

// DuplicateDetector defines the interface for duplicate detection
type DuplicateDetector interface {
	IsDuplicate(ctx context.Context, messageID string) (bool, error)
	MarkProcessed(ctx context.Context, messageID string) error
}

// NewDuplicateDetectionInterceptor creates a new duplicate detection interceptor
func NewDuplicateDetectionInterceptor(detector DuplicateDetector) *DuplicateDetectionInterceptor {
	return &DuplicateDetectionInterceptor{detector: detector}
}

// Intercept implements Interceptor
func (i *DuplicateDetectionInterceptor) Intercept(ctx context.Context, msg contracts.Message, next MessageHandler) error {
	messageID := msg.GetID()
	
	// Check if duplicate
	isDuplicate, err := i.detector.IsDuplicate(ctx, messageID)
	if err != nil {
		return err
	}
	
	if isDuplicate {
		// Short-circuit for duplicate
		return &ShortCircuitError{
			Result: &ShortCircuitResult{
				Result: nil,
				Reason: "duplicate message detected",
			},
		}
	}
	
	// Process the message
	err = next.Handle(ctx, msg)
	if err != nil {
		return err
	}
	
	// Mark as processed
	return i.detector.MarkProcessed(ctx, messageID)
}

// Name implements Interceptor
func (i *DuplicateDetectionInterceptor) Name() string {
	return "DuplicateDetectionInterceptor"
}

// ShortCircuitOnErrorInterceptor short-circuits the chain if specific errors occur
type ShortCircuitOnErrorInterceptor struct {
	errorEvaluator ErrorEvaluator
}

// ErrorEvaluator determines if an error should cause a short-circuit
type ErrorEvaluator interface {
	ShouldShortCircuitOnError(err error) (bool, *ShortCircuitResult)
}

// NewShortCircuitOnErrorInterceptor creates a new error-based short-circuit interceptor
func NewShortCircuitOnErrorInterceptor(errorEvaluator ErrorEvaluator) *ShortCircuitOnErrorInterceptor {
	return &ShortCircuitOnErrorInterceptor{errorEvaluator: errorEvaluator}
}

// Intercept implements Interceptor
func (i *ShortCircuitOnErrorInterceptor) Intercept(ctx context.Context, msg contracts.Message, next MessageHandler) error {
	err := next.Handle(ctx, msg)
	if err != nil {
		shouldShortCircuit, result := i.errorEvaluator.ShouldShortCircuitOnError(err)
		if shouldShortCircuit {
			return &ShortCircuitError{Result: result}
		}
	}
	return err
}

// Name implements Interceptor
func (i *ShortCircuitOnErrorInterceptor) Name() string {
	return "ShortCircuitOnErrorInterceptor"
}