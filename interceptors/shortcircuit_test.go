package interceptors

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/glimte/mmate-go/contracts"
)

func TestShortCircuitError(t *testing.T) {
	t.Run("Error message", func(t *testing.T) {
		err := &ShortCircuitError{}
		assert.Equal(t, "interceptor chain short-circuited", err.Error())
		
		err = &ShortCircuitError{
			Result: &ShortCircuitResult{Reason: "custom reason"},
		}
		assert.Equal(t, "custom reason", err.Error())
	})
}

func TestIsShortCircuit(t *testing.T) {
	t.Run("Detects ShortCircuitError", func(t *testing.T) {
		err := &ShortCircuitError{}
		assert.True(t, IsShortCircuit(err))
	})

	t.Run("Detects ErrShortCircuit", func(t *testing.T) {
		assert.True(t, IsShortCircuit(ErrShortCircuit))
	})

	t.Run("Returns false for other errors", func(t *testing.T) {
		err := errors.New("regular error")
		assert.False(t, IsShortCircuit(err))
	})

	t.Run("Returns false for nil", func(t *testing.T) {
		assert.False(t, IsShortCircuit(nil))
	})
}

func TestGetShortCircuitResult(t *testing.T) {
	t.Run("Extracts result from ShortCircuitError", func(t *testing.T) {
		expectedResult := &ShortCircuitResult{
			Result: "test result",
			Reason: "test reason",
		}
		err := &ShortCircuitError{Result: expectedResult}
		
		result, ok := GetShortCircuitResult(err)
		assert.True(t, ok)
		assert.Equal(t, expectedResult, result)
	})

	t.Run("Returns false for non-short-circuit error", func(t *testing.T) {
		err := errors.New("regular error")
		result, ok := GetShortCircuitResult(err)
		assert.False(t, ok)
		assert.Nil(t, result)
	})

	t.Run("Returns false for ShortCircuitError with nil result", func(t *testing.T) {
		err := &ShortCircuitError{}
		result, ok := GetShortCircuitResult(err)
		assert.False(t, ok)
		assert.Nil(t, result)
	})
}

// Mock evaluator for testing
type mockEvaluator struct {
	mock.Mock
}

func (m *mockEvaluator) ShouldShortCircuit(ctx context.Context, msg contracts.Message) (bool, *ShortCircuitResult, error) {
	args := m.Called(ctx, msg)
	if args.Get(1) == nil {
		return args.Bool(0), nil, args.Error(2)
	}
	return args.Bool(0), args.Get(1).(*ShortCircuitResult), args.Error(2)
}

func TestShortCircuitInterceptor(t *testing.T) {
	t.Run("Short-circuits when evaluator returns true", func(t *testing.T) {
		evaluator := new(mockEvaluator)
		interceptor := NewShortCircuitInterceptor(evaluator)
		
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID: "test-123",
			},
		}
		handler := new(mockHandler)
		expectedResult := &ShortCircuitResult{
			Result: "cached",
			Reason: "found in cache",
		}
		
		evaluator.On("ShouldShortCircuit", mock.Anything, msg).Return(true, expectedResult, nil)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.Error(t, err)
		var scErr *ShortCircuitError
		assert.True(t, errors.As(err, &scErr))
		assert.Equal(t, expectedResult, scErr.Result)
		
		evaluator.AssertExpectations(t)
		handler.AssertNotCalled(t, "Handle")
	})

	t.Run("Continues when evaluator returns false", func(t *testing.T) {
		evaluator := new(mockEvaluator)
		interceptor := NewShortCircuitInterceptor(evaluator)
		
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID: "test-123",
			},
		}
		handler := new(mockHandler)
		
		evaluator.On("ShouldShortCircuit", mock.Anything, msg).Return(false, nil, nil)
		handler.On("Handle", mock.Anything, msg).Return(nil)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.NoError(t, err)
		evaluator.AssertExpectations(t)
		handler.AssertExpectations(t)
	})

	t.Run("Returns error when evaluator fails", func(t *testing.T) {
		evaluator := new(mockEvaluator)
		interceptor := NewShortCircuitInterceptor(evaluator)
		
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID: "test-123",
			},
		}
		handler := new(mockHandler)
		evalErr := errors.New("evaluation failed")
		
		evaluator.On("ShouldShortCircuit", mock.Anything, msg).Return(false, nil, evalErr)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.Equal(t, evalErr, err)
		evaluator.AssertExpectations(t)
		handler.AssertNotCalled(t, "Handle")
	})

	t.Run("Name returns correct value", func(t *testing.T) {
		interceptor := NewShortCircuitInterceptor(nil)
		assert.Equal(t, "ShortCircuitInterceptor", interceptor.Name())
	})
}

// Mock cache for testing
type mockCache struct {
	mock.Mock
}

func (m *mockCache) Get(ctx context.Context, key string) (interface{}, bool, error) {
	args := m.Called(ctx, key)
	return args.Get(0), args.Bool(1), args.Error(2)
}

func (m *mockCache) Set(ctx context.Context, key string, value interface{}) error {
	args := m.Called(ctx, key, value)
	return args.Error(0)
}

func TestCachingInterceptor(t *testing.T) {
	t.Run("Short-circuits on cache hit", func(t *testing.T) {
		cache := new(mockCache)
		interceptor := NewCachingInterceptor(cache)
		
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID: "test-123",
			},
		}
		handler := new(mockHandler)
		cachedValue := "cached result"
		
		cache.On("Get", mock.Anything, "test-123").Return(cachedValue, true, nil)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.Error(t, err)
		var scErr *ShortCircuitError
		assert.True(t, errors.As(err, &scErr))
		assert.Equal(t, cachedValue, scErr.Result.Result)
		assert.Equal(t, "cache hit", scErr.Result.Reason)
		
		cache.AssertExpectations(t)
		handler.AssertNotCalled(t, "Handle")
	})

	t.Run("Processes and caches on cache miss", func(t *testing.T) {
		cache := new(mockCache)
		interceptor := NewCachingInterceptor(cache)
		
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID: "test-123",
			},
		}
		handler := new(mockHandler)
		
		cache.On("Get", mock.Anything, "test-123").Return(nil, false, nil)
		handler.On("Handle", mock.Anything, msg).Return(nil)
		cache.On("Set", mock.Anything, "test-123", true).Return(nil)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.NoError(t, err)
		cache.AssertExpectations(t)
		handler.AssertExpectations(t)
	})

	t.Run("Returns error on cache get failure", func(t *testing.T) {
		cache := new(mockCache)
		interceptor := NewCachingInterceptor(cache)
		
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID: "test-123",
			},
		}
		handler := new(mockHandler)
		cacheErr := errors.New("cache error")
		
		cache.On("Get", mock.Anything, "test-123").Return(nil, false, cacheErr)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.Equal(t, cacheErr, err)
		cache.AssertExpectations(t)
		handler.AssertNotCalled(t, "Handle")
	})

	t.Run("Name returns correct value", func(t *testing.T) {
		interceptor := NewCachingInterceptor(nil)
		assert.Equal(t, "CachingInterceptor", interceptor.Name())
	})
}

// Mock duplicate detector for testing
type mockDuplicateDetector struct {
	mock.Mock
}

func (m *mockDuplicateDetector) IsDuplicate(ctx context.Context, messageID string) (bool, error) {
	args := m.Called(ctx, messageID)
	return args.Bool(0), args.Error(1)
}

func (m *mockDuplicateDetector) MarkProcessed(ctx context.Context, messageID string) error {
	args := m.Called(ctx, messageID)
	return args.Error(0)
}

func TestDuplicateDetectionInterceptor(t *testing.T) {
	t.Run("Short-circuits on duplicate", func(t *testing.T) {
		detector := new(mockDuplicateDetector)
		interceptor := NewDuplicateDetectionInterceptor(detector)
		
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID: "test-123",
			},
		}
		handler := new(mockHandler)
		
		detector.On("IsDuplicate", mock.Anything, "test-123").Return(true, nil)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.Error(t, err)
		var scErr *ShortCircuitError
		assert.True(t, errors.As(err, &scErr))
		assert.Equal(t, "duplicate message detected", scErr.Result.Reason)
		
		detector.AssertExpectations(t)
		handler.AssertNotCalled(t, "Handle")
		detector.AssertNotCalled(t, "MarkProcessed")
	})

	t.Run("Processes and marks non-duplicate", func(t *testing.T) {
		detector := new(mockDuplicateDetector)
		interceptor := NewDuplicateDetectionInterceptor(detector)
		
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID: "test-123",
			},
		}
		handler := new(mockHandler)
		
		detector.On("IsDuplicate", mock.Anything, "test-123").Return(false, nil)
		handler.On("Handle", mock.Anything, msg).Return(nil)
		detector.On("MarkProcessed", mock.Anything, "test-123").Return(nil)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.NoError(t, err)
		detector.AssertExpectations(t)
		handler.AssertExpectations(t)
	})

	t.Run("Returns error on detection failure", func(t *testing.T) {
		detector := new(mockDuplicateDetector)
		interceptor := NewDuplicateDetectionInterceptor(detector)
		
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID: "test-123",
			},
		}
		handler := new(mockHandler)
		detectErr := errors.New("detection error")
		
		detector.On("IsDuplicate", mock.Anything, "test-123").Return(false, detectErr)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.Equal(t, detectErr, err)
		detector.AssertExpectations(t)
		handler.AssertNotCalled(t, "Handle")
	})

	t.Run("Name returns correct value", func(t *testing.T) {
		interceptor := NewDuplicateDetectionInterceptor(nil)
		assert.Equal(t, "DuplicateDetectionInterceptor", interceptor.Name())
	})
}

// Mock error evaluator for testing
type mockErrorEvaluator struct {
	mock.Mock
}

func (m *mockErrorEvaluator) ShouldShortCircuitOnError(err error) (bool, *ShortCircuitResult) {
	args := m.Called(err)
	if args.Get(1) == nil {
		return args.Bool(0), nil
	}
	return args.Bool(0), args.Get(1).(*ShortCircuitResult)
}

func TestShortCircuitOnErrorInterceptor(t *testing.T) {
	t.Run("Short-circuits on specific error", func(t *testing.T) {
		errorEvaluator := new(mockErrorEvaluator)
		interceptor := NewShortCircuitOnErrorInterceptor(errorEvaluator)
		
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID: "test-123",
			},
		}
		handler := new(mockHandler)
		handlerErr := errors.New("specific error")
		expectedResult := &ShortCircuitResult{
			Result: nil,
			Reason: "known error condition",
		}
		
		handler.On("Handle", mock.Anything, msg).Return(handlerErr)
		errorEvaluator.On("ShouldShortCircuitOnError", handlerErr).Return(true, expectedResult)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.Error(t, err)
		var scErr *ShortCircuitError
		assert.True(t, errors.As(err, &scErr))
		assert.Equal(t, expectedResult, scErr.Result)
		
		handler.AssertExpectations(t)
		errorEvaluator.AssertExpectations(t)
	})

	t.Run("Returns original error when not short-circuiting", func(t *testing.T) {
		errorEvaluator := new(mockErrorEvaluator)
		interceptor := NewShortCircuitOnErrorInterceptor(errorEvaluator)
		
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID: "test-123",
			},
		}
		handler := new(mockHandler)
		handlerErr := errors.New("regular error")
		
		handler.On("Handle", mock.Anything, msg).Return(handlerErr)
		errorEvaluator.On("ShouldShortCircuitOnError", handlerErr).Return(false, nil)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.Equal(t, handlerErr, err)
		handler.AssertExpectations(t)
		errorEvaluator.AssertExpectations(t)
	})

	t.Run("Returns nil when handler succeeds", func(t *testing.T) {
		errorEvaluator := new(mockErrorEvaluator)
		interceptor := NewShortCircuitOnErrorInterceptor(errorEvaluator)
		
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID: "test-123",
			},
		}
		handler := new(mockHandler)
		
		handler.On("Handle", mock.Anything, msg).Return(nil)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.NoError(t, err)
		handler.AssertExpectations(t)
		errorEvaluator.AssertNotCalled(t, "ShouldShortCircuitOnError")
	})

	t.Run("Name returns correct value", func(t *testing.T) {
		interceptor := NewShortCircuitOnErrorInterceptor(nil)
		assert.Equal(t, "ShortCircuitOnErrorInterceptor", interceptor.Name())
	})
}