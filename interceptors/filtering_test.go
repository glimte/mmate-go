package interceptors

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/glimte/mmate-go/contracts"
)

// Mock filter for testing
type mockFilter struct {
	mock.Mock
}

func (m *mockFilter) ShouldProcess(ctx context.Context, msg contracts.Message) (bool, error) {
	args := m.Called(ctx, msg)
	return args.Bool(0), args.Error(1)
}

// Mock interceptor for testing
type mockInterceptor struct {
	mock.Mock
}

func (m *mockInterceptor) Intercept(ctx context.Context, msg contracts.Message, next MessageHandler) error {
	args := m.Called(ctx, msg, next)
	return args.Error(0)
}

func (m *mockInterceptor) Name() string {
	args := m.Called()
	return args.String(0)
}

func TestFilteringInterceptor(t *testing.T) {
	t.Run("Allows message when filter returns true", func(t *testing.T) {
		filter := new(mockFilter)
		interceptor := NewFilteringInterceptor(filter, SkipSilently)
		
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID:   "test-123",
				Type: "TestMessage",
			},
		}
		handler := new(mockHandler)
		
		filter.On("ShouldProcess", mock.Anything, msg).Return(true, nil)
		handler.On("Handle", mock.Anything, msg).Return(nil)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.NoError(t, err)
		filter.AssertExpectations(t)
		handler.AssertExpectations(t)
	})

	t.Run("Skips silently when filter returns false", func(t *testing.T) {
		filter := new(mockFilter)
		interceptor := NewFilteringInterceptor(filter, SkipSilently)
		
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID:   "test-123",
				Type: "TestMessage",
			},
		}
		handler := new(mockHandler)
		
		filter.On("ShouldProcess", mock.Anything, msg).Return(false, nil)
		// Handler should not be called
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.NoError(t, err)
		filter.AssertExpectations(t)
		handler.AssertNotCalled(t, "Handle")
	})

	t.Run("Returns error when filter returns false with SkipWithError", func(t *testing.T) {
		filter := new(mockFilter)
		interceptor := NewFilteringInterceptor(filter, SkipWithError)
		
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID:   "test-123",
				Type: "TestMessage",
			},
		}
		handler := new(mockHandler)
		
		filter.On("ShouldProcess", mock.Anything, msg).Return(false, nil)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "message filtered")
		filter.AssertExpectations(t)
		handler.AssertNotCalled(t, "Handle")
	})

	t.Run("Handles filter error", func(t *testing.T) {
		filter := new(mockFilter)
		interceptor := NewFilteringInterceptor(filter, SkipSilently)
		
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID: "test-123",
			},
		}
		handler := new(mockHandler)
		filterErr := errors.New("filter error")
		
		filter.On("ShouldProcess", mock.Anything, msg).Return(false, filterErr)
		
		err := interceptor.Intercept(context.Background(), msg, handler)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "filter error")
		filter.AssertExpectations(t)
		handler.AssertNotCalled(t, "Handle")
	})

	t.Run("Name returns correct value", func(t *testing.T) {
		interceptor := NewFilteringInterceptor(nil, SkipSilently)
		assert.Equal(t, "FilteringInterceptor", interceptor.Name())
	})
}

func TestMessageFilterFunc(t *testing.T) {
	called := false
	filterFunc := MessageFilterFunc(func(ctx context.Context, msg contracts.Message) (bool, error) {
		called = true
		return true, nil
	})
	
	result, err := filterFunc.ShouldProcess(context.Background(), &testMessage{})
	
	assert.True(t, called)
	assert.True(t, result)
	assert.NoError(t, err)
}

func TestCompositeFilter(t *testing.T) {
	t.Run("All filters must return true", func(t *testing.T) {
		filter1 := new(mockFilter)
		filter2 := new(mockFilter)
		filter3 := new(mockFilter)
		
		composite := NewCompositeFilter(filter1, filter2, filter3)
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID: "test-123",
			},
		}
		
		filter1.On("ShouldProcess", mock.Anything, msg).Return(true, nil)
		filter2.On("ShouldProcess", mock.Anything, msg).Return(true, nil)
		filter3.On("ShouldProcess", mock.Anything, msg).Return(true, nil)
		
		result, err := composite.ShouldProcess(context.Background(), msg)
		
		assert.True(t, result)
		assert.NoError(t, err)
		filter1.AssertExpectations(t)
		filter2.AssertExpectations(t)
		filter3.AssertExpectations(t)
	})

	t.Run("Returns false if any filter returns false", func(t *testing.T) {
		filter1 := new(mockFilter)
		filter2 := new(mockFilter)
		filter3 := new(mockFilter)
		
		composite := NewCompositeFilter(filter1, filter2, filter3)
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID: "test-123",
			},
		}
		
		filter1.On("ShouldProcess", mock.Anything, msg).Return(true, nil)
		filter2.On("ShouldProcess", mock.Anything, msg).Return(false, nil)
		// filter3 should not be called
		
		result, err := composite.ShouldProcess(context.Background(), msg)
		
		assert.False(t, result)
		assert.NoError(t, err)
		filter1.AssertExpectations(t)
		filter2.AssertExpectations(t)
		filter3.AssertNotCalled(t, "ShouldProcess")
	})

	t.Run("Returns error if any filter errors", func(t *testing.T) {
		filter1 := new(mockFilter)
		filter2 := new(mockFilter)
		
		composite := NewCompositeFilter(filter1, filter2)
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID: "test-123",
			},
		}
		filterErr := errors.New("filter error")
		
		filter1.On("ShouldProcess", mock.Anything, msg).Return(true, nil)
		filter2.On("ShouldProcess", mock.Anything, msg).Return(false, filterErr)
		
		result, err := composite.ShouldProcess(context.Background(), msg)
		
		assert.False(t, result)
		assert.Equal(t, filterErr, err)
	})
}

func TestOrFilter(t *testing.T) {
	t.Run("Returns true if any filter returns true", func(t *testing.T) {
		filter1 := new(mockFilter)
		filter2 := new(mockFilter)
		filter3 := new(mockFilter)
		
		orFilter := NewOrFilter(filter1, filter2, filter3)
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID: "test-123",
			},
		}
		
		filter1.On("ShouldProcess", mock.Anything, msg).Return(false, nil)
		filter2.On("ShouldProcess", mock.Anything, msg).Return(true, nil)
		// filter3 should not be called
		
		result, err := orFilter.ShouldProcess(context.Background(), msg)
		
		assert.True(t, result)
		assert.NoError(t, err)
		filter1.AssertExpectations(t)
		filter2.AssertExpectations(t)
		filter3.AssertNotCalled(t, "ShouldProcess")
	})

	t.Run("Returns false if all filters return false", func(t *testing.T) {
		filter1 := new(mockFilter)
		filter2 := new(mockFilter)
		
		orFilter := NewOrFilter(filter1, filter2)
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID: "test-123",
			},
		}
		
		filter1.On("ShouldProcess", mock.Anything, msg).Return(false, nil)
		filter2.On("ShouldProcess", mock.Anything, msg).Return(false, nil)
		
		result, err := orFilter.ShouldProcess(context.Background(), msg)
		
		assert.False(t, result)
		assert.NoError(t, err)
	})
}

func TestMessageTypeFilter(t *testing.T) {
	t.Run("Allows specified message types", func(t *testing.T) {
		filter := NewMessageTypeFilter("OrderCreated", "OrderUpdated", "OrderDeleted")
		
		msg1 := &testMessage{BaseMessage: contracts.BaseMessage{Type: "OrderCreated"}}
		msg2 := &testMessage{BaseMessage: contracts.BaseMessage{Type: "OrderUpdated"}}
		msg3 := &testMessage{BaseMessage: contracts.BaseMessage{Type: "PaymentProcessed"}}
		
		result1, _ := filter.ShouldProcess(context.Background(), msg1)
		result2, _ := filter.ShouldProcess(context.Background(), msg2)
		result3, _ := filter.ShouldProcess(context.Background(), msg3)
		
		assert.True(t, result1)
		assert.True(t, result2)
		assert.False(t, result3)
	})
}

func TestConditionalInterceptor(t *testing.T) {
	t.Run("Executes interceptor when condition is met", func(t *testing.T) {
		condition := new(mockFilter)
		innerInterceptor := new(mockInterceptor)
		conditional := NewConditionalInterceptor(condition, innerInterceptor)
		
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID: "test-123",
			},
		}
		handler := new(mockHandler)
		
		condition.On("ShouldProcess", mock.Anything, msg).Return(true, nil)
		innerInterceptor.On("Intercept", mock.Anything, msg, handler).Return(nil)
		
		err := conditional.Intercept(context.Background(), msg, handler)
		
		assert.NoError(t, err)
		condition.AssertExpectations(t)
		innerInterceptor.AssertExpectations(t)
	})

	t.Run("Skips interceptor when condition is not met", func(t *testing.T) {
		condition := new(mockFilter)
		innerInterceptor := new(mockInterceptor)
		conditional := NewConditionalInterceptor(condition, innerInterceptor)
		
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID: "test-123",
			},
		}
		handler := new(mockHandler)
		
		condition.On("ShouldProcess", mock.Anything, msg).Return(false, nil)
		handler.On("Handle", mock.Anything, msg).Return(nil)
		
		err := conditional.Intercept(context.Background(), msg, handler)
		
		assert.NoError(t, err)
		condition.AssertExpectations(t)
		innerInterceptor.AssertNotCalled(t, "Intercept")
		handler.AssertExpectations(t)
	})

	t.Run("Name includes wrapped interceptor name", func(t *testing.T) {
		innerInterceptor := new(mockInterceptor)
		innerInterceptor.On("Name").Return("InnerInterceptor")
		
		conditional := NewConditionalInterceptor(nil, innerInterceptor)
		name := conditional.Name()
		
		assert.Equal(t, "ConditionalInterceptor[InnerInterceptor]", name)
	})
}

func TestContextBasedFilter(t *testing.T) {
	t.Run("Returns true when context value matches", func(t *testing.T) {
		filter := NewContextBasedFilter("role", "admin")
		
		ctx := context.Background()
		ic := NewInterceptorContext()
		ic.Set("role", "admin")
		ctx = WithInterceptorContext(ctx, ic)
		
		msg := &testMessage{}
		result, err := filter.ShouldProcess(ctx, msg)
		
		assert.True(t, result)
		assert.NoError(t, err)
	})

	t.Run("Returns false when context value doesn't match", func(t *testing.T) {
		filter := NewContextBasedFilter("role", "admin")
		
		ctx := context.Background()
		ic := NewInterceptorContext()
		ic.Set("role", "user")
		ctx = WithInterceptorContext(ctx, ic)
		
		msg := &testMessage{}
		result, err := filter.ShouldProcess(ctx, msg)
		
		assert.False(t, result)
		assert.NoError(t, err)
	})

	t.Run("Returns false when context key doesn't exist", func(t *testing.T) {
		filter := NewContextBasedFilter("role", "admin")
		
		ctx := context.Background()
		ic := NewInterceptorContext()
		ctx = WithInterceptorContext(ctx, ic)
		
		msg := &testMessage{}
		result, err := filter.ShouldProcess(ctx, msg)
		
		assert.False(t, result)
		assert.NoError(t, err)
	})

	t.Run("Returns false when no interceptor context", func(t *testing.T) {
		filter := NewContextBasedFilter("role", "admin")
		
		ctx := context.Background()
		msg := &testMessage{}
		result, err := filter.ShouldProcess(ctx, msg)
		
		assert.False(t, result)
		assert.NoError(t, err)
	})
}