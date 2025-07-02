package interceptors

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/glimte/mmate-go/contracts"
)

func TestInterceptorContext(t *testing.T) {
	t.Run("NewInterceptorContext", func(t *testing.T) {
		ic := NewInterceptorContext()
		assert.NotNil(t, ic)
		assert.NotNil(t, ic.values)
	})

	t.Run("Set and Get", func(t *testing.T) {
		ic := NewInterceptorContext()
		
		// Test string
		ic.Set("key1", "value1")
		val, exists := ic.Get("key1")
		assert.True(t, exists)
		assert.Equal(t, "value1", val)
		
		// Test int
		ic.Set("key2", 42)
		val, exists = ic.Get("key2")
		assert.True(t, exists)
		assert.Equal(t, 42, val)
		
		// Test non-existent key
		val, exists = ic.Get("nonexistent")
		assert.False(t, exists)
		assert.Nil(t, val)
	})

	t.Run("GetString", func(t *testing.T) {
		ic := NewInterceptorContext()
		
		ic.Set("stringKey", "stringValue")
		val, ok := ic.GetString("stringKey")
		assert.True(t, ok)
		assert.Equal(t, "stringValue", val)
		
		// Non-string value
		ic.Set("intKey", 42)
		val, ok = ic.GetString("intKey")
		assert.False(t, ok)
		assert.Empty(t, val)
		
		// Non-existent key
		val, ok = ic.GetString("nonexistent")
		assert.False(t, ok)
		assert.Empty(t, val)
	})

	t.Run("GetInt", func(t *testing.T) {
		ic := NewInterceptorContext()
		
		ic.Set("intKey", 42)
		val, ok := ic.GetInt("intKey")
		assert.True(t, ok)
		assert.Equal(t, 42, val)
		
		// Non-int value
		ic.Set("stringKey", "string")
		val, ok = ic.GetInt("stringKey")
		assert.False(t, ok)
		assert.Equal(t, 0, val)
	})

	t.Run("Delete", func(t *testing.T) {
		ic := NewInterceptorContext()
		
		ic.Set("key1", "value1")
		_, exists := ic.Get("key1")
		assert.True(t, exists)
		
		ic.Delete("key1")
		_, exists = ic.Get("key1")
		assert.False(t, exists)
	})

	t.Run("Clear", func(t *testing.T) {
		ic := NewInterceptorContext()
		
		ic.Set("key1", "value1")
		ic.Set("key2", "value2")
		
		ic.Clear()
		
		_, exists1 := ic.Get("key1")
		_, exists2 := ic.Get("key2")
		assert.False(t, exists1)
		assert.False(t, exists2)
	})

	t.Run("Copy", func(t *testing.T) {
		ic := NewInterceptorContext()
		ic.Set("key1", "value1")
		ic.Set("key2", 42)
		
		copy := ic.Copy()
		
		// Verify values are copied
		val1, _ := copy.Get("key1")
		val2, _ := copy.Get("key2")
		assert.Equal(t, "value1", val1)
		assert.Equal(t, 42, val2)
		
		// Verify it's a deep copy
		copy.Set("key3", "value3")
		_, exists := ic.Get("key3")
		assert.False(t, exists)
	})

	t.Run("Thread safety", func(t *testing.T) {
		ic := NewInterceptorContext()
		done := make(chan bool, 2)
		
		// Writer goroutine
		go func() {
			for i := 0; i < 100; i++ {
				ic.Set("key", i)
			}
			done <- true
		}()
		
		// Reader goroutine
		go func() {
			for i := 0; i < 100; i++ {
				ic.Get("key")
			}
			done <- true
		}()
		
		// Wait for both to complete
		<-done
		<-done
	})
}

func TestContextFunctions(t *testing.T) {
	t.Run("WithInterceptorContext and GetInterceptorContext", func(t *testing.T) {
		ctx := context.Background()
		ic := NewInterceptorContext()
		ic.Set("test", "value")
		
		// Add to context
		ctx = WithInterceptorContext(ctx, ic)
		
		// Retrieve from context
		retrieved, exists := GetInterceptorContext(ctx)
		assert.True(t, exists)
		assert.NotNil(t, retrieved)
		
		val, _ := retrieved.Get("test")
		assert.Equal(t, "value", val)
	})

	t.Run("GetInterceptorContext with no context", func(t *testing.T) {
		ctx := context.Background()
		
		ic, exists := GetInterceptorContext(ctx)
		assert.False(t, exists)
		assert.Nil(t, ic)
	})

	t.Run("EnsureInterceptorContext creates new if missing", func(t *testing.T) {
		ctx := context.Background()
		
		newCtx, ic := EnsureInterceptorContext(ctx)
		assert.NotNil(t, ic)
		
		// Verify it was added to context
		retrieved, exists := GetInterceptorContext(newCtx)
		assert.True(t, exists)
		assert.Equal(t, ic, retrieved)
	})

	t.Run("EnsureInterceptorContext returns existing", func(t *testing.T) {
		ctx := context.Background()
		existing := NewInterceptorContext()
		existing.Set("existing", "value")
		
		ctx = WithInterceptorContext(ctx, existing)
		
		newCtx, ic := EnsureInterceptorContext(ctx)
		assert.Equal(t, existing, ic)
		assert.Equal(t, ctx, newCtx) // Context should be unchanged
		
		val, _ := ic.Get("existing")
		assert.Equal(t, "value", val)
	})
}

// Mock enricher for testing
type mockEnricher struct {
	mock.Mock
}

func (m *mockEnricher) Enrich(ctx context.Context, ic *InterceptorContext, msg contracts.Message) error {
	args := m.Called(ctx, ic, msg)
	return args.Error(0)
}

func TestContextEnrichmentInterceptor(t *testing.T) {
	t.Run("Intercept enriches context", func(t *testing.T) {
		enricher := new(mockEnricher)
		interceptor := NewContextEnrichmentInterceptor(enricher)
		
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID:        "test-123",
				Type:      "TestMessage",
				Timestamp: time.Now(),
			},
		}
		
		handler := new(mockHandler)
		handler.On("Handle", mock.Anything, msg).Return(nil)
		
		enricher.On("Enrich", mock.Anything, mock.Anything, msg).Run(func(args mock.Arguments) {
			ic := args.Get(1).(*InterceptorContext)
			ic.Set("enriched", true)
			ic.Set("messageId", msg.GetID())
		}).Return(nil)
		
		ctx := context.Background()
		err := interceptor.Intercept(ctx, msg, handler)
		
		require.NoError(t, err)
		enricher.AssertExpectations(t)
		handler.AssertExpectations(t)
	})

	t.Run("Intercept handles enrichment error", func(t *testing.T) {
		enricher := new(mockEnricher)
		interceptor := NewContextEnrichmentInterceptor(enricher)
		
		msg := &testMessage{
			BaseMessage: contracts.BaseMessage{
				ID: "test-123",
			},
		}
		expectedErr := assert.AnError
		
		enricher.On("Enrich", mock.Anything, mock.Anything, msg).Return(expectedErr)
		
		handler := new(mockHandler)
		// Handler should not be called if enrichment fails
		
		ctx := context.Background()
		err := interceptor.Intercept(ctx, msg, handler)
		
		assert.Equal(t, expectedErr, err)
		enricher.AssertExpectations(t)
		handler.AssertNotCalled(t, "Handle")
	})

	t.Run("Name returns correct value", func(t *testing.T) {
		interceptor := NewContextEnrichmentInterceptor(nil)
		assert.Equal(t, "ContextEnrichmentInterceptor", interceptor.Name())
	})
}