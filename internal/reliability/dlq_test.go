package reliability

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Mock types for testing
type mockErrorStore struct {
	mock.Mock
}

func (m *mockErrorStore) Store(ctx context.Context, message FailedMessage) error {
	args := m.Called(ctx, message)
	return args.Error(0)
}

func (m *mockErrorStore) Get(ctx context.Context, id string) (*FailedMessage, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*FailedMessage), args.Error(1)
}

func (m *mockErrorStore) List(ctx context.Context, filter ErrorFilter) ([]FailedMessage, error) {
	args := m.Called(ctx, filter)
	return args.Get(0).([]FailedMessage), args.Error(1)
}

func (m *mockErrorStore) Delete(ctx context.Context, id string) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

type mockMetricsCollector struct {
	mock.Mock
}

func (m *mockMetricsCollector) RecordDLQMessage(queue string, action string) {
	m.Called(queue, action)
}

func (m *mockMetricsCollector) RecordRetryAttempt(queue string, success bool) {
	m.Called(queue, success)
}

func (m *mockMetricsCollector) RecordErrorStoreOperation(operation string, success bool) {
	m.Called(operation, success)
}

func TestDLQHandler(t *testing.T) {
	t.Run("creates with default options", func(t *testing.T) {
		handler := NewDLQHandler()
		
		assert.NotNil(t, handler.logger)
		assert.Equal(t, 3, handler.maxRetries)
		assert.Equal(t, time.Minute, handler.retryDelay)
		assert.Nil(t, handler.errorStore)
		assert.Nil(t, handler.metricsCollector)
	})

	t.Run("applies options", func(t *testing.T) {
		logger := slog.Default()
		errorStore := &mockErrorStore{}
		metricsCollector := &mockMetricsCollector{}
		
		handler := NewDLQHandler(
			WithDLQLogger(logger),
			WithDLQMaxRetries(5),
			WithDLQRetryDelay(30*time.Second),
			WithErrorStore(errorStore),
			WithMetricsCollector(metricsCollector),
		)
		
		assert.Equal(t, logger, handler.logger)
		assert.Equal(t, 5, handler.maxRetries)
		assert.Equal(t, 30*time.Second, handler.retryDelay)
		assert.Equal(t, errorStore, handler.errorStore)
		assert.Equal(t, metricsCollector, handler.metricsCollector)
	})
}

func TestDLQHandler_ExtractMetadata(t *testing.T) {
	handler := NewDLQHandler()
	
	t.Run("extracts from headers", func(t *testing.T) {
		msg := amqp.Delivery{
			Headers: amqp.Table{
				"x-original-queue":   "test.queue",
				"x-last-error":       "connection failed",
				"x-retry-count":      int64(2),
				"x-first-death-time": int64(1234567890),
			},
		}
		
		metadata := handler.extractMetadata(msg)
		
		assert.Equal(t, "test.queue", metadata.OriginalQueue)
		assert.Equal(t, "connection failed", metadata.LastError)
		assert.Equal(t, 2, metadata.RetryCount)
		assert.Equal(t, time.Unix(1234567890, 0), metadata.FirstDeathAt)
	})

	t.Run("extracts from x-death header", func(t *testing.T) {
		msg := amqp.Delivery{
			Headers: amqp.Table{
				"x-death": []interface{}{
					amqp.Table{
						"queue":  "dlq.test",
						"reason": "rejected",
						"count":  int64(3),
					},
				},
			},
		}
		
		metadata := handler.extractMetadata(msg)
		
		assert.Equal(t, "dlq.test", metadata.OriginalQueue)
		assert.Equal(t, "rejected", metadata.LastError)
		assert.Equal(t, 3, metadata.RetryCount)
	})

	t.Run("handles missing headers", func(t *testing.T) {
		msg := amqp.Delivery{}
		
		metadata := handler.extractMetadata(msg)
		
		assert.Empty(t, metadata.OriginalQueue)
		assert.Empty(t, metadata.LastError)
		assert.Equal(t, 0, metadata.RetryCount)
		assert.Zero(t, metadata.FirstDeathAt)
	})
}

func TestDLQHandler_ProcessDLQMessage(t *testing.T) {
	ctx := context.Background()
	
	t.Run("stores message when max retries exceeded", func(t *testing.T) {
		errorStore := &mockErrorStore{}
		metricsCollector := &mockMetricsCollector{}
		
		handler := NewDLQHandler(
			WithDLQMaxRetries(2),
			WithErrorStore(errorStore),
			WithMetricsCollector(metricsCollector),
		)
		
		msg := amqp.Delivery{
			MessageId: "msg-123",
			Headers: amqp.Table{
				"x-retry-count":    int64(2),
				"x-original-queue": "test.queue",
				"x-last-error":     "timeout",
			},
			Body: []byte("test message"),
		}
		ack := &mockAcknowledger{}
		ack.On("Ack", uint64(0), false).Return(nil)
		msg.Acknowledger = ack
		
		// Set expectations
		errorStore.On("Store", ctx, mock.MatchedBy(func(fm FailedMessage) bool {
			return fm.ID == "msg-123" &&
				fm.Queue == "test.queue" &&
				fm.Error == "timeout" &&
				fm.RetryCount == 2
		})).Return(nil)
		
		metricsCollector.On("RecordDLQMessage", "test.queue", "max_retries_exceeded")
		
		// Process message
		err := handler.ProcessDLQMessage(ctx, msg)
		
		assert.NoError(t, err)
		errorStore.AssertExpectations(t)
		metricsCollector.AssertExpectations(t)
	})

	t.Run("handles error store failure gracefully", func(t *testing.T) {
		errorStore := &mockErrorStore{}
		
		handler := NewDLQHandler(
			WithDLQMaxRetries(1),
			WithErrorStore(errorStore),
		)
		
		msg := amqp.Delivery{
			MessageId: "msg-456",
			Headers: amqp.Table{
				"x-retry-count": int64(1),
			},
		}
		ack := &mockAcknowledger{}
		ack.On("Ack", uint64(0), false).Return(nil)
		msg.Acknowledger = ack
		
		errorStore.On("Store", ctx, mock.Anything).Return(errors.New("store failed"))
		
		// Should not return error even if store fails
		err := handler.ProcessDLQMessage(ctx, msg)
		assert.NoError(t, err)
		
		errorStore.AssertExpectations(t)
	})
}

func TestDLQHandler_ProcessDLQBatch(t *testing.T) {
	handler := NewDLQHandler(WithDLQMaxRetries(1))
	ctx := context.Background()
	
	ack1 := &mockAcknowledger{}
	ack1.On("Ack", uint64(0), false).Return(nil)
	
	ack2 := &mockAcknowledger{}
	ack2.On("Ack", uint64(0), false).Return(nil)
	
	messages := []amqp.Delivery{
		{
			MessageId: "msg-1",
			Headers:   amqp.Table{"x-retry-count": int64(1)},
			Acknowledger: ack1,
		},
		{
			MessageId: "msg-2",
			Headers:   amqp.Table{"x-retry-count": int64(1)},
			Acknowledger: ack2,
		},
	}
	
	err := handler.ProcessDLQBatch(ctx, messages)
	assert.NoError(t, err)
}

func TestInMemoryErrorStore(t *testing.T) {
	store := NewInMemoryErrorStore()
	ctx := context.Background()
	
	t.Run("Store and Get", func(t *testing.T) {
		msg := FailedMessage{
			ID:            "test-123",
			Queue:         "test.queue",
			Error:         "test error",
			RetryCount:    3,
			LastFailedAt:  time.Now(),
			Body:          []byte("test body"),
			CorrelationID: "corr-123",
		}
		
		// Store
		err := store.Store(ctx, msg)
		assert.NoError(t, err)
		
		// Get
		retrieved, err := store.Get(ctx, "test-123")
		require.NoError(t, err)
		assert.Equal(t, msg.ID, retrieved.ID)
		assert.Equal(t, msg.Queue, retrieved.Queue)
		assert.Equal(t, msg.Error, retrieved.Error)
		assert.Equal(t, msg.Body, retrieved.Body)
	})

	t.Run("Get non-existent message", func(t *testing.T) {
		_, err := store.Get(ctx, "non-existent")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("Delete", func(t *testing.T) {
		msg := FailedMessage{ID: "delete-me"}
		store.Store(ctx, msg)
		
		// Verify it exists
		_, err := store.Get(ctx, "delete-me")
		assert.NoError(t, err)
		
		// Delete
		err = store.Delete(ctx, "delete-me")
		assert.NoError(t, err)
		
		// Verify it's gone
		_, err = store.Get(ctx, "delete-me")
		assert.Error(t, err)
	})

	t.Run("List with filters", func(t *testing.T) {
		// Clear store
		store = NewInMemoryErrorStore()
		
		now := time.Now()
		messages := []FailedMessage{
			{
				ID:           "msg-1",
				Queue:        "queue.a",
				LastFailedAt: now.Add(-2 * time.Hour),
			},
			{
				ID:           "msg-2",
				Queue:        "queue.b",
				LastFailedAt: now.Add(-1 * time.Hour),
			},
			{
				ID:           "msg-3",
				Queue:        "queue.a",
				LastFailedAt: now.Add(-30 * time.Minute),
			},
		}
		
		for _, msg := range messages {
			store.Store(ctx, msg)
		}
		
		// Filter by queue
		results, err := store.List(ctx, ErrorFilter{Queue: "queue.a"})
		assert.NoError(t, err)
		assert.Len(t, results, 2)
		
		// Filter by time range
		results, err = store.List(ctx, ErrorFilter{
			StartTime: now.Add(-90 * time.Minute),
			EndTime:   now,
		})
		assert.NoError(t, err)
		assert.Len(t, results, 2)
		
		// Filter with max results
		results, err = store.List(ctx, ErrorFilter{MaxResults: 2})
		assert.NoError(t, err)
		assert.Len(t, results, 2)
	})
}

func TestFailedMessage_MarshalJSON(t *testing.T) {
	msg := FailedMessage{
		ID:            "test-123",
		Queue:         "test.queue",
		Body:          []byte("test body"),
		Error:         "test error",
		RetryCount:    2,
		FirstFailedAt: time.Unix(1234567890, 0),
		LastFailedAt:  time.Unix(1234567900, 0),
	}
	
	data, err := json.Marshal(msg)
	require.NoError(t, err)
	
	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)
	
	assert.Equal(t, "test-123", result["ID"])
	assert.Equal(t, "test.queue", result["Queue"])
	assert.Equal(t, "test body", result["body"]) // Body is converted to string
	assert.Equal(t, "test error", result["Error"])
	assert.Equal(t, float64(2), result["RetryCount"])
}

func TestGetHeaderFunctions(t *testing.T) {
	handler := NewDLQHandler()
	
	t.Run("getHeaderString", func(t *testing.T) {
		headers := amqp.Table{
			"string-key": "value",
			"int-key":    123,
		}
		
		assert.Equal(t, "value", handler.getHeaderString(headers, "string-key"))
		assert.Equal(t, "", handler.getHeaderString(headers, "int-key"))
		assert.Equal(t, "", handler.getHeaderString(headers, "missing"))
		assert.Equal(t, "", handler.getHeaderString(nil, "any"))
	})

	t.Run("getHeaderInt", func(t *testing.T) {
		headers := amqp.Table{
			"int":     int(42),
			"int32":   int32(43),
			"int64":   int64(44),
			"float64": float64(45.0),
			"string":  "not-a-number",
		}
		
		assert.Equal(t, 42, handler.getHeaderInt(headers, "int"))
		assert.Equal(t, 43, handler.getHeaderInt(headers, "int32"))
		assert.Equal(t, 44, handler.getHeaderInt(headers, "int64"))
		assert.Equal(t, 45, handler.getHeaderInt(headers, "float64"))
		assert.Equal(t, 0, handler.getHeaderInt(headers, "string"))
		assert.Equal(t, 0, handler.getHeaderInt(headers, "missing"))
		assert.Equal(t, 0, handler.getHeaderInt(nil, "any"))
	})

	t.Run("getHeaderTime", func(t *testing.T) {
		now := time.Now()
		headers := amqp.Table{
			"int64":   int64(1234567890),
			"float64": float64(1234567890),
			"time":    now,
			"string":  "not-a-time",
		}
		
		assert.Equal(t, time.Unix(1234567890, 0), handler.getHeaderTime(headers, "int64"))
		assert.Equal(t, time.Unix(1234567890, 0), handler.getHeaderTime(headers, "float64"))
		assert.Equal(t, now, handler.getHeaderTime(headers, "time"))
		assert.Zero(t, handler.getHeaderTime(headers, "string"))
		assert.Zero(t, handler.getHeaderTime(headers, "missing"))
		assert.Zero(t, handler.getHeaderTime(nil, "any"))
	})
}

// Mock Acknowledger for testing
type mockAcknowledger struct {
	mock.Mock
}

func (m *mockAcknowledger) Ack(tag uint64, multiple bool) error {
	args := m.Called(tag, multiple)
	return args.Error(0)
}

func (m *mockAcknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
	args := m.Called(tag, multiple, requeue)
	return args.Error(0)
}

func (m *mockAcknowledger) Reject(tag uint64, requeue bool) error {
	args := m.Called(tag, requeue)
	return args.Error(0)
}