package rabbitmq

import (
	"fmt"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

// Test batch message creation
func TestBatchMessageCreation(t *testing.T) {
	t.Run("create batch messages", func(t *testing.T) {
		messages := []PublishMessage{
			{
				Exchange:   "test.exchange",
				RoutingKey: "test.key.1",
				Mandatory:  true,
				Message: amqp.Publishing{
					ContentType: "application/json",
					Body:        []byte(`{"id": 1}`),
				},
			},
			{
				Exchange:   "test.exchange",
				RoutingKey: "test.key.2",
				Immediate:  true,
				Message: amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte("message 2"),
				},
			},
		}
		
		// Verify message properties
		assert.Len(t, messages, 2)
		assert.True(t, messages[0].Mandatory)
		assert.False(t, messages[0].Immediate)
		assert.False(t, messages[1].Mandatory)
		assert.True(t, messages[1].Immediate)
		assert.Equal(t, "application/json", messages[0].Message.ContentType)
		assert.Equal(t, "text/plain", messages[1].Message.ContentType)
	})
}

// Test PublishMessage struct
func TestPublishMessageStruct(t *testing.T) {
	t.Run("publish message fields", func(t *testing.T) {
		msg := PublishMessage{
			Exchange:   "events",
			RoutingKey: "user.created",
			Mandatory:  true,
			Immediate:  false,
			Message: amqp.Publishing{
				Headers: amqp.Table{
					"x-custom": "value",
				},
				ContentType:     "application/json",
				ContentEncoding: "utf-8",
				DeliveryMode:    amqp.Persistent,
				Priority:        5,
				MessageId:       "msg-123",
				Timestamp:       time.Now(),
				Type:            "UserCreatedEvent",
				UserId:          "system",
				AppId:           "user-service",
				Body:            []byte(`{"userId": "123", "email": "test@example.com"}`),
			},
		}
		
		assert.Equal(t, "events", msg.Exchange)
		assert.Equal(t, "user.created", msg.RoutingKey)
		assert.True(t, msg.Mandatory)
		assert.False(t, msg.Immediate)
		assert.Equal(t, "application/json", msg.Message.ContentType)
		assert.Equal(t, uint8(5), msg.Message.Priority)
		assert.Equal(t, "msg-123", msg.Message.MessageId)
		assert.Equal(t, "UserCreatedEvent", msg.Message.Type)
	})
}

// Test batch publishing patterns
func TestBatchPublishingPatterns(t *testing.T) {
	t.Run("fan-out pattern", func(t *testing.T) {
		// Create messages for fan-out pattern
		event := []byte(`{"event": "order.created", "orderId": "123"}`)
		
		messages := []PublishMessage{
			{
				Exchange:   "orders.fanout",
				RoutingKey: "", // Empty routing key for fanout
				Message: amqp.Publishing{
					ContentType: "application/json",
					Body:        event,
				},
			},
		}
		
		assert.Len(t, messages, 1)
		assert.Empty(t, messages[0].RoutingKey)
		assert.Equal(t, "orders.fanout", messages[0].Exchange)
	})
	
	t.Run("topic pattern", func(t *testing.T) {
		// Create messages for topic pattern
		messages := []PublishMessage{
			{
				Exchange:   "logs.topic",
				RoutingKey: "app.server1.error",
				Message: amqp.Publishing{
					Body: []byte("Error: Connection timeout"),
				},
			},
			{
				Exchange:   "logs.topic",
				RoutingKey: "app.server2.info",
				Message: amqp.Publishing{
					Body: []byte("Info: Request processed"),
				},
			},
			{
				Exchange:   "logs.topic",
				RoutingKey: "app.server1.debug",
				Message: amqp.Publishing{
					Body: []byte("Debug: Cache hit"),
				},
			},
		}
		
		assert.Len(t, messages, 3)
		for _, msg := range messages {
			assert.Equal(t, "logs.topic", msg.Exchange)
			assert.Contains(t, msg.RoutingKey, "app.server")
		}
	})
	
	t.Run("priority queue pattern", func(t *testing.T) {
		// Create messages with different priorities
		messages := []PublishMessage{
			{
				Exchange:   "tasks",
				RoutingKey: "priority.tasks",
				Message: amqp.Publishing{
					Priority: 0, // Low priority
					Body:     []byte(`{"task": "cleanup", "priority": "low"}`),
				},
			},
			{
				Exchange:   "tasks",
				RoutingKey: "priority.tasks",
				Message: amqp.Publishing{
					Priority: 5, // Medium priority
					Body:     []byte(`{"task": "process", "priority": "medium"}`),
				},
			},
			{
				Exchange:   "tasks",
				RoutingKey: "priority.tasks",
				Message: amqp.Publishing{
					Priority: 9, // High priority
					Body:     []byte(`{"task": "alert", "priority": "high"}`),
				},
			},
		}
		
		assert.Len(t, messages, 3)
		assert.Equal(t, uint8(0), messages[0].Message.Priority)
		assert.Equal(t, uint8(5), messages[1].Message.Priority)
		assert.Equal(t, uint8(9), messages[2].Message.Priority)
	})
	
	t.Run("direct exchange pattern", func(t *testing.T) {
		// Create messages for direct exchange with specific routing
		messages := []PublishMessage{
			{
				Exchange:   "notifications.direct",
				RoutingKey: "email",
				Message: amqp.Publishing{
					ContentType: "application/json",
					Body:        []byte(`{"type": "email", "to": "user@example.com", "subject": "Welcome"}`),
				},
			},
			{
				Exchange:   "notifications.direct",
				RoutingKey: "sms",
				Message: amqp.Publishing{
					ContentType: "application/json",
					Body:        []byte(`{"type": "sms", "to": "+1234567890", "message": "Your code is 123456"}`),
				},
			},
			{
				Exchange:   "notifications.direct",
				RoutingKey: "push",
				Message: amqp.Publishing{
					ContentType: "application/json",
					Body:        []byte(`{"type": "push", "deviceId": "device123", "title": "New message"}`),
				},
			},
		}
		
		assert.Len(t, messages, 3)
		assert.Equal(t, "email", messages[0].RoutingKey)
		assert.Equal(t, "sms", messages[1].RoutingKey)
		assert.Equal(t, "push", messages[2].RoutingKey)
	})
}

// Test batch size limits
func TestBatchSizeLimits(t *testing.T) {
	t.Run("small batch", func(t *testing.T) {
		messages := createTestMessages(10)
		assert.Len(t, messages, 10)
		assert.Equal(t, "test.key.0", messages[0].RoutingKey)
		assert.Equal(t, "test.key.9", messages[9].RoutingKey)
	})
	
	t.Run("medium batch", func(t *testing.T) {
		messages := createTestMessages(100)
		assert.Len(t, messages, 100)
		assert.Equal(t, "test.key.0", messages[0].RoutingKey)
		assert.Equal(t, "test.key.50", messages[50].RoutingKey)
		assert.Equal(t, "test.key.99", messages[99].RoutingKey)
	})
	
	t.Run("large batch", func(t *testing.T) {
		messages := createTestMessages(1000)
		assert.Len(t, messages, 1000)
		assert.Equal(t, "test.key.0", messages[0].RoutingKey)
		assert.Equal(t, "test.key.500", messages[500].RoutingKey)
		assert.Equal(t, "test.key.999", messages[999].RoutingKey)
	})
}

// Test concurrent batch operations
func TestConcurrentBatchOperations(t *testing.T) {
	t.Run("concurrent message creation", func(t *testing.T) {
		numGoroutines := 10
		messagesPerGoroutine := 100
		
		var wg sync.WaitGroup
		messagesChan := make(chan []PublishMessage, numGoroutines)
		
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				
				messages := make([]PublishMessage, messagesPerGoroutine)
				for j := 0; j < messagesPerGoroutine; j++ {
					messages[j] = PublishMessage{
						Exchange:   fmt.Sprintf("exchange.%d", id),
						RoutingKey: fmt.Sprintf("key.%d.%d", id, j),
						Message: amqp.Publishing{
							Body: []byte(fmt.Sprintf(`{"goroutine": %d, "message": %d}`, id, j)),
						},
					}
				}
				
				messagesChan <- messages
			}(i)
		}
		
		wg.Wait()
		close(messagesChan)
		
		// Collect all messages
		allMessages := make([]PublishMessage, 0, numGoroutines*messagesPerGoroutine)
		for messages := range messagesChan {
			allMessages = append(allMessages, messages...)
		}
		
		assert.Len(t, allMessages, numGoroutines*messagesPerGoroutine)
		
		// Verify uniqueness
		seen := make(map[string]bool)
		for _, msg := range allMessages {
			key := fmt.Sprintf("%s:%s", msg.Exchange, msg.RoutingKey)
			assert.False(t, seen[key], "Duplicate message found: %s", key)
			seen[key] = true
		}
	})
}

// Test message headers and properties
func TestBatchMessageProperties(t *testing.T) {
	t.Run("messages with headers", func(t *testing.T) {
		messages := []PublishMessage{
			{
				Exchange:   "test.exchange",
				RoutingKey: "test.key",
				Message: amqp.Publishing{
					Headers: amqp.Table{
						"x-retry-count":    0,
						"x-original-queue": "source.queue",
						"x-trace-id":       "trace-123",
						"x-span-id":        "span-456",
						"x-custom-header":  "custom-value",
					},
					ContentType: "application/json",
					Body:        []byte(`{"data": "test"}`),
				},
			},
		}
		
		headers := messages[0].Message.Headers
		assert.Equal(t, 0, headers["x-retry-count"])
		assert.Equal(t, "source.queue", headers["x-original-queue"])
		assert.Equal(t, "trace-123", headers["x-trace-id"])
		assert.Equal(t, "span-456", headers["x-span-id"])
		assert.Equal(t, "custom-value", headers["x-custom-header"])
	})
	
	t.Run("messages with correlation", func(t *testing.T) {
		correlationId := "corr-123-456"
		replyTo := "reply.queue"
		
		messages := []PublishMessage{
			{
				Exchange:   "rpc.exchange",
				RoutingKey: "rpc.request",
				Message: amqp.Publishing{
					CorrelationId: correlationId,
					ReplyTo:       replyTo,
					ContentType:   "application/json",
					Body:          []byte(`{"method": "getUser", "params": {"id": "123"}}`),
				},
			},
		}
		
		assert.Equal(t, correlationId, messages[0].Message.CorrelationId)
		assert.Equal(t, replyTo, messages[0].Message.ReplyTo)
	})
	
	t.Run("messages with timestamps", func(t *testing.T) {
		now := time.Now()
		messages := []PublishMessage{
			{
				Exchange:   "events.exchange",
				RoutingKey: "event.occurred",
				Message: amqp.Publishing{
					Timestamp: now,
					Headers: amqp.Table{
						"x-event-time": now.Unix(),
					},
					ContentType: "application/json",
					Body:        []byte(`{"event": "user.login"}`),
				},
			},
		}
		
		assert.Equal(t, now.Unix(), messages[0].Message.Timestamp.Unix())
		assert.Equal(t, now.Unix(), messages[0].Message.Headers["x-event-time"])
	})
}

// Test message TTL and expiration
func TestBatchMessageTTL(t *testing.T) {
	t.Run("messages with TTL", func(t *testing.T) {
		messages := []PublishMessage{
			{
				Exchange:   "ttl.exchange",
				RoutingKey: "short.ttl",
				Message: amqp.Publishing{
					Expiration: "5000", // 5 seconds
					Body:       []byte("expires in 5 seconds"),
				},
			},
			{
				Exchange:   "ttl.exchange",
				RoutingKey: "long.ttl",
				Message: amqp.Publishing{
					Expiration: "3600000", // 1 hour
					Body:       []byte("expires in 1 hour"),
				},
			},
			{
				Exchange:   "ttl.exchange",
				RoutingKey: "no.ttl",
				Message: amqp.Publishing{
					// No expiration
					Body: []byte("never expires"),
				},
			},
		}
		
		assert.Equal(t, "5000", messages[0].Message.Expiration)
		assert.Equal(t, "3600000", messages[1].Message.Expiration)
		assert.Empty(t, messages[2].Message.Expiration)
	})
}

// Test message encoding
func TestBatchMessageEncoding(t *testing.T) {
	t.Run("different content types", func(t *testing.T) {
		messages := []PublishMessage{
			{
				Exchange:   "data.exchange",
				RoutingKey: "json.data",
				Message: amqp.Publishing{
					ContentType:     "application/json",
					ContentEncoding: "utf-8",
					Body:            []byte(`{"name": "test", "value": 123}`),
				},
			},
			{
				Exchange:   "data.exchange",
				RoutingKey: "xml.data",
				Message: amqp.Publishing{
					ContentType:     "application/xml",
					ContentEncoding: "utf-8",
					Body:            []byte(`<data><name>test</name><value>123</value></data>`),
				},
			},
			{
				Exchange:   "data.exchange",
				RoutingKey: "text.data",
				Message: amqp.Publishing{
					ContentType:     "text/plain",
					ContentEncoding: "utf-8",
					Body:            []byte("plain text message"),
				},
			},
			{
				Exchange:   "data.exchange",
				RoutingKey: "binary.data",
				Message: amqp.Publishing{
					ContentType: "application/octet-stream",
					Body:        []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05},
				},
			},
		}
		
		assert.Equal(t, "application/json", messages[0].Message.ContentType)
		assert.Equal(t, "application/xml", messages[1].Message.ContentType)
		assert.Equal(t, "text/plain", messages[2].Message.ContentType)
		assert.Equal(t, "application/octet-stream", messages[3].Message.ContentType)
	})
}

// Benchmark batch message creation
func BenchmarkBatchMessageCreation(b *testing.B) {
	b.Run("small batch (10 messages)", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = createTestMessages(10)
		}
	})
	
	b.Run("medium batch (100 messages)", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = createTestMessages(100)
		}
	})
	
	b.Run("large batch (1000 messages)", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = createTestMessages(1000)
		}
	})
	
	b.Run("single message with headers", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = PublishMessage{
				Exchange:   "bench.exchange",
				RoutingKey: "bench.key",
				Message: amqp.Publishing{
					Headers: amqp.Table{
						"header1": "value1",
						"header2": "value2",
						"header3": "value3",
					},
					ContentType: "application/json",
					Body:        []byte(`{"id": 1, "data": "benchmark"}`),
				},
			}
		}
	})
}

// Helper function to create test messages
func createTestMessages(count int) []PublishMessage {
	messages := make([]PublishMessage, count)
	for i := 0; i < count; i++ {
		messages[i] = PublishMessage{
			Exchange:   "test.exchange",
			RoutingKey: fmt.Sprintf("test.key.%d", i),
			Message: amqp.Publishing{
				ContentType: "application/json",
				Body:        []byte(fmt.Sprintf(`{"message": %d, "timestamp": "%s"}`, i, time.Now().Format(time.RFC3339))),
			},
		}
	}
	return messages
}