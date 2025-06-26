package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQClient provides monitoring capabilities for RabbitMQ
type RabbitMQClient struct {
	conn          *amqp.Connection
	managementURL string
	httpClient    *http.Client
	username      string
	password      string
}

// QueueInfo contains queue statistics
type QueueInfo struct {
	Name        string
	VHost       string
	Messages    int
	Consumers   int
	MessageRate float64
	State       string
	Durable     bool
	AutoDelete  bool
	Memory      int64
}

// ExchangeInfo contains exchange statistics
type ExchangeInfo struct {
	Name        string
	Type        string
	Durable     bool
	AutoDelete  bool
	MessageRate float64
}

// MessageInfo contains message details
type MessageInfo struct {
	MessageID     string
	Type          string
	CorrelationID string
	Timestamp     time.Time
	RoutingKey    string
	Exchange      string
	Redelivered   bool
	RetryCount    int
	Headers       map[string]interface{}
	BodyPreview   string
}

// DLQInfo contains dead letter queue information
type DLQInfo struct {
	Name              string
	MessageCount      int
	OriginalQueue     string
	OldestMessageTime time.Time
}

// HealthStatus contains system health information
type HealthStatus struct {
	Status               string
	RabbitMQVersion      string
	ErlangVersion        string
	MemoryUsed           int64
	MemoryLimit          int64
	DiskFree             int64
	FileDescriptorsUsed  int
	FileDescriptorsTotal int
	SocketsUsed          int
	SocketsTotal         int
	Alarms               []string
	TotalQueues          int
	TotalMessages        int
	MessagesReady        int
	MessagesUnacked      int
	TotalConnections     int
	TotalChannels        int
	TotalConsumers       int
}

// NewRabbitMQClient creates a new RabbitMQ monitoring client
func NewRabbitMQClient(amqpURL string) (*RabbitMQClient, error) {
	// Parse AMQP URL to extract connection details
	u, err := url.Parse(amqpURL)
	if err != nil {
		return nil, fmt.Errorf("invalid AMQP URL: %w", err)
	}

	// Extract username and password
	username := "guest"
	password := "guest"
	if u.User != nil {
		username = u.User.Username()
		if p, ok := u.User.Password(); ok {
			password = p
		}
	}

	// Construct management API URL (default port 15672)
	managementHost := u.Hostname()
	managementPort := "15672"
	if u.Port() == "5671" || u.Port() == "5672" {
		// Standard AMQP ports, use default management port
	} else if u.Port() != "" {
		// Custom port, assume management API is on port+10000
		managementPort = u.Port()
	}

	managementURL := fmt.Sprintf("http://%s:%s/api", managementHost, managementPort)

	// Create HTTP client
	httpClient := &http.Client{
		Timeout: 10 * time.Second,
	}

	// Connect to RabbitMQ
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	return &RabbitMQClient{
		conn:          conn,
		managementURL: managementURL,
		httpClient:    httpClient,
		username:      username,
		password:      password,
	}, nil
}

// Close closes the connection
func (c *RabbitMQClient) Close() error {
	if c.conn != nil && !c.conn.IsClosed() {
		return c.conn.Close()
	}
	return nil
}

// ListQueues returns information about all queues
func (c *RabbitMQClient) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	resp, err := c.managementRequest(ctx, "GET", "/queues", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var apiQueues []struct {
		Name        string `json:"name"`
		VHost       string `json:"vhost"`
		Messages    int    `json:"messages"`
		Consumers   int    `json:"consumers"`
		State       string `json:"state"`
		Durable     bool   `json:"durable"`
		AutoDelete  bool   `json:"auto_delete"`
		Memory      int64  `json:"memory"`
		MessageStats struct {
			PublishDetails struct {
				Rate float64 `json:"rate"`
			} `json:"publish_details"`
		} `json:"message_stats"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&apiQueues); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	queues := make([]QueueInfo, len(apiQueues))
	for i, q := range apiQueues {
		queues[i] = QueueInfo{
			Name:        q.Name,
			VHost:       q.VHost,
			Messages:    q.Messages,
			Consumers:   q.Consumers,
			MessageRate: q.MessageStats.PublishDetails.Rate,
			State:       q.State,
			Durable:     q.Durable,
			AutoDelete:  q.AutoDelete,
			Memory:      q.Memory,
		}
	}

	return queues, nil
}

// ListExchanges returns information about all exchanges
func (c *RabbitMQClient) ListExchanges(ctx context.Context) ([]ExchangeInfo, error) {
	resp, err := c.managementRequest(ctx, "GET", "/exchanges", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var apiExchanges []struct {
		Name       string `json:"name"`
		Type       string `json:"type"`
		Durable    bool   `json:"durable"`
		AutoDelete bool   `json:"auto_delete"`
		MessageStats struct {
			PublishInDetails struct {
				Rate float64 `json:"rate"`
			} `json:"publish_in_details"`
		} `json:"message_stats"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&apiExchanges); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	exchanges := make([]ExchangeInfo, 0, len(apiExchanges))
	for _, e := range apiExchanges {
		// Skip default exchanges
		if e.Name == "" || strings.HasPrefix(e.Name, "amq.") {
			continue
		}
		exchanges = append(exchanges, ExchangeInfo{
			Name:        e.Name,
			Type:        e.Type,
			Durable:     e.Durable,
			AutoDelete:  e.AutoDelete,
			MessageRate: e.MessageStats.PublishInDetails.Rate,
		})
	}

	return exchanges, nil
}

// PeekMessages returns a preview of messages in a queue without consuming them
func (c *RabbitMQClient) PeekMessages(ctx context.Context, queueName string, count int) ([]MessageInfo, error) {
	// Use management API to get messages
	endpoint := fmt.Sprintf("/queues/%%2F/%s/get", url.QueryEscape(queueName))
	payload := map[string]interface{}{
		"count":    count,
		"ackmode":  "ack_requeue_true",
		"encoding": "auto",
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	resp, err := c.managementRequest(ctx, "POST", endpoint, data)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var apiMessages []struct {
		MessageCount int `json:"message_count"`
		Properties   struct {
			MessageID     string                 `json:"message_id"`
			Type          string                 `json:"type"`
			CorrelationID string                 `json:"correlation_id"`
			Timestamp     int64                  `json:"timestamp"`
			Headers       map[string]interface{} `json:"headers"`
		} `json:"properties"`
		RoutingKey  string `json:"routing_key"`
		Exchange    string `json:"exchange"`
		Redelivered bool   `json:"redelivered"`
		Payload     string `json:"payload"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&apiMessages); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	messages := make([]MessageInfo, len(apiMessages))
	for i, m := range apiMessages {
		// Extract retry count from headers if available
		retryCount := 0
		if m.Properties.Headers != nil {
			if rc, ok := m.Properties.Headers["x-retry-count"]; ok {
				if count, ok := rc.(float64); ok {
					retryCount = int(count)
				}
			}
		}

		// Parse timestamp
		var timestamp time.Time
		if m.Properties.Timestamp > 0 {
			timestamp = time.Unix(m.Properties.Timestamp, 0)
		}

		// Extract message type from headers if not in properties
		messageType := m.Properties.Type
		if messageType == "" && m.Properties.Headers != nil {
			if mt, ok := m.Properties.Headers["x-message-type"].(string); ok {
				messageType = mt
			}
		}

		messages[i] = MessageInfo{
			MessageID:     m.Properties.MessageID,
			Type:          messageType,
			CorrelationID: m.Properties.CorrelationID,
			Timestamp:     timestamp,
			RoutingKey:    m.RoutingKey,
			Exchange:      m.Exchange,
			Redelivered:   m.Redelivered,
			RetryCount:    retryCount,
			Headers:       m.Properties.Headers,
			BodyPreview:   m.Payload,
		}
	}

	return messages, nil
}

// ListDLQs returns information about dead letter queues
func (c *RabbitMQClient) ListDLQs(ctx context.Context) ([]DLQInfo, error) {
	queues, err := c.ListQueues(ctx)
	if err != nil {
		return nil, err
	}

	var dlqs []DLQInfo
	for _, q := range queues {
		if strings.HasPrefix(q.Name, "dlq.") {
			// Extract original queue name
			originalQueue := strings.TrimPrefix(q.Name, "dlq.")
			
			dlq := DLQInfo{
				Name:          q.Name,
				MessageCount:  q.Messages,
				OriginalQueue: originalQueue,
			}

			// Get oldest message time if queue has messages
			if q.Messages > 0 {
				messages, err := c.PeekMessages(ctx, q.Name, 1)
				if err == nil && len(messages) > 0 {
					dlq.OldestMessageTime = messages[0].Timestamp
				}
			}

			dlqs = append(dlqs, dlq)
		}
	}

	return dlqs, nil
}

// RequeueDLQMessages moves messages from DLQ back to original queue
func (c *RabbitMQClient) RequeueDLQMessages(ctx context.Context, dlqName string, count int) (int, error) {
	// This is a simplified implementation
	// In production, you would want to use the shovel plugin or a dedicated requeue mechanism
	
	ch, err := c.conn.Channel()
	if err != nil {
		return 0, fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	// Set QoS
	if err := ch.Qos(1, 0, false); err != nil {
		return 0, fmt.Errorf("failed to set QoS: %w", err)
	}

	// Consume messages
	msgs, err := ch.Consume(
		dlqName,
		"",
		false, // manual ack
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to consume messages: %w", err)
	}

	// Extract original queue from DLQ name
	originalQueue := strings.TrimPrefix(dlqName, "dlq.")
	
	requeued := 0
	timeout := time.After(5 * time.Second)
	
	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				return requeued, nil
			}

			// Reset retry count
			if msg.Headers == nil {
				msg.Headers = make(amqp.Table)
			}
			delete(msg.Headers, "x-retry-count")
			delete(msg.Headers, "x-death")

			// Publish to original queue
			err := ch.PublishWithContext(
				ctx,
				"",           // exchange
				originalQueue, // routing key
				false,
				false,
				amqp.Publishing{
					Headers:         msg.Headers,
					ContentType:     msg.ContentType,
					ContentEncoding: msg.ContentEncoding,
					DeliveryMode:    msg.DeliveryMode,
					Priority:        msg.Priority,
					CorrelationId:   msg.CorrelationId,
					ReplyTo:         msg.ReplyTo,
					Expiration:      msg.Expiration,
					MessageId:       msg.MessageId,
					Timestamp:       msg.Timestamp,
					Type:            msg.Type,
					UserId:          msg.UserId,
					AppId:           msg.AppId,
					Body:            msg.Body,
				},
			)
			if err != nil {
				// Nack and stop
				msg.Nack(false, true)
				return requeued, fmt.Errorf("failed to republish message: %w", err)
			}

			// Ack the message
			if err := msg.Ack(false); err != nil {
				return requeued, fmt.Errorf("failed to ack message: %w", err)
			}

			requeued++
			
			if count > 0 && requeued >= count {
				// Cancel consumer
				ch.Cancel("", false)
				return requeued, nil
			}

		case <-timeout:
			// No more messages
			ch.Cancel("", false)
			return requeued, nil
			
		case <-ctx.Done():
			ch.Cancel("", false)
			return requeued, ctx.Err()
		}
	}
}

// CheckHealth returns system health status
func (c *RabbitMQClient) CheckHealth(ctx context.Context) (HealthStatus, error) {
	health := HealthStatus{Status: "unknown"}

	// Get overview
	resp, err := c.managementRequest(ctx, "GET", "/overview", nil)
	if err != nil {
		health.Status = "error"
		return health, err
	}
	defer resp.Body.Close()

	var overview struct {
		RabbitmqVersion string `json:"rabbitmq_version"`
		ErlangVersion   string `json:"erlang_version"`
		MessageStats    struct {
			MessageStatsDB struct {
				MessagesReady   int `json:"messages_ready"`
				MessagesUnacked int `json:"messages_unacknowledged"`
			} `json:"message_stats"`
		} `json:"message_stats"`
		QueueTotals struct {
			Messages      int `json:"messages"`
			MessagesReady int `json:"messages_ready"`
			MessagesUnack int `json:"messages_unacknowledged"`
		} `json:"queue_totals"`
		ObjectTotals struct {
			Connections int `json:"connections"`
			Channels    int `json:"channels"`
			Queues      int `json:"queues"`
			Consumers   int `json:"consumers"`
		} `json:"object_totals"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&overview); err != nil {
		return health, fmt.Errorf("failed to decode overview: %w", err)
	}

	health.RabbitMQVersion = overview.RabbitmqVersion
	health.ErlangVersion = overview.ErlangVersion
	health.TotalQueues = overview.ObjectTotals.Queues
	health.TotalMessages = overview.QueueTotals.Messages
	health.MessagesReady = overview.QueueTotals.MessagesReady
	health.MessagesUnacked = overview.QueueTotals.MessagesUnack
	health.TotalConnections = overview.ObjectTotals.Connections
	health.TotalChannels = overview.ObjectTotals.Channels
	health.TotalConsumers = overview.ObjectTotals.Consumers

	// Get node info
	resp, err = c.managementRequest(ctx, "GET", "/nodes", nil)
	if err == nil {
		defer resp.Body.Close()
		
		var nodes []struct {
			Name               string   `json:"name"`
			Running            bool     `json:"running"`
			MemUsed            int64    `json:"mem_used"`
			MemLimit           int64    `json:"mem_limit"`
			MemAlarm           bool     `json:"mem_alarm"`
			DiskFree           int64    `json:"disk_free"`
			DiskFreeAlarm      bool     `json:"disk_free_alarm"`
			FdUsed             int      `json:"fd_used"`
			FdTotal            int      `json:"fd_total"`
			SocketsUsed        int      `json:"sockets_used"`
			SocketsTotal       int      `json:"sockets_total"`
			Partitions         []string `json:"partitions"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&nodes); err == nil && len(nodes) > 0 {
			node := nodes[0] // Use first node
			health.MemoryUsed = node.MemUsed
			health.MemoryLimit = node.MemLimit
			health.DiskFree = node.DiskFree
			health.FileDescriptorsUsed = node.FdUsed
			health.FileDescriptorsTotal = node.FdTotal
			health.SocketsUsed = node.SocketsUsed
			health.SocketsTotal = node.SocketsTotal

			// Check for alarms
			if node.MemAlarm {
				health.Alarms = append(health.Alarms, "Memory alarm - high memory usage")
			}
			if node.DiskFreeAlarm {
				health.Alarms = append(health.Alarms, "Disk alarm - low disk space")
			}
			if len(node.Partitions) > 0 {
				health.Alarms = append(health.Alarms, fmt.Sprintf("Network partition detected: %v", node.Partitions))
			}

			// Determine overall status
			if !node.Running {
				health.Status = "critical"
			} else if len(health.Alarms) > 0 {
				health.Status = "warning"
			} else {
				health.Status = "healthy"
			}
		}
	}

	return health, nil
}

// managementRequest makes an authenticated request to the management API
func (c *RabbitMQClient) managementRequest(ctx context.Context, method, endpoint string, body []byte) (*http.Response, error) {
	url := c.managementURL + endpoint
	
	var bodyReader io.Reader
	if body != nil {
		bodyReader = strings.NewReader(string(body))
	}
	
	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(c.username, c.password)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		resp.Body.Close()
		return nil, fmt.Errorf("management API error: %s", resp.Status)
	}

	return resp, nil
}

// GetOverview returns broker overview information
func (c *RabbitMQClient) GetOverview(ctx context.Context) (*Overview, error) {
	// Get health status first
	health, err := c.CheckHealth(ctx)
	if err != nil {
		return nil, err
	}

	// Create overview from health data
	overview := &Overview{
		RabbitMQVersion: health.RabbitMQVersion,
		ErlangVersion:   health.ErlangVersion,
		Node:            "rabbit@localhost", // Default node name
		Version:         health.RabbitMQVersion,
		Uptime:          0, // Would need additional API call
		MemoryUsed:            health.MemoryUsed,
		MemoryLimit:           health.MemoryLimit,
		DiskFree:              health.DiskFree,
		FileDescriptorsUsed:   health.FileDescriptorsUsed,
		FileDescriptorsTotal:  health.FileDescriptorsTotal,
	}
	
	overview.QueueTotals.Messages = health.TotalMessages
	
	// Note: MessageStats and Connections would need additional API calls
	// For now, set defaults to avoid nil pointer issues
	overview.MessageStats.Publish = 0
	overview.MessageStats.PublishDetails.Rate = 0.0
	overview.MessageStats.Deliver = 0
	overview.MessageStats.DeliverDetails.Rate = 0.0
	overview.MessageStats.Ack = 0
	overview.MessageStats.AckDetails.Rate = 0.0
	overview.Connections = make([]struct {
		Name string `json:"name"`
	}, 0)
	overview.Channels = make([]struct {
		Name string `json:"name"`
	}, 0)
	overview.Queues = make([]struct {
		Name string `json:"name"`
	}, 0)
	overview.Exchanges = make([]struct {
		Name string `json:"name"`
	}, 0)

	return overview, nil
}

// GetQueues returns queue information (alias for ListQueues)
func (c *RabbitMQClient) GetQueues(ctx context.Context) ([]QueueInfo, error) {
	return c.ListQueues(ctx)
}

// Overview contains broker overview information
type Overview struct {
	RabbitMQVersion string `json:"rabbitmq_version"`
	ErlangVersion   string `json:"erlang_version"`
	Node            string `json:"node"`
	Version         string `json:"version"`
	Uptime          int64  `json:"uptime"`
	MessageStats struct {
		Publish int `json:"publish"`
		PublishDetails struct {
			Rate float64 `json:"rate"`
		} `json:"publish_details"`
		Deliver int `json:"deliver"`
		DeliverDetails struct {
			Rate float64 `json:"rate"`
		} `json:"deliver_details"`
		Ack int `json:"ack"`
		AckDetails struct {
			Rate float64 `json:"rate"`
		} `json:"ack_details"`
	} `json:"message_stats"`
	Connections []struct {
		Name string `json:"name"`
	} `json:"connections"`
	Channels []struct {
		Name string `json:"name"`
	} `json:"channels"`
	Queues []struct {
		Name string `json:"name"`
	} `json:"queues"`
	Exchanges []struct {
		Name string `json:"name"`
	} `json:"exchanges"`
	QueueTotals struct {
		Messages int `json:"messages"`
	} `json:"queue_totals"`
	MemoryUsed            int64 `json:"memory_used"`
	MemoryLimit           int64 `json:"memory_limit"`
	DiskFree              int64 `json:"disk_free"`
	FileDescriptorsUsed   int   `json:"file_descriptors_used"`
	FileDescriptorsTotal  int   `json:"file_descriptors_total"`
}

// Type aliases for backward compatibility with TUI
type Client = RabbitMQClient
type Queue = QueueInfo

// NewClient creates a new monitoring client (alias for NewRabbitMQClient)
func NewClient(amqpURL string) (*Client, error) {
	return NewRabbitMQClient(amqpURL)
}