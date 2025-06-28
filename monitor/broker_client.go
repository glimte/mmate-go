package monitor

import (
	"context"
	"time"
)

// QueueInfo represents information about a RabbitMQ queue
type QueueInfo struct {
	Name        string  `json:"name"`
	Messages    int     `json:"messages"`
	Consumers   int     `json:"consumers"`
	MessageRate float64 `json:"message_rate"`
	Memory      int64   `json:"memory"`
	State       string  `json:"state"`
	Vhost       string  `json:"vhost"`
	Durable     bool    `json:"durable"`
	AutoDelete  bool    `json:"auto_delete"`
}

// ExchangeInfo represents information about a RabbitMQ exchange
type ExchangeInfo struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Durable    bool   `json:"durable"`
	AutoDelete bool   `json:"auto_delete"`
	Internal   bool   `json:"internal"`
	Vhost      string `json:"vhost"`
}

// ConnectionInfo represents information about a RabbitMQ connection
type ConnectionInfo struct {
	Name      string `json:"name"`
	Host      string `json:"host"`
	Port      int    `json:"port"`
	User      string `json:"user"`
	Vhost     string `json:"vhost"`
	State     string `json:"state"`
	Channels  int    `json:"channels"`
	Protocol  string `json:"protocol"`
	Connected time.Time `json:"connected"`
}

// Overview represents broker overview information
type Overview struct {
	ManagementVersion   string `json:"management_version"`
	RabbitMQVersion     string `json:"rabbitmq_version"`
	ErlangVersion       string `json:"erlang_version"`
	MessageStats        MessageStats `json:"message_stats"`
	QueueTotals         QueueTotals  `json:"queue_totals"`
	ObjectTotals        ObjectTotals `json:"object_totals"`
	StatisticsDBNode    string `json:"statistics_db_node"`
	Node                string `json:"node"`
	StatisticsDBEventQueue int `json:"statistics_db_event_queue"`
}

// MessageStats represents message statistics
type MessageStats struct {
	PublishTotal         int     `json:"publish"`
	PublishDetails       Details `json:"publish_details"`
	DeliverGetTotal      int     `json:"deliver_get"`
	DeliverGetDetails    Details `json:"deliver_get_details"`
	ConfirmTotal         int     `json:"confirm"`
	ConfirmDetails       Details `json:"confirm_details"`
	ReturnUnroutableTotal int    `json:"return_unroutable"`
	ReturnUnroutableDetails Details `json:"return_unroutable_details"`
}

// QueueTotals represents queue totals
type QueueTotals struct {
	Messages        int `json:"messages"`
	MessagesReady   int `json:"messages_ready"`
	MessagesUnacked int `json:"messages_unacknowledged"`
}

// ObjectTotals represents object totals
type ObjectTotals struct {
	Consumers   int `json:"consumers"`
	Queues      int `json:"queues"`
	Exchanges   int `json:"exchanges"`
	Connections int `json:"connections"`
	Channels    int `json:"channels"`
}

// Details represents rate details
type Details struct {
	Rate float64 `json:"rate"`
}

// RabbitMQClient defines the interface for RabbitMQ management operations
type RabbitMQClient interface {
	// Queue operations
	ListQueues(ctx context.Context) ([]QueueInfo, error)
	GetQueues(ctx context.Context) ([]QueueInfo, error)
	GetQueue(ctx context.Context, vhost, name string) (*QueueInfo, error)
	
	// Exchange operations
	ListExchanges(ctx context.Context) ([]ExchangeInfo, error)
	GetExchange(ctx context.Context, vhost, name string) (*ExchangeInfo, error)
	
	// Connection operations
	ListConnections(ctx context.Context) ([]ConnectionInfo, error)
	GetConnection(ctx context.Context, name string) (*ConnectionInfo, error)
	
	// Broker operations
	GetOverview(ctx context.Context) (*Overview, error)
	
	// Health check
	CheckHealth(ctx context.Context) error
}