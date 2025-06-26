package rabbitmq

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TopologyManager manages RabbitMQ topology (exchanges, queues, bindings)
type TopologyManager struct {
	pool *ChannelPool
}

// ExchangeDeclaration defines an exchange to be declared
type ExchangeDeclaration struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Arguments  amqp.Table
}

// QueueDeclaration defines a queue to be declared
type QueueDeclaration struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	Arguments  amqp.Table
}

// Binding defines a queue-to-exchange binding
type Binding struct {
	Queue      string
	Exchange   string
	RoutingKey string
	Arguments  amqp.Table
}

// Topology represents the complete messaging topology
type Topology struct {
	Exchanges []ExchangeDeclaration
	Queues    []QueueDeclaration
	Bindings  []Binding
}

// NewTopologyManager creates a new topology manager
func NewTopologyManager(pool *ChannelPool) *TopologyManager {
	return &TopologyManager{
		pool: pool,
	}
}

// DeclareTopology declares the complete topology
func (tm *TopologyManager) DeclareTopology(ctx context.Context, topology Topology) error {
	return tm.pool.Execute(ctx, func(ch *amqp.Channel) error {
		// Declare exchanges
		for _, exchange := range topology.Exchanges {
			if err := tm.declareExchange(ch, exchange); err != nil {
				return fmt.Errorf("failed to declare exchange %s: %w", exchange.Name, err)
			}
		}

		// Declare queues
		for _, queue := range topology.Queues {
			if _, err := tm.declareQueue(ch, queue); err != nil {
				return fmt.Errorf("failed to declare queue %s: %w", queue.Name, err)
			}
		}

		// Create bindings
		for _, binding := range topology.Bindings {
			if err := tm.bindQueue(ch, binding); err != nil {
				return fmt.Errorf("failed to bind queue %s to exchange %s: %w", 
					binding.Queue, binding.Exchange, err)
			}
		}

		return nil
	})
}

// DeclareExchange declares a single exchange
func (tm *TopologyManager) DeclareExchange(ctx context.Context, exchange ExchangeDeclaration) error {
	return tm.pool.Execute(ctx, func(ch *amqp.Channel) error {
		return tm.declareExchange(ch, exchange)
	})
}

// DeclareQueue declares a single queue
func (tm *TopologyManager) DeclareQueue(ctx context.Context, queue QueueDeclaration) (amqp.Queue, error) {
	var q amqp.Queue
	err := tm.pool.Execute(ctx, func(ch *amqp.Channel) error {
		var err error
		q, err = tm.declareQueue(ch, queue)
		return err
	})
	return q, err
}

// BindQueue creates a queue binding
func (tm *TopologyManager) BindQueue(ctx context.Context, binding Binding) error {
	return tm.pool.Execute(ctx, func(ch *amqp.Channel) error {
		return tm.bindQueue(ch, binding)
	})
}

// DeleteQueue deletes a queue
func (tm *TopologyManager) DeleteQueue(ctx context.Context, name string, ifUnused, ifEmpty bool) error {
	return tm.pool.Execute(ctx, func(ch *amqp.Channel) error {
		_, err := ch.QueueDelete(name, ifUnused, ifEmpty, false)
		return err
	})
}

// DeleteExchange deletes an exchange
func (tm *TopologyManager) DeleteExchange(ctx context.Context, name string, ifUnused bool) error {
	return tm.pool.Execute(ctx, func(ch *amqp.Channel) error {
		return ch.ExchangeDelete(name, ifUnused, false)
	})
}

// GetQueueInfo retrieves queue information
func (tm *TopologyManager) GetQueueInfo(ctx context.Context, name string) (amqp.Queue, error) {
	var q amqp.Queue
	err := tm.pool.Execute(ctx, func(ch *amqp.Channel) error {
		var err error
		q, err = ch.QueueInspect(name)
		return err
	})
	return q, err
}

// declareExchange declares an exchange on the given channel
func (tm *TopologyManager) declareExchange(ch *amqp.Channel, exchange ExchangeDeclaration) error {
	return ch.ExchangeDeclare(
		exchange.Name,
		exchange.Type,
		exchange.Durable,
		exchange.AutoDelete,
		false, // internal
		false, // no-wait
		exchange.Arguments,
	)
}

// declareQueue declares a queue on the given channel
func (tm *TopologyManager) declareQueue(ch *amqp.Channel, queue QueueDeclaration) (amqp.Queue, error) {
	return ch.QueueDeclare(
		queue.Name,
		queue.Durable,
		queue.AutoDelete,
		queue.Exclusive,
		false, // no-wait
		queue.Arguments,
	)
}

// bindQueue binds a queue to an exchange on the given channel
func (tm *TopologyManager) bindQueue(ch *amqp.Channel, binding Binding) error {
	return ch.QueueBind(
		binding.Queue,
		binding.RoutingKey,
		binding.Exchange,
		false, // no-wait
		binding.Arguments,
	)
}

// CreateDefaultTopology creates the default topology for mmate
func CreateDefaultTopology() Topology {
	return Topology{
		Exchanges: []ExchangeDeclaration{
			{
				Name:    "commands",
				Type:    "direct",
				Durable: true,
			},
			{
				Name:    "events",
				Type:    "topic",
				Durable: true,
			},
			{
				Name:    "dlx", // Dead letter exchange
				Type:    "direct",
				Durable: true,
			},
		},
		Queues: []QueueDeclaration{
			// Example queues - actual queues would be created dynamically
		},
		Bindings: []Binding{
			// Example bindings - actual bindings would be created dynamically
		},
	}
}

// CreateQueueWithDLQ creates a queue with dead letter queue setup
func (tm *TopologyManager) CreateQueueWithDLQ(ctx context.Context, queueName string, dlqName string) error {
	return tm.pool.Execute(ctx, func(ch *amqp.Channel) error {
		// Create DLQ first
		_, err := ch.QueueDeclare(
			dlqName,
			true,  // durable
			false, // auto-delete
			false, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		if err != nil {
			return fmt.Errorf("failed to declare DLQ: %w", err)
		}

		// Create main queue with DLX settings
		_, err = ch.QueueDeclare(
			queueName,
			true,  // durable
			false, // auto-delete
			false, // exclusive
			false, // no-wait
			amqp.Table{
				"x-dead-letter-exchange":    "dlx",
				"x-dead-letter-routing-key": dlqName,
			},
		)
		if err != nil {
			return fmt.Errorf("failed to declare queue: %w", err)
		}

		// Bind DLQ to DLX
		err = ch.QueueBind(
			dlqName,
			dlqName, // routing key is same as queue name
			"dlx",
			false,
			nil,
		)
		if err != nil {
			return fmt.Errorf("failed to bind DLQ: %w", err)
		}

		return nil
	})
}