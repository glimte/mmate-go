package messaging

import (
	"context"
	"fmt"
	"sync"

	"github.com/glimte/mmate-go/contracts"
)

// Transaction represents a message publishing transaction
type Transaction struct {
	publisher    *MessagePublisher
	transportTx  TransportTransaction
	committed    bool
	rolledBack   bool
	mu           sync.Mutex
}

// BeginTx begins a new transaction
func (p *MessagePublisher) BeginTx() (*Transaction, error) {
	// Check if transport supports transactions
	transactionalPublisher, ok := p.transport.(TransactionalTransportPublisher)
	if !ok {
		return nil, fmt.Errorf("transport does not support transactions")
	}

	// Begin transport transaction
	transportTx, err := transactionalPublisher.BeginTx(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to begin transport transaction: %w", err)
	}

	return &Transaction{
		publisher:   p,
		transportTx: transportTx,
		committed:   false,
		rolledBack:  false,
	}, nil
}

// Publish publishes a message within the transaction
func (tx *Transaction) Publish(ctx context.Context, msg contracts.Message, options ...PublishOption) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.committed {
		return fmt.Errorf("transaction already committed")
	}
	if tx.rolledBack {
		return fmt.Errorf("transaction already rolled back")
	}

	// Apply default options
	opts := PublishOptions{
		Exchange:        "mmate.messages",
		RoutingKey:      tx.publisher.getRoutingKey(msg),
		TTL:             tx.publisher.defaultTTL,
		Priority:        0,
		DeliveryMode:    2, // Persistent by default
		Headers:         make(map[string]interface{}),
		ConfirmDelivery: true,
	}

	for _, opt := range options {
		opt(&opts)
	}

	// Add standard headers
	tx.publisher.addStandardHeaders(&opts, msg)
	
	// Create envelope with headers
	envelope, err := tx.publisher.factory.CreateEnvelopeWithOptions(msg, WithEnvelopeHeaders(opts.Headers))
	if err != nil {
		return fmt.Errorf("failed to create envelope: %w", err)
	}

	// Publish through transport transaction
	err = tx.transportTx.Publish(ctx, opts.Exchange, opts.RoutingKey, envelope)
	if err != nil {
		return fmt.Errorf("failed to publish in transaction: %w", err)
	}

	return nil
}

// PublishCommand publishes a command within the transaction
func (tx *Transaction) PublishCommand(ctx context.Context, cmd contracts.Command, options ...PublishOption) error {
	defaultOpts := []PublishOption{
		WithExchange("mmate.commands"),
		WithRoutingKey(fmt.Sprintf("cmd.%s.%s", cmd.GetTargetService(), cmd.GetType())),
	}
	return tx.Publish(ctx, cmd, append(defaultOpts, options...)...)
}

// PublishEvent publishes an event within the transaction
func (tx *Transaction) PublishEvent(ctx context.Context, evt contracts.Event, options ...PublishOption) error {
	defaultOpts := []PublishOption{
		WithExchange("mmate.events"),
		WithRoutingKey(fmt.Sprintf("evt.%s.%s", evt.GetAggregateID(), evt.GetType())),
	}
	return tx.Publish(ctx, evt, append(defaultOpts, options...)...)
}

// PublishQuery publishes a query within the transaction
func (tx *Transaction) PublishQuery(ctx context.Context, query contracts.Query, options ...PublishOption) error {
	defaultOpts := []PublishOption{
		WithExchange("mmate.queries"),
		WithRoutingKey(fmt.Sprintf("qry.%s", query.GetType())),
	}
	return tx.Publish(ctx, query, append(defaultOpts, options...)...)
}

// Commit commits the transaction, publishing all messages
func (tx *Transaction) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.committed {
		return fmt.Errorf("transaction already committed")
	}
	if tx.rolledBack {
		return fmt.Errorf("transaction already rolled back")
	}

	// Commit the transport transaction
	err := tx.transportTx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	tx.committed = true
	tx.publisher.logger.Info("Transaction committed successfully")
	
	return nil
}

// Rollback rolls back the transaction, discarding all messages
func (tx *Transaction) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.committed {
		return fmt.Errorf("transaction already committed")
	}
	if tx.rolledBack {
		return nil // Already rolled back
	}

	// Rollback the transport transaction
	err := tx.transportTx.Rollback()
	if err != nil {
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}

	tx.rolledBack = true
	tx.publisher.logger.Info("Transaction rolled back")
	
	return nil
}