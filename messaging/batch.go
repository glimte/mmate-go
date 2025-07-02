package messaging

import (
	"context"
	"fmt"
	"sync"

	"github.com/glimte/mmate-go/contracts"
)

// Batch represents a collection of messages to be published together
type Batch struct {
	publisher *MessagePublisher
	messages  []batchMessage
	mu        sync.Mutex
}

type batchMessage struct {
	message contracts.Message
	options []PublishOption
}

// NewBatch creates a new batch for the publisher
func (p *MessagePublisher) NewBatch() *Batch {
	return &Batch{
		publisher: p,
		messages:  make([]batchMessage, 0),
	}
}

// Add adds a message to the batch
func (b *Batch) Add(msg contracts.Message, options ...PublishOption) error {
	if msg == nil {
		return fmt.Errorf("message cannot be nil")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.messages = append(b.messages, batchMessage{
		message: msg,
		options: options,
	})

	return nil
}

// AddCommand adds a command to the batch
func (b *Batch) AddCommand(cmd contracts.Command, options ...PublishOption) error {
	defaultOpts := []PublishOption{
		WithExchange("mmate.commands"),
		WithRoutingKey(fmt.Sprintf("cmd.%s.%s", cmd.GetTargetService(), cmd.GetType())),
	}
	return b.Add(cmd, append(defaultOpts, options...)...)
}

// AddEvent adds an event to the batch
func (b *Batch) AddEvent(evt contracts.Event, options ...PublishOption) error {
	defaultOpts := []PublishOption{
		WithExchange("mmate.events"),
		WithRoutingKey(fmt.Sprintf("evt.%s.%s", evt.GetAggregateID(), evt.GetType())),
	}
	return b.Add(evt, append(defaultOpts, options...)...)
}

// AddQuery adds a query to the batch
func (b *Batch) AddQuery(query contracts.Query, options ...PublishOption) error {
	defaultOpts := []PublishOption{
		WithExchange("mmate.queries"),
		WithRoutingKey(fmt.Sprintf("qry.%s", query.GetType())),
	}
	return b.Add(query, append(defaultOpts, options...)...)
}

// Size returns the number of messages in the batch
func (b *Batch) Size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.messages)
}

// Clear removes all messages from the batch
func (b *Batch) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.messages = b.messages[:0]
}

// Publish publishes all messages in the batch
func (b *Batch) Publish(ctx context.Context) error {
	b.mu.Lock()
	messages := make([]batchMessage, len(b.messages))
	copy(messages, b.messages)
	b.mu.Unlock()

	if len(messages) == 0 {
		return nil
	}

	// Log batch publishing
	b.publisher.logger.Info("Publishing batch",
		"messageCount", len(messages),
	)

	// Publish all messages
	var firstError error
	successCount := 0

	for i, batchMsg := range messages {
		err := b.publisher.Publish(ctx, batchMsg.message, batchMsg.options...)
		if err != nil {
			b.publisher.logger.Error("Failed to publish message in batch",
				"index", i,
				"messageId", batchMsg.message.GetID(),
				"messageType", batchMsg.message.GetType(),
				"error", err,
			)
			if firstError == nil {
				firstError = err
			}
		} else {
			successCount++
		}
	}

	// Clear the batch after publishing
	b.Clear()

	if firstError != nil {
		return fmt.Errorf("batch publish completed with errors: %d/%d succeeded, first error: %w", 
			successCount, len(messages), firstError)
	}

	b.publisher.logger.Info("Batch published successfully",
		"messageCount", len(messages),
	)

	return nil
}

// PublishAtomic publishes all messages in the batch atomically (all or nothing)
// This requires transaction support
func (b *Batch) PublishAtomic(ctx context.Context) error {
	b.mu.Lock()
	messages := make([]batchMessage, len(b.messages))
	copy(messages, b.messages)
	b.mu.Unlock()

	if len(messages) == 0 {
		return nil
	}

	// Use transaction for atomic publishing
	tx, err := b.publisher.BeginTx()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Publish all messages in transaction
	for _, batchMsg := range messages {
		if err := tx.Publish(ctx, batchMsg.message, batchMsg.options...); err != nil {
			return fmt.Errorf("failed to publish message in transaction: %w", err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Clear the batch after successful atomic publish
	b.Clear()

	b.publisher.logger.Info("Batch published atomically",
		"messageCount", len(messages),
	)

	return nil
}