package messaging

import (
	"context"
	"fmt"
	"time"

	"github.com/glimte/mmate-go/contracts"
)

// ScheduleCommand schedules a command to be delivered at a specific time
func (p *MessagePublisher) ScheduleCommand(ctx context.Context, cmd contracts.Command, scheduledFor time.Time, options ...PublishOption) error {
	delay := time.Until(scheduledFor)
	if delay < 0 {
		delay = 0 // Send immediately if scheduled time is in the past
	}

	defaultOpts := []PublishOption{
		WithExchange("mmate.commands"),
		WithRoutingKey(fmt.Sprintf("cmd.%s.%s", cmd.GetTargetService(), cmd.GetType())),
		WithDelay(delay),
	}
	return p.Publish(ctx, cmd, append(defaultOpts, options...)...)
}

// ScheduleEvent schedules an event to be delivered at a specific time
func (p *MessagePublisher) ScheduleEvent(ctx context.Context, evt contracts.Event, scheduledFor time.Time, options ...PublishOption) error {
	delay := time.Until(scheduledFor)
	if delay < 0 {
		delay = 0 // Send immediately if scheduled time is in the past
	}

	defaultOpts := []PublishOption{
		WithExchange("mmate.events"),
		WithRoutingKey(fmt.Sprintf("evt.%s.%s", evt.GetAggregateID(), evt.GetType())),
		WithDelay(delay),
	}
	return p.Publish(ctx, evt, append(defaultOpts, options...)...)
}

// ScheduleQuery schedules a query to be delivered at a specific time
func (p *MessagePublisher) ScheduleQuery(ctx context.Context, query contracts.Query, scheduledFor time.Time, options ...PublishOption) error {
	delay := time.Until(scheduledFor)
	if delay < 0 {
		delay = 0 // Send immediately if scheduled time is in the past
	}

	defaultOpts := []PublishOption{
		WithExchange("mmate.queries"),
		WithRoutingKey(fmt.Sprintf("qry.%s", query.GetType())),
		WithDelay(delay),
	}
	return p.Publish(ctx, query, append(defaultOpts, options...)...)
}

// ScheduleRecurring schedules a message to be sent on a recurring basis using cron expression
func (p *MessagePublisher) ScheduleRecurring(ctx context.Context, msg contracts.Message, cronExpression string, options ...PublishOption) error {
	// This requires a scheduler service to handle cron expressions
	// For now, we'll return an error indicating this needs additional infrastructure
	return fmt.Errorf("recurring message scheduling requires a scheduler service - not yet implemented")
}

// WithDelay adds a delay to message delivery
func WithDelay(delay time.Duration) PublishOption {
	return func(opts *PublishOptions) {
		if opts.Headers == nil {
			opts.Headers = make(map[string]interface{})
		}
		// RabbitMQ delayed message plugin uses x-delay header
		opts.Headers["x-delay"] = int64(delay.Milliseconds())
	}
}

// DelayedPublisher wraps a publisher to add delay capabilities
type DelayedPublisher struct {
	*MessagePublisher
	delayedExchange string
}

// NewDelayedPublisher creates a publisher with delayed message support
// This requires the RabbitMQ delayed message exchange plugin
func NewDelayedPublisher(publisher *MessagePublisher, delayedExchange string) *DelayedPublisher {
	return &DelayedPublisher{
		MessagePublisher: publisher,
		delayedExchange:  delayedExchange,
	}
}

// PublishWithDelay publishes a message with a delay
func (dp *DelayedPublisher) PublishWithDelay(ctx context.Context, msg contracts.Message, delay time.Duration, options ...PublishOption) error {
	// Override exchange to use delayed exchange
	delayOpts := []PublishOption{
		WithExchange(dp.delayedExchange),
		WithDelay(delay),
	}
	return dp.Publish(ctx, msg, append(delayOpts, options...)...)
}

// SchedulingOptions provides configuration for message scheduling
type SchedulingOptions struct {
	// MaxDelay is the maximum allowed delay for scheduled messages
	MaxDelay time.Duration
	// DelayedExchange is the name of the delayed message exchange
	DelayedExchange string
	// EnableRecurring enables recurring message scheduling
	EnableRecurring bool
	// RecurringCheckInterval is how often to check for recurring messages
	RecurringCheckInterval time.Duration
}

// DefaultSchedulingOptions returns default scheduling options
func DefaultSchedulingOptions() SchedulingOptions {
	return SchedulingOptions{
		MaxDelay:               24 * time.Hour, // 24 hours max delay
		DelayedExchange:        "mmate.delayed",
		EnableRecurring:        false,
		RecurringCheckInterval: time.Minute,
	}
}

// ScheduledMessage represents a message scheduled for future delivery
type ScheduledMessage struct {
	ID             string
	Message        contracts.Message
	ScheduledFor   time.Time
	CronExpression string // For recurring messages
	Options        []PublishOption
	CreatedAt      time.Time
	LastExecutedAt *time.Time
}

// IsRecurring returns true if this is a recurring scheduled message
func (sm *ScheduledMessage) IsRecurring() bool {
	return sm.CronExpression != ""
}

// IsDue returns true if the message is due for delivery
func (sm *ScheduledMessage) IsDue() bool {
	return time.Now().After(sm.ScheduledFor)
}