package messaging

import (
	"context"
	"testing"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestScheduleCommand(t *testing.T) {
	transport := &MockTransportPublisher{}
	publisher := NewMessagePublisher(transport)

	cmd := &testCommand{
		BaseCommand: contracts.NewBaseCommand("testCommand"),
		Data:        "test",
	}

	// Schedule for future
	scheduledFor := time.Now().Add(5 * time.Minute)
	
	// Set up expectation - the delay should be approximately 5 minutes
	transport.On("Publish", mock.Anything, "mmate.commands", "cmd..testCommand", mock.Anything).
		Run(func(args mock.Arguments) {
			envelope := args.Get(3).(*contracts.Envelope)
			// Check that x-delay header is set
			assert.Contains(t, envelope.Headers, "x-delay")
			delay := envelope.Headers["x-delay"].(int64)
			// Should be approximately 5 minutes in milliseconds
			assert.Greater(t, delay, int64(299000)) // > 4m59s
			assert.Less(t, delay, int64(301000))    // < 5m1s
		}).Return(nil)

	ctx := context.Background()
	err := publisher.ScheduleCommand(ctx, cmd, scheduledFor)
	assert.NoError(t, err)
	transport.AssertExpectations(t)
}

func TestScheduleEvent(t *testing.T) {
	transport := &MockTransportPublisher{}
	publisher := NewMessagePublisher(transport)

	evt := &testEvent{
		BaseEvent: contracts.BaseEvent{
			BaseMessage:  contracts.NewBaseMessage("testEvent"),
			AggregateID:  "aggregate-123",
			Sequence: 1,
		},
		Data: "event-data",
	}

	// Schedule for past (should send immediately)
	scheduledFor := time.Now().Add(-5 * time.Minute)
	
	// Set up expectation - delay should be 0
	transport.On("Publish", mock.Anything, "mmate.events", "evt.aggregate-123.testEvent", mock.Anything).
		Run(func(args mock.Arguments) {
			envelope := args.Get(3).(*contracts.Envelope)
			// Check that x-delay header is set to 0
			assert.Contains(t, envelope.Headers, "x-delay")
			delay := envelope.Headers["x-delay"].(int64)
			assert.Equal(t, int64(0), delay)
		}).Return(nil)

	ctx := context.Background()
	err := publisher.ScheduleEvent(ctx, evt, scheduledFor)
	assert.NoError(t, err)
	transport.AssertExpectations(t)
}

func TestScheduleQuery(t *testing.T) {
	transport := &MockTransportPublisher{}
	publisher := NewMessagePublisher(transport)

	qry := &testQuery{
		BaseQuery: contracts.BaseQuery{
			BaseMessage: contracts.NewBaseMessage("testQuery"),
		},
		Filter: "filter",
	}

	// Schedule for 1 hour from now
	scheduledFor := time.Now().Add(time.Hour)
	
	// Set up expectation
	transport.On("Publish", mock.Anything, "mmate.queries", "qry.testQuery", mock.Anything).
		Run(func(args mock.Arguments) {
			envelope := args.Get(3).(*contracts.Envelope)
			// Check that x-delay header is set
			assert.Contains(t, envelope.Headers, "x-delay")
			delay := envelope.Headers["x-delay"].(int64)
			// Should be approximately 1 hour in milliseconds
			assert.Greater(t, delay, int64(3599000)) // > 59m59s
			assert.Less(t, delay, int64(3601000))    // < 1h1s
		}).Return(nil)

	ctx := context.Background()
	err := publisher.ScheduleQuery(ctx, qry, scheduledFor)
	assert.NoError(t, err)
	transport.AssertExpectations(t)
}

func TestScheduleRecurring(t *testing.T) {
	transport := &MockTransportPublisher{}
	publisher := NewMessagePublisher(transport)

	cmd := &testCommand{
		BaseCommand: contracts.NewBaseCommand("testCommand"),
		Data:        "test",
	}

	ctx := context.Background()
	err := publisher.ScheduleRecurring(ctx, cmd, "0 */5 * * * *")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "recurring message scheduling requires a scheduler service")
}

func TestWithDelay(t *testing.T) {
	transport := &MockTransportPublisher{}
	publisher := NewMessagePublisher(transport)

	cmd := &testCommand{
		BaseCommand: contracts.NewBaseCommand("testCommand"),
		Data:        "test",
	}

	delay := 10 * time.Second
	
	// Set up expectation
	transport.On("Publish", mock.Anything, "mmate.commands", "cmd..testCommand", mock.Anything).
		Run(func(args mock.Arguments) {
			envelope := args.Get(3).(*contracts.Envelope)
			// Check that x-delay header is set
			assert.Contains(t, envelope.Headers, "x-delay")
			delayMs := envelope.Headers["x-delay"].(int64)
			assert.Equal(t, int64(10000), delayMs) // 10 seconds in milliseconds
		}).Return(nil)

	ctx := context.Background()
	err := publisher.PublishCommand(ctx, cmd, WithDelay(delay))
	assert.NoError(t, err)
	transport.AssertExpectations(t)
}

func TestDelayedPublisher(t *testing.T) {
	transport := &MockTransportPublisher{}
	publisher := NewMessagePublisher(transport)
	delayedPublisher := NewDelayedPublisher(publisher, "mmate.delayed")

	cmd := &testCommand{
		BaseCommand: contracts.NewBaseCommand("testCommand"),
		Data:        "test",
	}

	delay := 30 * time.Second
	
	// Set up expectation - should use delayed exchange
	transport.On("Publish", mock.Anything, "mmate.delayed", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			envelope := args.Get(3).(*contracts.Envelope)
			// Check that x-delay header is set
			assert.Contains(t, envelope.Headers, "x-delay")
			delayMs := envelope.Headers["x-delay"].(int64)
			assert.Equal(t, int64(30000), delayMs) // 30 seconds in milliseconds
		}).Return(nil)

	ctx := context.Background()
	err := delayedPublisher.PublishWithDelay(ctx, cmd, delay)
	assert.NoError(t, err)
	transport.AssertExpectations(t)
}

func TestScheduledMessage(t *testing.T) {
	msg := &testCommand{
		BaseCommand: contracts.NewBaseCommand("testCommand"),
		Data:       "test",
	}

	// Test non-recurring message
	scheduled := ScheduledMessage{
		ID:           "sched-1",
		Message:      msg,
		ScheduledFor: time.Now().Add(time.Hour),
		CreatedAt:    time.Now(),
	}

	assert.False(t, scheduled.IsRecurring())
	assert.False(t, scheduled.IsDue())

	// Test recurring message
	recurringScheduled := ScheduledMessage{
		ID:             "sched-2",
		Message:        msg,
		ScheduledFor:   time.Now().Add(-time.Minute), // Past time
		CronExpression: "0 */5 * * * *",
		CreatedAt:      time.Now(),
	}

	assert.True(t, recurringScheduled.IsRecurring())
	assert.True(t, recurringScheduled.IsDue())
}

func TestDefaultSchedulingOptions(t *testing.T) {
	opts := DefaultSchedulingOptions()
	
	assert.Equal(t, 24*time.Hour, opts.MaxDelay)
	assert.Equal(t, "mmate.delayed", opts.DelayedExchange)
	assert.False(t, opts.EnableRecurring)
	assert.Equal(t, time.Minute, opts.RecurringCheckInterval)
}

func TestPublishWithDelayIntegration(t *testing.T) {
	transport := &MockTransportPublisher{}
	publisher := NewMessagePublisher(transport)

	// Test multiple messages with different delays
	messages := []struct {
		name  string
		delay time.Duration
	}{
		{"immediate", 0},
		{"5sec", 5 * time.Second},
		{"1min", time.Minute},
		{"1hour", time.Hour},
	}

	for _, tc := range messages {
		t.Run(tc.name, func(t *testing.T) {
			cmd := &testCommand{
				BaseCommand: contracts.NewBaseCommand("testCommand"),
				Data:        tc.name,
			}

			expectedDelay := tc.delay.Milliseconds()
			
			transport.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Run(func(args mock.Arguments) {
					envelope := args.Get(3).(*contracts.Envelope)
					if tc.delay > 0 {
						assert.Contains(t, envelope.Headers, "x-delay")
						delayMs := envelope.Headers["x-delay"].(int64)
						assert.Equal(t, expectedDelay, delayMs)
					}
				}).Return(nil).Once()

			ctx := context.Background()
			var err error
			if tc.delay > 0 {
				err = publisher.Publish(ctx, cmd, WithDelay(tc.delay))
			} else {
				err = publisher.Publish(ctx, cmd)
			}
			assert.NoError(t, err)
		})
	}

	transport.AssertExpectations(t)
}