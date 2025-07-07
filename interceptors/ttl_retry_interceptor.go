package interceptors

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/internal/reliability"
)

// TTLRetryInterceptor implements retry logic using TTL-based scheduling
type TTLRetryInterceptor struct {
	scheduler   *reliability.TTLRetryScheduler
	retryPolicy reliability.RetryPolicy
	queueName   string
	logger      *slog.Logger
}

// TTLRetryInterceptorOptions configures the TTL retry interceptor
type TTLRetryInterceptorOptions struct {
	RetryPolicy reliability.RetryPolicy
	QueueName   string
	Logger      *slog.Logger
}

// NewTTLRetryInterceptor creates a new TTL-based retry interceptor
func NewTTLRetryInterceptor(scheduler *reliability.TTLRetryScheduler, opts *TTLRetryInterceptorOptions) *TTLRetryInterceptor {
	if opts == nil {
		opts = &TTLRetryInterceptorOptions{}
	}

	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	if opts.RetryPolicy == nil {
		opts.RetryPolicy = reliability.NewExponentialBackoff(
			100*time.Millisecond,
			30*time.Second,
			2.0,
			5,
		)
	}

	return &TTLRetryInterceptor{
		scheduler:   scheduler,
		retryPolicy: opts.RetryPolicy,
		queueName:   opts.QueueName,
		logger:      opts.Logger,
	}
}

// Intercept implements the Interceptor interface
func (r *TTLRetryInterceptor) Intercept(ctx context.Context, msg contracts.Message, next MessageHandler) error {
	// Try to get retry information from message headers/metadata
	retryAttempt := r.getRetryAttempt(ctx, msg)
	
	r.logger.Debug("Processing message with TTL retry",
		"messageId", msg.GetID(),
		"attempt", retryAttempt)

	// Execute the handler
	err := next.Handle(ctx, msg)
	if err == nil {
		return nil
	}

	// Check if this is a scheduled retry error (message already scheduled)
	var scheduledErr *reliability.ScheduledRetryError
	if errors.As(err, &scheduledErr) {
		r.logger.Info("Message already scheduled for retry",
			"messageId", msg.GetID(),
			"attempt", scheduledErr.AttemptNumber,
			"retryAt", scheduledErr.RetryAt)
		return nil // Don't propagate error since retry is handled
	}

	// Check if we should retry
	shouldRetry, delay := r.retryPolicy.ShouldRetry(retryAttempt, err)
	if !shouldRetry {
		r.logger.Error("Message failed and won't be retried",
			"messageId", msg.GetID(),
			"attempt", retryAttempt,
			"error", err)
		return err
	}

	// Schedule retry using TTL
	retryErr := r.scheduler.ScheduleRetry(ctx, msg, r.queueName, retryAttempt, r.retryPolicy, delay, err)
	if retryErr != nil {
		r.logger.Error("Failed to schedule TTL retry",
			"messageId", msg.GetID(),
			"error", retryErr)
		return err // Return original error
	}

	r.logger.Info("Message scheduled for TTL retry",
		"messageId", msg.GetID(),
		"attempt", retryAttempt+1,
		"delay", delay)

	// Return nil to indicate message was handled (retry scheduled)
	return nil
}

// Name returns the interceptor name
func (r *TTLRetryInterceptor) Name() string {
	return "TTLRetryInterceptor"
}

// getRetryAttempt extracts retry attempt number from context or message
func (r *TTLRetryInterceptor) getRetryAttempt(ctx context.Context, msg contracts.Message) int {
	// Try to get from interceptor context first
	if intercCtx, ok := GetInterceptorContext(ctx); ok {
		if attempt, exists := intercCtx.Get("retry-attempt"); exists {
			if attemptInt, ok := attempt.(int); ok {
				return attemptInt
			}
		}
	}

	// Default to 0 for new messages
	return 0
}

// WithQueueName sets the queue name for this interceptor
func (r *TTLRetryInterceptor) WithQueueName(queueName string) *TTLRetryInterceptor {
	r.queueName = queueName
	return r
}

// WithLogger sets the logger for this interceptor
func (r *TTLRetryInterceptor) WithLogger(logger *slog.Logger) *TTLRetryInterceptor {
	r.logger = logger
	return r
}