package interceptors

import (
	"context"
	"log/slog"

	"github.com/glimte/mmate-go/contracts"
	"github.com/glimte/mmate-go/internal/reliability"
)

// RetryInterceptor implements retry logic for message processing
type RetryInterceptor struct {
	retryPolicy reliability.RetryPolicy
	logger      *slog.Logger
}

// NewRetryInterceptor creates a new retry interceptor
func NewRetryInterceptor(retryPolicy reliability.RetryPolicy) *RetryInterceptor {
	return &RetryInterceptor{
		retryPolicy: retryPolicy,
		logger:      slog.Default(),
	}
}

// WithRetryLogger sets the logger for the retry interceptor
func (r *RetryInterceptor) WithLogger(logger *slog.Logger) *RetryInterceptor {
	r.logger = logger
	return r
}

// Intercept implements the Interceptor interface
func (r *RetryInterceptor) Intercept(ctx context.Context, msg contracts.Message, next MessageHandler) error {
	return reliability.Retry(ctx, r.retryPolicy, func() error {
		return next.Handle(ctx, msg)
	})
}

// Name returns the interceptor name
func (r *RetryInterceptor) Name() string {
	return "RetryInterceptor"
}