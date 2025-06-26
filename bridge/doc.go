// Package bridge provides synchronous request-response patterns over asynchronous messaging.
//
// The bridge package enables traditional request-response communication patterns
// over asynchronous messaging infrastructure. This allows applications to maintain
// familiar synchronous APIs while benefiting from the decoupling and reliability
// of asynchronous messaging.
//
// Key features:
//   - Synchronous request-response over async messaging
//   - Timeout handling with context support
//   - Correlation ID management for request tracking
//   - Circuit breaker integration for fault tolerance
//   - Retry policies for transient failures
//   - Connection pooling for performance
//
// Basic usage:
//
//	bridge := bridge.NewSyncAsyncBridge(publisher, subscriber)
//	
//	// Send request and wait for response
//	response, err := bridge.Request(ctx, command, 30*time.Second)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// The bridge automatically handles:
//   - Unique correlation IDs for each request
//   - Temporary reply queues for responses
//   - Cleanup of expired requests
//   - Error propagation from remote services
package bridge