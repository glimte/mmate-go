# Mmate Go

A Go implementation of the Mmate messaging framework, providing reliable, asynchronous messaging patterns for distributed systems.

## Overview

Mmate Go is a flexible, extensible Go framework providing robust messaging and workflow capabilities for modern distributed applications. It simplifies complex messaging patterns and multi-stage processing workflows while maintaining high performance and reliability.

## Features

- **Messaging Infrastructure**: Publish-subscribe and request-response patterns with reliable message delivery
- **StageFlow Engine**: Multi-stage message processing with built-in state management and resilience
- **Schema Management**: Standard message format definitions and contract enforcement
- **Interceptors**: Pluggable components for cross-cutting concerns like logging, validation, and metrics
- **Monitoring**: Built-in tools to monitor message queues and workflow states
- **SyncAsyncBridge**: Bridge for synchronous-to-asynchronous messaging integration
- **Connection Resilience**: Automatic recovery with circuit breaker pattern

## Installation

```bash
go get github.com/glimte/mmate-go
```

## Quick Start

```go
package main

import (
    "context"
    "log"
    
    "github.com/glimte/mmate-go/messaging"
    "github.com/glimte/mmate-go/contracts"
)

func main() {
    // Configure messaging
    config := messaging.Config{
        ServiceName: "order-service",
        RabbitMQ: messaging.RabbitMQConfig{
            URL: "amqp://localhost:5672",
            
            // Connection resilience
            EnableAutomaticRecovery: true,
            EnableCircuitBreaker: true,
        },
    }
    
    // Create dispatcher
    dispatcher, err := messaging.NewDispatcher(config)
    if err != nil {
        log.Fatal(err)
    }
    defer dispatcher.Close()
    
    // Define and register handler
    dispatcher.Handle("order.created", func(ctx context.Context, msg *OrderCreated) error {
        // Process the message
        log.Printf("Processing order: %s", msg.OrderID)
        return nil
    })
    
    // Publish a message
    err = dispatcher.Publish(ctx, &OrderCreated{OrderID: "12345"})
    if err != nil {
        log.Fatal(err)
    }
    
    // Start processing
    if err := dispatcher.Start(context.Background()); err != nil {
        log.Fatal(err)
    }
}

type OrderCreated struct {
    contracts.Event
    OrderID string `json:"orderId"`
}
```

## Project Structure

- **contracts** - Core contracts and shared definitions
- **messaging** - Messaging infrastructure and publisher/subscriber implementation
- **schema** - Schema validation and message contract management
- **stageflow** - Workflow orchestration engine for multi-stage message processing
- **monitor** - CLI tools for monitoring RabbitMQ message brokers
- **interceptors** - Pluggable components for cross-cutting concerns
- **bridge** - Bridge for synchronous-to-asynchronous messaging integration

## Wire Format Compatibility

Mmate Go is fully compatible with the .NET implementation at the wire format level. Services written in Go can exchange messages with services written in .NET seamlessly.

## Documentation

For comprehensive documentation including advanced features, patterns, and examples:

- **[Mmate Documentation](../mmate-docs/README.md)** - Complete documentation for all platforms
- **[Getting Started](../mmate-docs/getting-started/go.md)** - Go specific setup guide
- **[API Reference](../mmate-docs/platform/go/api-reference.md)** - Go API documentation
- **[Examples](../mmate-docs/platform/go/examples.md)** - Go code examples

## Requirements

- Go 1.21 or later
- RabbitMQ 3.8+ (for messaging transport)

## License

MIT License