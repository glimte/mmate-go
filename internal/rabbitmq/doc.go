// Package rabbitmq provides RabbitMQ integration for the mmate messaging framework.
//
// This package includes:
//   - ConnectionManager: Manages RabbitMQ connections with automatic reconnection
//   - ChannelPool: Provides efficient channel pooling with idle timeout
//   - Publisher: Handles message publishing with confirmation support
//   - Consumer: Manages message consumption with various acknowledgment strategies
//   - TopologyManager: Manages exchanges, queues, and bindings
//
// The implementation focuses on reliability and performance with features like:
//   - Automatic reconnection on connection failures
//   - Channel pooling to reduce overhead
//   - Publisher confirms for reliable message delivery
//   - Flexible consumer acknowledgment strategies
//   - Connection state change notifications
package rabbitmq