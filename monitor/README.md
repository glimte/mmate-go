# Mmate Monitor

A command-line tool for monitoring and managing the Mmate messaging system with RabbitMQ.

## Installation

```bash
go install github.com/glimte/mmate-go/cmd/monitor@latest
```

Or build from source:

```bash
make build
```

## Usage

### Global Options

- `-u, --url`: RabbitMQ connection URL (default: `amqp://guest:guest@localhost:5672/`)
- `-v, --verbose`: Enable verbose output

### Commands

#### Queue Management

List all queues:
```bash
mmate-monitor queue list
```

Watch queue metrics in real-time:
```bash
# Watch all queues
mmate-monitor queue watch

# Watch specific queues
mmate-monitor queue watch order.created payment.processed

# Watch with pattern matching
mmate-monitor queue watch "order.*" "payment.*"

# Custom update interval (seconds)
mmate-monitor queue watch -i 5
```

#### Exchange Management

List all exchanges:
```bash
mmate-monitor exchange list
```

#### Message Inspection

Peek at messages in a queue without consuming:
```bash
# Peek at 10 messages (default)
mmate-monitor message peek order.created

# Peek at specific number of messages
mmate-monitor message peek order.created -n 20
```

#### Dead Letter Queue Management

List all dead letter queues:
```bash
mmate-monitor dlq list
```

Requeue messages from DLQ:
```bash
# Requeue one message
mmate-monitor dlq requeue dlq.order.created

# Requeue all messages
mmate-monitor dlq requeue dlq.order.created --all
```

#### Health Check

Check system health:
```bash
mmate-monitor health
```

## Features

### Real-time Queue Monitoring

The `queue watch` command provides a live dashboard showing:
- Queue name with visual indicators
- Message count
- Consumer count
- Message rate (messages/second)
- Memory usage
- Color-coded queue states (green=running, yellow=idle, red=error)

Visual indicators:
- `*` = Queue has >1000 messages
- `+` = Queue has >100 messages

### Dead Letter Queue Management

The monitor tool provides comprehensive DLQ management:
- List all DLQs with message counts
- Show original queue for each DLQ
- Display age of oldest message
- Requeue messages back to original queue
- Reset retry counts when requeuing

### Health Monitoring

Comprehensive health checks including:
- RabbitMQ and Erlang versions
- Memory usage and limits
- Disk space
- File descriptor usage
- Socket usage
- Active alarms
- Queue and connection statistics

## Examples

### Monitor High-Traffic Queues

```bash
# Watch order-related queues with 1-second updates
mmate-monitor queue watch "order.*" "payment.*" -i 1
```

### Investigate Failed Messages

```bash
# Check DLQs
mmate-monitor dlq list

# Peek at failed messages
mmate-monitor message peek dlq.order.created -n 5

# Requeue after fixing the issue
mmate-monitor dlq requeue dlq.order.created --all
```

### Production Health Check

```bash
# Full health check with custom RabbitMQ URL
mmate-monitor -u amqp://admin:password@rabbitmq.prod:5672/ health
```

## Requirements

- Go 1.21 or later
- RabbitMQ 3.8 or later
- RabbitMQ Management Plugin enabled (for API access)

## Note on RabbitMQ Management API

The monitor tool uses both AMQP connection and the RabbitMQ Management HTTP API. Ensure that:
1. The Management Plugin is enabled: `rabbitmq-plugins enable rabbitmq_management`
2. The management API is accessible (default port: 15672)
3. The user has management permissions

## Development

Run the monitor directly from source:
```bash
make run-monitor
```

Run with custom arguments:
```bash
go run ./cmd/monitor queue watch
```