package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/glimte/mmate-go/monitor"
	"github.com/spf13/cobra"
)

var (
	// Version information
	version   = "dev"
	buildTime = "unknown"
	gitCommit = "unknown"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "mmate-monitor",
		Short: "Monitor and manage Mmate messaging system",
		Long: `Mmate Monitor is a CLI tool for monitoring and managing the Mmate messaging system.
It provides real-time monitoring of queues, exchanges, and message flow.`,
		Version: fmt.Sprintf("%s (commit: %s, built: %s)", version, gitCommit, buildTime),
	}

	// Global flags
	var (
		rabbitURL string
		verbose   bool
	)

	rootCmd.PersistentFlags().StringVarP(&rabbitURL, "url", "u", "amqp://guest:guest@localhost:5672/", "RabbitMQ connection URL")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose output")

	// Queue command
	queueCmd := &cobra.Command{
		Use:   "queue",
		Short: "Monitor and manage queues",
		Long:  "Monitor queue depths, consumer counts, and message rates",
	}

	// Queue list command
	queueListCmd := &cobra.Command{
		Use:   "list",
		Short: "List all queues",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client, err := monitor.NewRabbitMQClient(rabbitURL)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}
			defer client.Close()

			queues, err := client.ListQueues(ctx)
			if err != nil {
				return fmt.Errorf("failed to list queues: %w", err)
			}

			printQueues(queues)
			return nil
		},
	}

	// Queue watch command
	var interval int
	queueWatchCmd := &cobra.Command{
		Use:   "watch [queue-names...]",
		Short: "Watch queue metrics in real-time",
		Long:  "Continuously monitor queue metrics. If no queue names are provided, all queues are monitored.",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Handle signals
			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
			go func() {
				<-sigChan
				cancel()
			}()

			client, err := monitor.NewRabbitMQClient(rabbitURL)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}
			defer client.Close()

			watcher := monitor.NewQueueWatcher(client, time.Duration(interval)*time.Second)

			fmt.Println("Starting queue monitoring... Press Ctrl+C to stop")
			fmt.Println(strings.Repeat("-", 80))

			return watcher.Watch(ctx, args)
		},
	}
	queueWatchCmd.Flags().IntVarP(&interval, "interval", "i", 2, "Update interval in seconds")

	queueCmd.AddCommand(queueListCmd, queueWatchCmd)

	// Exchange command
	exchangeCmd := &cobra.Command{
		Use:   "exchange",
		Short: "Monitor and manage exchanges",
	}

	// Exchange list command
	exchangeListCmd := &cobra.Command{
		Use:   "list",
		Short: "List all exchanges",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client, err := monitor.NewRabbitMQClient(rabbitURL)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}
			defer client.Close()

			exchanges, err := client.ListExchanges(ctx)
			if err != nil {
				return fmt.Errorf("failed to list exchanges: %w", err)
			}

			printExchanges(exchanges)
			return nil
		},
	}

	exchangeCmd.AddCommand(exchangeListCmd)

	// Messages command
	messageCmd := &cobra.Command{
		Use:   "message",
		Short: "Monitor and manage messages",
	}

	// Peek at messages in a queue
	var peekCount int
	messagePeekCmd := &cobra.Command{
		Use:   "peek <queue-name>",
		Short: "Peek at messages in a queue without consuming them",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client, err := monitor.NewRabbitMQClient(rabbitURL)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}
			defer client.Close()

			messages, err := client.PeekMessages(ctx, args[0], peekCount)
			if err != nil {
				return fmt.Errorf("failed to peek messages: %w", err)
			}

			printMessages(messages)
			return nil
		},
	}
	messagePeekCmd.Flags().IntVarP(&peekCount, "count", "n", 10, "Number of messages to peek")

	messageCmd.AddCommand(messagePeekCmd)

	// DLQ command
	dlqCmd := &cobra.Command{
		Use:   "dlq",
		Short: "Monitor and manage dead letter queues",
	}

	// List DLQ messages
	dlqListCmd := &cobra.Command{
		Use:   "list",
		Short: "List all dead letter queues and their message counts",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client, err := monitor.NewRabbitMQClient(rabbitURL)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}
			defer client.Close()

			dlqs, err := client.ListDLQs(ctx)
			if err != nil {
				return fmt.Errorf("failed to list DLQs: %w", err)
			}

			printDLQs(dlqs)
			return nil
		},
	}

	// Requeue DLQ messages
	var requeueAll bool
	dlqRequeueCmd := &cobra.Command{
		Use:   "requeue <dlq-name>",
		Short: "Requeue messages from a dead letter queue",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client, err := monitor.NewRabbitMQClient(rabbitURL)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}
			defer client.Close()

			count := 1
			if requeueAll {
				count = -1 // Requeue all
			}

			requeued, err := client.RequeueDLQMessages(ctx, args[0], count)
			if err != nil {
				return fmt.Errorf("failed to requeue messages: %w", err)
			}

			fmt.Printf("Successfully requeued %d messages from %s\n", requeued, args[0])
			return nil
		},
	}
	dlqRequeueCmd.Flags().BoolVarP(&requeueAll, "all", "a", false, "Requeue all messages")

	dlqCmd.AddCommand(dlqListCmd, dlqRequeueCmd)

	// Health command
	healthCmd := &cobra.Command{
		Use:   "health",
		Short: "Check system health",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client, err := monitor.NewRabbitMQClient(rabbitURL)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}
			defer client.Close()

			health, err := client.CheckHealth(ctx)
			if err != nil {
				return fmt.Errorf("failed to check health: %w", err)
			}

			printHealth(health)
			return nil
		},
	}

	// Add all commands
	rootCmd.AddCommand(queueCmd, exchangeCmd, messageCmd, dlqCmd, healthCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

// Output formatting functions

func printQueues(queues []monitor.QueueInfo) {
	if len(queues) == 0 {
		fmt.Println("No queues found")
		return
	}

	fmt.Printf("%-40s %-10s %-10s %-15s %-10s\n", "Name", "Messages", "Consumers", "Message Rate", "State")
	fmt.Println(strings.Repeat("-", 95))

	for _, q := range queues {
		fmt.Printf("%-40s %-10d %-10d %-15.2f %-10s\n",
			truncate(q.Name, 40),
			q.Messages,
			q.Consumers,
			q.MessageRate,
			q.State,
		)
	}
}

func printExchanges(exchanges []monitor.ExchangeInfo) {
	if len(exchanges) == 0 {
		fmt.Println("No exchanges found")
		return
	}

	fmt.Printf("%-40s %-10s %-10s %-15s\n", "Name", "Type", "Durable", "Message Rate")
	fmt.Println(strings.Repeat("-", 80))

	for _, e := range exchanges {
		fmt.Printf("%-40s %-10s %-10t %-15.2f\n",
			truncate(e.Name, 40),
			e.Type,
			e.Durable,
			e.MessageRate,
		)
	}
}

func printMessages(messages []monitor.MessageInfo) {
	if len(messages) == 0 {
		fmt.Println("No messages found")
		return
	}

	for i, msg := range messages {
		fmt.Printf("Message %d:\n", i+1)
		fmt.Printf("  ID: %s\n", msg.MessageID)
		fmt.Printf("  Type: %s\n", msg.Type)
		fmt.Printf("  Correlation ID: %s\n", msg.CorrelationID)
		fmt.Printf("  Timestamp: %s\n", msg.Timestamp.Format(time.RFC3339))
		fmt.Printf("  Routing Key: %s\n", msg.RoutingKey)
		fmt.Printf("  Redelivered: %t\n", msg.Redelivered)
		if msg.RetryCount > 0 {
			fmt.Printf("  Retry Count: %d\n", msg.RetryCount)
		}
		fmt.Printf("  Headers:\n")
		for k, v := range msg.Headers {
			fmt.Printf("    %s: %v\n", k, v)
		}
		fmt.Printf("  Body Preview: %s\n", truncate(msg.BodyPreview, 100))
		fmt.Println(strings.Repeat("-", 60))
	}
}

func printDLQs(dlqs []monitor.DLQInfo) {
	if len(dlqs) == 0 {
		fmt.Println("No dead letter queues found")
		return
	}

	fmt.Printf("%-40s %-10s %-20s %-15s\n", "DLQ Name", "Messages", "Original Queue", "Oldest Message")
	fmt.Println(strings.Repeat("-", 90))

	for _, dlq := range dlqs {
		oldestStr := "N/A"
		if !dlq.OldestMessageTime.IsZero() {
			oldestStr = time.Since(dlq.OldestMessageTime).Truncate(time.Second).String() + " ago"
		}
		fmt.Printf("%-40s %-10d %-20s %-15s\n",
			truncate(dlq.Name, 40),
			dlq.MessageCount,
			truncate(dlq.OriginalQueue, 20),
			oldestStr,
		)
	}
}

func printHealth(health monitor.HealthStatus) {
	fmt.Printf("System Health: %s\n", health.Status)
	fmt.Printf("RabbitMQ Version: %s\n", health.RabbitMQVersion)
	fmt.Printf("Erlang Version: %s\n", health.ErlangVersion)
	fmt.Printf("\nNode Status:\n")
	fmt.Printf("  Memory Used: %.2f MB (%.1f%%)\n",
		float64(health.MemoryUsed)/(1024*1024),
		float64(health.MemoryUsed)/float64(health.MemoryLimit)*100)
	fmt.Printf("  Disk Free: %.2f GB\n", float64(health.DiskFree)/(1024*1024*1024))
	fmt.Printf("  File Descriptors: %d/%d\n", health.FileDescriptorsUsed, health.FileDescriptorsTotal)
	fmt.Printf("  Sockets: %d/%d\n", health.SocketsUsed, health.SocketsTotal)

	if len(health.Alarms) > 0 {
		fmt.Printf("\nALARMS:\n")
		for _, alarm := range health.Alarms {
			fmt.Printf("  - %s\n", alarm)
		}
	}

	fmt.Printf("\nQueue Summary:\n")
	fmt.Printf("  Total Queues: %d\n", health.TotalQueues)
	fmt.Printf("  Total Messages: %d\n", health.TotalMessages)
	fmt.Printf("  Messages Ready: %d\n", health.MessagesReady)
	fmt.Printf("  Messages Unacked: %d\n", health.MessagesUnacked)

	fmt.Printf("\nConnection Summary:\n")
	fmt.Printf("  Total Connections: %d\n", health.TotalConnections)
	fmt.Printf("  Total Channels: %d\n", health.TotalChannels)
	fmt.Printf("  Total Consumers: %d\n", health.TotalConsumers)
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
