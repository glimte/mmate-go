package monitor

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"time"
)

// QueueWatcher monitors queue metrics in real-time
type QueueWatcher struct {
	client   *RabbitMQClient
	interval time.Duration
}

// NewQueueWatcher creates a new queue watcher
func NewQueueWatcher(client *RabbitMQClient, interval time.Duration) *QueueWatcher {
	return &QueueWatcher{
		client:   client,
		interval: interval,
	}
}

// Watch continuously monitors queues
func (w *QueueWatcher) Watch(ctx context.Context, queueFilter []string) error {
	// Clear screen initially
	clearScreen()

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	// Initial display
	if err := w.displayQueues(ctx, queueFilter); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			clearScreen()
			if err := w.displayQueues(ctx, queueFilter); err != nil {
				// Don't exit on error, just display it
				fmt.Printf("Error: %v\n", err)
			}
		}
	}
}

func (w *QueueWatcher) displayQueues(ctx context.Context, queueFilter []string) error {
	queues, err := w.client.ListQueues(ctx)
	if err != nil {
		return err
	}

	// Filter queues if specified
	if len(queueFilter) > 0 {
		filtered := make([]QueueInfo, 0)
		filterMap := make(map[string]bool)
		for _, f := range queueFilter {
			filterMap[f] = true
		}
		
		for _, q := range queues {
			if filterMap[q.Name] {
				filtered = append(filtered, q)
			} else {
				// Also check for pattern matching
				for _, pattern := range queueFilter {
					if matched, _ := matchPattern(q.Name, pattern); matched {
						filtered = append(filtered, q)
						break
					}
				}
			}
		}
		queues = filtered
	}

	// Sort by message count (descending)
	sort.Slice(queues, func(i, j int) bool {
		return queues[i].Messages > queues[j].Messages
	})

	// Display header
	fmt.Printf("Queue Monitor - %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println(strings.Repeat("=", 100))
	
	// Summary
	totalMessages := 0
	totalConsumers := 0
	for _, q := range queues {
		totalMessages += q.Messages
		totalConsumers += q.Consumers
	}
	
	fmt.Printf("Total Queues: %d | Total Messages: %d | Total Consumers: %d\n", 
		len(queues), totalMessages, totalConsumers)
	fmt.Println(strings.Repeat("-", 100))

	// Queue details
	if len(queues) == 0 {
		fmt.Println("No queues found matching filter")
	} else {
		fmt.Printf("%-40s %10s %10s %10s %15s %10s\n", 
			"Queue Name", "Messages", "Ready", "Consumers", "Msg/sec", "Memory(KB)")
		fmt.Println(strings.Repeat("-", 100))
		
		for _, q := range queues {
			// Highlight queues with high message count
			highlight := ""
			if q.Messages > 1000 {
				highlight = "*"
			} else if q.Messages > 100 {
				highlight = "+"
			}
			
			// Color coding for queue state
			stateColor := ""
			switch q.State {
			case "running":
				stateColor = "\033[32m" // Green
			case "idle":
				stateColor = "\033[33m" // Yellow
			default:
				stateColor = "\033[31m" // Red
			}
			
			// Reset color
			resetColor := "\033[0m"
			
			fmt.Printf("%s%-40s %10d %10s %10d %15.2f %10d%s\n",
				stateColor,
				truncateString(q.Name, 39) + highlight,
				q.Messages,
				"-", // Ready messages (would need additional API call)
				q.Consumers,
				q.MessageRate,
				q.Memory/1024,
				resetColor,
			)
		}
	}
	
	fmt.Println(strings.Repeat("-", 100))
	fmt.Println("Press Ctrl+C to exit | * = >1000 msgs | + = >100 msgs")
	
	return nil
}

// clearScreen clears the terminal screen
func clearScreen() {
	switch runtime.GOOS {
	case "linux", "darwin":
		cmd := exec.Command("clear")
		cmd.Stdout = os.Stdout
		cmd.Run()
	case "windows":
		cmd := exec.Command("cmd", "/c", "cls")
		cmd.Stdout = os.Stdout
		cmd.Run()
	}
}

// matchPattern checks if a name matches a pattern (supports * wildcard)
func matchPattern(name, pattern string) (bool, error) {
	// Simple wildcard matching
	if !strings.Contains(pattern, "*") {
		return name == pattern, nil
	}
	
	// Convert pattern to regex-like matching
	pattern = strings.ReplaceAll(pattern, ".", "\\.")
	pattern = strings.ReplaceAll(pattern, "*", ".*")
	pattern = "^" + pattern + "$"
	
	// For simplicity, we'll use string matching
	// In production, you'd use regexp
	if strings.HasPrefix(pattern, "^") && strings.HasSuffix(pattern, "$") {
		pattern = pattern[1 : len(pattern)-1]
	}
	
	parts := strings.Split(pattern, ".*")
	if len(parts) == 2 {
		return strings.HasPrefix(name, parts[0]) && strings.HasSuffix(name, parts[1]), nil
	}
	
	return false, nil
}

// truncateString truncates a string to the specified length
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}