package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/glimte/mmate-go/monitor"
)

const (
	// Colors
	primaryColor   = lipgloss.Color("#7C3AED")
	secondaryColor = lipgloss.Color("#10B981")
	warningColor   = lipgloss.Color("#F59E0B")
	errorColor     = lipgloss.Color("#EF4444")
	mutedColor     = lipgloss.Color("#6B7280")
	bgColor        = lipgloss.Color("#1F2937")
)

var (
	// Styles
	titleStyle = lipgloss.NewStyle().
			Foreground(primaryColor).
			Bold(true).
			Margin(1, 0).
			Padding(0, 1)

	headerStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(primaryColor).
			Bold(true).
			Padding(0, 1).
			Margin(0, 0, 1, 0)

	tabStyle = lipgloss.NewStyle().
			Foreground(mutedColor).
			Padding(0, 2).
			Margin(0, 1, 0, 0)

	activeTabStyle = lipgloss.NewStyle().
			Foreground(primaryColor).
			Background(lipgloss.Color("#374151")).
			Bold(true).
			Padding(0, 2).
			Margin(0, 1, 0, 0)

	statusHealthyStyle = lipgloss.NewStyle().
				Foreground(secondaryColor).
				Bold(true)

	statusWarningStyle = lipgloss.NewStyle().
				Foreground(warningColor).
				Bold(true)

	statusErrorStyle = lipgloss.NewStyle().
				Foreground(errorColor).
				Bold(true)

	cardStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(mutedColor).
			Padding(1, 2).
			Margin(1, 0)

	helpStyle = lipgloss.NewStyle().
			Foreground(mutedColor).
			Margin(1, 0)
)

type tab int

const (
	overviewTab tab = iota
	queuesTab
	alertsTab
	healthTab
)

type model struct {
	client        *monitor.Client
	activeTab     tab
	width         int
	height        int
	lastUpdate    time.Time
	autoRefresh   bool
	refreshTicker *time.Ticker

	// Data
	overview *monitor.Overview
	queues   []monitor.Queue
	alerts   []*monitor.Alert
	health   map[string]interface{}

	// UI state
	selectedQueue int
	selectedAlert int
	error         error
}

type tickMsg struct{}
type dataMsg struct {
	overview *monitor.Overview
	queues   []monitor.Queue
	alerts   []*monitor.Alert
	health   map[string]interface{}
	err      error
}

func initialModel() model {
	// Get RabbitMQ URL from environment
	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		amqpURL = "amqp://guest:guest@localhost:5672/"
	}

	client, err := monitor.NewClient(amqpURL)
	if err != nil {
		log.Fatalf("Failed to create monitoring client: %v", err)
	}

	return model{
		client:      client,
		activeTab:   overviewTab,
		autoRefresh: true,
		lastUpdate:  time.Now(),
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(
		m.fetchData(),
		m.tickCmd(),
	)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			if m.refreshTicker != nil {
				m.refreshTicker.Stop()
			}
			return m, tea.Quit

		case "tab", "right":
			m.activeTab = (m.activeTab + 1) % 4
			return m, nil

		case "shift+tab", "left":
			if m.activeTab == 0 {
				m.activeTab = 3
			} else {
				m.activeTab--
			}
			return m, nil

		case "r":
			return m, m.fetchData()

		case " ":
			m.autoRefresh = !m.autoRefresh
			if m.autoRefresh {
				return m, m.tickCmd()
			} else {
				if m.refreshTicker != nil {
					m.refreshTicker.Stop()
				}
			}
			return m, nil

		case "up":
			switch m.activeTab {
			case queuesTab:
				if m.selectedQueue > 0 {
					m.selectedQueue--
				}
			case alertsTab:
				if m.selectedAlert > 0 {
					m.selectedAlert--
				}
			}
			return m, nil

		case "down":
			switch m.activeTab {
			case queuesTab:
				if m.selectedQueue < len(m.queues)-1 {
					m.selectedQueue++
				}
			case alertsTab:
				if m.selectedAlert < len(m.alerts)-1 {
					m.selectedAlert++
				}
			}
			return m, nil
		}

	case tickMsg:
		if m.autoRefresh {
			return m, tea.Batch(
				m.fetchData(),
				m.tickCmd(),
			)
		}

	case dataMsg:
		m.error = msg.err
		if msg.err == nil {
			m.overview = msg.overview
			m.queues = msg.queues
			m.alerts = msg.alerts
			m.health = msg.health
			m.lastUpdate = time.Now()
		}
		return m, nil
	}

	return m, nil
}

func (m model) View() string {
	if m.width == 0 {
		return "Loading..."
	}

	// Header
	header := headerStyle.Width(m.width - 2).Render("Mmate Monitor TUI")

	// Tabs
	tabs := m.renderTabs()

	// Content
	var content string
	switch m.activeTab {
	case overviewTab:
		content = m.renderOverview()
	case queuesTab:
		content = m.renderQueues()
	case alertsTab:
		content = m.renderAlerts()
	case healthTab:
		content = m.renderHealth()
	}

	// Status bar
	statusBar := m.renderStatusBar()

	// Help
	help := m.renderHelp()

	return lipgloss.JoinVertical(lipgloss.Left,
		header,
		tabs,
		content,
		statusBar,
		help,
	)
}

func (m model) renderTabs() string {
	tabs := []string{
		m.renderTab("Overview", overviewTab),
		m.renderTab("Queues", queuesTab),
		m.renderTab("Alerts", alertsTab),
		m.renderTab("Health", healthTab),
	}

	return lipgloss.JoinHorizontal(lipgloss.Left, tabs...)
}

func (m model) renderTab(title string, t tab) string {
	if m.activeTab == t {
		return activeTabStyle.Render(title)
	}
	return tabStyle.Render(title)
}

func (m model) renderOverview() string {
	if m.overview == nil {
		return cardStyle.Render("Loading overview...")
	}

	var parts []string

	// Broker info
	brokerInfo := fmt.Sprintf(
		"Broker: %s\nVersion: %s\nUptime: %s",
		m.overview.Node,
		m.overview.Version,
		formatDuration(time.Duration(m.overview.Uptime)*time.Millisecond),
	)
	parts = append(parts, cardStyle.Render("Broker Information\n\n"+brokerInfo))

	// Message stats
	if m.overview.MessageStats.Publish > 0 || m.overview.MessageStats.Deliver > 0 {
		msgStats := fmt.Sprintf(
			"Published: %d (%.1f/s)\nDelivered: %d (%.1f/s)\nAcknowledged: %d (%.1f/s)",
			m.overview.MessageStats.Publish,
			m.overview.MessageStats.PublishDetails.Rate,
			m.overview.MessageStats.Deliver,
			m.overview.MessageStats.DeliverDetails.Rate,
			m.overview.MessageStats.Ack,
			m.overview.MessageStats.AckDetails.Rate,
		)
		parts = append(parts, cardStyle.Render("Message Statistics\n\n"+msgStats))
	}

	// Resource usage
	memUsed := float64(m.overview.MemoryUsed) / float64(m.overview.MemoryLimit) * 100

	resources := fmt.Sprintf(
		"Memory: %.1f%% (%s used)\nDisk Free: %s\nFile Descriptors: %d/%d",
		memUsed,
		formatBytes(m.overview.MemoryUsed),
		formatBytes(m.overview.DiskFree),
		m.overview.FileDescriptorsUsed,
		m.overview.FileDescriptorsTotal,
	)
	parts = append(parts, cardStyle.Render("Resource Usage\n\n"+resources))

	// Connection info
	connections := fmt.Sprintf(
		"Connections: %d\nChannels: %d\nQueues: %d\nExchanges: %d",
		len(m.overview.Connections),
		len(m.overview.Channels),
		len(m.overview.Queues),
		len(m.overview.Exchanges),
	)
	parts = append(parts, cardStyle.Render("Topology\n\n"+connections))

	return lipgloss.JoinVertical(lipgloss.Left, parts...)
}

func (m model) renderQueues() string {
	if len(m.queues) == 0 {
		return cardStyle.Render("No queues found")
	}

	var rows []string
	rows = append(rows, "Name                     Messages  Consumers  Memory")
	rows = append(rows, strings.Repeat("─", 60))

	for i, queue := range m.queues {
		style := lipgloss.NewStyle()
		if i == m.selectedQueue {
			style = style.Background(lipgloss.Color("#374151"))
		}

		row := fmt.Sprintf("%-24s %8d %9d %8s",
			truncateString(queue.Name, 24),
			queue.Messages,
			queue.Consumers,
			formatBytes(queue.Memory),
		)
		rows = append(rows, style.Render(row))
	}

	content := strings.Join(rows, "\n")

	// Queue details
	if m.selectedQueue < len(m.queues) {
		selected := m.queues[m.selectedQueue]
		details := fmt.Sprintf(
			"Queue: %s\nVHost: %s\nDurable: %t\nAuto Delete: %t\nMessages: %d\nConsumers: %d\nMemory: %s",
			selected.Name,
			selected.VHost,
			selected.Durable,
			selected.AutoDelete,
			selected.Messages,
			selected.Consumers,
			formatBytes(selected.Memory),
		)

		return lipgloss.JoinVertical(lipgloss.Left,
			cardStyle.Render("Queue List\n\n"+content),
			cardStyle.Render("Queue Details\n\n"+details),
		)
	}

	return cardStyle.Render("Queue List\n\n" + content)
}

func (m model) renderAlerts() string {
	if len(m.alerts) == 0 {
		return cardStyle.Render("No active alerts")
	}

	var parts []string

	for i, alert := range m.alerts {
		style := cardStyle
		if i == m.selectedAlert {
			style = style.Background(lipgloss.Color("#374151"))
		}

		var levelStyle lipgloss.Style
		switch alert.Level {
		case monitor.AlertLevelCritical:
			levelStyle = statusErrorStyle
		case monitor.AlertLevelWarning:
			levelStyle = statusWarningStyle
		default:
			levelStyle = statusHealthyStyle
		}

		status := "ACTIVE"
		if alert.Resolved {
			status = "RESOLVED"
			levelStyle = statusHealthyStyle
		}

		alertContent := fmt.Sprintf(
			"%s %s\n%s - %s\n%s\nOccurrences: %d\nFirst Seen: %s",
			levelStyle.Render(strings.ToUpper(string(alert.Level))),
			status,
			alert.Service,
			alert.Component,
			alert.Message,
			alert.Occurrences,
			alert.FirstSeen.Format("15:04:05"),
		)

		parts = append(parts, style.Render(alertContent))
	}

	return lipgloss.JoinVertical(lipgloss.Left, parts...)
}

func (m model) renderHealth() string {
	if m.health == nil {
		return cardStyle.Render("Loading health data...")
	}

	var parts []string

	// Overall status
	status, ok := m.health["status"].(string)
	if !ok {
		status = "unknown"
	}

	var statusStyle lipgloss.Style
	switch status {
	case "healthy":
		statusStyle = statusHealthyStyle
	case "degraded":
		statusStyle = statusWarningStyle
	case "unhealthy":
		statusStyle = statusErrorStyle
	default:
		statusStyle = lipgloss.NewStyle()
	}

	overall := fmt.Sprintf("Overall Status: %s", statusStyle.Render(strings.ToUpper(status)))
	parts = append(parts, cardStyle.Render(overall))

	// Individual checks
	if checks, ok := m.health["checks"].(map[string]interface{}); ok {
		for name, checkData := range checks {
			if checkMap, ok := checkData.(map[string]interface{}); ok {
				checkStatus := "unknown"
				if s, ok := checkMap["status"].(string); ok {
					checkStatus = s
				}

				message := ""
				if msg, ok := checkMap["message"].(string); ok {
					message = msg
				}

				var checkStyle lipgloss.Style
				switch checkStatus {
				case "healthy":
					checkStyle = statusHealthyStyle
				case "degraded":
					checkStyle = statusWarningStyle
				case "unhealthy":
					checkStyle = statusErrorStyle
				default:
					checkStyle = lipgloss.NewStyle()
				}

				checkContent := fmt.Sprintf(
					"%s: %s\n%s",
					name,
					checkStyle.Render(strings.ToUpper(checkStatus)),
					message,
				)
				parts = append(parts, cardStyle.Render(checkContent))
			}
		}
	}

	return lipgloss.JoinVertical(lipgloss.Left, parts...)
}

func (m model) renderStatusBar() string {
	refreshStatus := "Auto-refresh: ON"
	if !m.autoRefresh {
		refreshStatus = "Auto-refresh: OFF"
	}

	errorStatus := ""
	if m.error != nil {
		errorStatus = statusErrorStyle.Render(fmt.Sprintf("Error: %v", m.error))
	}

	lastUpdate := fmt.Sprintf("Last update: %s", m.lastUpdate.Format("15:04:05"))

	statusParts := []string{refreshStatus, lastUpdate}
	if errorStatus != "" {
		statusParts = append(statusParts, errorStatus)
	}

	return helpStyle.Render(strings.Join(statusParts, " | "))
}

func (m model) renderHelp() string {
	help := "Tab/→: Next tab | Shift+Tab/←: Previous tab | ↑↓: Navigate | R: Refresh | Space: Toggle auto-refresh | Q: Quit"
	return helpStyle.Render(help)
}

func (m model) fetchData() tea.Cmd {
	return func() tea.Msg {
		ctx := context.Background()
		_ = ctx // Used in goroutines below

		// Fetch all data concurrently
		overviewCh := make(chan *monitor.Overview, 1)
		queuesCh := make(chan []monitor.Queue, 1)
		alertsCh := make(chan []*monitor.Alert, 1)
		healthCh := make(chan map[string]interface{}, 1)
		errCh := make(chan error, 4)

		// Fetch overview
		go func() {
			overview, err := m.client.GetOverview(ctx)
			if err != nil {
				errCh <- err
				overviewCh <- nil
			} else {
				overviewCh <- overview
				errCh <- nil
			}
		}()

		// Fetch queues
		go func() {
			queues, err := m.client.GetQueues(ctx)
			if err != nil {
				errCh <- err
				queuesCh <- nil
			} else {
				queuesCh <- queues
				errCh <- nil
			}
		}()

		// Fetch alerts (placeholder - would come from monitoring service)
		go func() {
			// For now, return empty alerts
			alertsCh <- []*monitor.Alert{}
			errCh <- nil
		}()

		// Fetch health (placeholder - would come from health registry)
		go func() {
			health := map[string]interface{}{
				"status": "healthy",
				"checks": map[string]interface{}{
					"rabbitmq": map[string]interface{}{
						"status":  "healthy",
						"message": "Connection is healthy",
					},
					"memory": map[string]interface{}{
						"status":  "healthy",
						"message": "Memory usage is normal",
					},
				},
			}
			healthCh <- health
			errCh <- nil
		}()

		// Wait for all to complete
		overview := <-overviewCh
		queues := <-queuesCh
		alerts := <-alertsCh
		health := <-healthCh

		// Check for errors
		var firstErr error
		for i := 0; i < 4; i++ {
			if err := <-errCh; err != nil && firstErr == nil {
				firstErr = err
			}
		}

		return dataMsg{
			overview: overview,
			queues:   queues,
			alerts:   alerts,
			health:   health,
			err:      firstErr,
		}
	}
}

func (m model) tickCmd() tea.Cmd {
	return tea.Tick(5*time.Second, func(time.Time) tea.Msg {
		return tickMsg{}
	})
}

// Utility functions

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	} else if d < time.Hour {
		return fmt.Sprintf("%.1fm", d.Minutes())
	} else if d < 24*time.Hour {
		return fmt.Sprintf("%.1fh", d.Hours())
	} else {
		return fmt.Sprintf("%.1fd", d.Hours()/24)
	}
}

func truncateString(s string, length int) string {
	if len(s) <= length {
		return s
	}
	return s[:length-3] + "..."
}

func main() {
	// Check for help flag
	if len(os.Args) > 1 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		fmt.Println("Mmate Monitor TUI")
		fmt.Println("")
		fmt.Println("A terminal user interface for monitoring Mmate messaging infrastructure.")
		fmt.Println("")
		fmt.Println("Environment Variables:")
		fmt.Println("  AMQP_URL                    RabbitMQ connection URL (default: amqp://guest:guest@localhost:5672/)")
		fmt.Println("  RABBITMQ_MANAGEMENT_URL     RabbitMQ Management API URL (default: http://localhost:15672)")
		fmt.Println("")
		fmt.Println("Navigation:")
		fmt.Println("  Tab/→                       Next tab")
		fmt.Println("  Shift+Tab/←                 Previous tab")
		fmt.Println("  ↑↓                          Navigate lists")
		fmt.Println("  R                           Refresh data")
		fmt.Println("  Space                       Toggle auto-refresh")
		fmt.Println("  Q                           Quit")
		return
	}

	p := tea.NewProgram(
		initialModel(),
		tea.WithAltScreen(),
	)

	if _, err := p.Run(); err != nil {
		log.Fatalf("Error running TUI: %v", err)
	}
}
