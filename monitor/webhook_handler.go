package monitor

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// WebhookAlertHandler sends alerts to webhook endpoints
type WebhookAlertHandler struct {
	name      string
	url       string
	secret    string
	client    *http.Client
	logger    *slog.Logger
	template  *WebhookTemplate
	retries   int
	timeout   time.Duration
}

// WebhookTemplate defines how to format webhook payloads
type WebhookTemplate struct {
	SlackFormat    bool `json:"slackFormat"`
	DiscordFormat  bool `json:"discordFormat"`
	TeamsFormat    bool `json:"teamsFormat"`
	CustomFormat   bool `json:"customFormat"`
	IncludeDetails bool `json:"includeDetails"`
}

// WebhookPayload represents the webhook payload structure
type WebhookPayload struct {
	// Common fields
	Service   string                 `json:"service"`
	Component string                 `json:"component"`
	Alert     *Alert                 `json:"alert,omitempty"`
	Message   string                 `json:"message"`
	Level     AlertLevel             `json:"level"`
	Timestamp time.Time              `json:"timestamp"`
	Details   map[string]interface{} `json:"details,omitempty"`
	
	// Slack-specific fields
	Text        string              `json:"text,omitempty"`
	Username    string              `json:"username,omitempty"`
	IconEmoji   string              `json:"icon_emoji,omitempty"`
	Channel     string              `json:"channel,omitempty"`
	Attachments []SlackAttachment   `json:"attachments,omitempty"`
	
	// Discord-specific fields
	Content string         `json:"content,omitempty"`
	Embeds  []DiscordEmbed `json:"embeds,omitempty"`
	
	// Teams-specific fields
	ThemeColor string                 `json:"themeColor,omitempty"`
	Summary    string                 `json:"summary,omitempty"`
	Sections   []TeamsSection         `json:"sections,omitempty"`
	Facts      []TeamsFact            `json:"facts,omitempty"`
}

// SlackAttachment represents a Slack message attachment
type SlackAttachment struct {
	Color     string       `json:"color,omitempty"`
	Title     string       `json:"title,omitempty"`
	Text      string       `json:"text,omitempty"`
	Fields    []SlackField `json:"fields,omitempty"`
	Footer    string       `json:"footer,omitempty"`
	Timestamp int64        `json:"ts,omitempty"`
}

// SlackField represents a Slack attachment field
type SlackField struct {
	Title string `json:"title"`
	Value string `json:"value"`
	Short bool   `json:"short"`
}

// DiscordEmbed represents a Discord embed
type DiscordEmbed struct {
	Title       string        `json:"title,omitempty"`
	Description string        `json:"description,omitempty"`
	Color       int           `json:"color,omitempty"`
	Fields      []DiscordField `json:"fields,omitempty"`
	Footer      DiscordFooter `json:"footer,omitempty"`
	Timestamp   string        `json:"timestamp,omitempty"`
}

// DiscordField represents a Discord embed field
type DiscordField struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline"`
}

// DiscordFooter represents a Discord embed footer
type DiscordFooter struct {
	Text string `json:"text"`
}

// TeamsSection represents a Teams message section
type TeamsSection struct {
	ActivityTitle    string `json:"activityTitle,omitempty"`
	ActivitySubtitle string `json:"activitySubtitle,omitempty"`
	ActivityText     string `json:"activityText,omitempty"`
	ActivityImage    string `json:"activityImage,omitempty"`
}

// TeamsFact represents a Teams fact
type TeamsFact struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// NewWebhookAlertHandler creates a new webhook alert handler
func NewWebhookAlertHandler(name, url string, logger *slog.Logger) *WebhookAlertHandler {
	return &WebhookAlertHandler{
		name:   name,
		url:    url,
		logger: logger,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		template: &WebhookTemplate{
			IncludeDetails: true,
		},
		retries: 3,
		timeout: 30 * time.Second,
	}
}

// NewSlackWebhookHandler creates a Slack-formatted webhook handler
func NewSlackWebhookHandler(name, url string, logger *slog.Logger) *WebhookAlertHandler {
	handler := NewWebhookAlertHandler(name, url, logger)
	handler.template.SlackFormat = true
	return handler
}

// NewDiscordWebhookHandler creates a Discord-formatted webhook handler
func NewDiscordWebhookHandler(name, url string, logger *slog.Logger) *WebhookAlertHandler {
	handler := NewWebhookAlertHandler(name, url, logger)
	handler.template.DiscordFormat = true
	return handler
}

// NewTeamsWebhookHandler creates a Microsoft Teams-formatted webhook handler
func NewTeamsWebhookHandler(name, url string, logger *slog.Logger) *WebhookAlertHandler {
	handler := NewWebhookAlertHandler(name, url, logger)
	handler.template.TeamsFormat = true
	return handler
}

// WithSecret sets the webhook secret for HMAC verification
func (w *WebhookAlertHandler) WithSecret(secret string) *WebhookAlertHandler {
	w.secret = secret
	return w
}

// WithRetries sets the number of retry attempts
func (w *WebhookAlertHandler) WithRetries(retries int) *WebhookAlertHandler {
	w.retries = retries
	return w
}

// WithTimeout sets the request timeout
func (w *WebhookAlertHandler) WithTimeout(timeout time.Duration) *WebhookAlertHandler {
	w.timeout = timeout
	w.client.Timeout = timeout
	return w
}

// WithTemplate sets a custom webhook template
func (w *WebhookAlertHandler) WithTemplate(template *WebhookTemplate) *WebhookAlertHandler {
	w.template = template
	return w
}

// Name returns the handler name
func (w *WebhookAlertHandler) Name() string {
	return w.name
}

// HandleAlert sends an alert to the webhook endpoint
func (w *WebhookAlertHandler) HandleAlert(ctx context.Context, alert *Alert) error {
	payload := w.formatPayload(alert)
	
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal webhook payload: %w", err)
	}
	
	return w.sendWithRetries(ctx, jsonData)
}

// formatPayload formats the alert into a webhook payload
func (w *WebhookAlertHandler) formatPayload(alert *Alert) *WebhookPayload {
	payload := &WebhookPayload{
		Service:   alert.Service,
		Component: alert.Component,
		Alert:     alert,
		Message:   alert.Message,
		Level:     alert.Level,
		Timestamp: alert.Timestamp,
	}
	
	if w.template.IncludeDetails {
		payload.Details = alert.Details
	}
	
	switch {
	case w.template.SlackFormat:
		w.formatSlackPayload(payload, alert)
	case w.template.DiscordFormat:
		w.formatDiscordPayload(payload, alert)
	case w.template.TeamsFormat:
		w.formatTeamsPayload(payload, alert)
	}
	
	return payload
}

// formatSlackPayload formats the payload for Slack
func (w *WebhookAlertHandler) formatSlackPayload(payload *WebhookPayload, alert *Alert) {
	var color string
	var emoji string
	
	switch alert.Level {
	case AlertLevelCritical:
		color = "danger"
		emoji = ":rotating_light:"
	case AlertLevelWarning:
		color = "warning"
		emoji = ":warning:"
	case AlertLevelInfo:
		color = "good"
		emoji = ":information_source:"
	}
	
	if alert.Resolved {
		color = "good"
		emoji = ":white_check_mark:"
	}
	
	status := "TRIGGERED"
	if alert.Resolved {
		status = "RESOLVED"
	}
	
	payload.Username = "Mmate Monitor"
	payload.IconEmoji = ":robot_face:"
	payload.Text = fmt.Sprintf("%s Alert %s", emoji, status)
	
	attachment := SlackAttachment{
		Color:     color,
		Title:     fmt.Sprintf("%s - %s", alert.Service, alert.Component),
		Text:      alert.Message,
		Footer:    "Mmate Monitoring",
		Timestamp: alert.Timestamp.Unix(),
		Fields: []SlackField{
			{
				Title: "Level",
				Value: string(alert.Level),
				Short: true,
			},
			{
				Title: "Occurrences",
				Value: fmt.Sprintf("%d", alert.Occurrences),
				Short: true,
			},
		},
	}
	
	if alert.Resolved && alert.ResolvedAt != nil {
		duration := alert.ResolvedAt.Sub(alert.FirstSeen)
		attachment.Fields = append(attachment.Fields, SlackField{
			Title: "Duration",
			Value: duration.Round(time.Second).String(),
			Short: true,
		})
	}
	
	// Add details as fields
	if w.template.IncludeDetails && alert.Details != nil {
		for key, value := range alert.Details {
			attachment.Fields = append(attachment.Fields, SlackField{
				Title: strings.Title(strings.ReplaceAll(key, "_", " ")),
				Value: fmt.Sprintf("%v", value),
				Short: true,
			})
		}
	}
	
	payload.Attachments = []SlackAttachment{attachment}
}

// formatDiscordPayload formats the payload for Discord
func (w *WebhookAlertHandler) formatDiscordPayload(payload *WebhookPayload, alert *Alert) {
	var color int
	var title string
	
	switch alert.Level {
	case AlertLevelCritical:
		color = 0xFF0000 // Red
	case AlertLevelWarning:
		color = 0xFFA500 // Orange
	case AlertLevelInfo:
		color = 0x0000FF // Blue
	}
	
	if alert.Resolved {
		color = 0x00FF00 // Green
		title = fmt.Sprintf("✅ Alert Resolved: %s - %s", alert.Service, alert.Component)
	} else {
		title = fmt.Sprintf("🚨 Alert Triggered: %s - %s", alert.Service, alert.Component)
	}
	
	embed := DiscordEmbed{
		Title:       title,
		Description: alert.Message,
		Color:       color,
		Timestamp:   alert.Timestamp.Format(time.RFC3339),
		Footer: DiscordFooter{
			Text: "Mmate Monitoring",
		},
		Fields: []DiscordField{
			{
				Name:   "Level",
				Value:  string(alert.Level),
				Inline: true,
			},
			{
				Name:   "Occurrences",
				Value:  fmt.Sprintf("%d", alert.Occurrences),
				Inline: true,
			},
		},
	}
	
	if alert.Resolved && alert.ResolvedAt != nil {
		duration := alert.ResolvedAt.Sub(alert.FirstSeen)
		embed.Fields = append(embed.Fields, DiscordField{
			Name:   "Duration",
			Value:  duration.Round(time.Second).String(),
			Inline: true,
		})
	}
	
	// Add details as fields
	if w.template.IncludeDetails && alert.Details != nil {
		for key, value := range alert.Details {
			embed.Fields = append(embed.Fields, DiscordField{
				Name:   strings.Title(strings.ReplaceAll(key, "_", " ")),
				Value:  fmt.Sprintf("%v", value),
				Inline: true,
			})
		}
	}
	
	payload.Embeds = []DiscordEmbed{embed}
}

// formatTeamsPayload formats the payload for Microsoft Teams
func (w *WebhookAlertHandler) formatTeamsPayload(payload *WebhookPayload, alert *Alert) {
	var themeColor string
	
	switch alert.Level {
	case AlertLevelCritical:
		themeColor = "FF0000"
	case AlertLevelWarning:
		themeColor = "FFA500"
	case AlertLevelInfo:
		themeColor = "0000FF"
	}
	
	if alert.Resolved {
		themeColor = "00FF00"
	}
	
	status := "Alert Triggered"
	if alert.Resolved {
		status = "Alert Resolved"
	}
	
	payload.ThemeColor = themeColor
	payload.Summary = fmt.Sprintf("%s: %s - %s", status, alert.Service, alert.Component)
	
	payload.Sections = []TeamsSection{
		{
			ActivityTitle:    status,
			ActivitySubtitle: fmt.Sprintf("%s - %s", alert.Service, alert.Component),
			ActivityText:     alert.Message,
		},
	}
	
	payload.Facts = []TeamsFact{
		{
			Name:  "Level",
			Value: string(alert.Level),
		},
		{
			Name:  "Occurrences",
			Value: fmt.Sprintf("%d", alert.Occurrences),
		},
		{
			Name:  "First Seen",
			Value: alert.FirstSeen.Format(time.RFC3339),
		},
	}
	
	if alert.Resolved && alert.ResolvedAt != nil {
		duration := alert.ResolvedAt.Sub(alert.FirstSeen)
		payload.Facts = append(payload.Facts, TeamsFact{
			Name:  "Duration",
			Value: duration.Round(time.Second).String(),
		})
	}
	
	// Add details as facts
	if w.template.IncludeDetails && alert.Details != nil {
		for key, value := range alert.Details {
			payload.Facts = append(payload.Facts, TeamsFact{
				Name:  strings.Title(strings.ReplaceAll(key, "_", " ")),
				Value: fmt.Sprintf("%v", value),
			})
		}
	}
}

// sendWithRetries sends the webhook with retry logic
func (w *WebhookAlertHandler) sendWithRetries(ctx context.Context, jsonData []byte) error {
	var lastErr error
	
	for attempt := 0; attempt <= w.retries; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			backoff := time.Duration(attempt*attempt) * time.Second
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		
		err := w.sendWebhook(ctx, jsonData)
		if err == nil {
			w.logger.Debug("Webhook sent successfully",
				"handler", w.name,
				"attempt", attempt+1,
			)
			return nil
		}
		
		lastErr = err
		w.logger.Warn("Webhook send failed",
			"handler", w.name,
			"attempt", attempt+1,
			"error", err,
		)
	}
	
	return fmt.Errorf("webhook failed after %d attempts: %w", w.retries+1, lastErr)
}

// sendWebhook sends a single webhook request
func (w *WebhookAlertHandler) sendWebhook(ctx context.Context, jsonData []byte) error {
	req, err := http.NewRequestWithContext(ctx, "POST", w.url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Mmate-Monitor/1.0")
	
	// Add HMAC signature if secret is provided
	if w.secret != "" {
		signature := w.generateSignature(jsonData)
		req.Header.Set("X-Hub-Signature-256", "sha256="+signature)
	}
	
	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}
	
	return nil
}

// generateSignature generates HMAC SHA256 signature
func (w *WebhookAlertHandler) generateSignature(payload []byte) string {
	h := hmac.New(sha256.New, []byte(w.secret))
	h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}

// EmailAlertHandler sends alerts via email (placeholder for future implementation)
type EmailAlertHandler struct {
	name     string
	smtpHost string
	smtpPort int
	username string
	password string
	from     string
	to       []string
	logger   *slog.Logger
}

// NewEmailAlertHandler creates a new email alert handler
func NewEmailAlertHandler(name, smtpHost string, smtpPort int, username, password, from string, to []string, logger *slog.Logger) *EmailAlertHandler {
	return &EmailAlertHandler{
		name:     name,
		smtpHost: smtpHost,
		smtpPort: smtpPort,
		username: username,
		password: password,
		from:     from,
		to:       to,
		logger:   logger,
	}
}

// Name returns the handler name
func (e *EmailAlertHandler) Name() string {
	return e.name
}

// HandleAlert sends an alert via email
func (e *EmailAlertHandler) HandleAlert(ctx context.Context, alert *Alert) error {
	// TODO: Implement email sending using net/smtp
	e.logger.Info("Email alert would be sent",
		"handler", e.name,
		"alert", alert.ID,
		"level", alert.Level,
		"to", e.to,
	)
	return nil
}

// LogAlertHandler logs alerts (useful for testing and debugging)
type LogAlertHandler struct {
	name   string
	logger *slog.Logger
}

// NewLogAlertHandler creates a new log alert handler
func NewLogAlertHandler(name string, logger *slog.Logger) *LogAlertHandler {
	return &LogAlertHandler{
		name:   name,
		logger: logger,
	}
}

// Name returns the handler name
func (l *LogAlertHandler) Name() string {
	return l.name
}

// HandleAlert logs the alert
func (l *LogAlertHandler) HandleAlert(ctx context.Context, alert *Alert) error {
	status := "TRIGGERED"
	if alert.Resolved {
		status = "RESOLVED"
	}
	
	l.logger.Info("Alert notification",
		"status", status,
		"id", alert.ID,
		"level", alert.Level,
		"service", alert.Service,
		"component", alert.Component,
		"message", alert.Message,
		"occurrences", alert.Occurrences,
		"first_seen", alert.FirstSeen,
		"last_seen", alert.LastSeen,
	)
	
	return nil
}