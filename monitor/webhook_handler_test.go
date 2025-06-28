package monitor

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebhookTemplate_Fields(t *testing.T) {
	template := &WebhookTemplate{
		SlackFormat:    true,
		DiscordFormat:  false,
		TeamsFormat:    false,
		CustomFormat:   true,
		IncludeDetails: true,
	}
	
	assert.True(t, template.SlackFormat)
	assert.False(t, template.DiscordFormat)
	assert.False(t, template.TeamsFormat)
	assert.True(t, template.CustomFormat)
	assert.True(t, template.IncludeDetails)
}

func TestWebhookPayload_Fields(t *testing.T) {
	now := time.Now()
	alert := &Alert{
		ID:        "test-alert",
		Level:     AlertLevelWarning,
		Service:   "test-service",
		Component: "test-component",
		Message:   "Test message",
		Timestamp: now,
	}
	
	details := map[string]interface{}{
		"key1": "value1",
		"key2": 42,
	}
	
	payload := &WebhookPayload{
		Service:   "test-service",
		Component: "test-component",
		Alert:     alert,
		Message:   "Test message",
		Level:     AlertLevelWarning,
		Timestamp: now,
		Details:   details,
	}
	
	assert.Equal(t, "test-service", payload.Service)
	assert.Equal(t, "test-component", payload.Component)
	assert.Equal(t, alert, payload.Alert)
	assert.Equal(t, "Test message", payload.Message)
	assert.Equal(t, AlertLevelWarning, payload.Level)
	assert.Equal(t, now, payload.Timestamp)
	assert.Equal(t, details, payload.Details)
}

func TestNewWebhookAlertHandler(t *testing.T) {
	name := "test-handler"
	url := "https://example.com/webhook"
	logger := slog.Default()
	
	handler := NewWebhookAlertHandler(name, url, logger)
	
	assert.NotNil(t, handler)
	assert.Equal(t, name, handler.name)
	assert.Equal(t, url, handler.url)
	assert.Equal(t, logger, handler.logger)
	assert.NotNil(t, handler.client)
	assert.NotNil(t, handler.template)
	assert.True(t, handler.template.IncludeDetails)
	assert.Equal(t, 3, handler.retries)
	assert.Equal(t, 30*time.Second, handler.timeout)
}

func TestNewSlackWebhookHandler(t *testing.T) {
	name := "slack-handler"
	url := "https://hooks.slack.com/webhook"
	logger := slog.Default()
	
	handler := NewSlackWebhookHandler(name, url, logger)
	
	assert.NotNil(t, handler)
	assert.Equal(t, name, handler.name)
	assert.Equal(t, url, handler.url)
	assert.True(t, handler.template.SlackFormat)
	assert.False(t, handler.template.DiscordFormat)
	assert.False(t, handler.template.TeamsFormat)
}

func TestNewDiscordWebhookHandler(t *testing.T) {
	name := "discord-handler"
	url := "https://discord.com/api/webhooks/123/abc"
	logger := slog.Default()
	
	handler := NewDiscordWebhookHandler(name, url, logger)
	
	assert.NotNil(t, handler)
	assert.Equal(t, name, handler.name)
	assert.Equal(t, url, handler.url)
	assert.False(t, handler.template.SlackFormat)
	assert.True(t, handler.template.DiscordFormat)
	assert.False(t, handler.template.TeamsFormat)
}

func TestNewTeamsWebhookHandler(t *testing.T) {
	name := "teams-handler"
	url := "https://outlook.office.com/webhook/123"
	logger := slog.Default()
	
	handler := NewTeamsWebhookHandler(name, url, logger)
	
	assert.NotNil(t, handler)
	assert.Equal(t, name, handler.name)
	assert.Equal(t, url, handler.url)
	assert.False(t, handler.template.SlackFormat)
	assert.False(t, handler.template.DiscordFormat)
	assert.True(t, handler.template.TeamsFormat)
}

func TestWebhookAlertHandler_FluentConfiguration(t *testing.T) {
	handler := NewWebhookAlertHandler("test", "https://example.com", slog.Default())
	
	// Test fluent configuration
	configured := handler.
		WithSecret("secret123").
		WithRetries(5).
		WithTimeout(60 * time.Second).
		WithTemplate(&WebhookTemplate{
			CustomFormat:   true,
			IncludeDetails: false,
		})
	
	assert.Equal(t, handler, configured) // Should return same instance
	assert.Equal(t, "secret123", handler.secret)
	assert.Equal(t, 5, handler.retries)
	assert.Equal(t, 60*time.Second, handler.timeout)
	assert.Equal(t, 60*time.Second, handler.client.Timeout)
	assert.True(t, handler.template.CustomFormat)
	assert.False(t, handler.template.IncludeDetails)
}

func TestWebhookAlertHandler_Name(t *testing.T) {
	name := "test-webhook"
	handler := NewWebhookAlertHandler(name, "https://example.com", slog.Default())
	
	assert.Equal(t, name, handler.Name())
}

func TestWebhookAlertHandler_HandleAlert_Success(t *testing.T) {
	// Create test server
	var receivedPayload *WebhookPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, "Mmate-Monitor/1.0", r.Header.Get("User-Agent"))
		
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		
		err = json.Unmarshal(body, &receivedPayload)
		require.NoError(t, err)
		
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	
	handler := NewWebhookAlertHandler("test", server.URL, slog.Default())
	
	alert := &Alert{
		ID:          "test-alert",
		Level:       AlertLevelWarning,
		Service:     "test-service",
		Component:   "test-component",
		Message:     "Test alert message",
		Timestamp:   time.Now(),
		FirstSeen:   time.Now(),
		LastSeen:    time.Now(),
		Occurrences: 1,
		Details: map[string]interface{}{
			"metric": "cpu_usage",
			"value":  85.5,
		},
	}
	
	err := handler.HandleAlert(context.Background(), alert)
	
	assert.NoError(t, err)
	assert.NotNil(t, receivedPayload)
	assert.Equal(t, alert.Service, receivedPayload.Service)
	assert.Equal(t, alert.Component, receivedPayload.Component)
	assert.Equal(t, alert.Message, receivedPayload.Message)
	assert.Equal(t, alert.Level, receivedPayload.Level)
	assert.Equal(t, alert.Details, receivedPayload.Details)
}

func TestWebhookAlertHandler_HandleAlert_WithSecret(t *testing.T) {
	var receivedSignature string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedSignature = r.Header.Get("X-Hub-Signature-256")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	
	secret := "test-secret"
	handler := NewWebhookAlertHandler("test", server.URL, slog.Default()).
		WithSecret(secret)
	
	alert := &Alert{
		ID:        "test-alert",
		Level:     AlertLevelCritical,
		Service:   "test-service",
		Component: "test-component",
		Message:   "Critical alert",
		Timestamp: time.Now(),
	}
	
	err := handler.HandleAlert(context.Background(), alert)
	
	assert.NoError(t, err)
	assert.NotEmpty(t, receivedSignature)
	assert.True(t, strings.HasPrefix(receivedSignature, "sha256="))
}

func TestWebhookAlertHandler_HandleAlert_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	
	handler := NewWebhookAlertHandler("test", server.URL, slog.Default()).
		WithRetries(1) // Reduce retries for faster test
	
	alert := &Alert{
		ID:        "test-alert",
		Level:     AlertLevelCritical,
		Service:   "test-service",
		Component: "test-component",
		Message:   "Test alert",
		Timestamp: time.Now(),
	}
	
	err := handler.HandleAlert(context.Background(), alert)
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "webhook failed after")
}

func TestWebhookAlertHandler_FormatSlackPayload(t *testing.T) {
	handler := NewSlackWebhookHandler("slack", "https://hooks.slack.com", slog.Default())
	
	now := time.Now()
	resolvedAt := now.Add(10 * time.Minute)
	alert := &Alert{
		ID:          "test-alert",
		Level:       AlertLevelCritical,
		Service:     "app-service",
		Component:   "database",
		Message:     "Database connection failed",
		Timestamp:   now,
		FirstSeen:   now,
		LastSeen:    now,
		Occurrences: 3,
		Resolved:    true,
		ResolvedAt:  &resolvedAt,
		Details: map[string]interface{}{
			"connection_pool": "exhausted",
			"retry_count":     5,
		},
	}
	
	payload := handler.formatPayload(alert)
	
	assert.Equal(t, "Mmate Monitor", payload.Username)
	assert.Equal(t, ":robot_face:", payload.IconEmoji)
	assert.Contains(t, payload.Text, "RESOLVED")
	assert.Equal(t, 1, len(payload.Attachments))
	
	attachment := payload.Attachments[0]
	assert.Equal(t, "good", attachment.Color) // Resolved alerts are green
	assert.Equal(t, "app-service - database", attachment.Title)
	assert.Equal(t, "Database connection failed", attachment.Text)
	assert.Equal(t, "Mmate Monitoring", attachment.Footer)
	
	// Check fields
	fieldMap := make(map[string]string)
	for _, field := range attachment.Fields {
		fieldMap[field.Title] = field.Value
	}
	
	assert.Equal(t, "critical", fieldMap["Level"])
	assert.Equal(t, "3", fieldMap["Occurrences"])
	assert.Contains(t, fieldMap, "Duration")
	assert.Equal(t, "exhausted", fieldMap["Connection Pool"])
	assert.Equal(t, "5", fieldMap["Retry Count"])
}

func TestWebhookAlertHandler_FormatDiscordPayload(t *testing.T) {
	handler := NewDiscordWebhookHandler("discord", "https://discord.com/api/webhooks", slog.Default())
	
	now := time.Now()
	alert := &Alert{
		ID:          "test-alert",
		Level:       AlertLevelWarning,
		Service:     "web-app",
		Component:   "api",
		Message:     "High response time detected",
		Timestamp:   now,
		FirstSeen:   now,
		LastSeen:    now,
		Occurrences: 1,
		Resolved:    false,
		Details: map[string]interface{}{
			"avg_response_time": 2500,
			"endpoint":          "/api/users",
		},
	}
	
	payload := handler.formatPayload(alert)
	
	assert.Equal(t, 1, len(payload.Embeds))
	
	embed := payload.Embeds[0]
	assert.Contains(t, embed.Title, "🚨 Alert Triggered")
	assert.Contains(t, embed.Title, "web-app - api")
	assert.Equal(t, "High response time detected", embed.Description)
	assert.Equal(t, 0xFFA500, embed.Color) // Orange for warning
	assert.Equal(t, "Mmate Monitoring", embed.Footer.Text)
	
	// Check fields
	fieldMap := make(map[string]string)
	for _, field := range embed.Fields {
		fieldMap[field.Name] = field.Value
	}
	
	assert.Equal(t, "warning", fieldMap["Level"])
	assert.Equal(t, "1", fieldMap["Occurrences"])
	assert.Equal(t, "2500", fieldMap["Avg Response Time"])
	assert.Equal(t, "/api/users", fieldMap["Endpoint"])
}

func TestWebhookAlertHandler_FormatTeamsPayload(t *testing.T) {
	handler := NewTeamsWebhookHandler("teams", "https://outlook.office.com/webhook", slog.Default())
	
	now := time.Now()
	alert := &Alert{
		ID:          "test-alert",
		Level:       AlertLevelInfo,
		Service:     "monitoring",
		Component:   "metrics",
		Message:     "Metrics collection restored",
		Timestamp:   now,
		FirstSeen:   now.Add(-5 * time.Minute),
		LastSeen:    now,
		Occurrences: 1,
		Resolved:    true,
		ResolvedAt:  &now,
		Details: map[string]interface{}{
			"metrics_collected": 1250,
			"storage_used":      "2.5GB",
		},
	}
	
	payload := handler.formatPayload(alert)
	
	assert.Equal(t, "00FF00", payload.ThemeColor) // Green for resolved
	assert.Contains(t, payload.Summary, "Alert Resolved")
	assert.Contains(t, payload.Summary, "monitoring - metrics")
	
	assert.Equal(t, 1, len(payload.Sections))
	section := payload.Sections[0]
	assert.Equal(t, "Alert Resolved", section.ActivityTitle)
	assert.Equal(t, "monitoring - metrics", section.ActivitySubtitle)
	assert.Equal(t, "Metrics collection restored", section.ActivityText)
	
	// Check facts
	factMap := make(map[string]string)
	for _, fact := range payload.Facts {
		factMap[fact.Name] = fact.Value
	}
	
	assert.Equal(t, "info", factMap["Level"])
	assert.Equal(t, "1", factMap["Occurrences"])
	assert.Contains(t, factMap, "Duration")
	assert.Equal(t, "1250", factMap["Metrics Collected"])
	assert.Equal(t, "2.5GB", factMap["Storage Used"])
}

func TestWebhookAlertHandler_RetryLogic(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount < 3 {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()
	
	handler := NewWebhookAlertHandler("test", server.URL, slog.Default()).
		WithRetries(3)
	
	alert := &Alert{
		ID:        "test-alert",
		Level:     AlertLevelWarning,
		Service:   "test",
		Component: "test",
		Message:   "Test message",
		Timestamp: time.Now(),
	}
	
	err := handler.HandleAlert(context.Background(), alert)
	
	assert.NoError(t, err)
	assert.Equal(t, 3, callCount) // Should succeed on third attempt
}

func TestWebhookAlertHandler_ContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond) // Simulate slow server
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	
	handler := NewWebhookAlertHandler("test", server.URL, slog.Default())
	
	alert := &Alert{
		ID:        "test-alert",
		Level:     AlertLevelCritical,
		Service:   "test",
		Component: "test",
		Message:   "Test message",
		Timestamp: time.Now(),
	}
	
	// Create context that will be cancelled
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	
	err := handler.HandleAlert(ctx, alert)
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")
}

func TestWebhookAlertHandler_GenerateSignature(t *testing.T) {
	handler := NewWebhookAlertHandler("test", "https://example.com", slog.Default()).
		WithSecret("test-secret")
	
	payload := []byte(`{"message": "test"}`)
	signature := handler.generateSignature(payload)
	
	assert.NotEmpty(t, signature)
	assert.Len(t, signature, 64) // SHA256 hex string length
	
	// Same payload should generate same signature
	signature2 := handler.generateSignature(payload)
	assert.Equal(t, signature, signature2)
	
	// Different payload should generate different signature
	differentPayload := []byte(`{"message": "different"}`)
	differentSignature := handler.generateSignature(differentPayload)
	assert.NotEqual(t, signature, differentSignature)
}

func TestEmailAlertHandler(t *testing.T) {
	name := "email-handler"
	smtpHost := "smtp.example.com"
	smtpPort := 587
	username := "user@example.com"
	password := "password"
	from := "alerts@example.com"
	to := []string{"admin@example.com", "ops@example.com"}
	logger := slog.Default()
	
	handler := NewEmailAlertHandler(name, smtpHost, smtpPort, username, password, from, to, logger)
	
	assert.NotNil(t, handler)
	assert.Equal(t, name, handler.name)
	assert.Equal(t, smtpHost, handler.smtpHost)
	assert.Equal(t, smtpPort, handler.smtpPort)
	assert.Equal(t, username, handler.username)
	assert.Equal(t, password, handler.password)
	assert.Equal(t, from, handler.from)
	assert.Equal(t, to, handler.to)
	assert.Equal(t, logger, handler.logger)
	
	assert.Equal(t, name, handler.Name())
	
	// Test HandleAlert (currently just logs)
	alert := &Alert{
		ID:        "test-alert",
		Level:     AlertLevelWarning,
		Service:   "test",
		Component: "test",
		Message:   "Test message",
		Timestamp: time.Now(),
	}
	
	err := handler.HandleAlert(context.Background(), alert)
	assert.NoError(t, err) // Currently always returns nil
}

func TestLogAlertHandler(t *testing.T) {
	name := "log-handler"
	logger := slog.Default()
	
	handler := NewLogAlertHandler(name, logger)
	
	assert.NotNil(t, handler)
	assert.Equal(t, name, handler.name)
	assert.Equal(t, logger, handler.logger)
	assert.Equal(t, name, handler.Name())
	
	// Test HandleAlert with triggered alert
	now := time.Now()
	alert := &Alert{
		ID:          "test-alert",
		Level:       AlertLevelCritical,
		Service:     "app",
		Component:   "database",
		Message:     "Connection failed",
		Timestamp:   now,
		FirstSeen:   now,
		LastSeen:    now,
		Occurrences: 1,
		Resolved:    false,
	}
	
	err := handler.HandleAlert(context.Background(), alert)
	assert.NoError(t, err)
	
	// Test HandleAlert with resolved alert
	resolvedAt := now.Add(5 * time.Minute)
	alert.Resolved = true
	alert.ResolvedAt = &resolvedAt
	
	err = handler.HandleAlert(context.Background(), alert)
	assert.NoError(t, err)
}

// Test data structure serialization
func TestWebhookPayload_JSONSerialization(t *testing.T) {
	now := time.Now()
	payload := &WebhookPayload{
		Service:   "test-service",
		Component: "test-component",
		Message:   "Test message",
		Level:     AlertLevelWarning,
		Timestamp: now,
		Details: map[string]interface{}{
			"string_value": "test",
			"int_value":    42,
			"float_value":  3.14,
			"bool_value":   true,
		},
		// Slack-specific
		Text:      "Slack text",
		Username:  "Monitor Bot",
		IconEmoji: ":warning:",
		Attachments: []SlackAttachment{
			{
				Color: "warning",
				Title: "Test Title",
				Text:  "Test attachment text",
				Fields: []SlackField{
					{Title: "Field1", Value: "Value1", Short: true},
					{Title: "Field2", Value: "Value2", Short: false},
				},
			},
		},
		// Discord-specific
		Content: "Discord content",
		Embeds: []DiscordEmbed{
			{
				Title:       "Discord Title",
				Description: "Discord description",
				Color:       0xFF0000,
				Fields: []DiscordField{
					{Name: "Field1", Value: "Value1", Inline: true},
				},
				Footer: DiscordFooter{Text: "Footer text"},
			},
		},
		// Teams-specific
		ThemeColor: "FF0000",
		Summary:    "Teams summary",
		Sections: []TeamsSection{
			{
				ActivityTitle:    "Teams Title",
				ActivitySubtitle: "Teams Subtitle",
				ActivityText:     "Teams Text",
			},
		},
		Facts: []TeamsFact{
			{Name: "Fact1", Value: "Value1"},
			{Name: "Fact2", Value: "Value2"},
		},
	}
	
	data, err := json.Marshal(payload)
	assert.NoError(t, err)
	
	var unmarshaledPayload WebhookPayload
	err = json.Unmarshal(data, &unmarshaledPayload)
	assert.NoError(t, err)
	
	assert.Equal(t, payload.Service, unmarshaledPayload.Service)
	assert.Equal(t, payload.Component, unmarshaledPayload.Component)
	assert.Equal(t, payload.Message, unmarshaledPayload.Message)
	assert.Equal(t, payload.Level, unmarshaledPayload.Level)
	// Note: JSON unmarshaling converts int to float64, so we check individual fields
	assert.Equal(t, "test", unmarshaledPayload.Details["string_value"])
	assert.Equal(t, float64(42), unmarshaledPayload.Details["int_value"]) // JSON converts int to float64
	assert.Equal(t, 3.14, unmarshaledPayload.Details["float_value"])
	assert.Equal(t, true, unmarshaledPayload.Details["bool_value"])
	assert.Equal(t, len(payload.Attachments), len(unmarshaledPayload.Attachments))
	assert.Equal(t, len(payload.Embeds), len(unmarshaledPayload.Embeds))
	assert.Equal(t, len(payload.Sections), len(unmarshaledPayload.Sections))
	assert.Equal(t, len(payload.Facts), len(unmarshaledPayload.Facts))
}

// Benchmark tests
func BenchmarkWebhookAlertHandler_FormatPayload(b *testing.B) {
	handler := NewSlackWebhookHandler("bench", "https://example.com", slog.Default())
	
	alert := &Alert{
		ID:          "bench-alert",
		Level:       AlertLevelWarning,
		Service:     "bench-service",
		Component:   "bench-component",
		Message:     "Benchmark message",
		Timestamp:   time.Now(),
		FirstSeen:   time.Now(),
		LastSeen:    time.Now(),
		Occurrences: 5,
		Details: map[string]interface{}{
			"metric1": 42.5,
			"metric2": "value",
			"metric3": true,
		},
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.formatPayload(alert)
	}
}

func BenchmarkWebhookAlertHandler_GenerateSignature(b *testing.B) {
	handler := NewWebhookAlertHandler("bench", "https://example.com", slog.Default()).
		WithSecret("benchmark-secret")
	
	payload := []byte(`{"service":"test","component":"test","message":"benchmark message","level":"warning"}`)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.generateSignature(payload)
	}
}