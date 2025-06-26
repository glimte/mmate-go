package journal

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/google/uuid"
)

// MutationType represents the type of mutation
type MutationType string

const (
	MutationCreate      MutationType = "create"
	MutationTransform   MutationType = "transform"
	MutationEnrich      MutationType = "enrich"
	MutationFilter      MutationType = "filter"
	MutationRoute       MutationType = "route"
	MutationSerialize   MutationType = "serialize"
	MutationDeserialize MutationType = "deserialize"
	MutationValidate    MutationType = "validate"
	MutationError       MutationType = "error"
)

// MutationEntry represents a single mutation in the journal
type MutationEntry struct {
	ID            string                 `json:"id"`
	Timestamp     time.Time              `json:"timestamp"`
	MessageID     string                 `json:"messageId"`
	MessageType   string                 `json:"messageType"`
	MutationType  MutationType           `json:"mutationType"`
	Component     string                 `json:"component"`
	Operation     string                 `json:"operation"`
	BeforeState   json.RawMessage        `json:"beforeState,omitempty"`
	AfterState    json.RawMessage        `json:"afterState,omitempty"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
	Duration      time.Duration          `json:"duration"`
	Error         string                 `json:"error,omitempty"`
	CorrelationID string                 `json:"correlationId,omitempty"`
	TraceID       string                 `json:"traceId,omitempty"`
}

// MutationJournal tracks message mutations through the system
type MutationJournal interface {
	// Record records a mutation entry
	Record(ctx context.Context, entry *MutationEntry) error

	// RecordMutation records a message mutation with before/after states
	RecordMutation(ctx context.Context, messageID string, mutation MutationType, component string, before, after interface{}) error

	// RecordError records an error mutation
	RecordError(ctx context.Context, messageID string, component string, err error) error

	// GetByMessageID retrieves all mutations for a message
	GetByMessageID(ctx context.Context, messageID string) ([]*MutationEntry, error)

	// GetByTimeRange retrieves mutations within a time range
	GetByTimeRange(ctx context.Context, start, end time.Time) ([]*MutationEntry, error)

	// GetByComponent retrieves mutations for a specific component
	GetByComponent(ctx context.Context, component string, limit int) ([]*MutationEntry, error)

	// GetStats returns journal statistics
	GetStats(ctx context.Context) (*JournalStats, error)

	// Clear removes entries older than the specified duration
	Clear(ctx context.Context, olderThan time.Duration) (int, error)
}

// JournalStats represents journal statistics
type JournalStats struct {
	TotalEntries       int64                  `json:"totalEntries"`
	EntriesByType      map[MutationType]int64 `json:"entriesByType"`
	EntriesByComponent map[string]int64       `json:"entriesByComponent"`
	ErrorCount         int64                  `json:"errorCount"`
	AverageDuration    time.Duration          `json:"averageDuration"`
	LastEntry          time.Time              `json:"lastEntry"`
}

// InMemoryJournal provides an in-memory implementation of MutationJournal
type InMemoryJournal struct {
	entries       []*MutationEntry
	byMessageID   map[string][]*MutationEntry
	byComponent   map[string][]*MutationEntry
	mu            sync.RWMutex
	maxEntries    int
	rotatePercent float64
}

// InMemoryJournalOption configures the in-memory journal
type InMemoryJournalOption func(*InMemoryJournal)

// WithMaxEntries sets the maximum number of entries
func WithMaxEntries(max int) InMemoryJournalOption {
	return func(j *InMemoryJournal) {
		j.maxEntries = max
	}
}

// WithRotatePercent sets the percentage of entries to remove when max is reached
func WithRotatePercent(percent float64) InMemoryJournalOption {
	return func(j *InMemoryJournal) {
		j.rotatePercent = percent
	}
}

// NewInMemoryJournal creates a new in-memory journal
func NewInMemoryJournal(opts ...InMemoryJournalOption) *InMemoryJournal {
	j := &InMemoryJournal{
		entries:       make([]*MutationEntry, 0),
		byMessageID:   make(map[string][]*MutationEntry),
		byComponent:   make(map[string][]*MutationEntry),
		maxEntries:    10000,
		rotatePercent: 0.2,
	}

	for _, opt := range opts {
		opt(j)
	}

	return j
}

// Record records a mutation entry
func (j *InMemoryJournal) Record(ctx context.Context, entry *MutationEntry) error {
	if entry == nil {
		return fmt.Errorf("entry cannot be nil")
	}

	if entry.ID == "" {
		entry.ID = uuid.New().String()
	}

	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now()
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	// Check if we need to rotate
	if len(j.entries) >= j.maxEntries {
		j.rotate()
	}

	// Add entry
	j.entries = append(j.entries, entry)

	// Update indexes
	if entry.MessageID != "" {
		j.byMessageID[entry.MessageID] = append(j.byMessageID[entry.MessageID], entry)
	}

	if entry.Component != "" {
		j.byComponent[entry.Component] = append(j.byComponent[entry.Component], entry)
	}

	return nil
}

// RecordMutation records a message mutation with before/after states
func (j *InMemoryJournal) RecordMutation(ctx context.Context, messageID string, mutation MutationType, component string, before, after interface{}) error {
	start := time.Now()

	var beforeState, afterState json.RawMessage
	var err error

	if before != nil {
		beforeState, err = json.Marshal(before)
		if err != nil {
			return fmt.Errorf("failed to marshal before state: %w", err)
		}
	}

	if after != nil {
		afterState, err = json.Marshal(after)
		if err != nil {
			return fmt.Errorf("failed to marshal after state: %w", err)
		}
	}

	// Extract metadata from context
	metadata := make(map[string]interface{})
	if traceID := ctx.Value("traceID"); traceID != nil {
		metadata["traceID"] = traceID
	}
	if spanID := ctx.Value("spanID"); spanID != nil {
		metadata["spanID"] = spanID
	}

	entry := &MutationEntry{
		MessageID:    messageID,
		MutationType: mutation,
		Component:    component,
		Operation:    string(mutation),
		BeforeState:  beforeState,
		AfterState:   afterState,
		Metadata:     metadata,
		Duration:     time.Since(start),
	}

	// Extract message type if possible
	if msg, ok := after.(contracts.Message); ok {
		entry.MessageType = msg.GetType()
		entry.CorrelationID = msg.GetCorrelationID()
	}

	return j.Record(ctx, entry)
}

// RecordError records an error mutation
func (j *InMemoryJournal) RecordError(ctx context.Context, messageID string, component string, err error) error {
	if err == nil {
		return nil
	}

	entry := &MutationEntry{
		MessageID:    messageID,
		MutationType: MutationError,
		Component:    component,
		Operation:    "error",
		Error:        err.Error(),
	}

	return j.Record(ctx, entry)
}

// GetByMessageID retrieves all mutations for a message
func (j *InMemoryJournal) GetByMessageID(ctx context.Context, messageID string) ([]*MutationEntry, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	entries, exists := j.byMessageID[messageID]
	if !exists {
		return []*MutationEntry{}, nil
	}

	// Return a copy to prevent external modifications
	result := make([]*MutationEntry, len(entries))
	for i, entry := range entries {
		entryCopy := *entry
		result[i] = &entryCopy
	}

	return result, nil
}

// GetByTimeRange retrieves mutations within a time range
func (j *InMemoryJournal) GetByTimeRange(ctx context.Context, start, end time.Time) ([]*MutationEntry, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	var result []*MutationEntry

	for _, entry := range j.entries {
		if entry.Timestamp.After(start) && entry.Timestamp.Before(end) {
			entryCopy := *entry
			result = append(result, &entryCopy)
		}
	}

	return result, nil
}

// GetByComponent retrieves mutations for a specific component
func (j *InMemoryJournal) GetByComponent(ctx context.Context, component string, limit int) ([]*MutationEntry, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	entries, exists := j.byComponent[component]
	if !exists {
		return []*MutationEntry{}, nil
	}

	// Apply limit
	count := len(entries)
	if limit > 0 && count > limit {
		count = limit
		// Get the most recent entries
		entries = entries[len(entries)-limit:]
	}

	// Return a copy
	result := make([]*MutationEntry, count)
	for i, entry := range entries {
		entryCopy := *entry
		result[i] = &entryCopy
	}

	return result, nil
}

// GetStats returns journal statistics
func (j *InMemoryJournal) GetStats(ctx context.Context) (*JournalStats, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	stats := &JournalStats{
		TotalEntries:       int64(len(j.entries)),
		EntriesByType:      make(map[MutationType]int64),
		EntriesByComponent: make(map[string]int64),
		ErrorCount:         0,
	}

	var totalDuration time.Duration
	var lastEntry time.Time

	for _, entry := range j.entries {
		stats.EntriesByType[entry.MutationType]++
		stats.EntriesByComponent[entry.Component]++

		if entry.Error != "" {
			stats.ErrorCount++
		}

		totalDuration += entry.Duration

		if entry.Timestamp.After(lastEntry) {
			lastEntry = entry.Timestamp
		}
	}

	if len(j.entries) > 0 {
		stats.AverageDuration = totalDuration / time.Duration(len(j.entries))
		stats.LastEntry = lastEntry
	}

	return stats, nil
}

// Clear removes entries older than the specified duration
func (j *InMemoryJournal) Clear(ctx context.Context, olderThan time.Duration) (int, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	cutoff := time.Now().Add(-olderThan)
	removed := 0

	// Filter entries
	newEntries := make([]*MutationEntry, 0)
	for _, entry := range j.entries {
		if entry.Timestamp.After(cutoff) {
			newEntries = append(newEntries, entry)
		} else {
			removed++
		}
	}

	// Rebuild indexes
	j.entries = newEntries
	j.rebuildIndexes()

	return removed, nil
}

// rotate removes oldest entries when max is reached
func (j *InMemoryJournal) rotate() {
	removeCount := int(float64(j.maxEntries) * j.rotatePercent)
	if removeCount < 1 {
		removeCount = 1
	}

	// Remove oldest entries
	j.entries = j.entries[removeCount:]

	// Rebuild indexes
	j.rebuildIndexes()
}

// rebuildIndexes rebuilds the internal indexes
func (j *InMemoryJournal) rebuildIndexes() {
	j.byMessageID = make(map[string][]*MutationEntry)
	j.byComponent = make(map[string][]*MutationEntry)

	for _, entry := range j.entries {
		if entry.MessageID != "" {
			j.byMessageID[entry.MessageID] = append(j.byMessageID[entry.MessageID], entry)
		}

		if entry.Component != "" {
			j.byComponent[entry.Component] = append(j.byComponent[entry.Component], entry)
		}
	}
}
