package journal

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// EntityMutationType represents entity-level mutations for sync
type EntityMutationType string

const (
	EntityCreate EntityMutationType = "entity.create"
	EntityUpdate EntityMutationType = "entity.update"
	EntityDelete EntityMutationType = "entity.delete"
	EntityPatch  EntityMutationType = "entity.patch"
	EntityMerge  EntityMutationType = "entity.merge"
)

// SyncStatus represents the synchronization status of a mutation
type SyncStatus string

const (
	SyncStatusPending   SyncStatus = "pending"
	SyncStatusSynced    SyncStatus = "synced"
	SyncStatusFailed    SyncStatus = "failed"
	SyncStatusConflict  SyncStatus = "conflict"
	SyncStatusIgnored   SyncStatus = "ignored"
)

// EntityMutationRecord represents an entity-level mutation for synchronization
type EntityMutationRecord struct {
	ID             string                 `json:"id"`
	ServiceID      string                 `json:"serviceId"`
	EntityType     string                 `json:"entityType"`
	EntityID       string                 `json:"entityId"`
	EntityVersion  int64                  `json:"entityVersion"`
	MutationType   EntityMutationType     `json:"mutationType"`
	Timestamp      time.Time              `json:"timestamp"`
	CorrelationID  string                 `json:"correlationId"`
	CausationID    string                 `json:"causationId,omitempty"`
	Payload        json.RawMessage        `json:"payload"`
	BeforeState    json.RawMessage        `json:"beforeState,omitempty"`
	AfterState     json.RawMessage        `json:"afterState,omitempty"`
	SyncStatus     SyncStatus             `json:"syncStatus"`
	SyncedAt       *time.Time             `json:"syncedAt,omitempty"`
	SyncError      string                 `json:"syncError,omitempty"`
	ConflictsWith  []string               `json:"conflictsWith,omitempty"`
	Metadata       map[string]interface{} `json:"metadata,omitempty"`
}

// MutationQuery represents a query for mutations
type MutationQuery struct {
	EntityType    string     `json:"entityType,omitempty"`
	EntityID      string     `json:"entityId,omitempty"`
	ServiceID     string     `json:"serviceId,omitempty"`
	CorrelationID string     `json:"correlationId,omitempty"`
	SyncStatus    SyncStatus `json:"syncStatus,omitempty"`
	FromTime      *time.Time `json:"fromTime,omitempty"`
	ToTime        *time.Time `json:"toTime,omitempty"`
	Limit         int        `json:"limit,omitempty"`
	Offset        int        `json:"offset,omitempty"`
}

// SyncMutationJournal extends the basic mutation journal with sync capabilities
type SyncMutationJournal interface {
	MutationJournal

	// RecordEntityMutation records an entity-level mutation
	RecordEntityMutation(ctx context.Context, record *EntityMutationRecord) error

	// GetEntityMutations retrieves mutations for a specific entity
	GetEntityMutations(ctx context.Context, entityType, entityID string) ([]*EntityMutationRecord, error)

	// GetCorrelatedMutations retrieves mutations with the same correlation ID
	GetCorrelatedMutations(ctx context.Context, correlationID string) ([]*EntityMutationRecord, error)

	// GetUnsyncedMutations retrieves mutations that haven't been synced
	GetUnsyncedMutations(ctx context.Context, limit int) ([]*EntityMutationRecord, error)

	// MarkAsSynced marks mutations as successfully synced
	MarkAsSynced(ctx context.Context, mutationIDs []string, syncedAt time.Time) error

	// MarkAsFailed marks mutations as sync failed
	MarkAsFailed(ctx context.Context, mutationIDs []string, syncError string) error

	// QueryMutations retrieves mutations based on query criteria
	QueryMutations(ctx context.Context, query *MutationQuery) ([]*EntityMutationRecord, error)

	// GetConflicts retrieves mutations that have sync conflicts
	GetConflicts(ctx context.Context) ([]*EntityMutationRecord, error)

	// ResolveConflict resolves a sync conflict
	ResolveConflict(ctx context.Context, mutationID string, resolution SyncStatus) error
}

// InMemorySyncMutationJournal provides an in-memory implementation with sync capabilities
type InMemorySyncMutationJournal struct {
	*InMemoryJournal
	entityMutations  []*EntityMutationRecord
	byEntityID       map[string][]*EntityMutationRecord
	byCorrelationID  map[string][]*EntityMutationRecord
	bySyncStatus     map[SyncStatus][]*EntityMutationRecord
	syncMu           sync.RWMutex
	serviceID        string
}

// SyncJournalOption configures the sync mutation journal
type SyncJournalOption func(*InMemorySyncMutationJournal)

// WithServiceID sets the service ID for the journal
func WithServiceID(serviceID string) SyncJournalOption {
	return func(j *InMemorySyncMutationJournal) {
		j.serviceID = serviceID
	}
}

// NewInMemorySyncMutationJournal creates a new in-memory sync mutation journal
func NewInMemorySyncMutationJournal(baseOpts []InMemoryJournalOption, syncOpts ...SyncJournalOption) *InMemorySyncMutationJournal {
	baseJournal := NewInMemoryJournal(baseOpts...)
	
	j := &InMemorySyncMutationJournal{
		InMemoryJournal:  baseJournal,
		entityMutations:  make([]*EntityMutationRecord, 0),
		byEntityID:       make(map[string][]*EntityMutationRecord),
		byCorrelationID:  make(map[string][]*EntityMutationRecord),
		bySyncStatus:     make(map[SyncStatus][]*EntityMutationRecord),
		serviceID:        "unknown-service",
	}

	for _, opt := range syncOpts {
		opt(j)
	}

	return j
}

// RecordEntityMutation records an entity-level mutation
func (j *InMemorySyncMutationJournal) RecordEntityMutation(ctx context.Context, record *EntityMutationRecord) error {
	if record == nil {
		return fmt.Errorf("record cannot be nil")
	}

	if record.ID == "" {
		record.ID = uuid.New().String()
	}

	if record.ServiceID == "" {
		record.ServiceID = j.serviceID
	}

	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now()
	}

	if record.SyncStatus == "" {
		record.SyncStatus = SyncStatusPending
	}

	j.syncMu.Lock()
	defer j.syncMu.Unlock()

	// Add to main list
	j.entityMutations = append(j.entityMutations, record)

	// Update indexes
	entityKey := fmt.Sprintf("%s:%s", record.EntityType, record.EntityID)
	j.byEntityID[entityKey] = append(j.byEntityID[entityKey], record)

	if record.CorrelationID != "" {
		j.byCorrelationID[record.CorrelationID] = append(j.byCorrelationID[record.CorrelationID], record)
	}

	j.bySyncStatus[record.SyncStatus] = append(j.bySyncStatus[record.SyncStatus], record)

	// Also record in base journal for general mutation tracking
	entry := &MutationEntry{
		ID:            record.ID,
		Timestamp:     record.Timestamp,
		MessageID:     record.CorrelationID,
		MessageType:   string(record.MutationType),
		MutationType:  MutationType(record.MutationType),
		Component:     "entity-sync",
		Operation:     string(record.MutationType),
		BeforeState:   record.BeforeState,
		AfterState:    record.AfterState,
		CorrelationID: record.CorrelationID,
		Metadata: map[string]interface{}{
			"entityType":    record.EntityType,
			"entityId":      record.EntityID,
			"entityVersion": record.EntityVersion,
			"syncStatus":    record.SyncStatus,
		},
	}

	return j.InMemoryJournal.Record(ctx, entry)
}

// GetEntityMutations retrieves mutations for a specific entity
func (j *InMemorySyncMutationJournal) GetEntityMutations(ctx context.Context, entityType, entityID string) ([]*EntityMutationRecord, error) {
	j.syncMu.RLock()
	defer j.syncMu.RUnlock()

	entityKey := fmt.Sprintf("%s:%s", entityType, entityID)
	mutations, exists := j.byEntityID[entityKey]
	if !exists {
		return []*EntityMutationRecord{}, nil
	}

	// Return copies to prevent external modifications
	result := make([]*EntityMutationRecord, len(mutations))
	for i, mutation := range mutations {
		mutationCopy := *mutation
		result[i] = &mutationCopy
	}

	return result, nil
}

// GetCorrelatedMutations retrieves mutations with the same correlation ID
func (j *InMemorySyncMutationJournal) GetCorrelatedMutations(ctx context.Context, correlationID string) ([]*EntityMutationRecord, error) {
	j.syncMu.RLock()
	defer j.syncMu.RUnlock()

	mutations, exists := j.byCorrelationID[correlationID]
	if !exists {
		return []*EntityMutationRecord{}, nil
	}

	// Return copies
	result := make([]*EntityMutationRecord, len(mutations))
	for i, mutation := range mutations {
		mutationCopy := *mutation
		result[i] = &mutationCopy
	}

	return result, nil
}

// GetUnsyncedMutations retrieves mutations that haven't been synced
func (j *InMemorySyncMutationJournal) GetUnsyncedMutations(ctx context.Context, limit int) ([]*EntityMutationRecord, error) {
	j.syncMu.RLock()
	defer j.syncMu.RUnlock()

	pending := j.bySyncStatus[SyncStatusPending]
	failed := j.bySyncStatus[SyncStatusFailed]

	// Combine pending and failed mutations
	unsynced := make([]*EntityMutationRecord, 0, len(pending)+len(failed))
	unsynced = append(unsynced, pending...)
	unsynced = append(unsynced, failed...)

	// Apply limit
	if limit > 0 && len(unsynced) > limit {
		unsynced = unsynced[:limit]
	}

	// Return copies
	result := make([]*EntityMutationRecord, len(unsynced))
	for i, mutation := range unsynced {
		mutationCopy := *mutation
		result[i] = &mutationCopy
	}

	return result, nil
}

// MarkAsSynced marks mutations as successfully synced
func (j *InMemorySyncMutationJournal) MarkAsSynced(ctx context.Context, mutationIDs []string, syncedAt time.Time) error {
	j.syncMu.Lock()
	defer j.syncMu.Unlock()

	for _, mutationID := range mutationIDs {
		if err := j.updateSyncStatus(mutationID, SyncStatusSynced, "", &syncedAt); err != nil {
			return fmt.Errorf("failed to mark mutation %s as synced: %w", mutationID, err)
		}
	}

	return nil
}

// MarkAsFailed marks mutations as sync failed
func (j *InMemorySyncMutationJournal) MarkAsFailed(ctx context.Context, mutationIDs []string, syncError string) error {
	j.syncMu.Lock()
	defer j.syncMu.Unlock()

	for _, mutationID := range mutationIDs {
		if err := j.updateSyncStatus(mutationID, SyncStatusFailed, syncError, nil); err != nil {
			return fmt.Errorf("failed to mark mutation %s as failed: %w", mutationID, err)
		}
	}

	return nil
}

// updateSyncStatus updates the sync status of a mutation (must be called with lock held)
func (j *InMemorySyncMutationJournal) updateSyncStatus(mutationID string, status SyncStatus, syncError string, syncedAt *time.Time) error {
	// Find mutation
	var targetMutation *EntityMutationRecord
	for _, mutation := range j.entityMutations {
		if mutation.ID == mutationID {
			targetMutation = mutation
			break
		}
	}

	if targetMutation == nil {
		return fmt.Errorf("mutation not found: %s", mutationID)
	}

	// Remove from old status index
	j.removeFromSyncStatusIndex(targetMutation)

	// Update status
	targetMutation.SyncStatus = status
	targetMutation.SyncError = syncError
	targetMutation.SyncedAt = syncedAt

	// Add to new status index
	j.bySyncStatus[status] = append(j.bySyncStatus[status], targetMutation)

	return nil
}

// removeFromSyncStatusIndex removes a mutation from its current status index
func (j *InMemorySyncMutationJournal) removeFromSyncStatusIndex(mutation *EntityMutationRecord) {
	statusMutations := j.bySyncStatus[mutation.SyncStatus]
	for i, m := range statusMutations {
		if m.ID == mutation.ID {
			// Remove from slice
			j.bySyncStatus[mutation.SyncStatus] = append(statusMutations[:i], statusMutations[i+1:]...)
			break
		}
	}
}

// QueryMutations retrieves mutations based on query criteria
func (j *InMemorySyncMutationJournal) QueryMutations(ctx context.Context, query *MutationQuery) ([]*EntityMutationRecord, error) {
	j.syncMu.RLock()
	defer j.syncMu.RUnlock()

	var result []*EntityMutationRecord

	// Start with all mutations and filter
	for _, mutation := range j.entityMutations {
		if j.matchesQuery(mutation, query) {
			mutationCopy := *mutation
			result = append(result, &mutationCopy)
		}
	}

	// Apply offset and limit
	if query.Offset > 0 && query.Offset < len(result) {
		result = result[query.Offset:]
	}

	if query.Limit > 0 && len(result) > query.Limit {
		result = result[:query.Limit]
	}

	return result, nil
}

// matchesQuery checks if a mutation matches the query criteria
func (j *InMemorySyncMutationJournal) matchesQuery(mutation *EntityMutationRecord, query *MutationQuery) bool {
	if query.EntityType != "" && mutation.EntityType != query.EntityType {
		return false
	}

	if query.EntityID != "" && mutation.EntityID != query.EntityID {
		return false
	}

	if query.ServiceID != "" && mutation.ServiceID != query.ServiceID {
		return false
	}

	if query.CorrelationID != "" && mutation.CorrelationID != query.CorrelationID {
		return false
	}

	if query.SyncStatus != "" && mutation.SyncStatus != query.SyncStatus {
		return false
	}

	if query.FromTime != nil && mutation.Timestamp.Before(*query.FromTime) {
		return false
	}

	if query.ToTime != nil && mutation.Timestamp.After(*query.ToTime) {
		return false
	}

	return true
}

// GetConflicts retrieves mutations that have sync conflicts
func (j *InMemorySyncMutationJournal) GetConflicts(ctx context.Context) ([]*EntityMutationRecord, error) {
	j.syncMu.RLock()
	defer j.syncMu.RUnlock()

	conflicts := j.bySyncStatus[SyncStatusConflict]

	// Return copies
	result := make([]*EntityMutationRecord, len(conflicts))
	for i, mutation := range conflicts {
		mutationCopy := *mutation
		result[i] = &mutationCopy
	}

	return result, nil
}

// ResolveConflict resolves a sync conflict
func (j *InMemorySyncMutationJournal) ResolveConflict(ctx context.Context, mutationID string, resolution SyncStatus) error {
	j.syncMu.Lock()
	defer j.syncMu.Unlock()

	now := time.Now()
	return j.updateSyncStatus(mutationID, resolution, "", &now)
}