package journal

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryJournal(t *testing.T) {
	t.Run("creates with default options", func(t *testing.T) {
		journal := NewInMemoryJournal()
		assert.NotNil(t, journal)
		assert.Equal(t, 10000, journal.maxEntries)
		assert.Equal(t, 0.2, journal.rotatePercent)
	})
	
	t.Run("applies options", func(t *testing.T) {
		journal := NewInMemoryJournal(
			WithMaxEntries(5000),
			WithRotatePercent(0.3),
		)
		assert.Equal(t, 5000, journal.maxEntries)
		assert.Equal(t, 0.3, journal.rotatePercent)
	})
}

func TestInMemoryJournal_Record(t *testing.T) {
	ctx := context.Background()
	
	t.Run("records entry with auto-generated fields", func(t *testing.T) {
		journal := NewInMemoryJournal()
		
		entry := &MutationEntry{
			MessageID:    "msg-123",
			MutationType: MutationTransform,
			Component:    "transformer",
		}
		
		err := journal.Record(ctx, entry)
		require.NoError(t, err)
		
		// Check auto-generated fields
		assert.NotEmpty(t, entry.ID)
		assert.False(t, entry.Timestamp.IsZero())
		
		// Verify it's stored
		entries, err := journal.GetByMessageID(ctx, "msg-123")
		require.NoError(t, err)
		assert.Len(t, entries, 1)
	})
	
	t.Run("rejects nil entry", func(t *testing.T) {
		journal := NewInMemoryJournal()
		
		err := journal.Record(ctx, nil)
		assert.Error(t, err)
	})
	
	t.Run("handles rotation when max entries reached", func(t *testing.T) {
		journal := NewInMemoryJournal(
			WithMaxEntries(10),
			WithRotatePercent(0.3),
		)
		
		// Add 11 entries to trigger rotation
		for i := 0; i < 11; i++ {
			entry := &MutationEntry{
				MessageID:    "msg-" + string(rune(i)),
				MutationType: MutationCreate,
				Component:    "test",
			}
			err := journal.Record(ctx, entry)
			require.NoError(t, err)
		}
		
		// Should have 8 entries after rotation (10 - 3 = 7, plus 1 new)
		assert.Equal(t, 8, len(journal.entries))
	})
}

func TestInMemoryJournal_RecordMutation(t *testing.T) {
	ctx := context.Background()
	
	t.Run("records mutation with before/after states", func(t *testing.T) {
		journal := NewInMemoryJournal()
		
		before := map[string]string{"status": "pending"}
		after := map[string]string{"status": "processed"}
		
		err := journal.RecordMutation(ctx, "msg-123", MutationTransform, "processor", before, after)
		require.NoError(t, err)
		
		entries, err := journal.GetByMessageID(ctx, "msg-123")
		require.NoError(t, err)
		require.Len(t, entries, 1)
		
		entry := entries[0]
		assert.Equal(t, MutationTransform, entry.MutationType)
		assert.Equal(t, "processor", entry.Component)
		
		// Check serialized states
		var beforeState, afterState map[string]string
		err = json.Unmarshal(entry.BeforeState, &beforeState)
		require.NoError(t, err)
		assert.Equal(t, "pending", beforeState["status"])
		
		err = json.Unmarshal(entry.AfterState, &afterState)
		require.NoError(t, err)
		assert.Equal(t, "processed", afterState["status"])
	})
	
	t.Run("handles nil states", func(t *testing.T) {
		journal := NewInMemoryJournal()
		
		err := journal.RecordMutation(ctx, "msg-456", MutationCreate, "creator", nil, map[string]string{"new": "data"})
		require.NoError(t, err)
		
		entries, err := journal.GetByMessageID(ctx, "msg-456")
		require.NoError(t, err)
		require.Len(t, entries, 1)
		
		assert.Nil(t, entries[0].BeforeState)
		assert.NotNil(t, entries[0].AfterState)
	})
}

func TestInMemoryJournal_RecordError(t *testing.T) {
	ctx := context.Background()
	
	t.Run("records error mutation", func(t *testing.T) {
		journal := NewInMemoryJournal()
		
		err := journal.RecordError(ctx, "msg-789", "validator", assert.AnError)
		require.NoError(t, err)
		
		entries, err := journal.GetByMessageID(ctx, "msg-789")
		require.NoError(t, err)
		require.Len(t, entries, 1)
		
		entry := entries[0]
		assert.Equal(t, MutationError, entry.MutationType)
		assert.Equal(t, "validator", entry.Component)
		assert.Equal(t, assert.AnError.Error(), entry.Error)
	})
	
	t.Run("ignores nil error", func(t *testing.T) {
		journal := NewInMemoryJournal()
		
		err := journal.RecordError(ctx, "msg-000", "handler", nil)
		assert.NoError(t, err)
		
		entries, err := journal.GetByMessageID(ctx, "msg-000")
		require.NoError(t, err)
		assert.Empty(t, entries)
	})
}

func TestInMemoryJournal_GetByTimeRange(t *testing.T) {
	ctx := context.Background()
	journal := NewInMemoryJournal()
	
	now := time.Now()
	
	// Add entries at different times
	entries := []*MutationEntry{
		{
			MessageID:    "msg-1",
			Timestamp:    now.Add(-2 * time.Hour),
			MutationType: MutationCreate,
		},
		{
			MessageID:    "msg-2",
			Timestamp:    now.Add(-1 * time.Hour),
			MutationType: MutationTransform,
		},
		{
			MessageID:    "msg-3",
			Timestamp:    now.Add(-30 * time.Minute),
			MutationType: MutationValidate,
		},
	}
	
	for _, entry := range entries {
		err := journal.Record(ctx, entry)
		require.NoError(t, err)
	}
	
	// Query for last hour
	result, err := journal.GetByTimeRange(ctx, now.Add(-90*time.Minute), now)
	require.NoError(t, err)
	assert.Len(t, result, 2) // msg-2 and msg-3
}

func TestInMemoryJournal_GetByComponent(t *testing.T) {
	ctx := context.Background()
	journal := NewInMemoryJournal()
	
	// Add entries for different components
	components := []string{"processor", "validator", "processor", "router", "processor"}
	for i, comp := range components {
		entry := &MutationEntry{
			MessageID:    fmt.Sprintf("msg-%d", i),
			Component:    comp,
			MutationType: MutationTransform,
		}
		err := journal.Record(ctx, entry)
		require.NoError(t, err)
	}
	
	t.Run("gets all entries for component", func(t *testing.T) {
		entries, err := journal.GetByComponent(ctx, "processor", 0)
		require.NoError(t, err)
		assert.Len(t, entries, 3)
	})
	
	t.Run("respects limit", func(t *testing.T) {
		entries, err := journal.GetByComponent(ctx, "processor", 2)
		require.NoError(t, err)
		assert.Len(t, entries, 2)
	})
	
	t.Run("returns empty for unknown component", func(t *testing.T) {
		entries, err := journal.GetByComponent(ctx, "unknown", 0)
		require.NoError(t, err)
		assert.Empty(t, entries)
	})
}

func TestInMemoryJournal_GetStats(t *testing.T) {
	ctx := context.Background()
	journal := NewInMemoryJournal()
	
	// Add various entries
	testData := []struct {
		mutationType MutationType
		component    string
		hasError     bool
	}{
		{MutationCreate, "creator", false},
		{MutationTransform, "transformer", false},
		{MutationValidate, "validator", true},
		{MutationTransform, "transformer", false},
		{MutationError, "handler", true},
	}
	
	for i, data := range testData {
		entry := &MutationEntry{
			MessageID:    fmt.Sprintf("msg-%d", i),
			MutationType: data.mutationType,
			Component:    data.component,
			Duration:     time.Duration(i+1) * time.Millisecond,
		}
		if data.hasError {
			entry.Error = "test error"
		}
		err := journal.Record(ctx, entry)
		require.NoError(t, err)
	}
	
	stats, err := journal.GetStats(ctx)
	require.NoError(t, err)
	
	assert.Equal(t, int64(5), stats.TotalEntries)
	assert.Equal(t, int64(2), stats.ErrorCount)
	assert.Equal(t, int64(1), stats.EntriesByType[MutationCreate])
	assert.Equal(t, int64(2), stats.EntriesByType[MutationTransform])
	assert.Equal(t, int64(2), stats.EntriesByComponent["transformer"])
	assert.NotZero(t, stats.AverageDuration)
	assert.False(t, stats.LastEntry.IsZero())
}

func TestInMemoryJournal_Clear(t *testing.T) {
	ctx := context.Background()
	journal := NewInMemoryJournal()
	
	now := time.Now()
	
	// Add entries at different times
	for i := 0; i < 5; i++ {
		entry := &MutationEntry{
			MessageID:    fmt.Sprintf("msg-%d", i),
			Timestamp:    now.Add(time.Duration(-i) * time.Hour),
			MutationType: MutationCreate,
			Component:    "test",
		}
		err := journal.Record(ctx, entry)
		require.NoError(t, err)
	}
	
	// Clear entries older than 2 hours
	removed, err := journal.Clear(ctx, 2*time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 3, removed) // Entries 2, 3, 4 hours old
	
	// Verify remaining entries
	assert.Equal(t, 2, len(journal.entries))
	
	// Verify indexes are rebuilt correctly
	entries, err := journal.GetByComponent(ctx, "test", 0)
	require.NoError(t, err)
	assert.Len(t, entries, 2)
}

func TestMutationType_String(t *testing.T) {
	tests := []struct {
		mutationType MutationType
		expected     string
	}{
		{MutationCreate, "create"},
		{MutationTransform, "transform"},
		{MutationEnrich, "enrich"},
		{MutationFilter, "filter"},
		{MutationRoute, "route"},
		{MutationSerialize, "serialize"},
		{MutationDeserialize, "deserialize"},
		{MutationValidate, "validate"},
		{MutationError, "error"},
	}
	
	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, string(tt.mutationType))
		})
	}
}