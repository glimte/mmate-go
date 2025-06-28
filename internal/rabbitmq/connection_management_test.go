package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockConnectionStateListener tracks connection state changes
type MockConnectionStateListener struct {
	mu                  sync.Mutex
	connectedCount      int
	disconnectedCount   int
	reconnectingCount   int
	lastDisconnectError error
	lastReconnectAttempt int
	events              []string
}

func (m *MockConnectionStateListener) OnConnected() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connectedCount++
	m.events = append(m.events, "connected")
}

func (m *MockConnectionStateListener) OnDisconnected(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.disconnectedCount++
	m.lastDisconnectError = err
	m.events = append(m.events, "disconnected")
}

func (m *MockConnectionStateListener) OnReconnecting(attempt int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconnectingCount++
	m.lastReconnectAttempt = attempt
	m.events = append(m.events, "reconnecting")
}

func (m *MockConnectionStateListener) GetStats() (connected, disconnected, reconnecting int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.connectedCount, m.disconnectedCount, m.reconnectingCount
}

func (m *MockConnectionStateListener) GetEvents() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	events := make([]string, len(m.events))
	copy(events, m.events)
	return events
}

func TestConnectionManagerStateListeners(t *testing.T) {
	t.Run("AddStateListener adds listener", func(t *testing.T) {
		cm := NewConnectionManager("amqp://localhost:5672")
		listener := &MockConnectionStateListener{}
		
		cm.AddStateListener(listener)
		
		cm.listenersMu.RLock()
		assert.Len(t, cm.stateListeners, 1)
		cm.listenersMu.RUnlock()
	})
	
	t.Run("RemoveStateListener removes listener", func(t *testing.T) {
		cm := NewConnectionManager("amqp://localhost:5672")
		listener1 := &MockConnectionStateListener{}
		listener2 := &MockConnectionStateListener{}
		
		cm.AddStateListener(listener1)
		cm.AddStateListener(listener2)
		
		cm.RemoveStateListener(listener1)
		
		cm.listenersMu.RLock()
		assert.Len(t, cm.stateListeners, 1)
		assert.Equal(t, listener2, cm.stateListeners[0])
		cm.listenersMu.RUnlock()
	})
	
	t.Run("notifyConnected calls all listeners", func(t *testing.T) {
		cm := NewConnectionManager("amqp://localhost:5672")
		listener1 := &MockConnectionStateListener{}
		listener2 := &MockConnectionStateListener{}
		
		cm.AddStateListener(listener1)
		cm.AddStateListener(listener2)
		
		cm.notifyConnected()
		
		// Allow goroutines to complete
		time.Sleep(10 * time.Millisecond)
		
		connected1, _, _ := listener1.GetStats()
		connected2, _, _ := listener2.GetStats()
		
		assert.Equal(t, 1, connected1)
		assert.Equal(t, 1, connected2)
	})
}

func TestConnectionManagerReconnectLogic(t *testing.T) {
	t.Run("calculateBackoff increases exponentially", func(t *testing.T) {
		cm := NewConnectionManager("amqp://localhost:5672",
			WithReconnectDelay(1*time.Second))
		
		// Test exponential backoff
		delays := []time.Duration{
			cm.calculateBackoff(0),
			cm.calculateBackoff(1),
			cm.calculateBackoff(2),
			cm.calculateBackoff(3),
			cm.calculateBackoff(4),
		}
		
		// First attempt should have base delay (with jitter)
		assert.GreaterOrEqual(t, delays[0], 750*time.Millisecond) // 75% of base due to jitter
		assert.LessOrEqual(t, delays[0], 2*time.Second)
		
		// Subsequent attempts should increase
		for i := 1; i < len(delays); i++ {
			assert.Greater(t, delays[i], delays[i-1])
		}
		
		// Max backoff should be capped (with some margin for jitter)
		maxDelay := cm.calculateBackoff(10)
		assert.LessOrEqual(t, maxDelay, 6*time.Minute)
	})
	
	t.Run("WithMaxRetries limits retries", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid:5672",
			WithReconnectDelay(100*time.Millisecond),
			WithMaxRetries(3))
		
		listener := &MockConnectionStateListener{}
		cm.AddStateListener(listener)
		
		// This should fail and trigger reconnection
		err := cm.Connect(context.Background())
		assert.Error(t, err)
		
		// Manually trigger reconnect logic
		cm.mu.Lock()
		cm.isConnected = false
		cm.mu.Unlock()
		
		// Start reconnection in background
		done := make(chan bool)
		go func() {
			cm.reconnect()
			done <- true
		}()
		
		// Wait for reconnection attempts to complete
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("Reconnection did not complete in time")
		}
		
		// Should have attempted 3 times
		_, _, reconnecting := listener.GetStats()
		assert.Equal(t, 3, reconnecting)
	})
}

func TestConnectionManagerConcurrency(t *testing.T) {
	t.Run("Concurrent GetConnection calls", func(t *testing.T) {
		cm := NewConnectionManager("amqp://localhost:5672")
		
		// Simulate connected state without actual connection
		cm.mu.Lock()
		cm.isConnected = true
		cm.mu.Unlock()
		
		var wg sync.WaitGroup
		errors := make([]error, 100)
		
		// 100 concurrent GetConnection calls
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				_, err := cm.GetConnection()
				errors[idx] = err
			}(i)
		}
		
		wg.Wait()
		
		// All should return the same error (no actual connection)
		for _, err := range errors {
			assert.Error(t, err)
		}
	})
	
	t.Run("Concurrent state changes", func(t *testing.T) {
		cm := NewConnectionManager("amqp://localhost:5672")
		listener := &MockConnectionStateListener{}
		cm.AddStateListener(listener)
		
		var wg sync.WaitGroup
		
		// Simulate rapid connection state changes
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				cm.notifyConnected()
				cm.notifyDisconnected(errors.New("test error"))
				cm.notifyReconnecting(1)
			}()
		}
		
		wg.Wait()
		
		// Wait for all events to be processed
		time.Sleep(50 * time.Millisecond)
		
		connected, disconnected, reconnecting := listener.GetStats()
		assert.Equal(t, 10, connected)
		assert.Equal(t, 10, disconnected)
		assert.Equal(t, 10, reconnecting)
	})
}



func TestConnectionManagerErrorHandling(t *testing.T) {
	t.Run("Connection error includes context", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid:5672")
		
		err := cm.Connect(context.Background())
		require.Error(t, err)
		
		var connErr *ConnectionError
		if assert.ErrorAs(t, err, &connErr) {
			assert.Equal(t, "connect", connErr.Op)
			assert.NotEmpty(t, connErr.URL)
			assert.NotZero(t, connErr.Timestamp)
			assert.Equal(t, 1, connErr.Attempts)
		}
	})
	
	t.Run("Connection with bad URL fails", func(t *testing.T) {
		cm := NewConnectionManager("amqp://nonexistent-host:5672")
		
		err := cm.Connect(context.Background())
		require.Error(t, err)
	})
}

func TestConnectionManagerGracefulShutdown(t *testing.T) {
	t.Run("Close stops reconnection attempts", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid:5672",
			WithReconnectDelay(50*time.Millisecond),
			WithMaxRetries(10))
		
		listener := &MockConnectionStateListener{}
		cm.AddStateListener(listener)
		
		// Start reconnection in background
		cm.mu.Lock()
		cm.isConnected = false
		cm.mu.Unlock()
		
		go cm.reconnect()
		
		// Let it attempt a few times
		time.Sleep(150 * time.Millisecond)
		
		// Close should stop reconnection
		err := cm.Close()
		assert.NoError(t, err)
		
		// Record current reconnection count
		_, _, reconnectingBefore := listener.GetStats()
		
		// Wait a bit more
		time.Sleep(200 * time.Millisecond)
		
		// No significant new reconnection attempts should happen (allowing for 1 more due to timing)
		_, _, reconnectingAfter := listener.GetStats()
		assert.LessOrEqual(t, reconnectingAfter-reconnectingBefore, 1)
	})
	
	t.Run("Multiple Close calls are safe", func(t *testing.T) {
		cm := NewConnectionManager("amqp://localhost:5672")
		
		// Multiple close calls should not panic
		assert.NoError(t, cm.Close())
		assert.NoError(t, cm.Close())
		assert.NoError(t, cm.Close())
	})
}

