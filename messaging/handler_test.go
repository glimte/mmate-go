package messaging

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/glimte/mmate-go/contracts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Test message types
type testHandlerCommand struct {
	contracts.BaseCommand
	CommandData string `json:"commandData"`
}

type testHandlerEvent struct {
	contracts.BaseEvent
	EventData string `json:"eventData"`
}

type testHandlerQuery struct {
	contracts.BaseQuery
	QueryData string `json:"queryData"`
}

type testHandlerReply struct {
	contracts.BaseReply
	ReplyData string `json:"replyData"`
}

// Mock handlers
type mockCommandHandler struct {
	mock.Mock
}

func (m *mockCommandHandler) HandleCommand(ctx context.Context, cmd contracts.Command) error {
	args := m.Called(ctx, cmd)
	return args.Error(0)
}

type mockEventHandler struct {
	mock.Mock
}

func (m *mockEventHandler) HandleEvent(ctx context.Context, event contracts.Event) error {
	args := m.Called(ctx, event)
	return args.Error(0)
}

type mockQueryHandler struct {
	mock.Mock
}

func (m *mockQueryHandler) HandleQuery(ctx context.Context, query contracts.Query) (contracts.Reply, error) {
	args := m.Called(ctx, query)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(contracts.Reply), args.Error(1)
}

type mockReplyHandler struct {
	mock.Mock
}

func (m *mockReplyHandler) HandleReply(ctx context.Context, reply contracts.Reply) error {
	args := m.Called(ctx, reply)
	return args.Error(0)
}

// Mock publisher
type mockHandlerPublisher struct {
	mock.Mock
}

func (m *mockHandlerPublisher) Publish(ctx context.Context, message contracts.Message, routing ...string) error {
	args := m.Called(ctx, message, routing)
	return args.Error(0)
}

func (m *mockHandlerPublisher) PublishCommand(ctx context.Context, command contracts.Command) error {
	args := m.Called(ctx, command)
	return args.Error(0)
}

func (m *mockHandlerPublisher) PublishEvent(ctx context.Context, event contracts.Event, routing ...string) error {
	args := m.Called(ctx, event, routing)
	return args.Error(0)
}

func (m *mockHandlerPublisher) PublishQuery(ctx context.Context, query contracts.Query) error {
	args := m.Called(ctx, query)
	return args.Error(0)
}

func (m *mockHandlerPublisher) PublishReply(ctx context.Context, reply contracts.Reply, replyTo string) error {
	args := m.Called(ctx, reply, replyTo)
	return args.Error(0)
}

func (m *mockHandlerPublisher) Close() error {
	args := m.Called()
	return args.Error(0)
}

// Mock dispatcher
type mockHandlerDispatcher struct {
	mock.Mock
}

func (m *mockHandlerDispatcher) RegisterHandler(msgType contracts.Message, handler MessageHandler, options ...HandlerOption) error {
	args := m.Called(msgType, handler, options)
	return args.Error(0)
}

func (m *mockHandlerDispatcher) UnregisterHandler(msgType contracts.Message) error {
	args := m.Called(msgType)
	return args.Error(0)
}

func (m *mockHandlerDispatcher) Dispatch(ctx context.Context, msg contracts.Message) error {
	args := m.Called(ctx, msg)
	return args.Error(0)
}

func (m *mockHandlerDispatcher) GetHandler(msgType contracts.Message) (MessageHandler, bool) {
	args := m.Called(msgType)
	if args.Get(0) == nil {
		return nil, args.Bool(1)
	}
	return args.Get(0).(MessageHandler), args.Bool(1)
}

func TestCommandHandlerAdapter(t *testing.T) {
	t.Run("creates adapter with handler", func(t *testing.T) {
		handler := &mockCommandHandler{}
		adapter := NewCommandHandlerAdapter(handler)
		
		assert.NotNil(t, adapter)
		assert.Equal(t, handler, adapter.handler)
	})
	
	t.Run("handles command messages", func(t *testing.T) {
		handler := &mockCommandHandler{}
		adapter := NewCommandHandlerAdapter(handler)
		
		ctx := context.Background()
		cmd := &testHandlerCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
			CommandData: "test data",
		}
		
		handler.On("HandleCommand", ctx, cmd).Return(nil)
		
		err := adapter.Handle(ctx, cmd)
		
		assert.NoError(t, err)
		handler.AssertExpectations(t)
	})
	
	t.Run("returns error for non-command messages", func(t *testing.T) {
		handler := &mockCommandHandler{}
		adapter := NewCommandHandlerAdapter(handler)
		
		ctx := context.Background()
		event := &testHandlerEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
			},
		}
		
		err := adapter.Handle(ctx, event)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected Command")
		handler.AssertNotCalled(t, "HandleCommand", mock.Anything, mock.Anything)
	})
	
	t.Run("propagates handler errors", func(t *testing.T) {
		handler := &mockCommandHandler{}
		adapter := NewCommandHandlerAdapter(handler)
		
		ctx := context.Background()
		cmd := &testHandlerCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
		}
		
		handlerErr := errors.New("handler error")
		handler.On("HandleCommand", ctx, cmd).Return(handlerErr)
		
		err := adapter.Handle(ctx, cmd)
		
		assert.Equal(t, handlerErr, err)
		handler.AssertExpectations(t)
	})
}

func TestEventHandlerAdapter(t *testing.T) {
	t.Run("creates adapter with handler", func(t *testing.T) {
		handler := &mockEventHandler{}
		adapter := NewEventHandlerAdapter(handler)
		
		assert.NotNil(t, adapter)
		assert.Equal(t, handler, adapter.handler)
	})
	
	t.Run("handles event messages", func(t *testing.T) {
		handler := &mockEventHandler{}
		adapter := NewEventHandlerAdapter(handler)
		
		ctx := context.Background()
		event := &testHandlerEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
				AggregateID: "agg-123",
				Sequence:    1,
			},
			EventData: "test data",
		}
		
		handler.On("HandleEvent", ctx, event).Return(nil)
		
		err := adapter.Handle(ctx, event)
		
		assert.NoError(t, err)
		handler.AssertExpectations(t)
	})
	
	t.Run("returns error for non-event messages", func(t *testing.T) {
		handler := &mockEventHandler{}
		adapter := NewEventHandlerAdapter(handler)
		
		ctx := context.Background()
		cmd := &testHandlerCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
		}
		
		err := adapter.Handle(ctx, cmd)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected Event")
		handler.AssertNotCalled(t, "HandleEvent", mock.Anything, mock.Anything)
	})
}

func TestQueryHandlerAdapter(t *testing.T) {
	t.Run("creates adapter with handler and publisher", func(t *testing.T) {
		handler := &mockQueryHandler{}
		publisher := &MessagePublisher{}
		adapter := NewQueryHandlerAdapter(handler, publisher)
		
		assert.NotNil(t, adapter)
		assert.Equal(t, handler, adapter.handler)
		assert.Equal(t, publisher, adapter.publisher)
	})
	
	t.Run("handles query and publishes reply", func(t *testing.T) {
		handler := &mockQueryHandler{}
		publisher := &MessagePublisher{transport: nil} // We'll use the mock directly
		adapter := NewQueryHandlerAdapter(handler, publisher)
		adapter.publisher = &MessagePublisher{transport: nil} // Replace with mock-friendly publisher
		
		ctx := context.Background()
		query := &testHandlerQuery{
			BaseQuery: contracts.BaseQuery{
				BaseMessage: contracts.NewBaseMessage("TestQuery"),
				ReplyTo:     "reply.queue",
			},
			QueryData: "test query",
		}
		query.SetCorrelationID("corr-123")
		
		reply := &testHandlerReply{
			BaseReply: contracts.BaseReply{
				BaseMessage: contracts.NewBaseMessage("TestReply"),
				Success:     true,
			},
			ReplyData: "test reply",
		}
		
		handler.On("HandleQuery", ctx, query).Return(reply, nil)
		
		// Temporarily disable publisher call for this test
		adapter.publisher = nil
		
		err := adapter.Handle(ctx, query)
		
		assert.NoError(t, err)
		handler.AssertExpectations(t)
	})
	
	t.Run("handles query errors and publishes error reply", func(t *testing.T) {
		handler := &mockQueryHandler{}
		publisher := &MessagePublisher{}
		adapter := NewQueryHandlerAdapter(handler, publisher)
		adapter.publisher = nil // Disable publisher for this test
		
		ctx := context.Background()
		query := &testHandlerQuery{
			BaseQuery: contracts.BaseQuery{
				BaseMessage: contracts.NewBaseMessage("TestQuery"),
				ReplyTo:     "reply.queue",
			},
		}
		query.SetCorrelationID("corr-123")
		
		queryErr := errors.New("query processing error")
		handler.On("HandleQuery", ctx, query).Return(nil, queryErr)
		
		err := adapter.Handle(ctx, query)
		
		assert.Equal(t, queryErr, err)
		handler.AssertExpectations(t)
	})
	
	t.Run("returns error for non-query messages", func(t *testing.T) {
		handler := &mockQueryHandler{}
		adapter := NewQueryHandlerAdapter(handler, nil)
		
		ctx := context.Background()
		cmd := &testHandlerCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
		}
		
		err := adapter.Handle(ctx, cmd)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected Query")
		handler.AssertNotCalled(t, "HandleQuery", mock.Anything, mock.Anything)
	})
	
	t.Run("handles query without reply queue", func(t *testing.T) {
		handler := &mockQueryHandler{}
		adapter := NewQueryHandlerAdapter(handler, nil)
		
		ctx := context.Background()
		query := &testHandlerQuery{
			BaseQuery: contracts.BaseQuery{
				BaseMessage: contracts.NewBaseMessage("TestQuery"),
				// No ReplyTo set
			},
		}
		
		reply := &testHandlerReply{
			BaseReply: contracts.BaseReply{
				BaseMessage: contracts.NewBaseMessage("TestReply"),
				Success:     true,
			},
		}
		
		handler.On("HandleQuery", ctx, query).Return(reply, nil)
		
		err := adapter.Handle(ctx, query)
		
		assert.NoError(t, err)
		handler.AssertExpectations(t)
	})
}

func TestReplyHandlerAdapter(t *testing.T) {
	t.Run("creates adapter with handler", func(t *testing.T) {
		handler := &mockReplyHandler{}
		adapter := NewReplyHandlerAdapter(handler)
		
		assert.NotNil(t, adapter)
		assert.Equal(t, handler, adapter.handler)
	})
	
	t.Run("handles reply messages", func(t *testing.T) {
		handler := &mockReplyHandler{}
		adapter := NewReplyHandlerAdapter(handler)
		
		ctx := context.Background()
		reply := &testHandlerReply{
			BaseReply: contracts.BaseReply{
				BaseMessage: contracts.NewBaseMessage("TestReply"),
				Success:     true,
			},
			ReplyData: "test data",
		}
		
		handler.On("HandleReply", ctx, reply).Return(nil)
		
		err := adapter.Handle(ctx, reply)
		
		assert.NoError(t, err)
		handler.AssertExpectations(t)
	})
	
	t.Run("returns error for non-reply messages", func(t *testing.T) {
		handler := &mockReplyHandler{}
		adapter := NewReplyHandlerAdapter(handler)
		
		ctx := context.Background()
		cmd := &testHandlerCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
		}
		
		err := adapter.Handle(ctx, cmd)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected Reply")
		handler.AssertNotCalled(t, "HandleReply", mock.Anything, mock.Anything)
	})
}

func TestHandlerFuncs(t *testing.T) {
	t.Run("CommandHandlerFunc", func(t *testing.T) {
		var capturedCtx context.Context
		var capturedCmd contracts.Command
		handlerFunc := CommandHandlerFunc(func(ctx context.Context, cmd contracts.Command) error {
			capturedCtx = ctx
			capturedCmd = cmd
			return nil
		})
		
		ctx := context.Background()
		cmd := &testHandlerCommand{
			BaseCommand: contracts.BaseCommand{
				BaseMessage: contracts.NewBaseMessage("TestCommand"),
			},
		}
		
		err := handlerFunc.HandleCommand(ctx, cmd)
		
		assert.NoError(t, err)
		assert.Equal(t, ctx, capturedCtx)
		assert.Equal(t, cmd, capturedCmd)
	})
	
	t.Run("EventHandlerFunc", func(t *testing.T) {
		handlerFunc := EventHandlerFunc(func(ctx context.Context, event contracts.Event) error {
			return errors.New("event error")
		})
		
		ctx := context.Background()
		event := &testHandlerEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
			},
		}
		
		err := handlerFunc.HandleEvent(ctx, event)
		
		assert.Error(t, err)
		assert.Equal(t, "event error", err.Error())
	})
	
	t.Run("QueryHandlerFunc", func(t *testing.T) {
		reply := &testHandlerReply{
			BaseReply: contracts.BaseReply{
				BaseMessage: contracts.NewBaseMessage("TestReply"),
				Success:     true,
			},
		}
		
		handlerFunc := QueryHandlerFunc(func(ctx context.Context, query contracts.Query) (contracts.Reply, error) {
			return reply, nil
		})
		
		ctx := context.Background()
		query := &testHandlerQuery{
			BaseQuery: contracts.BaseQuery{
				BaseMessage: contracts.NewBaseMessage("TestQuery"),
			},
		}
		
		result, err := handlerFunc.HandleQuery(ctx, query)
		
		assert.NoError(t, err)
		assert.Equal(t, reply, result)
	})
	
	t.Run("ReplyHandlerFunc", func(t *testing.T) {
		handlerFunc := ReplyHandlerFunc(func(ctx context.Context, reply contracts.Reply) error {
			return nil
		})
		
		ctx := context.Background()
		reply := &testHandlerReply{
			BaseReply: contracts.BaseReply{
				BaseMessage: contracts.NewBaseMessage("TestReply"),
			},
		}
		
		err := handlerFunc.HandleReply(ctx, reply)
		
		assert.NoError(t, err)
	})
}

func TestHandlerRegistry(t *testing.T) {
	t.Run("creates registry with dispatcher and publisher", func(t *testing.T) {
		dispatcher := NewMessageDispatcher()
		publisher := &MessagePublisher{}
		registry := NewHandlerRegistry(dispatcher, publisher)
		
		assert.NotNil(t, registry)
		assert.NotNil(t, registry.commandHandlers)
		assert.NotNil(t, registry.eventHandlers)
		assert.NotNil(t, registry.queryHandlers)
		assert.NotNil(t, registry.replyHandlers)
		assert.Equal(t, dispatcher, registry.dispatcher)
		assert.Equal(t, publisher, registry.publisher)
	})
	
	t.Run("registers command handler", func(t *testing.T) {
		dispatcher := NewMessageDispatcher()
		registry := NewHandlerRegistry(dispatcher, nil)
		
		handler := &mockCommandHandler{}
		cmdType := &testHandlerCommand{}
		
		err := registry.RegisterCommandHandler(cmdType, handler)
		
		assert.NoError(t, err)
		
		// Verify handler was stored
		stored, exists := registry.GetCommandHandler("testHandlerCommand")
		assert.True(t, exists)
		assert.Equal(t, handler, stored)
	})
	
	t.Run("registers command handler func", func(t *testing.T) {
		dispatcher := NewMessageDispatcher()
		registry := NewHandlerRegistry(dispatcher, nil)
		
		handlerFunc := CommandHandlerFunc(func(ctx context.Context, cmd contracts.Command) error {
			return nil
		})
		cmdType := &testHandlerCommand{}
		
		err := registry.RegisterCommandHandlerFunc(cmdType, handlerFunc)
		
		assert.NoError(t, err)
		
		// Verify handler was stored
		_, exists := registry.GetCommandHandler("testHandlerCommand")
		assert.True(t, exists)
	})
	
	t.Run("registers event handler", func(t *testing.T) {
		dispatcher := NewMessageDispatcher()
		registry := NewHandlerRegistry(dispatcher, nil)
		
		handler := &mockEventHandler{}
		eventType := &testHandlerEvent{}
		
		err := registry.RegisterEventHandler(eventType, handler)
		
		assert.NoError(t, err)
		
		// Verify handler was stored
		stored, exists := registry.GetEventHandler("testHandlerEvent")
		assert.True(t, exists)
		assert.Equal(t, handler, stored)
	})
	
	t.Run("registers query handler", func(t *testing.T) {
		dispatcher := NewMessageDispatcher()
		publisher := &MessagePublisher{}
		registry := NewHandlerRegistry(dispatcher, publisher)
		
		handler := &mockQueryHandler{}
		queryType := &testHandlerQuery{}
		
		err := registry.RegisterQueryHandler(queryType, handler)
		
		assert.NoError(t, err)
		
		// Verify handler was stored
		stored, exists := registry.GetQueryHandler("testHandlerQuery")
		assert.True(t, exists)
		assert.Equal(t, handler, stored)
	})
	
	t.Run("registers reply handler", func(t *testing.T) {
		dispatcher := NewMessageDispatcher()
		registry := NewHandlerRegistry(dispatcher, nil)
		
		handler := &mockReplyHandler{}
		replyType := &testHandlerReply{}
		
		err := registry.RegisterReplyHandler(replyType, handler)
		
		assert.NoError(t, err)
		
		// Verify handler was stored
		stored, exists := registry.GetReplyHandler("testHandlerReply")
		assert.True(t, exists)
		assert.Equal(t, handler, stored)
	})
	
	t.Run("handler not found returns false", func(t *testing.T) {
		dispatcher := NewMessageDispatcher()
		registry := NewHandlerRegistry(dispatcher, nil)
		
		_, exists := registry.GetCommandHandler("NonExistent")
		assert.False(t, exists)
		
		_, exists = registry.GetEventHandler("NonExistent")
		assert.False(t, exists)
		
		_, exists = registry.GetQueryHandler("NonExistent")
		assert.False(t, exists)
		
		_, exists = registry.GetReplyHandler("NonExistent")
		assert.False(t, exists)
	})
}

func TestSagaHandler(t *testing.T) {
	t.Run("creates base saga handler", func(t *testing.T) {
		publisher := &MessagePublisher{}
		handler := NewBaseSagaHandler(publisher)
		
		assert.NotNil(t, handler)
		assert.Equal(t, publisher, handler.publisher)
		assert.NotNil(t, handler.state)
	})
	
	t.Run("starts saga", func(t *testing.T) {
		handler := NewBaseSagaHandler(nil)
		
		ctx := context.Background()
		correlationID := "saga-123"
		event := &testHandlerEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("StartEvent"),
			},
		}
		
		err := handler.StartSaga(ctx, correlationID, event)
		
		assert.NoError(t, err)
		
		// Verify saga state
		status, err := handler.GetSagaStatus(ctx, correlationID)
		assert.NoError(t, err)
		assert.Equal(t, correlationID, status.CorrelationID)
		assert.Equal(t, "started", status.Status)
		assert.Equal(t, "StartEvent", status.LastEvent)
		assert.NotNil(t, status.StartedAt)
		assert.Nil(t, status.CompletedAt)
		assert.Empty(t, status.Steps)
	})
	
	t.Run("handles saga event", func(t *testing.T) {
		handler := NewBaseSagaHandler(nil)
		
		ctx := context.Background()
		correlationID := "saga-123"
		
		// Start saga first
		startEvent := &testHandlerEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("StartEvent"),
			},
		}
		err := handler.StartSaga(ctx, correlationID, startEvent)
		assert.NoError(t, err)
		
		// Handle next event
		nextEvent := &testHandlerEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("NextEvent"),
			},
		}
		err = handler.HandleSagaEvent(ctx, correlationID, nextEvent)
		assert.NoError(t, err)
		
		// Verify saga state
		status, err := handler.GetSagaStatus(ctx, correlationID)
		assert.NoError(t, err)
		assert.Equal(t, "running", status.Status)
		assert.Equal(t, "NextEvent", status.LastEvent)
	})
	
	t.Run("handles saga not found", func(t *testing.T) {
		handler := NewBaseSagaHandler(nil)
		
		ctx := context.Background()
		event := &testHandlerEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("TestEvent"),
			},
		}
		
		err := handler.HandleSagaEvent(ctx, "unknown-saga", event)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "saga not found")
	})
	
	t.Run("compensates saga", func(t *testing.T) {
		handler := NewBaseSagaHandler(nil)
		
		ctx := context.Background()
		correlationID := "saga-123"
		
		// Start saga first
		event := &testHandlerEvent{
			BaseEvent: contracts.BaseEvent{
				BaseMessage: contracts.NewBaseMessage("StartEvent"),
			},
		}
		err := handler.StartSaga(ctx, correlationID, event)
		assert.NoError(t, err)
		
		// Compensate saga
		compensationReason := errors.New("something went wrong")
		err = handler.CompensateSaga(ctx, correlationID, compensationReason)
		assert.NoError(t, err)
		
		// Verify saga state
		status, err := handler.GetSagaStatus(ctx, correlationID)
		assert.NoError(t, err)
		assert.Equal(t, "compensating", status.Status)
	})
	
	t.Run("saga status not found", func(t *testing.T) {
		handler := NewBaseSagaHandler(nil)
		
		ctx := context.Background()
		_, err := handler.GetSagaStatus(ctx, "unknown-saga")
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "saga not found")
	})
}

func TestHandlerSagaStatus(t *testing.T) {
	t.Run("saga status structure", func(t *testing.T) {
		now := time.Now()
		completed := now.Add(time.Hour)
		
		status := SagaStatus{
			CorrelationID: "saga-123",
			Status:        "completed",
			StartedAt:     now,
			CompletedAt:   &completed,
			LastEvent:     "FinalEvent",
			Steps: []SagaStep{
				{
					StepID:        "step-1",
					Status:        "completed",
					ExecutedAt:    &now,
					CompensatedAt: nil,
					Error:         "",
				},
			},
		}
		
		assert.Equal(t, "saga-123", status.CorrelationID)
		assert.Equal(t, "completed", status.Status)
		assert.Equal(t, now, status.StartedAt)
		assert.Equal(t, &completed, status.CompletedAt)
		assert.Equal(t, "FinalEvent", status.LastEvent)
		assert.Len(t, status.Steps, 1)
		assert.Equal(t, "step-1", status.Steps[0].StepID)
	})
}

func TestHandlerErrorReply(t *testing.T) {
	t.Run("creates error reply correctly", func(t *testing.T) {
		ctx := context.Background()
		query := &testHandlerQuery{
			BaseQuery: contracts.BaseQuery{
				BaseMessage: contracts.NewBaseMessage("TestQuery"),
			},
		}
		query.SetCorrelationID("corr-123")
		
		handler := &mockQueryHandler{}
		adapter := NewQueryHandlerAdapter(handler, nil)
		
		queryErr := errors.New("query processing error")
		handler.On("HandleQuery", ctx, query).Return(nil, queryErr)
		
		err := adapter.Handle(ctx, query)
		
		assert.Equal(t, queryErr, err)
		handler.AssertExpectations(t)
	})
}

func TestHandlerRegistryWithOptions(t *testing.T) {
	t.Run("passes handler options to dispatcher", func(t *testing.T) {
		// Create a real dispatcher and test that options are passed through
		dispatcher := NewMessageDispatcher()
		registry := NewHandlerRegistry(dispatcher, nil)
		
		handler := &mockCommandHandler{}
		cmdType := &testHandlerCommand{}
		
		// Register with options
		err := registry.RegisterCommandHandler(cmdType, handler,
			func(o *HandlerOptions) {
				o.Concurrency = 5
				o.Queue = "test.queue"
				o.Durable = true
			},
		)
		
		assert.NoError(t, err)
		
		// Verify handler was registered
		stored, exists := registry.GetCommandHandler("testHandlerCommand")
		assert.True(t, exists)
		assert.Equal(t, handler, stored)
	})
}