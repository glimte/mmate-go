package messaging

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/glimte/mmate-go/contracts"
)

// Specialized handler interfaces for different message types

// CommandHandler handles command messages
type CommandHandler interface {
	HandleCommand(ctx context.Context, cmd contracts.Command) error
}

// EventHandler handles event messages
type EventHandler interface {
	HandleEvent(ctx context.Context, event contracts.Event) error
}

// QueryHandler handles query messages and returns responses
type QueryHandler interface {
	HandleQuery(ctx context.Context, query contracts.Query) (contracts.Reply, error)
}

// ReplyHandler handles reply messages
type ReplyHandler interface {
	HandleReply(ctx context.Context, reply contracts.Reply) error
}

// Handler adapters that implement MessageHandler interface

// CommandHandlerAdapter adapts CommandHandler to MessageHandler
type CommandHandlerAdapter struct {
	handler CommandHandler
}

// NewCommandHandlerAdapter creates a new command handler adapter
func NewCommandHandlerAdapter(handler CommandHandler) *CommandHandlerAdapter {
	return &CommandHandlerAdapter{handler: handler}
}

// Handle implements MessageHandler
func (a *CommandHandlerAdapter) Handle(ctx context.Context, msg contracts.Message) error {
	cmd, ok := msg.(contracts.Command)
	if !ok {
		return fmt.Errorf("expected Command, got %T", msg)
	}
	return a.handler.HandleCommand(ctx, cmd)
}

// EventHandlerAdapter adapts EventHandler to MessageHandler
type EventHandlerAdapter struct {
	handler EventHandler
}

// NewEventHandlerAdapter creates a new event handler adapter
func NewEventHandlerAdapter(handler EventHandler) *EventHandlerAdapter {
	return &EventHandlerAdapter{handler: handler}
}

// Handle implements MessageHandler
func (a *EventHandlerAdapter) Handle(ctx context.Context, msg contracts.Message) error {
	event, ok := msg.(contracts.Event)
	if !ok {
		return fmt.Errorf("expected Event, got %T", msg)
	}
	return a.handler.HandleEvent(ctx, event)
}

// QueryHandlerAdapter adapts QueryHandler to MessageHandler
type QueryHandlerAdapter struct {
	handler   QueryHandler
	publisher *MessagePublisher
}

// NewQueryHandlerAdapter creates a new query handler adapter
func NewQueryHandlerAdapter(handler QueryHandler, publisher *MessagePublisher) *QueryHandlerAdapter {
	return &QueryHandlerAdapter{
		handler:   handler,
		publisher: publisher,
	}
}

// Handle implements MessageHandler
func (a *QueryHandlerAdapter) Handle(ctx context.Context, msg contracts.Message) error {
	query, ok := msg.(contracts.Query)
	if !ok {
		return fmt.Errorf("expected Query, got %T", msg)
	}

	// Process query and get reply
	reply, err := a.handler.HandleQuery(ctx, query)
	if err != nil {
		// Create error reply
		errorReply := contracts.NewErrorReply(
			fmt.Sprintf("%sError", query.GetType()),
			"QUERY_HANDLER_ERROR",
			err.Error(),
		)
		errorReply.SetCorrelationID(query.GetCorrelationID())

		// Publish error reply
		if a.publisher != nil && query.GetReplyTo() != "" {
			return a.publisher.PublishReply(ctx, errorReply, query.GetReplyTo())
		}
		return err
	}

	// Set correlation ID on success reply
	if reply != nil {
		reply.SetCorrelationID(query.GetCorrelationID())

		// Publish success reply
		if a.publisher != nil && query.GetReplyTo() != "" {
			return a.publisher.PublishReply(ctx, reply, query.GetReplyTo())
		}
	}

	return nil
}

// ReplyHandlerAdapter adapts ReplyHandler to MessageHandler
type ReplyHandlerAdapter struct {
	handler ReplyHandler
}

// NewReplyHandlerAdapter creates a new reply handler adapter
func NewReplyHandlerAdapter(handler ReplyHandler) *ReplyHandlerAdapter {
	return &ReplyHandlerAdapter{handler: handler}
}

// Handle implements MessageHandler
func (a *ReplyHandlerAdapter) Handle(ctx context.Context, msg contracts.Message) error {
	reply, ok := msg.(contracts.Reply)
	if !ok {
		return fmt.Errorf("expected Reply, got %T", msg)
	}
	return a.handler.HandleReply(ctx, reply)
}

// Function-based handlers

// CommandHandlerFunc is a function adapter for CommandHandler
type CommandHandlerFunc func(ctx context.Context, cmd contracts.Command) error

// HandleCommand implements CommandHandler
func (f CommandHandlerFunc) HandleCommand(ctx context.Context, cmd contracts.Command) error {
	return f(ctx, cmd)
}

// EventHandlerFunc is a function adapter for EventHandler
type EventHandlerFunc func(ctx context.Context, event contracts.Event) error

// HandleEvent implements EventHandler
func (f EventHandlerFunc) HandleEvent(ctx context.Context, event contracts.Event) error {
	return f(ctx, event)
}

// QueryHandlerFunc is a function adapter for QueryHandler
type QueryHandlerFunc func(ctx context.Context, query contracts.Query) (contracts.Reply, error)

// HandleQuery implements QueryHandler
func (f QueryHandlerFunc) HandleQuery(ctx context.Context, query contracts.Query) (contracts.Reply, error) {
	return f(ctx, query)
}

// ReplyHandlerFunc is a function adapter for ReplyHandler
type ReplyHandlerFunc func(ctx context.Context, reply contracts.Reply) error

// HandleReply implements ReplyHandler
func (f ReplyHandlerFunc) HandleReply(ctx context.Context, reply contracts.Reply) error {
	return f(ctx, reply)
}

// Specialized handler registry

// HandlerRegistry provides type-safe handler registration
type HandlerRegistry struct {
	commandHandlers map[string]CommandHandler
	eventHandlers   map[string]EventHandler
	queryHandlers   map[string]QueryHandler
	replyHandlers   map[string]ReplyHandler
	dispatcher      *MessageDispatcher
	publisher       *MessagePublisher
}

// NewHandlerRegistry creates a new handler registry
func NewHandlerRegistry(dispatcher *MessageDispatcher, publisher *MessagePublisher) *HandlerRegistry {
	return &HandlerRegistry{
		commandHandlers: make(map[string]CommandHandler),
		eventHandlers:   make(map[string]EventHandler),
		queryHandlers:   make(map[string]QueryHandler),
		replyHandlers:   make(map[string]ReplyHandler),
		dispatcher:      dispatcher,
		publisher:       publisher,
	}
}

// RegisterCommandHandler registers a command handler
func (r *HandlerRegistry) RegisterCommandHandler(cmdType contracts.Command, handler CommandHandler, options ...HandlerOption) error {
	typeName := reflect.TypeOf(cmdType).Elem().Name()
	r.commandHandlers[typeName] = handler

	adapter := NewCommandHandlerAdapter(handler)
	return r.dispatcher.RegisterHandler(cmdType, adapter, options...)
}

// RegisterCommandHandlerFunc registers a command handler function
func (r *HandlerRegistry) RegisterCommandHandlerFunc(cmdType contracts.Command, handler CommandHandlerFunc, options ...HandlerOption) error {
	return r.RegisterCommandHandler(cmdType, handler, options...)
}

// RegisterEventHandler registers an event handler
func (r *HandlerRegistry) RegisterEventHandler(eventType contracts.Event, handler EventHandler, options ...HandlerOption) error {
	typeName := reflect.TypeOf(eventType).Elem().Name()
	r.eventHandlers[typeName] = handler

	adapter := NewEventHandlerAdapter(handler)
	return r.dispatcher.RegisterHandler(eventType, adapter, options...)
}

// RegisterEventHandlerFunc registers an event handler function
func (r *HandlerRegistry) RegisterEventHandlerFunc(eventType contracts.Event, handler EventHandlerFunc, options ...HandlerOption) error {
	return r.RegisterEventHandler(eventType, handler, options...)
}

// RegisterQueryHandler registers a query handler
func (r *HandlerRegistry) RegisterQueryHandler(queryType contracts.Query, handler QueryHandler, options ...HandlerOption) error {
	typeName := reflect.TypeOf(queryType).Elem().Name()
	r.queryHandlers[typeName] = handler

	adapter := NewQueryHandlerAdapter(handler, r.publisher)
	return r.dispatcher.RegisterHandler(queryType, adapter, options...)
}

// RegisterQueryHandlerFunc registers a query handler function
func (r *HandlerRegistry) RegisterQueryHandlerFunc(queryType contracts.Query, handler QueryHandlerFunc, options ...HandlerOption) error {
	return r.RegisterQueryHandler(queryType, handler, options...)
}

// RegisterReplyHandler registers a reply handler
func (r *HandlerRegistry) RegisterReplyHandler(replyType contracts.Reply, handler ReplyHandler, options ...HandlerOption) error {
	typeName := reflect.TypeOf(replyType).Elem().Name()
	r.replyHandlers[typeName] = handler

	adapter := NewReplyHandlerAdapter(handler)
	return r.dispatcher.RegisterHandler(replyType, adapter, options...)
}

// RegisterReplyHandlerFunc registers a reply handler function
func (r *HandlerRegistry) RegisterReplyHandlerFunc(replyType contracts.Reply, handler ReplyHandlerFunc, options ...HandlerOption) error {
	return r.RegisterReplyHandler(replyType, handler, options...)
}

// GetCommandHandler gets a registered command handler
func (r *HandlerRegistry) GetCommandHandler(typeName string) (CommandHandler, bool) {
	handler, exists := r.commandHandlers[typeName]
	return handler, exists
}

// GetEventHandler gets a registered event handler
func (r *HandlerRegistry) GetEventHandler(typeName string) (EventHandler, bool) {
	handler, exists := r.eventHandlers[typeName]
	return handler, exists
}

// GetQueryHandler gets a registered query handler
func (r *HandlerRegistry) GetQueryHandler(typeName string) (QueryHandler, bool) {
	handler, exists := r.queryHandlers[typeName]
	return handler, exists
}

// GetReplyHandler gets a registered reply handler
func (r *HandlerRegistry) GetReplyHandler(typeName string) (ReplyHandler, bool) {
	handler, exists := r.replyHandlers[typeName]
	return handler, exists
}

// Saga handler for long-running processes

// SagaHandler manages long-running business processes
type SagaHandler interface {
	// StartSaga starts a new saga instance
	StartSaga(ctx context.Context, correlationID string, initialEvent contracts.Event) error

	// HandleSagaEvent processes an event in the context of a saga
	HandleSagaEvent(ctx context.Context, correlationID string, event contracts.Event) error

	// CompensateSaga performs compensation actions
	CompensateSaga(ctx context.Context, correlationID string, reason error) error

	// GetSagaStatus returns the current status of a saga
	GetSagaStatus(ctx context.Context, correlationID string) (SagaStatus, error)
}

// SagaStatus represents the state of a saga
type SagaStatus struct {
	CorrelationID string     `json:"correlationId"`
	Status        string     `json:"status"` // started, running, completed, compensating, failed
	StartedAt     time.Time  `json:"startedAt"`
	CompletedAt   *time.Time `json:"completedAt,omitempty"`
	LastEvent     string     `json:"lastEvent"`
	Steps         []SagaStep `json:"steps"`
}

// SagaStep represents a step in a saga
type SagaStep struct {
	StepID        string     `json:"stepId"`
	Status        string     `json:"status"` // pending, completed, failed, compensated
	ExecutedAt    *time.Time `json:"executedAt,omitempty"`
	CompensatedAt *time.Time `json:"compensatedAt,omitempty"`
	Error         string     `json:"error,omitempty"`
}

// BaseSagaHandler provides basic saga functionality
type BaseSagaHandler struct {
	publisher *MessagePublisher
	state     map[string]*SagaStatus // In-memory store - use persistent store in production
}

// NewBaseSagaHandler creates a new base saga handler
func NewBaseSagaHandler(publisher *MessagePublisher) *BaseSagaHandler {
	return &BaseSagaHandler{
		publisher: publisher,
		state:     make(map[string]*SagaStatus),
	}
}

// StartSaga implements SagaHandler
func (h *BaseSagaHandler) StartSaga(ctx context.Context, correlationID string, initialEvent contracts.Event) error {
	status := &SagaStatus{
		CorrelationID: correlationID,
		Status:        "started",
		StartedAt:     time.Now(),
		LastEvent:     initialEvent.GetType(),
		Steps:         make([]SagaStep, 0),
	}

	h.state[correlationID] = status
	return nil
}

// HandleSagaEvent implements SagaHandler
func (h *BaseSagaHandler) HandleSagaEvent(ctx context.Context, correlationID string, event contracts.Event) error {
	status, exists := h.state[correlationID]
	if !exists {
		return fmt.Errorf("saga not found: %s", correlationID)
	}

	status.LastEvent = event.GetType()
	status.Status = "running"

	// Override in concrete implementations
	return nil
}

// CompensateSaga implements SagaHandler
func (h *BaseSagaHandler) CompensateSaga(ctx context.Context, correlationID string, reason error) error {
	status, exists := h.state[correlationID]
	if !exists {
		return fmt.Errorf("saga not found: %s", correlationID)
	}

	status.Status = "compensating"

	// Override in concrete implementations
	return nil
}

// GetSagaStatus implements SagaHandler
func (h *BaseSagaHandler) GetSagaStatus(ctx context.Context, correlationID string) (SagaStatus, error) {
	status, exists := h.state[correlationID]
	if !exists {
		return SagaStatus{}, fmt.Errorf("saga not found: %s", correlationID)
	}

	return *status, nil
}
