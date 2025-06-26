package contracts

import (
	"fmt"
)

// ErrorReply represents an error response
type ErrorReply struct {
	BaseReply
	ErrorCode    string `json:"errorCode"`
	ErrorMessage string `json:"errorMessage"`
	StackTrace   string `json:"stackTrace,omitempty"`
	Error        string `json:"error"` // For compatibility
}

// NewErrorReply creates a new error reply
func NewErrorReply(messageType string, errorCode string, errorMessage string) *ErrorReply {
	reply := &ErrorReply{
		BaseReply:    BaseReply{BaseMessage: NewBaseMessage(messageType), Success: false},
		ErrorCode:    errorCode,
		ErrorMessage: errorMessage,
		Error:        errorMessage,
	}
	return reply
}

// IsSuccess returns false for error replies
func (e ErrorReply) IsSuccess() bool {
	return false
}

// GetError returns the error
func (e ErrorReply) GetError() error {
	return fmt.Errorf("%s: %s", e.ErrorCode, e.ErrorMessage)
}