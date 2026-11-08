package protocol

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dockfleetv1 "github.com/dockfleet/dockfleet/internal/protocol/dockfleet/v1"
)

// Error is a protocol-level failure reported by the peer (see ProtocolError in
// the proto file). Client methods return it for any failed call whose gRPC
// status carries a ProtocolError detail.
type Error struct {
	Code      dockfleetv1.ProtocolErrorCode
	Message   string
	Supported *dockfleetv1.ProtocolVersion
	cause     error
}

func (e *Error) Error() string {
	return fmt.Sprintf("protocol error %s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.cause }

// newStatusError builds a gRPC error carrying a ProtocolError detail.
func newStatusError(c codes.Code, code dockfleetv1.ProtocolErrorCode, msg string, own *dockfleetv1.ProtocolVersion) error {
	st := status.New(c, msg)
	if withDetails, err := st.WithDetails(&dockfleetv1.ProtocolError{Code: code, Message: msg, SupportedVersion: own}); err == nil {
		st = withDetails
	}
	return st.Err()
}

// decodeError turns a gRPC error with a ProtocolError detail into *Error and
// leaves every other error untouched.
func decodeError(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	for _, d := range st.Details() {
		if pe, ok := d.(*dockfleetv1.ProtocolError); ok {
			return &Error{Code: pe.GetCode(), Message: pe.GetMessage(), Supported: pe.GetSupportedVersion(), cause: err}
		}
	}
	return err
}

// ErrorCode returns the ProtocolErrorCode carried by err, or UNSPECIFIED.
func ErrorCode(err error) dockfleetv1.ProtocolErrorCode {
	var pe *Error
	if errors.As(err, &pe) {
		return pe.Code
	}
	return dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_UNSPECIFIED
}

// IsIncompatibleVersion reports a protocol version mismatch with the peer.
func IsIncompatibleVersion(err error) bool {
	return ErrorCode(err) == dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_INCOMPATIBLE_VERSION
}

// NeedsRegistration reports that the manager has no usable session for the
// node and the agent must register again.
func NeedsRegistration(err error) bool {
	switch ErrorCode(err) {
	case dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_REGISTERED,
		dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_SESSION_SUPERSEDED:
		return true
	}
	return false
}

// IsPermanent reports failures that retrying the same request cannot fix:
// version mismatch, invalid request, or rejected credentials.
func IsPermanent(err error) bool {
	switch ErrorCode(err) {
	case dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_INCOMPATIBLE_VERSION,
		dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_INVALID_REQUEST:
		return true
	}
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied:
		return true
	}
	return false
}
