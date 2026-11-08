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

// IsNotEnrolled reports that the node has no active enrollment.
func IsNotEnrolled(err error) bool {
	return ErrorCode(err) == dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_ENROLLED
}

// IsIdentityMismatch reports that the request named a node other than the one
// the client certificate belongs to.
func IsIdentityMismatch(err error) bool {
	return ErrorCode(err) == dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_IDENTITY_MISMATCH
}

// IsCertificateInvalid reports an expired, not yet valid or identity-less
// client certificate.
func IsCertificateInvalid(err error) bool {
	return ErrorCode(err) == dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_CERTIFICATE_INVALID
}

// IsUnimplemented reports that the manager does not offer the call at all: it
// is older than the feature, or the feature is switched off there. Retrying
// cannot change that.
func IsUnimplemented(err error) bool { return status.Code(err) == codes.Unimplemented }

// NeedsRegistration reports that the manager has no usable session for the
// node and the agent must register again. For NOT_ENROLLED the registration
// will keep failing until an administrator enrolls the node again.
func NeedsRegistration(err error) bool {
	switch ErrorCode(err) {
	case dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_REGISTERED,
		dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_SESSION_SUPERSEDED,
		dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_ENROLLED:
		return true
	}
	return false
}

// IsPermanent reports failures that retrying the same request cannot fix:
// version mismatch, invalid request, a request naming another node than the
// certificate, or rejected credentials. A missing enrollment and a bad
// client certificate are not permanent: an administrator can enroll the node
// and a renewed certificate is used by the next connection.
func IsPermanent(err error) bool {
	switch ErrorCode(err) {
	case dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_INCOMPATIBLE_VERSION,
		dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_INVALID_REQUEST,
		dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_IDENTITY_MISMATCH:
		return true
	case dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_ENROLLED,
		dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_CERTIFICATE_INVALID:
		return false
	}
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied:
		return true
	}
	return false
}
