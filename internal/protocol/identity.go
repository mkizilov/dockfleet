package protocol

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/dockfleet/dockfleet/internal/pki"
	dockfleetv1 "github.com/dockfleet/dockfleet/internal/protocol/dockfleet/v1"
)

type identityKey struct{}

// identityFromContext returns the node identity the caller proved with its
// client certificate. It is absent on plaintext (loopback development) servers.
func identityFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(identityKey{}).(string)
	return id, ok
}

// UnaryIdentityInterceptor authenticates callers by their client certificate
// and stores the node identity it carries in the call context.
//
// The TLS handshake has already verified the certificate chain against the
// client CA bundle. This interceptor additionally requires a verified chain,
// re-checks the validity window on every call (a connection can outlive the
// certificate it was opened with) and requires a node identity.
func UnaryIdentityInterceptor(now func() time.Time) grpc.UnaryServerInterceptor {
	if now == nil {
		now = time.Now
	}
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		id, err := peerIdentity(ctx, now())
		if err != nil {
			return nil, err
		}
		return handler(context.WithValue(ctx, identityKey{}, id), req)
	}
}

// StreamIdentityInterceptor is UnaryIdentityInterceptor for streaming calls.
// The certificate is checked when the stream opens; a stream cannot outlive its
// connection, and the manager ends connections after a bounded age, so a
// certificate that expires is not accepted for long.
func StreamIdentityInterceptor(now func() time.Time) grpc.StreamServerInterceptor {
	if now == nil {
		now = time.Now
	}
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		id, err := peerIdentity(ss.Context(), now())
		if err != nil {
			return err
		}
		return handler(srv, &identityStream{ServerStream: ss, ctx: context.WithValue(ss.Context(), identityKey{}, id)})
	}
}

// identityStream is a server stream whose context carries the caller's identity.
type identityStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *identityStream) Context() context.Context { return s.ctx }

func peerIdentity(ctx context.Context, now time.Time) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "no peer information")
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "a TLS client certificate is required")
	}
	state := info.State
	if len(state.PeerCertificates) == 0 || len(state.VerifiedChains) == 0 {
		return "", status.Error(codes.Unauthenticated, "no verified client certificate")
	}
	leaf := state.PeerCertificates[0]
	if err := pki.CheckValidAt(leaf, now); err != nil {
		return "", newStatusError(codes.Unauthenticated, dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_CERTIFICATE_INVALID, "client "+err.Error(), nil)
	}
	id, err := pki.NodeIDFromCert(leaf)
	if err != nil {
		return "", newStatusError(codes.Unauthenticated, dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_CERTIFICATE_INVALID, err.Error(), nil)
	}
	return id, nil
}
