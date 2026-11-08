package protocol

import (
	"context"
	"crypto/subtle"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const authorizationKey = "authorization"

// UnaryTokenInterceptor rejects calls that do not carry "Bearer <token>".
func UnaryTokenInterceptor(token string) grpc.UnaryServerInterceptor {
	want := []byte("Bearer " + token)
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		got := md.Get(authorizationKey)
		if len(got) != 1 || subtle.ConstantTimeCompare([]byte(got[0]), want) != 1 {
			return nil, status.Error(codes.Unauthenticated, "invalid or missing credentials")
		}
		return handler(ctx, req)
	}
}

// StreamTokenInterceptor is UnaryTokenInterceptor for streaming calls: a
// stream without "Bearer <token>" is refused before its handler runs.
func StreamTokenInterceptor(token string) grpc.StreamServerInterceptor {
	want := []byte("Bearer " + token)
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, _ := metadata.FromIncomingContext(ss.Context())
		got := md.Get(authorizationKey)
		if len(got) != 1 || subtle.ConstantTimeCompare([]byte(got[0]), want) != 1 {
			return status.Error(codes.Unauthenticated, "invalid or missing credentials")
		}
		return handler(srv, ss)
	}
}

// tokenCredentials attaches the bearer token to every call. It is only used
// on plaintext loopback connections; mutual TLS replaces it.
type tokenCredentials struct {
	token string
}

func (c tokenCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{authorizationKey: "Bearer " + c.token}, nil
}

func (c tokenCredentials) RequireTransportSecurity() bool { return false }
