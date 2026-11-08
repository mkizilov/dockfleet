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

// tokenCredentials attaches the bearer token to every call. gRPC refuses to
// send it over a plaintext connection when secure is true.
type tokenCredentials struct {
	token  string
	secure bool
}

func (c tokenCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{authorizationKey: "Bearer " + c.token}, nil
}

func (c tokenCredentials) RequireTransportSecurity() bool { return c.secure }
