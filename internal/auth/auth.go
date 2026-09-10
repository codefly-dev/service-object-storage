// Package auth enforces caller authentication on the gateway's gRPC surface.
// It mirrors the mechanism the codefly host already uses to talk to agent
// plugins (core's agents.AuthMetadataKey): a shared per-session secret carried
// as the "x-codefly-token" metadata header and compared in constant time. The
// key is redeclared here rather than imported so the gateway image does not
// link the agent runtime.
package auth

import (
	"context"
	"crypto/subtle"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// MetadataKey is the gRPC metadata key carrying the bearer token. Lowercase per
// gRPC convention — metadata keys are case-insensitive but the wire form is
// lowercase.
const MetadataKey = "x-codefly-token"

// UnaryInterceptor rejects every unary call that does not present token.
func UnaryInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := verify(ctx, token); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamInterceptor rejects every streaming call that does not present token.
// gRPC runs it before the handler observes a single frame, so an unauthorized
// Put is refused at stream open rather than after its header is accepted.
func StreamInterceptor(token string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := verify(ss.Context(), token); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

// DialOption is how a client presents token on every call it makes to the
// gateway.
func DialOption(token string) grpc.DialOption {
	return grpc.WithPerRPCCredentials(bearer(token))
}

func verify(ctx context.Context, expected string) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing "+MetadataKey)
	}
	presented := md.Get(MetadataKey)
	// An empty presented value would compare equal to an empty expected one, so
	// it is refused as absent rather than reaching the comparison.
	if len(presented) == 0 || presented[0] == "" {
		return status.Error(codes.Unauthenticated, "missing "+MetadataKey)
	}
	if subtle.ConstantTimeCompare([]byte(presented[0]), []byte(expected)) != 1 {
		return status.Error(codes.Unauthenticated, "invalid "+MetadataKey)
	}
	return nil
}

// bearer carries the token as per-RPC credentials.
type bearer string

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{MetadataKey: string(b)}, nil
}

// RequireTransportSecurity is false: the local profile reaches the gateway over
// the Docker host bridge without TLS, and the deployed profile terminates
// transport security at the mesh. Demanding it here would make the credential
// unusable on both.
func (b bearer) RequireTransportSecurity() bool { return false }
