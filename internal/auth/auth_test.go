package auth_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/auth"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/backend/mem"
	"github.com/codefly-dev/service-object-storage/internal/events"
	"github.com/codefly-dev/service-object-storage/internal/probetest"
	"github.com/codefly-dev/service-object-storage/internal/server"
)

// serverToken is the secret the test gateway enforces.
const serverToken = "89d0d1f6d0e2f5a4c3b2a1908f7e6d5c"

// rpcs is the whole surface an unauthenticated peer could try: unary calls, the
// client-streaming write, and both server streams. Each returns the RPC's
// error, so a caller can assert on its status code either way.
var rpcs = map[string]func(context.Context, storagev0.ObjectStorageClient) error{
	"Capabilities": func(ctx context.Context, c storagev0.ObjectStorageClient) error {
		_, err := c.Capabilities(ctx, &storagev0.CapabilitiesRequest{})
		return err
	},
	"Stat": func(ctx context.Context, c storagev0.ObjectStorageClient) error {
		_, err := c.Stat(ctx, &storagev0.StatRequest{Key: "probe"})
		return err
	},
	"List": func(ctx context.Context, c storagev0.ObjectStorageClient) error {
		_, err := c.List(ctx, &storagev0.ListRequest{})
		return err
	},
	"Delete": func(ctx context.Context, c storagev0.ObjectStorageClient) error {
		_, err := c.Delete(ctx, &storagev0.DeleteRequest{Key: "probe"})
		return err
	},
	"Presign": func(ctx context.Context, c storagev0.ObjectStorageClient) error {
		_, err := c.Presign(ctx, &storagev0.PresignRequest{Key: "probe", ExpirySeconds: 60})
		return err
	},
	"Native": func(ctx context.Context, c storagev0.ObjectStorageClient) error {
		_, err := c.Native(ctx, &storagev0.NativeRequest{Verb: "probe"})
		return err
	},
	"Put": func(ctx context.Context, c storagev0.ObjectStorageClient) error {
		st, err := c.Put(ctx)
		if err != nil {
			return err
		}
		// A refused stream makes Send fail with io.EOF; CloseAndRecv then
		// carries the real status.
		if err := st.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Header{
			Header: &storagev0.PutHeader{Key: "probe", TotalSize: 5},
		}}); err != nil && err != io.EOF {
			return err
		}
		if err := st.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Data{
			Data: []byte("bytes"),
		}}); err != nil && err != io.EOF {
			return err
		}
		_, err = st.CloseAndRecv()
		return err
	},
	"Get": func(ctx context.Context, c storagev0.ObjectStorageClient) error {
		st, err := c.Get(ctx, &storagev0.GetRequest{Key: "probe"})
		if err != nil {
			return err
		}
		_, err = st.Recv()
		return err
	},
	"Watch": func(ctx context.Context, c storagev0.ObjectStorageClient) error {
		st, err := c.Watch(ctx, &storagev0.WatchRequest{})
		if err != nil {
			return err
		}
		_, err = st.Recv()
		return err
	},
}

// startGateway serves the real ObjectStorage implementation over a real TCP
// socket with the interceptors installed, and returns its address.
func startGateway(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	be, err := mem.New(context.Background(), backend.Config{})
	require.NoError(t, err)
	hub := events.NewHub(be.Name(), be.Identity(), nil)

	s := grpc.NewServer(
		grpc.ChainUnaryInterceptor(auth.UnaryInterceptor(serverToken)),
		grpc.ChainStreamInterceptor(auth.StreamInterceptor(serverToken)),
	)
	storagev0.RegisterObjectStorageServer(s, server.New(be, hub, probetest.Monitor(t, be)))
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(func() { s.Stop(); hub.Close(); _ = be.Close() })
	return lis.Addr().String()
}

func dial(t *testing.T, address string, opts ...grpc.DialOption) storagev0.ObjectStorageClient {
	t.Helper()
	conn, err := grpc.NewClient(address, append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return storagev0.NewObjectStorageClient(conn)
}

// TestUnauthenticatedPeerIsRefused is the core guarantee: a peer that reaches
// the published port without the session's token can neither read, write,
// delete, presign nor subscribe — over unary and streaming RPCs alike.
func TestUnauthenticatedPeerIsRefused(t *testing.T) {
	address := startGateway(t)

	peers := map[string]storagev0.ObjectStorageClient{
		"no credentials":  dial(t, address),
		"wrong token":     dial(t, address, auth.DialOption("00000000000000000000000000000000")),
		"prefix of token": dial(t, address, auth.DialOption(serverToken[:len(serverToken)-1])),
		"empty token":     dial(t, address, auth.DialOption("")),
	}

	for peerName, client := range peers {
		for rpcName, call := range rpcs {
			t.Run(peerName+"/"+rpcName, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				err := call(ctx, client)
				require.Error(t, err)
				require.Equal(t, codes.Unauthenticated, status.Code(err), "got %v", err)
			})
		}
	}
}

// TestAuthorizedPeerReachesTheHandler pairs with the refusal test: the same
// calls must get through once the session token is presented, or the gateway
// would be locked shut rather than protected. A handler-level status (NotFound,
// Unimplemented) still proves the interceptor let the call through; Watch has
// nothing to deliver, so it only has to not be refused before the deadline.
func TestAuthorizedPeerReachesTheHandler(t *testing.T) {
	client := dial(t, startGateway(t), auth.DialOption(serverToken))

	for rpcName, call := range rpcs {
		t.Run(rpcName, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err := call(ctx, client)
			require.NotEqual(t, codes.Unauthenticated, status.Code(err), "got %v", err)
		})
	}
}

// TestTokenTravelsUnderTheEcosystemKey pins the wire contract: the gateway reads
// the same metadata key the codefly host uses for agent plugins, so a caller
// built against that convention interoperates.
func TestTokenTravelsUnderTheEcosystemKey(t *testing.T) {
	require.Equal(t, "x-codefly-token", auth.MetadataKey)

	client := dial(t, startGateway(t), grpc.WithUnaryInterceptor(withMetadata(auth.MetadataKey, serverToken)))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.Capabilities(ctx, &storagev0.CapabilitiesRequest{})
	require.NoError(t, err)
}

// TestTokenUnderAnotherHeaderIsRefused guards against a caller that presents the
// right secret the wrong way — an HTTP-style Authorization bearer is not the
// gateway's credential and must not be accepted.
func TestTokenUnderAnotherHeaderIsRefused(t *testing.T) {
	client := dial(t, startGateway(t), grpc.WithUnaryInterceptor(withMetadata("authorization", "Bearer "+serverToken)))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.Capabilities(ctx, &storagev0.CapabilitiesRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err), "got %v", err)
}

// withMetadata attaches a raw header to every unary call, so a test can present
// a credential the DialOption helper would never build.
func withMetadata(key, value string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(metadata.AppendToOutgoingContext(ctx, key, value), method, req, reply, cc, opts...)
	}
}
