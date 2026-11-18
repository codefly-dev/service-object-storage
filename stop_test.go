package main

import (
	"context"
	"errors"
	"testing"

	runtimev0 "github.com/codefly-dev/core/generated/go/codefly/services/runtime/v0"
	"github.com/stretchr/testify/require"
)

// recordingEnvironment is a container environment that says, in one shared
// order, what the Runtime asked of it.
type recordingEnvironment struct {
	name    string
	calls   *[]string
	stopErr error
}

func (e *recordingEnvironment) ContainerID() (string, error) { return e.name + "-id", nil }

func (e *recordingEnvironment) Init(context.Context) error {
	*e.calls = append(*e.calls, "start "+e.name)
	return nil
}

func (e *recordingEnvironment) Stop(context.Context) error {
	*e.calls = append(*e.calls, "stop "+e.name)
	return e.stopErr
}

func (e *recordingEnvironment) Shutdown(context.Context) error {
	*e.calls = append(*e.calls, "shutdown "+e.name)
	return nil
}

func newStoppableRuntime(t *testing.T) (*Runtime, *fakeNetworks, *[]string) {
	t.Helper()
	fake := newFakeNetworks()
	rt := newNetworkRuntime(t, fake)
	calls := &[]string{}
	rt.gatewayEnv = &recordingEnvironment{name: "gateway", calls: calls}
	rt.minioEnv = &recordingEnvironment{name: "minio", calls: calls}
	require.NoError(t, rt.joinMinIONetwork(context.Background(), "minio-id", minioNetworkAlias))
	require.NoError(t, rt.joinMinIONetwork(context.Background(), "gateway-id"))
	return rt, fake, calls
}

// TestStopStopsTheContainersAndKeepsThem: Stop is what a run sends when it
// ends in order, and Destroy is never sent by one. A Stop that answered at once
// left the gateway and MinIO serving after every `codefly run service`. Stop
// stops both — the gateway first, so nothing is served from a store that is
// going away — and keeps the containers, their network and the MinIO data for
// the next run: removing is Destroy's.
func TestStopStopsTheContainersAndKeepsThem(t *testing.T) {
	rt, fake, calls := newStoppableRuntime(t)
	network := rt.minioNetwork

	resp, err := rt.Stop(context.Background(), &runtimev0.StopRequest{})
	require.NoError(t, err)
	require.Equal(t, runtimev0.StopStatus_SUCCESS, resp.GetStatus().GetState(), resp.GetStatus().GetMessage())
	require.Equal(t, []string{"stop gateway", "stop minio"}, *calls)

	require.NotNil(t, rt.gatewayEnv, "Destroy still has to remove the gateway")
	require.NotNil(t, rt.minioEnv, "Destroy still has to remove MinIO")
	require.Contains(t, fake.networks, network, "a stopped container whose network is removed can never start again")
	require.Equal(t, network, rt.minioNetwork)
}

// TestStopReportsAContainerItCouldNotStop: a gateway that cannot be stopped
// does not leave MinIO running, and the failure is the Stop's answer.
func TestStopReportsAContainerItCouldNotStop(t *testing.T) {
	rt, _, calls := newStoppableRuntime(t)
	rt.gatewayEnv.(*recordingEnvironment).stopErr = errors.New("daemon unreachable")

	resp, err := rt.Stop(context.Background(), &runtimev0.StopRequest{})
	require.NoError(t, err, "Stop reports failure in its response, not as a transport error")
	require.Equal(t, runtimev0.StopStatus_ERROR, resp.GetStatus().GetState())
	require.Contains(t, resp.GetStatus().GetMessage(), "daemon unreachable")
	require.Equal(t, []string{"stop gateway", "stop minio"}, *calls)
}

// TestStartAfterStopStartsTheSameContainersAgain: a service restarts inside a
// run, and Start is all a caller sends then. MinIO comes back before the
// gateway that dials it. A Start that follows Init starts nothing twice.
func TestStartAfterStopStartsTheSameContainersAgain(t *testing.T) {
	rt, _, calls := newStoppableRuntime(t)
	ctx := context.Background()

	require.NoError(t, rt.resume(ctx))
	require.Empty(t, *calls, "containers Init just started are not started again")

	_, err := rt.Stop(ctx, &runtimev0.StopRequest{})
	require.NoError(t, err)
	*calls = nil
	require.NoError(t, rt.resume(ctx))
	require.Equal(t, []string{"start minio", "start gateway"}, *calls)

	*calls = nil
	require.NoError(t, rt.resume(ctx))
	require.Empty(t, *calls)
}

// TestStopWithNothingStartedSucceeds: a Stop before Init, or with a cloud
// backend that runs no MinIO, has nothing or only the gateway to stop.
func TestStopWithNothingStartedSucceeds(t *testing.T) {
	rt := newNetworkRuntime(t, newFakeNetworks())
	resp, err := rt.Stop(context.Background(), &runtimev0.StopRequest{})
	require.NoError(t, err)
	require.Equal(t, runtimev0.StopStatus_SUCCESS, resp.GetStatus().GetState())
}
