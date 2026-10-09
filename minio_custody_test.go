package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	"github.com/codefly-dev/core/resources"
	"github.com/codefly-dev/core/runners/dockerrun"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/gofrs/flock"
	"github.com/stretchr/testify/require"
)

func TestMinIOCustody(t *testing.T) {
	for _, scenario := range []string{"reuse", "missing record", "missing data", "missing marker", "mismatched marker", "owner", "bucket", "symlink", "corrupt record"} {
		t.Run(scenario, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "store")
			require.NoError(t, ensureMinIOCustody(root, "owner", "documents", false))
			record := filepath.Join(root, "custody.json")
			marker := filepath.Join(root, "data", ".codefly-custody.json")
			object := filepath.Join(root, "data", "retained-object")
			require.NoError(t, os.WriteFile(object, []byte("retain me"), 0o600))
			owner, bucket := "owner", "documents"
			switch scenario {
			case "missing record":
				require.NoError(t, os.Remove(record))
			case "missing data":
				require.NoError(t, os.Rename(filepath.Join(root, "data"), filepath.Join(root, "retained")))
			case "missing marker":
				require.NoError(t, os.Remove(marker))
			case "mismatched marker":
				require.NoError(t, os.WriteFile(marker, []byte(`{"version":1,"owner":"other"}`), 0o600))
			case "owner":
				owner = "other"
			case "bucket":
				bucket = "other"
			case "symlink":
				require.NoError(t, os.Rename(filepath.Join(root, "data"), filepath.Join(root, "retained")))
				require.NoError(t, os.Symlink(filepath.Join(root, "retained"), filepath.Join(root, "data")))
			case "corrupt record":
				require.NoError(t, os.WriteFile(record, []byte("bad"), 0o600))
			}
			err := ensureMinIOCustody(root, owner, bucket, false)
			if scenario == "reuse" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			if scenario == "missing data" {
				object = filepath.Join(root, "retained", "retained-object")
			}
			body, err := os.ReadFile(object)
			require.NoError(t, err)
			require.Equal(t, "retain me", string(body))
		})
	}
}

// An absent or empty custody directory holds nothing to lose, so it is
// initialized without an opt-in: that is every first run, including agent CI in
// a throwaway Codefly home.
func TestMinIOCustodyInitializesNewStore(t *testing.T) {
	for name, prepare := range map[string]func(root string){
		"absent": func(string) {},
		"empty":  func(root string) { require.NoError(t, os.Mkdir(root, 0o700)) },
	} {
		t.Run(name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "store")
			prepare(root)
			require.NoError(t, ensureMinIOCustody(root, "owner", "documents", false))
			record, err := os.ReadFile(filepath.Join(root, "custody.json"))
			require.NoError(t, err)
			require.Contains(t, string(record), `"phase":"pending"`)
			marker, err := os.ReadFile(filepath.Join(root, "data", ".codefly-custody.json"))
			require.NoError(t, err)
			require.Equal(t, string(record), string(marker))
		})
	}
}

// A directory holding anything without a custody record is existing data: the
// guard refuses and leaves it exactly as it found it.
func TestMinIOCustodyRefusesUnrecordedData(t *testing.T) {
	for name, prepare := range map[string]func(root string){
		"retained file": func(root string) {
			require.NoError(t, os.WriteFile(filepath.Join(root, "retained-object"), []byte("retain me"), 0o600))
		},
		"retained data dir": func(root string) {
			require.NoError(t, os.Mkdir(filepath.Join(root, "data"), 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(root, "data", "retained-object"), []byte("retain me"), 0o600))
		},
	} {
		t.Run(name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "store")
			require.NoError(t, os.Mkdir(root, 0o700))
			prepare(root)
			require.ErrorContains(t, ensureMinIOCustody(root, "owner", "documents", false), "existing data")
			_, err := os.Stat(filepath.Join(root, "custody.json"))
			require.True(t, os.IsNotExist(err), "no custody may be written over unrecorded data")
		})
	}
	t.Run("symlink", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(dir, "elsewhere")
		require.NoError(t, os.Mkdir(target, 0o700))
		root := filepath.Join(dir, "store")
		require.NoError(t, os.Symlink(target, root))
		require.Error(t, ensureMinIOCustody(root, "owner", "documents", false))
		_, err := os.Stat(filepath.Join(target, "custody.json"))
		require.True(t, os.IsNotExist(err))
	})
	t.Run("live container", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "store")
		require.Error(t, ensureMinIOCustody(root, "owner", "documents", true))
		_, err := os.Stat(root)
		require.True(t, os.IsNotExist(err))
	})
}

// An established store is reused as-is: a second pass keeps its custody ID.
func TestMinIOCustodyReusesExistingRecord(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	require.NoError(t, ensureMinIOCustody(root, "owner", "documents", false))
	first, err := os.ReadFile(filepath.Join(root, "custody.json"))
	require.NoError(t, err)
	require.NoError(t, ensureMinIOCustody(root, "owner", "documents", false))
	second, err := os.ReadFile(filepath.Join(root, "custody.json"))
	require.NoError(t, err)
	require.Equal(t, string(first), string(second))
}

// A renamed bucket is a configuration edit. Reporting it as a generic custody
// mismatch sent operators to hand-edit the record the docs forbid touching.
func TestMinIOCustodyNamesBucketChange(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	require.NoError(t, ensureMinIOCustody(root, "owner", "documents", false))
	err := ensureMinIOCustody(root, "owner", "documents-v2", false)
	require.ErrorContains(t, err, "configured bucket changed")
	require.ErrorContains(t, err, "documents-v2")
	require.NotContains(t, err.Error(), "owner, version or phase mismatch")
}

// A concurrent run of the same service is provisioning the same store. The wait
// is bounded, so a stuck peer still surfaces instead of hanging the agent.
func TestMinIOCustodyLockWaits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.lock")
	held := flock.New(path)
	locked, err := held.TryLock()
	require.NoError(t, err)
	require.True(t, locked)
	defer func() { _ = held.Unlock() }()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	got, err := flock.New(path).TryLockContext(ctx, minioCustodyLockRetry)
	require.False(t, got, "a held lock must not be acquired")
	require.Error(t, err)
	require.GreaterOrEqual(t, time.Since(start), 200*time.Millisecond, "it must wait, not fail instantly")
}

func TestMinIORejectsUnownedMount(t *testing.T) {
	for _, m := range []container.MountPoint{
		{Type: mount.TypeVolume, Name: "retained", Destination: "/data", RW: true},
		{Type: mount.TypeBind, Source: "/wrong", Destination: "/data", RW: true},
		{Type: mount.TypeBind, Source: "/owned", Destination: "/data", RW: false},
		{Type: mount.TypeBind, Source: "/owned", Destination: "/elsewhere", RW: true},
	} {
		require.Error(t, validateMinIOMount(container.InspectResponse{ContainerJSONBase: &container.ContainerJSONBase{ID: "disposable"}, Mounts: []container.MountPoint{m}}, "/owned"))
	}
	require.Error(t, validateMinIOMount(container.InspectResponse{ContainerJSONBase: &container.ContainerJSONBase{ID: "disposable"}}, "/owned"))
	require.NoError(t, validateMinIOMount(container.InspectResponse{ContainerJSONBase: &container.ContainerJSONBase{ID: "disposable"}, Mounts: []container.MountPoint{{Type: mount.TypeBind, Source: "/owned", Destination: "/data", RW: true}}}, "/owned"))
}

func TestStructuredMinIOIdentity(t *testing.T) {
	t.Setenv("CODEFLY_HOME", t.TempDir())
	first, second := NewRuntime(), NewRuntime()
	first.Environment, second.Environment = &basev0.Environment{}, &basev0.Environment{}
	first.Identity = &resources.ServiceIdentity{Workspace: "wiki", Module: "documents-archive", Name: "object-storage"}
	second.Identity = &resources.ServiceIdentity{Workspace: "wiki", Module: "documents", Name: "archive-object-storage"}
	require.Equal(t, dockerrun.ContainerName(first.UniqueWithWorkspace()), dockerrun.ContainerName(second.UniqueWithWorkspace()))
	require.NotEqual(t, first.minioOwner(), second.minioOwner())
	root := minioCustodyDir(first.minioOwner())
	require.NoError(t, os.MkdirAll(filepath.Dir(root), 0o700))
	require.NoError(t, ensureMinIOCustody(root, first.minioOwner(), "documents", false))
	require.Error(t, ensureMinIOCustody(root, second.minioOwner(), "documents", false))
	require.NoError(t, ensureMinIOCustody(minioCustodyDir(second.minioOwner()), second.minioOwner(), "documents", false), "a distinct owner gets its own store")
	second.Identity = first.Identity
	second.Environment.NamingScope = "isolated"
	require.NotEqual(t, first.minioOwner(), second.minioOwner())
}

// fakeProbe stands in for the probe container: per candidate user it either
// refuses the write, fails as Docker would, or writes a file the fake host
// stat reports as owned by owner.
type fakeProbe struct {
	t       *testing.T
	parent  string
	outcome map[string]fakeOutcome
	tried   []string
	dirs    []string
	last    string
}

type fakeOutcome struct {
	wrote bool
	owner int
	err   error
}

func (f *fakeProbe) run(_ context.Context, user, dir string) (minioProbeRun, error) {
	f.tried = append(f.tried, user)
	f.dirs = append(f.dirs, dir)
	f.last = user
	require.Equal(f.t, f.parent, filepath.Dir(dir), "the probe binds a scratch directory under the custody parent")
	info, err := os.Stat(dir)
	require.NoError(f.t, err)
	require.Equal(f.t, os.FileMode(0o700), info.Mode().Perm())
	outcome := f.outcome[user]
	if outcome.err != nil {
		return minioProbeRun{}, outcome.err
	}
	run := minioProbeRun{Wrote: outcome.wrote, Mapping: "0 1001 1\n1 165536 65536\n"}
	if !outcome.wrote {
		run.Stderr = "sh: /probe/owner: Permission denied\n"
		return run, nil
	}
	require.NoError(f.t, os.WriteFile(filepath.Join(dir, minioProbeFile), nil, 0o600))
	return run, nil
}

func (f *fakeProbe) ownerOf(path string) (int, int, error) {
	if _, err := os.Lstat(path); err != nil {
		return 0, 0, err
	}
	return f.outcome[f.last].owner, 1002, nil
}

func TestMinIOContainerUserIsMeasured(t *testing.T) {
	for _, tc := range []struct {
		name     string
		outcome  map[string]fakeOutcome
		user     string
		tried    []string
		refusal  []string
		probeErr string
	}{
		{name: "one-to-one mapping", outcome: map[string]fakeOutcome{"1001:1002": {wrote: true, owner: 1001}}, user: "1001:1002", tried: []string{"1001:1002"}},
		{name: "rootless run by the agent", outcome: map[string]fakeOutcome{"1001:1002": {}, "0:0": {wrote: true, owner: 1001}}, user: "0:0", tried: []string{"1001:1002", "0:0"}},
		{name: "own ids land as a foreign owner", outcome: map[string]fakeOutcome{"1001:1002": {wrote: true, owner: 166537}, "0:0": {wrote: true, owner: 1001}}, user: "0:0", tried: []string{"1001:1002", "0:0"}},
		{name: "userns-remap", outcome: map[string]fakeOutcome{"1001:1002": {}, "0:0": {}}, tried: []string{"1001:1002", "0:0"},
			refusal: []string{"uid 1001", "--user 1001:1002 could not write", "Permission denied", "container ID map: 0 1001 1 1 165536 65536", "retained data was not changed"}},
		{name: "foreign owners only", outcome: map[string]fakeOutcome{"1001:1002": {wrote: true, owner: 166537}, "0:0": {wrote: true, owner: 0}}, tried: []string{"1001:1002", "0:0"},
			refusal: []string{"--user 1001:1002 wrote a file the host sees as 166537:1002", "--user 0:0 wrote a file the host sees as 0:1002"}},
		{name: "docker failure is not a refusal", outcome: map[string]fakeOutcome{"1001:1002": {err: errors.New("daemon gone")}}, tried: []string{"1001:1002"}, probeErr: "daemon gone"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := t.TempDir()
			probe := &fakeProbe{t: t, parent: parent, outcome: tc.outcome}
			user, _, err := chooseMinIOContainerUser(context.Background(), probe, probe.ownerOf, parent, 1001, 1002)
			require.Equal(t, tc.tried, probe.tried, "candidates are tried in order and the first that lands wins")
			switch {
			case tc.probeErr != "":
				require.ErrorContains(t, err, tc.probeErr)
			case tc.refusal != nil:
				for _, want := range tc.refusal {
					require.ErrorContains(t, err, want)
				}
			default:
				require.NoError(t, err)
				require.Equal(t, tc.user, user)
			}
			entries, err := os.ReadDir(parent)
			require.NoError(t, err)
			require.Empty(t, entries, "every probe directory is removed, whatever owner its file landed as")
			require.Len(t, probe.dirs, len(probe.tried))
			if len(probe.dirs) == 2 {
				require.NotEqual(t, probe.dirs[0], probe.dirs[1], "each candidate writes into a fresh directory")
			}
		})
	}
}

func TestHostFileOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owned")
	require.NoError(t, os.WriteFile(path, nil, 0o600))
	uid, _, err := hostFileOwner(path)
	require.NoError(t, err)
	require.Equal(t, os.Geteuid(), uid)
}

func TestMinIOProvisioningCommit(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	require.NoError(t, ensureMinIOCustody(root, "owner", "documents", false))
	require.NoError(t, ensureMinIOCustody(root, "owner", "documents", false), "verified pending bootstrap can resume")
	rt := &Runtime{minioCustodyRoot: root, minioNewStore: true}
	require.NoError(t, rt.commitMinIOProvisioning())
	require.False(t, rt.minioNewStore)
	require.NoError(t, ensureMinIOCustody(root, "owner", "documents", false))
	record, err := os.ReadFile(filepath.Join(root, "custody.json"))
	require.NoError(t, err)
	require.Contains(t, string(record), `"phase":"committed"`)
	require.NoError(t, os.Remove(filepath.Join(root, "custody.json")))
	require.Error(t, ensureMinIOCustody(root, "owner", "documents", false), "loss of commit evidence cannot reopen bootstrap")
}

// A daemon on which no candidate writes as the agent (userns-remap here: the
// probe's write is refused) is rejected after the real Docker probe protocol
// and before a lock, a custody record or retained data exists.
func TestMinIORejectsUserMappingBeforeCustodyMutation(t *testing.T) {
	// Canonical: the mount source the runtime reports is symlink-resolved, and
	// macOS keeps TempDir under /var → /private/var.
	home, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	t.Setenv("CODEFLY_HOME", home)
	// Short independent socket path fits Darwin's sockaddr_un limit.
	socketDir, err := os.MkdirTemp("/tmp", "sos27-daemon-")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(socketDir)) }()
	listener, err := net.Listen("unix", filepath.Join(socketDir, "docker.sock"))
	require.NoError(t, err)
	var mu sync.Mutex
	var created, removed []string
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("API-Version", "1.47")
		mu.Lock()
		defer mu.Unlock()
		path := req.URL.Path
		switch {
		case strings.HasSuffix(path, "/_ping"):
		case strings.HasSuffix(path, "/images/json"):
			_, _ = fmt.Fprintf(w, `[{"RepoTags":[%q]}]`, minioImage.FullName())
		case strings.HasSuffix(path, "/containers/create"):
			var body struct {
				User       string
				Entrypoint []string
				HostConfig struct{ Mounts []mount.Mount }
			}
			require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
			require.Len(t, body.HostConfig.Mounts, 1)
			require.Equal(t, "/probe", body.HostConfig.Mounts[0].Target)
			require.Equal(t, filepath.Join(home, "object-storage"), filepath.Dir(body.HostConfig.Mounts[0].Source))
			created = append(created, body.User)
			_, _ = fmt.Fprintf(w, `{"Id":"probe%d"}`, len(created))
		case strings.HasSuffix(path, "/wait"):
			_, _ = w.Write([]byte(`{"StatusCode":1}`))
		case strings.HasSuffix(path, "/start"):
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(path, "/logs"):
			w.Header().Set("Content-Type", "application/vnd.docker.raw-stream")
			for stream, text := range map[byte]string{1: "0 165536 65536\n", 2: "sh: /probe/owner: Permission denied\n"} {
				_, _ = w.Write(append([]byte{stream, 0, 0, 0, 0, 0, 0, byte(len(text))}, text...))
			}
		case req.Method == http.MethodDelete && strings.Contains(path, "/containers/probe"):
			removed = append(removed, path)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected Docker operation: %s %s", req.Method, path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	require.NoError(t, server.Listener.Close())
	server.Listener = listener
	server.Start()
	defer server.Close()
	t.Setenv("DOCKER_HOST", "unix://"+listener.Addr().String())
	rt := NewRuntime()
	rt.Identity = &resources.ServiceIdentity{Workspace: "test", Module: "test", Name: "store"}
	rt.Environment = &basev0.Environment{}
	_, _, err = rt.prepareMinIOData(context.Background())
	require.ErrorContains(t, err, "offers none")
	require.ErrorContains(t, err, "container ID map: 0 165536 65536")
	require.ErrorContains(t, err, "Permission denied")
	require.Equal(t, []string{fmt.Sprintf("%d:%d", os.Geteuid(), os.Getegid()), "0:0"}, created)
	require.Len(t, removed, 2, "every probe container is removed")
	entries, err := os.ReadDir(filepath.Join(home, "object-storage"))
	require.NoError(t, err)
	require.Empty(t, entries, "rejected mapping must not create even a custody lock")
}
