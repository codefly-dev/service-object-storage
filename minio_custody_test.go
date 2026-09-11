package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	"github.com/codefly-dev/core/resources"
	"github.com/codefly-dev/core/runners/dockerrun"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/stretchr/testify/require"
)

func TestMinIOCustody(t *testing.T) {
	for _, scenario := range []string{"reuse", "missing record", "missing data", "missing marker", "mismatched marker", "owner", "bucket", "symlink", "corrupt record"} {
		t.Run(scenario, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "store")
			require.NoError(t, ensureMinIOCustody(root, "owner", "documents", true, false))
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
			err := ensureMinIOCustody(root, owner, bucket, true, false)
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

func TestMinIOCustodyRequiresExplicitNewStore(t *testing.T) {
	for _, present := range []bool{false, true} {
		root := filepath.Join(t.TempDir(), "store")
		require.Error(t, ensureMinIOCustody(root, "owner", "documents", false, present))
		_, err := os.Stat(root)
		require.True(t, os.IsNotExist(err))
		if present {
			require.Error(t, ensureMinIOCustody(root, "owner", "documents", true, present))
		}
	}
	root := filepath.Join(t.TempDir(), "partial")
	require.NoError(t, os.Mkdir(root, 0o700))
	require.Error(t, ensureMinIOCustody(root, "owner", "documents", true, false))
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
	require.NoError(t, ensureMinIOCustody(root, first.minioOwner(), "documents", true, false))
	require.Error(t, ensureMinIOCustody(root, second.minioOwner(), "documents", false, false))
	require.Error(t, ensureMinIOCustody(minioCustodyDir(second.minioOwner()), second.minioOwner(), "documents", false, false))
	second.Identity = first.Identity
	second.Environment.NamingScope = "isolated"
	require.NotEqual(t, first.minioOwner(), second.minioOwner())
}

func TestMinIODaemonUserMapping(t *testing.T) {
	for _, option := range []string{"name=rootless", "name=userns", "name=rootless,version=1", "userns"} {
		require.ErrorContains(t, validateMinIODaemon([]string{"name=seccomp,profile=builtin", option}), "UID mapping")
	}
	require.NoError(t, validateMinIODaemon([]string{"name=seccomp,profile=builtin", "name=cgroupns", "name=apparmor"}))
}

func TestMinIOProvisioningCommit(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	require.NoError(t, ensureMinIOCustody(root, "owner", "documents", true, false))
	require.NoError(t, ensureMinIOCustody(root, "owner", "documents", false, false), "verified pending bootstrap can resume")
	rt := &Runtime{minioCustodyRoot: root, minioNewStore: true}
	require.NoError(t, rt.commitMinIOProvisioning())
	require.False(t, rt.minioNewStore)
	require.NoError(t, ensureMinIOCustody(root, "owner", "documents", false, false))
	record, err := os.ReadFile(filepath.Join(root, "custody.json"))
	require.NoError(t, err)
	require.Contains(t, string(record), `"phase":"committed"`)
	require.NoError(t, os.Remove(filepath.Join(root, "custody.json")))
	require.Error(t, ensureMinIOCustody(root, "owner", "documents", true, false), "loss of commit evidence cannot reopen bootstrap")
}

func TestMinIORejectsUserMappingBeforeCustodyMutation(t *testing.T) {
	for _, mode := range []string{"rootless", "userns"} {
		t.Run(mode, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("CODEFLY_HOME", home)
			// Short independent socket path fits Darwin's sockaddr_un limit.
			socketDir, err := os.MkdirTemp("/tmp", "sos27-daemon-")
			require.NoError(t, err)
			defer func() { require.NoError(t, os.RemoveAll(socketDir)) }()
			listener, err := net.Listen("unix", filepath.Join(socketDir, "docker.sock"))
			require.NoError(t, err)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("API-Version", "1.47")
				if strings.HasSuffix(req.URL.Path, "/_ping") {
					return
				}
				if !strings.HasSuffix(req.URL.Path, "/info") {
					t.Errorf("unexpected Docker operation: %s", req.URL.Path)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				_, _ = w.Write([]byte(`{"SecurityOptions":["name=` + mode + `"]}`))
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
			require.ErrorContains(t, err, "UID mapping")
			entries, err := os.ReadDir(home)
			require.NoError(t, err)
			require.Empty(t, entries, "rejected mapping must not create even a custody lock")
		})
	}
}
