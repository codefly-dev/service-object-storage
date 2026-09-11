package main

import (
	"os"
	"path/filepath"
	"testing"

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
