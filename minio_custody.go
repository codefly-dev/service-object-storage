package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/codefly-dev/core/resources"
	"github.com/codefly-dev/core/runners/dockerrun"
	"github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/gofrs/flock"
)

// Custody lives outside source checkouts and runtime caches. Destroy removes
// containers only. The independent record and in-data marker must agree before
// Docker gets an opportunity to replace any container.
type minioCustody struct {
	Version int    `json:"version"`
	Owner   string `json:"owner"`
	Bucket  string `json:"bucket"`
	ID      string `json:"id"`
}

func minioCustodyDir(owner string) string {
	digest := sha256.Sum256([]byte(owner))
	return filepath.Join(resources.CodeflyHomeDir(), "object-storage", fmt.Sprintf("%x", digest))
}

func (s *Runtime) prepareMinIOData(ctx context.Context) (string, func(), error) {
	owner := dockerrun.ContainerName(s.UniqueWithWorkspace() + "-minio")
	root, err := filepath.Abs(minioCustodyDir(owner))
	if err != nil {
		return "", nil, err
	}
	if err = os.MkdirAll(filepath.Dir(root), 0o700); err != nil {
		return "", nil, err
	}
	lock := flock.New(root + ".lock")
	locked, err := lock.TryLock()
	if err != nil {
		return "", nil, err
	}
	if !locked {
		return "", nil, fmt.Errorf("MinIO custody for %s is in use", owner)
	}
	release := func() { _ = lock.Unlock() }
	fail := func(err error) (string, func(), error) {
		release()
		return "", nil, fmt.Errorf("MinIO custody: %w; preserve existing data and follow README recovery instructions", err)
	}
	cli, err := dockerrun.NewClient()
	if err != nil {
		return fail(err)
	}
	defer func() { _ = cli.Close() }()
	if host := cli.DaemonHost(); !strings.HasPrefix(host, "unix://") && !strings.HasPrefix(host, "npipe://") {
		return fail(fmt.Errorf("local MinIO custody requires a Docker daemon sharing the agent host filesystem"))
	}
	existing, err := cli.ContainerInspect(ctx, owner)
	present := err == nil
	if err != nil && !errdefs.IsNotFound(err) {
		return fail(err)
	}
	data := filepath.Join(root, "data")
	// Reject legacy anonymous volumes and conflicting binds before provisioning
	// or core's configuration fingerprint can remove their container.
	if present {
		if err = validateMinIOMount(existing, data); err != nil {
			return fail(err)
		}
	}
	_, recordErr := os.Stat(filepath.Join(root, "custody.json"))
	s.minioNewStore = errors.Is(recordErr, os.ErrNotExist)
	if err = ensureMinIOCustody(root, owner, s.conf.bucket, os.Getenv("SOS_LOCAL_MINIO_INITIALIZE") == "true", present); err != nil {
		return fail(err)
	}
	return data, release, nil
}

func validateMinIOMount(existing container.InspectResponse, data string) error {
	for _, m := range existing.Mounts {
		if m.Destination == "/data" {
			if m.Type == mount.TypeBind && filepath.Clean(m.Source) == data && m.RW {
				return nil
			}
			return fmt.Errorf("existing container %s has a different /data mount (type %s, source %s, volume %s); refusing replacement", existing.ID, m.Type, m.Source, m.Name)
		}
	}
	return fmt.Errorf("existing container %s has no owned /data mount; refusing replacement", existing.ID)
}

func ensureMinIOCustody(root, owner, bucket string, initialize, present bool) error {
	recordPath := filepath.Join(root, "custody.json")
	data := filepath.Join(root, "data")
	markerPath := filepath.Join(data, ".codefly-custody.json")
	record, err := os.ReadFile(recordPath)
	if errors.Is(err, os.ErrNotExist) {
		if !initialize || present {
			return fmt.Errorf("missing custody record at %s (new stores require explicit SOS_LOCAL_MINIO_INITIALIZE=true)", recordPath)
		}
		// Mkdir, not MkdirAll: partial state or retained data must never be adopted
		// automatically, even with initialization enabled.
		if err = os.Mkdir(root, 0o700); err != nil {
			return fmt.Errorf("refusing to initialize existing or inaccessible custody directory %s: %w", root, err)
		}
		id, genErr := randomSecret()
		if genErr != nil {
			return genErr
		}
		record, err = json.Marshal(minioCustody{Version: 1, Owner: owner, Bucket: bucket, ID: id})
		if err != nil {
			return err
		}
		if err = os.Mkdir(data, 0o700); err != nil {
			return err
		}
		if err = os.WriteFile(markerPath, record, 0o600); err != nil {
			return err
		}
		if err = os.WriteFile(recordPath, record, 0o600); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	var custody minioCustody
	if err = json.Unmarshal(record, &custody); err != nil {
		return fmt.Errorf("invalid custody record: %w", err)
	}
	if custody.Version != 1 || custody.Owner != owner || custody.Bucket != bucket || custody.ID == "" {
		return fmt.Errorf("custody record owner, bucket, or version mismatch at %s", recordPath)
	}
	// Refuse symlink substitutions and missing data; Docker must not create an
	// empty directory as a side effect of mounting a lost location.
	for _, p := range []string{root, data, recordPath, markerPath} {
		info, statErr := os.Lstat(p)
		if statErr != nil {
			return statErr
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("custody path is a symlink: %s", p)
		}
	}
	marker, err := os.ReadFile(markerPath)
	if err != nil {
		return err
	}
	var actual minioCustody
	if err = json.Unmarshal(marker, &actual); err != nil {
		return err
	}
	if actual != custody {
		return fmt.Errorf("data marker disagrees with custody record at %s", markerPath)
	}
	return nil
}
