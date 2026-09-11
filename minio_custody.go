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
	Phase   string `json:"phase,omitempty"`
}

func minioCustodyDir(owner string) string {
	digest := sha256.Sum256([]byte(owner))
	return filepath.Join(resources.CodeflyHomeDir(), "object-storage", fmt.Sprintf("%x", digest))
}

// JSON encodes boundaries that Docker's display-name normalization erases.
func (s *Runtime) minioOwner() string {
	identity, _ := json.Marshal([4]string{s.Identity.Workspace, s.Identity.Module, s.Identity.Name, s.Environment.NamingScope})
	return string(identity)
}

func (s *Runtime) prepareMinIOData(ctx context.Context) (string, func(), error) {
	owner := s.minioOwner()
	displayName := dockerrun.ContainerName(s.UniqueWithWorkspace() + "-minio")
	root, err := filepath.Abs(minioCustodyDir(owner))
	if err != nil {
		return "", nil, err
	}
	cli, err := dockerrun.NewClient()
	if err != nil {
		return "", nil, err
	}
	defer func() { _ = cli.Close() }()
	if host := cli.DaemonHost(); !strings.HasPrefix(host, "unix://") && !strings.HasPrefix(host, "npipe://") {
		return "", nil, fmt.Errorf("local MinIO custody requires a Docker daemon sharing the agent host filesystem")
	}
	info, err := cli.Info(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("inspect Docker user mapping: %w", err)
	}
	if err = validateMinIODaemon(info.SecurityOptions); err != nil {
		return "", nil, err
	}
	if err = os.MkdirAll(filepath.Dir(root), 0o700); err != nil {
		return "", nil, err
	}
	// Serialize even identities whose legacy Docker display names collide.
	lock := flock.New(minioCustodyDir(displayName) + ".lock")
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
	// A v1 location cannot establish which structured identity owns it.
	if _, legacyErr := os.Lstat(minioCustodyDir(displayName)); !errors.Is(legacyErr, os.ErrNotExist) {
		return fail(fmt.Errorf("ambiguous legacy custody at %s; explicit verified recovery is required (stat: %v)", minioCustodyDir(displayName), legacyErr))
	}
	existing, err := cli.ContainerInspect(ctx, displayName)
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
	if err = ensureMinIOCustody(root, owner, s.conf.bucket, os.Getenv("SOS_LOCAL_MINIO_INITIALIZE") == "true", present); err != nil {
		return fail(err)
	}
	record, err := os.ReadFile(filepath.Join(root, "custody.json"))
	if err != nil {
		return fail(err)
	}
	var custody minioCustody
	if err = json.Unmarshal(record, &custody); err != nil {
		return fail(err)
	}
	s.minioNewStore = custody.Phase == "pending"
	s.minioCustodyRoot = root
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
		record, err = json.Marshal(minioCustody{Version: 2, Owner: owner, Bucket: bucket, ID: id, Phase: "pending"})
		if err != nil {
			return err
		}
		if err = os.Mkdir(data, 0o700); err != nil {
			return err
		}
		if err = writeCustodyRecord(markerPath, record); err != nil {
			return err
		}
		if err = writeCustodyRecord(recordPath, record); err != nil {
			return err
		}
		parent, openErr := os.Open(filepath.Dir(root))
		if openErr != nil {
			return openErr
		}
		syncErr := parent.Sync()
		closeErr := parent.Close()
		if err = errors.Join(syncErr, closeErr); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	var custody minioCustody
	if err = json.Unmarshal(record, &custody); err != nil {
		return fmt.Errorf("invalid custody record: %w", err)
	}
	if custody.Version != 2 || custody.Owner != owner || custody.Bucket != bucket || custody.ID == "" || (custody.Phase != "pending" && custody.Phase != "committed") {
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
	// The marker is immutable; the independent record commits provisioning.
	if actual.Phase != "pending" {
		return fmt.Errorf("invalid data marker phase at %s", markerPath)
	}
	actual.Phase = custody.Phase
	if actual != custody {
		return fmt.Errorf("data marker disagrees with custody record at %s", markerPath)
	}
	return nil
}

// Rename and fsync make pending -> committed a single durable transition.
// A failed update leaves evidence and never grants permission to reinitialize.
func writeCustodyRecord(path string, record []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".custody-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(file.Name()) }()
	if _, err = file.Write(record); err != nil {
		_ = file.Close()
		return err
	}
	if err = file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = os.Rename(file.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}

func (s *Runtime) commitMinIOProvisioning() error {
	path := filepath.Join(s.minioCustodyRoot, "custody.json")
	record, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var custody minioCustody
	if err = json.Unmarshal(record, &custody); err != nil {
		return err
	}
	custody.Phase = "committed"
	record, err = json.Marshal(custody)
	if err != nil {
		return err
	}
	if err = writeCustodyRecord(path, record); err != nil {
		return err
	}
	s.minioNewStore = false
	return nil
}

// Host UID:GID is valid only without a daemon-wide user namespace translation.
// Reject before creating locks or custody; never chown retained data to guess.
func validateMinIODaemon(options []string) error {
	for _, option := range options {
		name := strings.SplitN(strings.TrimPrefix(option, "name="), ",", 2)[0]
		if name == "rootless" || name == "userns" {
			return fmt.Errorf("local MinIO custody does not support Docker %s UID mapping; use a local daemon without rootless/userns-remap, or configure an external storage backend; retained data was not changed", name)
		}
	}
	return nil
}
