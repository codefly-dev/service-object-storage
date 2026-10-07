package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

// The probe writes exactly this file into its scratch directory, and nothing
// else, so the host can remove whatever owner it lands as by unlinking it from
// the agent-owned directory: no chown, no privileged cleanup.
const minioProbeFile = "owner"

// The script prints the container's ID maps for diagnostics, then writes. Only
// the write decides: a VM or file-sharing layer (Docker Desktop, Colima, WSL)
// translates ownership below the container's own namespace, which only a file
// on the host shows. The image's entrypoint is a shell script, so the shell is
// already a dependency of the image the agent runs.
const minioProbeScript = `cat /proc/self/uid_map /proc/self/gid_map 2>/dev/null; : > /probe/` + minioProbeFile

// minioProbeRun is what one probe container reported.
type minioProbeRun struct {
	// Wrote reports whether the container's write succeeded.
	Wrote bool
	// Mapping is its uid_map and gid_map; Stderr says why a write failed.
	Mapping, Stderr string
}

// minioProbeRunner runs one short-lived container as user with dir bind-mounted
// at /probe, which tries to create the probe file. err is a Docker failure,
// never a refused write.
type minioProbeRunner interface {
	run(ctx context.Context, user, dir string) (minioProbeRun, error)
}

// chooseMinIOContainerUser measures, rather than infers, which container user
// writes bind-mounted files as the agent's own host user, so the custody
// directory MinIO fills stays owned by that user under any daemon: rootful,
// rootless, userns-remap, Podman, or a VM-backed Desktop.
//
// Each candidate writes one file into a fresh 0700 scratch directory under
// parent, and the host stats it. The agent's own euid:egid is tried first
// (a one-to-one mapping), then 0:0 (a rootless daemon run by the agent's user
// maps container root to it). The first whose file the host sees as the
// agent's euid wins. The group is reported but not required: on macOS and
// under a setgid directory a new file takes its directory's group, not the
// writer's. Nothing is cached: core fingerprints the container user, so a
// daemon whose mapping changed gets its MinIO container recreated with the
// newly measured user. When no candidate lands as the agent, the error names
// what each one observed and nothing outside the scratch directories has been
// touched; retained data is never chowned to guess.
func chooseMinIOContainerUser(ctx context.Context, probe minioProbeRunner, ownerOf func(string) (uid, gid int, err error), parent string, euid, egid int) (user, mapping string, err error) {
	var observed []string
	for _, candidate := range []string{fmt.Sprintf("%d:%d", euid, egid), "0:0"} {
		result, mapping, err := probeMinIOUser(ctx, probe, ownerOf, parent, candidate, euid)
		if err != nil {
			return "", "", err
		}
		if result == "" {
			return candidate, mapping, nil
		}
		observed = append(observed, fmt.Sprintf("--user %s %s (container ID map: %s)", candidate, result, mapping))
	}
	return "", "", fmt.Errorf("local MinIO custody needs a container user whose files the host sees as uid %d, and this Docker daemon offers none: %s; use a daemon that maps a container user to this user (rootful without userns-remap, or rootless run by this user), or configure an external storage backend; retained data was not changed", euid, strings.Join(observed, "; "))
}

// probeMinIOUser returns "" when candidate's file lands as euid, or what
// happened instead. err is a failure to measure at all.
func probeMinIOUser(ctx context.Context, probe minioProbeRunner, ownerOf func(string) (int, int, error), parent, candidate string, euid int) (result, mapping string, err error) {
	scratch, err := os.MkdirTemp(parent, ".user-probe-")
	if err != nil {
		return "", "", fmt.Errorf("create container user probe directory: %w", err)
	}
	defer func() {
		// The directory is the agent's and holds at most the probe file, which
		// unlinking needs no ownership of.
		if removeErr := os.RemoveAll(scratch); removeErr != nil && err == nil {
			err = fmt.Errorf("remove container user probe directory %s: %w", scratch, removeErr)
		}
	}()
	run, err := probe.run(ctx, candidate, scratch)
	if err != nil {
		return "", "", fmt.Errorf("probe container user %s: %w", candidate, err)
	}
	mapping = strings.Join(strings.Fields(run.Mapping), " ")
	if mapping == "" {
		mapping = "unavailable"
	}
	if !run.Wrote {
		result = "could not write to an agent-owned directory"
		if detail := strings.TrimSpace(run.Stderr); detail != "" {
			result += " (" + detail + ")"
		}
		return result, mapping, nil
	}
	uid, gid, err := ownerOf(filepath.Join(scratch, minioProbeFile))
	if err != nil {
		return "", mapping, fmt.Errorf("inspect container user probe file: %w", err)
	}
	if uid != euid {
		return fmt.Sprintf("wrote a file the host sees as %d:%d", uid, gid), mapping, nil
	}
	return "", mapping, nil
}

// hostFileOwner reports the numeric owner of path as this host sees it. The
// agent ships for Linux and macOS, where Stat_t carries both.
func hostFileOwner(path string) (uid, gid int, err error) {
	info, err := os.Lstat(path)
	if err != nil {
		return 0, 0, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, fmt.Errorf("no numeric owner for %s", path)
	}
	return int(stat.Uid), int(stat.Gid), nil
}

// dockerProbeRunner runs the probe from the MinIO image the custody container
// itself uses, so it costs no image beyond the one being pulled anyway.
type dockerProbeRunner struct {
	cli   *client.Client
	image string
}

const minioProbeTimeout = 60 * time.Second

func (p dockerProbeRunner) run(ctx context.Context, user, dir string) (result minioProbeRun, err error) {
	ctx, cancel := context.WithTimeout(ctx, minioProbeTimeout)
	defer cancel()
	created, err := p.cli.ContainerCreate(ctx, &container.Config{
		Image:      p.image,
		User:       user,
		Entrypoint: []string{"/bin/sh", "-c", minioProbeScript},
		Cmd:        []string{},
	}, &container.HostConfig{
		Mounts:      []mount.Mount{{Type: mount.TypeBind, Source: dir, Target: "/probe"}},
		NetworkMode: "none",
	}, nil, nil, "")
	if err != nil {
		return result, err
	}
	defer func() {
		// Removal outlives a cancelled caller so a probe never leaks a container.
		removeCtx, cancelRemove := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancelRemove()
		if removeErr := p.cli.ContainerRemove(removeCtx, created.ID, container.RemoveOptions{Force: true}); removeErr != nil && err == nil {
			err = fmt.Errorf("remove probe container %s: %w", created.ID, removeErr)
		}
	}()
	waitC, errC := p.cli.ContainerWait(ctx, created.ID, container.WaitConditionNextExit)
	if err = p.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return result, err
	}
	var exit container.WaitResponse
	select {
	case exit = <-waitC:
	case err = <-errC:
		return result, err
	}
	if exit.Error != nil {
		return result, errors.New(exit.Error.Message)
	}
	logs, err := p.cli.ContainerLogs(ctx, created.ID, container.LogsOptions{ShowStdout: true, ShowStderr: true})
	if err != nil {
		return result, err
	}
	defer func() { _ = logs.Close() }()
	var stdout, stderr bytes.Buffer
	if _, err = stdcopy.StdCopy(&stdout, &stderr, logs); err != nil {
		return result, err
	}
	return minioProbeRun{Wrote: exit.StatusCode == 0, Mapping: stdout.String(), Stderr: stderr.String()}, nil
}
