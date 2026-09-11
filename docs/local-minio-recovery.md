# Local MinIO custody and recovery

Owner: `service-object-storage`'s `Runtime.startLocalMinIO`, confirmed in the
`v0.0.1` tag used by Wiki and the current source. Core v0.3.20's
`dockerrun.GetContainer` correctly replaces a container when its runtime
fingerprint changes (including the random root password and allocated port).
Previously the agent declared no `/data` mount, so MinIO's image volume became
an anonymous Docker volume. Core retained the old volume but the next container
received a new empty one. Keeping credentials fixed would hide the trigger and
weaken session revocation; it would not establish data custody.

The agent now declares a bind mount at `/data`, backed by:

```
<CODEFLY_HOME>/object-storage/<sha256(container-name)>/data
<CODEFLY_HOME>/object-storage/<sha256(container-name)>/custody.json
```

`CODEFLY_HOME` defaults to `~/.codefly`. The container name is Core's name for
`UniqueWithWorkspace() + "-minio"`, including the workspace, module, service and
naming scope. This is durable local data, **not a runtime cache**. Preserve the
whole custody directory in backups. MinIO runs with the agent process's effective
UID:GID so files remain manageable by that host user on Linux. The Runtime does
not recursively chown existing data or broaden its permissions. Stop, Destroy,
failed startup and normal configuration replacement never delete this directory. Keep the same
`CODEFLY_HOME` and naming scope across runs. A changed scope is a different store.
The Docker daemon must share the agent's host filesystem (local Docker or Docker
Desktop). This is not a remote-daemon storage provisioner.

The record binds the owner, bucket and format version to a random custody ID;
`data/.codefly-custody.json` must match. A process lock serializes provisioning
and MinIO initialization. Before Core can recreate a container, the agent checks
its actual `/data` mount against the declared location. Missing records, missing
or mismatched markers, symlinks, changed buckets and legacy volume mounts fail
visibly. An established store whose bucket is absent also fails rather than
creating a new empty bucket. The record is not an object inventory or a backup;
external deletion of individual objects remains an operator recovery concern.

## Provisioning a genuinely new store

Only after confirming that this is a new service/scope with no data to recover,
set `SOS_LOCAL_MINIO_INITIALIZE=true` **on the agent process** for its first run.
Unset it afterward. This is a bootstrap switch, not a gateway configuration or
an adoption switch. It cannot override an existing directory, legacy container,
or custody mismatch. A failed partial bootstrap leaves evidence and fails
closed on retry; inspect and preserve that directory before repairing it.
Existing local services need explicit recovery before adopting this release.

## Recover a retained Docker volume

These are operator instructions for a separately scheduled recovery, **not an
instruction to restart the shared Wiki during implementation or review**.
Coordinate that work through [Wiki handoff #133](https://github.com/obin-ai/core-solutions/issues/133)
and [object-storage #27](https://github.com/codefly-dev/service-object-storage/issues/27).
No recovery of the Wiki's retained objects is claimed by this PR.

1. Identify the retained source volume from the incident record and inspect its
   mount users. Record its exact name, owning service, bucket and MinIO image.
   Inspect only the necessary mount fields, not container environment variables.
   Verify that the volume already exists; never let a misspelled source name
   implicitly create a volume. Obtain a consistent offline snapshot: a volume
   still written by MinIO must be quiesced in an approved maintenance window or
   recovered from a consistent backup. Do not stop a shared container as a
   debugging shortcut. Retain the original volume and all keys and backups.
2. Create a **separate recovery naming scope** and provision a disposable empty
   candidate with the same bucket and compatible MinIO image using the bootstrap
   switch. Record the candidate custody directory from its `/data` mount. Unset
   the switch, then Destroy **only the candidate** so no process writes it. Keep
   its `custody.json` and matching data marker. Do not change document records,
   document versions, grants, tokens or Vault keys to make storage pass.
3. Create an empty staging directory alongside the candidate's `data` directory.
   Mount the already verified retained source volume **read-only** into a
   disposable copy container, and mount only that staging directory writable.
   Copy the complete filesystem, including `.minio.sys`, hidden files, bucket
   metadata and object versions. Run the following from the account that will run
   the candidate agent, with operator-verified variables. Only the new staging
   copy is assigned to that account; the retained source remains read-only:

   ```sh
   docker volume inspect "$recovery_source_volume" >/dev/null || exit 1
   mkdir -m 700 "$recovery_staging" || exit 1
   docker run --rm --network none --user 0:0 \
     -e RECOVERY_UID="$(id -u)" -e RECOVERY_GID="$(id -g)" \
     --mount "type=volume,source=$recovery_source_volume,target=/source,readonly,volume-nocopy" \
     --mount "type=bind,source=$recovery_staging,target=/destination" \
     busybox:1.36 sh -ec 'cp -a /source/. /destination/; chown -R "$RECOVERY_UID:$RECOVERY_GID" /destination'
   ```

   Check the copy command's status and verify a full relative-path/content-hash
   inventory from the read-only source against staging before proceeding. Do not log object contents or
   credentials. **Do not merge two MinIO data directories.** If the source has a
   custody marker, retain it separately as provenance. Copy the candidate's
   marker into staging only after validating the source snapshot. Keep the
   candidate's original `data` directory under a distinct retained name, then
   rename staging to `data` while the candidate remains stopped. Keep the
   `custody.json` and marker pair from the candidate together; do not hand-edit
   IDs or copy another service's custody record to suppress an error.
4. Start only the recovered candidate, with bootstrap disabled. Through its
   authenticated storage API, compare exact object keys, bytes, metadata and
   available version IDs with the source inventory and expected SQL references.
   Repeat after Stop/Start and configuration recreation. Preserve the previous
   directory, source volume and inventory until recovery has been reviewed.
   Raw filesystem restoration preserves versions; uploading every object again
   through Put would create new versions and is not equivalent recovery.
5. Adoption into the shared Wiki requires a separate reviewed rollout and
   maintenance decision. The default legacy mount guard deliberately blocks
   automatic replacement. Keep the original volume even after adoption; never
   run volume prune, anonymous-volume cleanup or broad container removal.

No storage credential is required for the offline copy. The normal Runtime
continues generating new MinIO credentials and gateway tokens; consumers must
use the secret-marked token supplied by the current run. Never use anonymous
gateway mode, print container environments, or paste keys into recovery logs.
