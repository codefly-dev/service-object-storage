#!/usr/bin/env bash
# Live proof for codefly-dev/service-object-storage#111: bound List/Stat/Get/
# GetMany through a gateway under `codefly run`, the tenant store a second
# object-storage instance. Run from the repository root while this workspace
# runs (gateway image and agent built from the commit under test):
#
#   services/tenant: codefly.dev/object-storage, no backend, endpoints grpc + `minio` (tcp, internal)
#   services/store:  codefly.dev/object-storage, service-dependency on tenant (grpc, minio), kind runtime
#   SOS_GATEWAY_IMAGE=<image> codefly run service store --plugin-path <home> --local-agents
#
# Prints a transcript to stdout with the tenant's per-run secret replaced by
# <tenant secret>. It reads every value it needs from the running containers;
# nothing is hard-coded but the container names and the ports codefly
# allocated in this run, which it also reads.
set -euo pipefail

env_of() { docker inspect "$1" --format '{{range .Config.Env}}{{println .}}{{end}}' | sed -n "s/^$2=//p"; }
port_of() { docker port "$1" | head -1 | sed 's/.*://'; }

TENANT_GW=codefly-sos108b-tenant-gateway
TENANT_MINIO=codefly-sos108b-tenant-minio
STORE_GW=codefly-sos108b-store-gateway
STORE_MINIO=codefly-sos108b-store-minio

TT=$(env_of $TENANT_GW SOS_AUTH_TOKEN)
ST=$(env_of $STORE_GW SOS_AUTH_TOKEN)
TB=$(env_of $TENANT_GW SOS_BUCKET)
TU=$(env_of $TENANT_MINIO MINIO_ROOT_USER)
TP=$(env_of $TENANT_MINIO MINIO_ROOT_PASSWORD)
TENANT_GW_PORT=$(port_of $TENANT_GW)
STORE_GW_PORT=$(port_of $STORE_GW)
OWN_MINIO_PORT=$(port_of $STORE_MINIO)
COMPOSED=$(env_of $STORE_GW SOS_COMPOSED_ENDPOINTS)
TENANT_MINIO_PORT=${COMPOSED##*:}

G=(grpcurl -plaintext -import-path proto -proto codefly/storage/v0/storage.proto)

redact() { sed -E "s/${TP}/<tenant secret>/g; s/\"lastModifiedUnixMs\": \"[0-9]+\"/\"lastModifiedUnixMs\": \"…\"/"; }
call() { # label, rpc, json
  echo
  echo "\$ $2 $1"
  "${G[@]}" -H "x-codefly-token: $ST" -d "$3" "127.0.0.1:$STORE_GW_PORT" "codefly.storage.v0.ObjectStorage/$2" 2>&1 | grep -v '^ERROR:' | redact || true
}

{
echo "# commit under test: $(git rev-parse HEAD)"
echo "# store gateway image: $(docker inspect $STORE_GW --format '{{.Image}}')"
docker ps --format '{{.Names}} {{.Ports}}' | grep sos108b | sort
echo "store gateway env: SOS_COMPOSED_ENDPOINTS=$COMPOSED"

echo
echo "# seed the tenant through its own gateway Put (tenant token, :$TENANT_GW_PORT)"
for k in docs/a.md docs/b.md docs/sub/c.md other.txt; do
  body=$(printf '# %s\nissue-108 tenant store proof\n' "$k")
  n=$(printf '%s' "$body" | wc -c | tr -d ' ')
  r=$(printf '%s' "{\"header\":{\"key\":\"$k\",\"contentType\":\"text/markdown\",\"totalSize\":\"$n\"}} {\"data\":\"$(printf '%s' "$body" | base64)\"}" \
      | "${G[@]}" -H "x-codefly-token: $TT" -d @ "127.0.0.1:$TENANT_GW_PORT" codefly.storage.v0.ObjectStorage/Put 2>&1 | tr -d '\n ')
  echo "Put $k ($n B) -> $r"
done
half=$(head -c 3000000 /dev/zero | base64)
r=$(printf '%s' "{\"header\":{\"key\":\"docs/big.bin\",\"totalSize\":\"6000000\"}} {\"data\":\"$half\"} {\"data\":\"$half\"}" \
    | "${G[@]}" -H "x-codefly-token: $TT" -d @ "127.0.0.1:$TENANT_GW_PORT" codefly.storage.v0.ObjectStorage/Put 2>&1 | tr -d '\n ')
echo "Put docs/big.bin (6000000 B) -> $r"

K="\"credential\":{\"access_key\":{\"id\":\"$TU\",\"secret\":\"$TP\"}}"
EP="http://host.docker.internal:$TENANT_MINIO_PORT"
B="\"binding\":{\"kind\":\"minio\",\"endpoint\":\"$EP\",\"bucket\":\"$TB\",\"prefix\":\"docs/\",$K}"

call "{B}" List "{$B}"
etag() { "${G[@]}" -H "x-codefly-token: $ST" -d "{$B}" "127.0.0.1:$STORE_GW_PORT" codefly.storage.v0.ObjectStorage/List \
  | python3 -c "import sys,json;print({o['key']:o['etag'] for o in json.load(sys.stdin)['objects']}['$1'])"; }
EA=$(etag a.md); EC=$(etag sub/c.md)
call "{key:b.md, B}" Stat "{\"key\":\"b.md\",$B}"
call "{key:sub/c.md, B}" Get "{\"key\":\"sub/c.md\",$B}"
call "{[sub/c.md@listed, a.md@stale, gone.md, big.bin, a.md@listed], B}" GetMany \
  "{\"items\":[{\"key\":\"sub/c.md\",\"ifMatch\":\"$EC\"},{\"key\":\"a.md\",\"ifMatch\":\"stale\"},{\"key\":\"gone.md\"},{\"key\":\"big.bin\"},{\"key\":\"a.md\",\"ifMatch\":\"$EA\"}],$B}"
call "{key:big.bin, B}" Get "{\"key\":\"big.bin\",$B}"
call "{B, prefix \"/docs/\" — literal: no key begins with a slash}" List \
  "{\"binding\":{\"kind\":\"minio\",\"endpoint\":\"$EP\",\"bucket\":\"$TB\",\"prefix\":\"/docs/\",$K}}"
MB="\"binding\":{\"kind\":\"minio\",\"endpoint\":\"$EP\",\"bucket\":\"no-such-bucket\",$K}"
call "{bucket no-such-bucket}" List "{$MB}"
call "{[a.md, b.md], bucket no-such-bucket}" GetMany "{\"items\":[{\"key\":\"a.md\"},{\"key\":\"b.md\"}],$MB}"
for ep in "http://localhost:$TENANT_MINIO_PORT" "http://127.0.0.1:$TENANT_MINIO_PORT" \
          "http://host.docker.internal:$OWN_MINIO_PORT" "http://host.docker.internal:$((TENANT_MINIO_PORT + 1))"; do
  call "{endpoint $ep}" List "{\"binding\":{\"kind\":\"minio\",\"endpoint\":\"$ep\",\"bucket\":\"$TB\",$K}}"
done
call "{B, wrong secret}" List \
  "{\"binding\":{\"kind\":\"minio\",\"endpoint\":\"$EP\",\"bucket\":\"$TB\",\"credential\":{\"access_key\":{\"id\":\"$TU\",\"secret\":\"wrong-secret\"}}}}"

echo
echo "# credentials in both gateways' logs: fixed-string matches, one credential at a time"
LOGS=$(docker logs $STORE_GW 2>&1; docker logs $TENANT_GW 2>&1)
printf 'log lines: %s\n' "$(printf '%s\n' "$LOGS" | wc -l | tr -d ' ')"
control="object-storage gateway listening on"
printf 'control, a string the logs do contain (%s): %s\n' "$control" "$(printf '%s\n' "$LOGS" | grep -cF -- "$control" || true)"
printf 'tenant secret: %s\n' "$(printf '%s\n' "$LOGS" | grep -cF -- "$TP" || true)"
printf 'wrong-secret: %s\n' "$(printf '%s\n' "$LOGS" | grep -cF -- "wrong-secret" || true)"
} | redact
