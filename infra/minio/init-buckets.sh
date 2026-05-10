#!/usr/bin/env bash
set -euo pipefail

echo "[minio-init] Configuring MinIO client…"
mc alias set nexus http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

echo "[minio-init] Creating buckets…"
mc mb --ignore-existing nexus/nexus-submissions
mc mb --ignore-existing nexus/nexus-reports

echo "[minio-init] Buckets ready."
