"""MinIO client wrapper for streaming ZIP processing.

Provides a clean interface for:
- Streaming .cpp/.h files from ZIP archives stored in MinIO
- Uploading JSON result blobs
- Health-checking MinIO connectivity

Never imports from pipeline.py or algorithm modules — clean separation.
"""

from __future__ import annotations

import io
import json
import logging
import zipfile
from dataclasses import dataclass
from typing import Any, Iterator

from minio import Minio

logger = logging.getLogger(__name__)


@dataclass
class ZipEntry:
    """A single file entry extracted from a ZIP archive."""

    filename: str        # path inside the ZIP e.g. "submissions/sol.cpp"
    source_code: str     # decoded UTF-8 content
    size_bytes: int


class MinIOClient:
    """Wrapper around the MinIO Python SDK for Nexus operations."""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool = False,
    ) -> None:
        self._client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def stream_cpp_files(
        self,
        bucket: str,
        object_key: str,
        max_file_bytes: int = 500_000,
    ) -> Iterator[ZipEntry]:
        """Stream a ZIP from MinIO, yielding only .cpp and .h files.

        Downloads the ZIP object via ``get_object()``, wraps it in
        ``io.BytesIO``, and opens with ``zipfile.ZipFile`` — individual
        entries are read one at a time (no ``extractall``).

        Skips files larger than *max_file_bytes* and files that cannot
        be decoded as UTF-8.  Warnings are logged but never raised.
        Never yields partial entries.
        """
        response = self._client.get_object(bucket, object_key)
        try:
            zip_bytes = io.BytesIO(response.read())
        finally:
            response.close()
            response.release_conn()

        with zipfile.ZipFile(zip_bytes, "r") as zf:
            for info in zf.infolist():
                # Skip directories
                if info.is_dir():
                    continue

                # Filter: only .cpp and .h files
                if not info.filename.endswith((".cpp", ".h")):
                    continue

                # Size check before reading content
                if info.file_size > max_file_bytes:
                    logger.warning(
                        "SKIP %s: %d bytes exceeds limit %d",
                        info.filename, info.file_size, max_file_bytes,
                    )
                    continue

                # Read and decode
                try:
                    raw = zf.read(info.filename)
                    source_code = raw.decode("utf-8")
                except (UnicodeDecodeError, KeyError) as exc:
                    logger.warning("SKIP %s: %s", info.filename, exc)
                    continue

                yield ZipEntry(
                    filename=info.filename,
                    source_code=source_code,
                    size_bytes=info.file_size,
                )

    def put_json(
        self,
        bucket: str,
        object_key: str,
        data: dict[str, Any],
    ) -> None:
        """Serialise *data* as JSON and upload to MinIO."""
        json_bytes = json.dumps(data, indent=2).encode("utf-8")
        self._client.put_object(
            bucket_name=bucket,
            object_name=object_key,
            data=io.BytesIO(json_bytes),
            length=len(json_bytes),
            content_type="application/json",
        )

    def health_check(self) -> bool:
        """Return True if MinIO is reachable, False otherwise. Never raises."""
        try:
            self._client.list_buckets()
            return True
        except Exception:
            return False
