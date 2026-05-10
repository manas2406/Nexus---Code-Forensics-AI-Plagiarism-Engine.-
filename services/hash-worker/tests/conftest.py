"""
Pytest fixtures for Nexus hash-worker tests.

Infrastructure fixtures (minio_client, redis_client) connect to the real
services running in Docker. They are skipped automatically if the services
aren't reachable — this keeps unit tests runnable without Docker.

Usage:
  Unit tests (no infra needed):
    pytest tests/unit/ -v

  Integration tests (requires: docker compose up):
    MINIO_ENDPOINT=localhost:9000 REDIS_HOST=localhost KAFKA_BROKERS=localhost:9093 \
      pytest tests/integration/ -v
"""

from __future__ import annotations
import io
import os
import uuid
import zipfile
import pytest


# ── Environment config (mirrors .env.example defaults) ───────────────────────

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS = os.environ.get("MINIO_ACCESS_KEY", "nexus")
MINIO_SECRET = os.environ.get("MINIO_SECRET_KEY", "nexus-secret-change-in-prod")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET_SUBMISSIONS", "nexus-submissions")

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9093")


# ── MinIO fixture ──────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def minio_client():
    """
    Returns a MinIO client connected to the local test instance.
    Skips the test if MinIO is unreachable.
    """
    from minio import Minio

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS,
        secret_key=MINIO_SECRET,
        secure=False,
    )

    try:
        client.list_buckets()
    except Exception as e:
        pytest.skip(f"MinIO not reachable at {MINIO_ENDPOINT}: {e}")

    return client


@pytest.fixture(scope="session")
def redis_client():
    """
    Returns a Redis client connected to the local test instance.
    Skips the test if Redis is unreachable.
    """
    import redis

    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    try:
        client.ping()
    except Exception as e:
        pytest.skip(f"Redis not reachable at {REDIS_HOST}:{REDIS_PORT}: {e}")

    return client


# ── ZIP factory fixture ────────────────────────────────────────────────────────

@pytest.fixture
def make_test_zip():
    """
    Factory fixture: returns a function that builds an in-memory ZIP of C++ files.

    Usage:
        def test_something(make_test_zip):
            zip_bytes = make_test_zip({
                "file_a.cpp": "int main() { return 0; }",
                "file_b.cpp": "int main() { return 1; }",
            })
    """
    def _make(files: dict[str, str]) -> bytes:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for filename, source in files.items():
                zf.writestr(filename, source.encode("utf-8"))
        return buf.getvalue()

    return _make


@pytest.fixture
def upload_test_zip(minio_client, make_test_zip):
    """
    Factory fixture: builds a ZIP, uploads it to MinIO, returns (job_id, object_key).
    Automatically cleans up the object after the test.

    Usage:
        def test_something(upload_test_zip):
            job_id, key = upload_test_zip({
                "file_a.cpp": "int main() { return 0; }",
            })
            # key = "submissions/{job_id}.zip"
    """
    created: list[str] = []

    def _upload(files: dict[str, str]) -> tuple[str, str]:
        job_id = str(uuid.uuid4())
        object_key = f"submissions/{job_id}.zip"
        zip_bytes = make_test_zip(files)

        minio_client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_key,
            data=io.BytesIO(zip_bytes),
            length=len(zip_bytes),
            content_type="application/zip",
        )
        created.append(object_key)
        return job_id, object_key

    yield _upload

    # Cleanup: remove all uploaded objects after the test
    for key in created:
        try:
            minio_client.remove_object(MINIO_BUCKET, key)
        except Exception:
            pass


# ── Sample C++ sources for test cases ─────────────────────────────────────────

SAMPLE_CPP = {
    "original": """
#include <iostream>
using namespace std;

int bubble_sort(int arr[], int n) {
    for (int i = 0; i < n - 1; i++) {
        for (int j = 0; j < n - i - 1; j++) {
            if (arr[j] > arr[j + 1]) {
                int temp = arr[j];
                arr[j] = arr[j + 1];
                arr[j + 1] = temp;
            }
        }
    }
    return 0;
}
""",
    "variable_renamed": """
#include <iostream>
using namespace std;

int sortArray(int data[], int size) {
    for (int x = 0; x < size - 1; x++) {
        for (int y = 0; y < size - x - 1; y++) {
            if (data[y] > data[y + 1]) {
                int tmp = data[y];
                data[y] = data[y + 1];
                data[y + 1] = tmp;
            }
        }
    }
    return 0;
}
""",
    "different": """
#include <iostream>
using namespace std;

int fibonacci(int n) {
    if (n <= 1) return n;
    int a = 0, b = 1;
    for (int i = 2; i <= n; i++) {
        int c = a + b;
        a = b;
        b = c;
    }
    return b;
}
""",
    "syntax_error": """
#include <iostream>
int broken_function( {    // intentional syntax error
    int x = ;             // missing value
    return x
}
""",
}


@pytest.fixture
def sample_cpp():
    """Provides the SAMPLE_CPP dict to tests without importing it directly."""
    return SAMPLE_CPP
