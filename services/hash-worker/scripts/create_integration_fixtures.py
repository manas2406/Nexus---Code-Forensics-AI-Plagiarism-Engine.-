"""Create and upload the Phase 2 integration test ZIP to MinIO.

Usage:
    python scripts/create_integration_fixtures.py

Requires: docker compose up (MinIO at localhost:9000)
"""

from __future__ import annotations

import io
import os
import sys
import zipfile

from minio import Minio

# ── Test C++ files ────────────────────────────────────────────────────────────

INTEGRATION_FILES = {
    # Pair 1: Alice & Bob — variable renaming (should detect ≥ 0.70)
    "student_alice.cpp": """\
#include <iostream>
int solve(int n) {
    int result = 0;
    for (int i = 0; i < n; i++) {
        result += i * i;
        if (i % 2 == 0) { result -= i; }
    }
    int final_val = result + n;
    for (int j = 0; j < n; j++) { final_val += j; }
    return final_val;
}

int main() {
    std::cout << solve(10) << std::endl;
    return 0;
}
""",
    "student_bob.cpp": """\
#include <iostream>
int solve(int count) {
    int total = 0;
    for (int idx = 0; idx < count; idx++) {
        total += idx * idx;
        if (idx % 2 == 0) { total -= idx; }
    }
    int answer = total + count;
    for (int k = 0; k < count; k++) { answer += k; }
    return answer;
}

int main() {
    std::cout << solve(10) << std::endl;
    return 0;
}
""",
    # Pair 2: Charlie & Dave — loop restructuring (should detect ≥ 0.55)
    "student_charlie.cpp": """\
int sort_arr(int arr[], int size) {
    for (int i = 0; i < size - 1; i++) {
        for (int j = 0; j < size - i - 1; j++) {
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
    "student_dave.cpp": """\
int sort_arr(int data[], int length) {
    for (int x = 0; x < length - 1; x++) {
        for (int y = 0; y < length - x - 1; y++) {
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
    # Eve — completely different (should NOT flag as suspicious with anyone)
    "student_eve.cpp": """\
#include <cmath>
double distance(double x1, double y1, double x2, double y2) {
    return sqrt((x2 - x1) * (x2 - x1) + (y2 - y1) * (y2 - y1));
}

double triangle_area(double a, double b, double c) {
    double s = (a + b + c) / 2.0;
    return sqrt(s * (s - a) * (s - b) * (s - c));
}
""",
    # Frank — has syntax errors (should partial-parse, NOT crash)
    "student_frank.cpp": """\
#include <iostream>
int broken_function( {    // intentional syntax error
    int x = ;             // missing value
    for (int i = 0; i < 10; i++) {
        x += i;
    }
    return x
}
""",
    # Grace — similar structure to Alice (should detect ≥ 0.55)
    "student_grace.cpp": """\
#include <iostream>
int compute(int n) {
    int result = 0;
    for (int i = 0; i < n; i++) {
        result += i * i;
        if (i % 2 == 0) { result -= i; }
    }
    int output = result + n;
    return output;
}

int main() {
    std::cout << compute(10) << std::endl;
    return 0;
}
""",
}


def create_and_upload() -> None:
    """Create ZIP and upload to MinIO."""
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "nexus")
    secret_key = os.getenv("MINIO_SECRET_KEY", "nexus-secret-change-in-prod")

    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)

    bucket = os.getenv("MINIO_BUCKET_SUBMISSIONS", "nexus-submissions")
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    # Create ZIP
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in INTEGRATION_FILES.items():
            zf.writestr(name, content)
    zip_size = buf.tell()
    buf.seek(0)

    object_key = "test-integration.zip"
    client.put_object(
        bucket_name=bucket,
        object_name=object_key,
        data=buf,
        length=zip_size,
        content_type="application/zip",
    )

    print(f"✓ Uploaded test ZIP to MinIO")
    print(f"  Bucket: {bucket}")
    print(f"  Object key: {object_key}")
    print(f"  Files: {len(INTEGRATION_FILES)}")
    print(f"  Size: {zip_size:,} bytes")
    print(f"  Run: python main.py --zip {bucket}/{object_key} --job-id test-001")


if __name__ == "__main__":
    create_and_upload()
