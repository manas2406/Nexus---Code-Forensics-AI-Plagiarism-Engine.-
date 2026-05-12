"""Create edge case ZIP files and upload to MinIO.

Usage:
    python scripts/create_edge_case_zip.py --case single_file
    python scripts/create_edge_case_zip.py --case all_broken
    python scripts/create_edge_case_zip.py --case no_cpp_files
    python scripts/create_edge_case_zip.py --case giant_cheating_ring

Requires: docker compose up (MinIO at localhost:9000)
"""

from __future__ import annotations

import argparse
import io
import os
import zipfile

from minio import Minio

CASES: dict[str, dict[str, str | bytes]] = {
    "single_file": {
        "only_one.cpp": "int main() { return 0; }",
    },
    "all_broken": {
        "broken_a.cpp": "int bad_function( { int x = ; return x }",
        "broken_b.cpp": "class Incomplete { void method( }",
        "broken_c.cpp": "#include <iostream> int main { cout << ; }",
    },
    "no_cpp_files": {
        "readme.txt":   "No C++ here",
        "notes.pdf":    "binary data",
    },
    "giant_cheating_ring": {
        # 10 identical files — all should flag as suspicious pairs
        **{
            f"student_{chr(65 + i)}.cpp": (
                "int main() {\n"
                "    int x = 1;\n"
                "    for (int i = 0; i < 10; i++) { x += i * i; }\n"
                "    if (x > 100) { x = x / 2; }\n"
                "    return x;\n"
                "}\n"
            )
            for i in range(10)
        },
    },
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Create edge case test ZIPs")
    parser.add_argument("--case", choices=list(CASES.keys()), required=True)
    args = parser.parse_args()

    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "nexus")
    secret_key = os.getenv("MINIO_SECRET_KEY", "nexus-secret-change-in-prod")

    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)

    bucket = os.getenv("MINIO_BUCKET_SUBMISSIONS", "nexus-submissions")
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    files = CASES[args.case]
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            if isinstance(content, str):
                content = content.encode("utf-8")
            zf.writestr(name, content)
    zip_size = buf.tell()
    buf.seek(0)

    object_key = f"edge-{args.case}.zip"
    client.put_object(
        bucket_name=bucket,
        object_name=object_key,
        data=buf,
        length=zip_size,
        content_type="application/zip",
    )

    print(f"✓ Uploaded edge case ZIP: {object_key}")
    print(f"  Files: {len(files)}")
    print(f"  Run: python main.py --zip {bucket}/{object_key} --job-id edge-{args.case}")


if __name__ == "__main__":
    main()
