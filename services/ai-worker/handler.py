import dataclasses
import logging
from typing import Protocol, Any
from forensic_analyst import ForensicAnalyst, PairInput

logger = logging.getLogger("nexus.handler")

class MinIOClient(Protocol):
    def get_source(self, bucket: str, key: str) -> str: ...
    def put_json(self, bucket: str, key: str, data: dict[str, Any]) -> None: ...

class JobStateManager(Protocol):
    def update_status(self, job_id: str, status: str, detail: str) -> None: ...
    def publish_event(self, job_id: str, event: dict[str, Any]) -> None: ...

@dataclasses.dataclass
class AIWorkerConfig:
    max_source_chars: int

async def handle_suspicious_pair(
    payload: dict[str, Any],
    analyst: ForensicAnalyst,
    minio: MinIOClient,
    state: JobStateManager,
    config: AIWorkerConfig,
) -> None:
    """
    Handle a single SUSPICIOUS_PAIR Kafka event.
    """
    # 1. Validate payload
    required_keys = ("jobId", "pairId", "fileA", "fileB", "similarity")
    for key in required_keys:
        if key not in payload:
            raise ValueError(f"Missing required field: {key}")

    job_id = payload["jobId"]
    pair_id = payload["pairId"]
    file_a = payload["fileA"]
    file_b = payload["fileB"]
    similarity = payload["similarity"]

    # 2 & 3. Fetch sources
    bucket = "nexus-submissions"
    
    def _fetch_source(filename: str) -> str:
        try:
            return minio.get_source(bucket, f"{job_id}/{filename}")
        except Exception as e:
            logger.warning(f"Could not fetch source for {filename}: {e}")
            return ""

    source_a = _fetch_source(file_a)
    source_b = _fetch_source(file_b)

    # 4. Build PairInput
    def truncate(s: str) -> str:
        if len(s) > config.max_source_chars:
            trunc = s[:config.max_source_chars]
            # Truncate at a newline boundary
            last_newline = trunc.rfind('\n')
            if last_newline != -1:
                trunc = trunc[:last_newline]
            return trunc + "\n... [truncated]"
        return s

    pair_input = PairInput(
        pair_id=pair_id,
        file_a=file_a,
        file_b=file_b,
        source_a=truncate(source_a),
        source_b=truncate(source_b),
        similarity=float(similarity),
    )

    # 5. update_status
    state.update_status(job_id, "AI_ANALYSIS", detail=f"analysing pair {pair_id}")

    # 6. analyse
    report = await analyst.analyse(pair_input)

    # 7. Store report in MinIO
    report_dict = dataclasses.asdict(report)
    report_key = f"reports/{job_id}/{pair_id}.json"
    minio.put_json("nexus-reports", report_key, report_dict)

    # 8. Publish to Redis
    state.publish_event(job_id, {
        "type": "FORENSIC_REPORT_READY",
        "pairId": pair_id,
        "verdict": report.verdict,
        "confidence": report.confidence,
        "reportKey": report_key,
    })

    # 9. Log
    logger.info(f"[ai-worker] {pair_id} → {report.verdict} ({report.confidence:.2f})")
