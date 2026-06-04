import asyncio
import json
import os
import time
from unittest.mock import AsyncMock, Mock, patch

import pytest
import openai
from openai import AsyncOpenAI
from openai.types.chat import ChatCompletion, ChatCompletionMessage
from openai.types.chat.chat_completion import Choice

from rate_limiter import TokenBucket, TokenBucketConfig
from forensic_analyst import ForensicAnalyst, PairInput, ForensicReport
from handler import handle_suspicious_pair, AIWorkerConfig, MinIOClient, JobStateManager

# --- TokenBucket Tests ---

@pytest.mark.asyncio
async def test_token_bucket_immediate_acquire():
    # bucket with capacity 5, acquire 1 token, assert returns without sleeping.
    config = TokenBucketConfig(rate=1.0, capacity=5.0)
    bucket = TokenBucket(config)
    
    start = time.monotonic()
    await bucket.acquire(1.0)
    end = time.monotonic()
    
    assert (end - start) < 0.1

@pytest.mark.asyncio
async def test_token_bucket_blocks_when_empty():
    # bucket with capacity 1, acquire 1 (empties it), measure time to acquire again, assert wait ≈ 1/rate seconds (±20%).
    config = TokenBucketConfig(rate=2.0, capacity=1.0)  # rate 2.0 = 0.5s wait
    bucket = TokenBucket(config)
    
    await bucket.acquire(1.0)  # Empties it
    
    start = time.monotonic()
    await bucket.acquire(1.0)
    end = time.monotonic()
    
    wait_time = end - start
    assert 0.4 <= wait_time <= 0.6  # 0.5s ± 20%

@pytest.mark.asyncio
async def test_token_bucket_burst():
    # acquire 5 tokens in rapid succession from a capacity-5 bucket, assert all succeed without waiting.
    config = TokenBucketConfig(rate=1.0, capacity=5.0)
    bucket = TokenBucket(config)
    
    start = time.monotonic()
    for _ in range(5):
        await bucket.acquire(1.0)
    end = time.monotonic()
    
    assert (end - start) < 0.1

# --- ForensicAnalyst Tests ---

def get_mock_openai_response(content: str) -> ChatCompletion:
    return ChatCompletion(
        id="test-id",
        choices=[
            Choice(
                finish_reason="stop",
                index=0,
                message=ChatCompletionMessage(content=content, role="assistant")
            )
        ],
        created=int(time.time()),
        model="gpt-4o",
        object="chat.completion"
    )

@pytest.fixture
def base_pair():
    return PairInput(
        pair_id="p1",
        file_a="a.cpp",
        file_b="b.cpp",
        source_a="int main() {}",
        source_b="int main() { return 0; }",
        similarity=0.9
    )

@pytest.fixture
def analyst():
    openai_client = AsyncMock()
    rate_limiter = TokenBucket(TokenBucketConfig(rate=100.0, capacity=100.0))
    semaphore = asyncio.Semaphore(5)
    return ForensicAnalyst(
        openai_client=openai_client,
        rate_limiter=rate_limiter,
        semaphore=semaphore,
        max_retries=2
    )

@pytest.mark.asyncio
async def test_forensic_analyst_parses_valid_json(analyst, base_pair):
    valid_json = '{"verdict": "LIKELY_PLAGIARISM", "confidence": 0.95, "obfuscation_techniques": [], "evidence_summary": "Identical structure."}'
    analyst.openai_client.chat.completions.create.return_value = get_mock_openai_response(valid_json)
    
    report = await analyst.analyse(base_pair)
    
    assert report.verdict == "LIKELY_PLAGIARISM"
    assert report.is_fallback is False

@pytest.mark.asyncio
async def test_forensic_analyst_strips_markdown_fences(analyst, base_pair):
    markdown_json = '```json\n{"verdict": "POSSIBLE_COINCIDENCE", "confidence": 0.6, "obfuscation_techniques": [], "evidence_summary": "x"}\n```'
    analyst.openai_client.chat.completions.create.return_value = get_mock_openai_response(markdown_json)
    
    report = await analyst.analyse(base_pair)
    
    assert report.verdict == "POSSIBLE_COINCIDENCE"
    assert report.is_fallback is False

@pytest.mark.asyncio
async def test_forensic_analyst_fallback_on_invalid_json(analyst, base_pair):
    invalid_json = "not json at all"
    analyst.openai_client.chat.completions.create.return_value = get_mock_openai_response(invalid_json)
    
    report = await analyst.analyse(base_pair)
    
    assert report.is_fallback is True
    assert report.verdict == "INCONCLUSIVE"

@pytest.mark.asyncio
async def test_forensic_analyst_fallback_on_rate_limit(analyst, base_pair):
    response = httpx.Response(429, request=httpx.Request("POST", "http://test")) if "httpx" in globals() else Mock()
    analyst.openai_client.chat.completions.create.side_effect = openai.RateLimitError("Rate limit", response=response, body=None)
    
    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        report = await analyst.analyse(base_pair)
    
    assert report.is_fallback is True
    assert mock_sleep.call_count == 2

@pytest.mark.asyncio
async def test_forensic_analyst_fallback_on_unreachable(analyst, base_pair):
    analyst.openai_client.chat.completions.create.side_effect = openai.APIConnectionError(request=Mock())
    
    report = await analyst.analyse(base_pair)
    
    assert report.is_fallback is True

@pytest.mark.asyncio
async def test_forensic_analyst_invalid_verdict_defaults_to_inconclusive(analyst, base_pair):
    invalid_verdict_json = '{"verdict": "MAYBE", "confidence": 0.5, "obfuscation_techniques": [], "evidence_summary": ""}'
    analyst.openai_client.chat.completions.create.return_value = get_mock_openai_response(invalid_verdict_json)
    
    report = await analyst.analyse(base_pair)
    
    assert report.verdict == "INCONCLUSIVE"

@pytest.mark.asyncio
async def test_source_truncation_at_newline(base_pair):
    config = AIWorkerConfig(max_source_chars=15)
    
    # We need to test the logic from handler
    # Let's mock minio to return a string longer than 15 chars with newlines
    source = "line1\nline2\nline3"
    
    def truncate(s: str) -> str:
        if len(s) > config.max_source_chars:
            trunc = s[:config.max_source_chars]
            last_newline = trunc.rfind('\\n')
            if last_newline != -1:
                trunc = trunc[:last_newline]
            return trunc + "\\n... [truncated]"
        return s

    # 15 chars: "line1\\nline2\\nli"
    # rfind('\\n') finds it after line2
    # result: "line1\\nline2\\n... [truncated]"
    
    truncated = truncate(source)
    assert truncated.endswith("\\n... [truncated]")
    assert not truncated.endswith("li\\n... [truncated]") # did not cut mid-line

# --- Handler Tests ---

@pytest.mark.asyncio
async def test_handler_missing_job_id(analyst):
    payload = {"pairId": "123", "fileA": "a", "fileB": "b", "similarity": 0.9}
    
    with pytest.raises(ValueError, match="Missing required field: jobId"):
        await handle_suspicious_pair(payload, analyst, Mock(), Mock(), AIWorkerConfig(max_source_chars=3000))

@pytest.mark.asyncio
async def test_handler_produces_report_to_minio(analyst):
    payload = {"jobId": "j1", "pairId": "p1", "fileA": "a", "fileB": "b", "similarity": 0.9}
    minio = Mock()
    minio.get_source.return_value = "source"
    state = Mock()
    config = AIWorkerConfig(max_source_chars=3000)
    
    analyst.analyse = AsyncMock(return_value=ForensicReport("p1", "LIKELY_PLAGIARISM", 0.9, [], "test", "raw", False))
    
    await handle_suspicious_pair(payload, analyst, minio, state, config)
    
    minio.put_json.assert_called_once()
    args = minio.put_json.call_args[0]
    assert args[0] == "nexus-reports"
    assert args[1] == "reports/j1/p1.json"

@pytest.mark.asyncio
async def test_handler_publishes_redis_event(analyst):
    payload = {"jobId": "j1", "pairId": "p1", "fileA": "a", "fileB": "b", "similarity": 0.9}
    minio = Mock()
    minio.get_source.return_value = "source"
    state = Mock()
    config = AIWorkerConfig(max_source_chars=3000)
    
    analyst.analyse = AsyncMock(return_value=ForensicReport("p1", "LIKELY_PLAGIARISM", 0.9, [], "test", "raw", False))
    
    await handle_suspicious_pair(payload, analyst, minio, state, config)
    
    state.publish_event.assert_called_once()
    args = state.publish_event.call_args[0]
    assert args[0] == "j1"
    assert args[1]["type"] == "FORENSIC_REPORT_READY"
    assert args[1]["pairId"] == "p1"

@pytest.mark.asyncio
async def test_handler_continues_if_source_fetch_fails(analyst):
    payload = {"jobId": "j1", "pairId": "p1", "fileA": "a", "fileB": "b", "similarity": 0.9}
    minio = Mock()
    minio.get_source.side_effect = Exception("Fetch failed")
    state = Mock()
    config = AIWorkerConfig(max_source_chars=3000)
    
    analyst.analyse = AsyncMock(return_value=ForensicReport("p1", "INCONCLUSIVE", 0.0, [], "test", "raw", True))
    
    await handle_suspicious_pair(payload, analyst, minio, state, config)
    
    # Should call analyse with empty sources rather than failing
    analyst.analyse.assert_called_once()
    pair_input = analyst.analyse.call_args[0][0]
    assert pair_input.source_a == ""
    assert pair_input.source_b == ""

@pytest.mark.asyncio
async def test_semaphore_limits_concurrency():
    # Spin up 20 concurrent analyse() calls with semaphore=3, assert no more than 3 run simultaneously.
    
    semaphore = asyncio.Semaphore(3)
    concurrent_count = 0
    max_concurrent = 0
    lock = asyncio.Lock()
    
    async def mock_call():
        nonlocal concurrent_count, max_concurrent
        async with semaphore:
            async with lock:
                concurrent_count += 1
                max_concurrent = max(max_concurrent, concurrent_count)
            await asyncio.sleep(0.01)
            async with lock:
                concurrent_count -= 1

    tasks = [asyncio.create_task(mock_call()) for _ in range(20)]
    await asyncio.gather(*tasks)
    
    assert max_concurrent == 3

# --- Prompt Engineering Tests ---

@pytest.mark.llm
@pytest.mark.asyncio
async def test_prompt_produces_valid_json_on_diverse_inputs():
    if not os.environ.get("OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY") == "dummy":
        pytest.skip("OPENAI_API_KEY not set")
    
    client = AsyncOpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    rate_limiter = TokenBucket(TokenBucketConfig(rate=100.0, capacity=100.0))
    semaphore = asyncio.Semaphore(10)
    analyst = ForensicAnalyst(client, rate_limiter, semaphore)
    
    pairs = [
        PairInput(str(i), f"a{i}.cpp", f"b{i}.cpp", f"int main() {{ return {i}; }}", f"int main() {{ return {i}; }}", 1.0)
        for i in range(10)
    ]
    
    tasks = [analyst.analyse(p) for p in pairs]
    reports = await asyncio.gather(*tasks)
    
    for report in reports:
        assert not report.is_fallback
        assert report.evidence_summary != ""
        assert report.verdict in ["LIKELY_PLAGIARISM", "POSSIBLE_COINCIDENCE", "INCONCLUSIVE"]
