import asyncio
from forensic_analyst import ForensicAnalyst, PairInput
from rate_limiter import TokenBucket, TokenBucketConfig
from unittest.mock import AsyncMock
from test_phase4 import get_mock_openai_response

async def debug():
    openai_client = AsyncMock()
    rate_limiter = TokenBucket(TokenBucketConfig(rate=100.0, capacity=100.0))
    semaphore = asyncio.Semaphore(5)
    analyst = ForensicAnalyst(
        openai_client=openai_client,
        rate_limiter=rate_limiter,
        semaphore=semaphore,
        max_retries=2
    )

    valid_json = '{"verdict": "LIKELY_PLAGIARISM", "confidence": 0.95, "obfuscation_techniques": [], "evidence_summary": "Identical structure."}'
    analyst.openai_client.chat.completions.create.return_value = get_mock_openai_response(valid_json)
    
    pair = PairInput(
        pair_id="p1",
        file_a="a.cpp",
        file_b="b.cpp",
        source_a="int main() {}",
        source_b="int main() { return 0; }",
        similarity=0.9
    )
    
    report = await analyst.analyse(pair)
    print("Verdict:", report.verdict)
    print("Fallback:", report.is_fallback)
    if report.is_fallback:
        print("Reason:", report.evidence_summary)

if __name__ == "__main__":
    asyncio.run(debug())
