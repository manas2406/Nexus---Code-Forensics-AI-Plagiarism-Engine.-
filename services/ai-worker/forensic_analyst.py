import asyncio
import json
import logging
import re
import random
from dataclasses import dataclass
import openai
from openai import AsyncOpenAI
from rate_limiter import TokenBucket

logger = logging.getLogger("nexus.forensic-analyst")

@dataclass
class PairInput:
    pair_id: str
    file_a: str           # filename
    file_b: str           # filename
    source_a: str         # C++ source, truncated to max_chars
    source_b: str         # C++ source, truncated to max_chars
    similarity: float

@dataclass
class ForensicReport:
    pair_id: str
    verdict: str          # "LIKELY_PLAGIARISM" | "POSSIBLE_COINCIDENCE" | "INCONCLUSIVE"
    confidence: float     # 0.0–1.0
    obfuscation_techniques: list[str]
    evidence_summary: str
    raw_llm_response: str
    is_fallback: bool     # True if LLM was unreachable and this is a default report

class ForensicAnalyst:
    def __init__(
        self,
        openai_client: AsyncOpenAI,
        rate_limiter: TokenBucket,
        semaphore: asyncio.Semaphore,
        model: str = "gpt-4o",
        temperature: float = 0.2,
        max_tokens: int = 1000,
        max_source_chars: int = 3000,
        max_retries: int = 3,
    ):
        self.openai_client = openai_client
        self.rate_limiter = rate_limiter
        self.semaphore = semaphore
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.max_source_chars = max_source_chars
        self.max_retries = max_retries

    async def analyse(self, pair: PairInput) -> ForensicReport:
        """
        Acquire semaphore + rate limiter, call LLM, parse response.
        On total failure: return fallback report, never raise.
        """
        async with self.semaphore:
            await self.rate_limiter.acquire(1.0)
            
            messages = self._build_prompt(pair)
            
            for attempt in range(1, self.max_retries + 1):
                try:
                    response = await self.openai_client.chat.completions.create(
                        model=self.model,
                        messages=messages, # type: ignore
                        temperature=self.temperature,
                        max_tokens=self.max_tokens,
                    )
                    
                    content = response.choices[0].message.content or ""
                    return self._parse_response(content, pair.pair_id)
                    
                except openai.RateLimitError:
                    wait = (2 ** attempt) + random.uniform(0, 1)
                    await asyncio.sleep(wait)
                except openai.APIStatusError as e:
                    if e.status_code >= 500:
                        wait = (2 ** attempt) + random.uniform(0, 1)
                        await asyncio.sleep(wait)
                    else:
                        return self._fallback_report(pair.pair_id, f"API error {e.status_code}")
                except Exception as e:
                    return self._fallback_report(pair.pair_id, str(e))
            
            return self._fallback_report(pair.pair_id, "max retries exceeded")

    def _build_prompt(self, pair: PairInput) -> list[dict[str, str]]:
        """Build the messages array for the OpenAI call."""
        
        system_prompt = (
            "You are a forensic code analyst specialising in academic plagiarism detection for C++ submissions.\n"
            "You will be given two C++ source files and their structural similarity score.\n"
            "Analyse whether the similarity indicates plagiarism, coincidental similarity, or is inconclusive.\n"
            "Respond ONLY with a valid JSON object. No markdown, no explanation outside the JSON."
        )
        
        user_prompt_template = (
            "File A: {file_a}\n"
            "File B: {file_b}\n"
            "Structural similarity score: {similarity:.3f}\n\n"
            "--- FILE A SOURCE ---\n"
            "{source_a}\n\n"
            "--- FILE B SOURCE ---\n"
            "{source_b}\n\n"
            "Respond with this exact JSON schema:\n"
            "{{\n"
            '  "verdict": "LIKELY_PLAGIARISM" | "POSSIBLE_COINCIDENCE" | "INCONCLUSIVE",\n'
            '  "confidence": <float 0.0-1.0>,\n'
            '  "obfuscation_techniques": [<list of strings, empty if none>],\n'
            '  "evidence_summary": "<2-3 sentence explanation>"\n'
            "}}"
        )
        
        user_prompt = user_prompt_template.format(
            file_a=pair.file_a,
            file_b=pair.file_b,
            similarity=pair.similarity,
            source_a=pair.source_a,
            source_b=pair.source_b,
        )
        
        return [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]

    def _parse_response(self, raw: str, pair_id: str) -> ForensicReport:
        """
        Parse LLM JSON response into ForensicReport.
        Strips markdown fences if present.
        Returns fallback report if JSON is invalid.
        """
        cleaned = re.sub(r'^```json\s*|^```\s*|```$', '', raw.strip(), flags=re.MULTILINE)
        try:
            data = json.loads(cleaned)
            verdict = data.get('verdict', 'INCONCLUSIVE')
            if verdict not in ('LIKELY_PLAGIARISM', 'POSSIBLE_COINCIDENCE', 'INCONCLUSIVE'):
                verdict = 'INCONCLUSIVE'
            
            return ForensicReport(
                pair_id=pair_id,
                verdict=verdict,
                confidence=float(data.get('confidence', 0.5)),
                obfuscation_techniques=data.get('obfuscation_techniques', []),
                evidence_summary=data.get('evidence_summary', ''),
                raw_llm_response=raw,
                is_fallback=False,
            )
        except (json.JSONDecodeError, KeyError, ValueError):
            return self._fallback_report(pair_id, f"unparseable response: {raw[:100]}")

    def _fallback_report(self, pair_id: str, reason: str) -> ForensicReport:
        """Return a safe default report when LLM is unreachable or unparseable."""
        return ForensicReport(
            pair_id=pair_id,
            verdict="INCONCLUSIVE",
            confidence=0.0,
            obfuscation_techniques=[],
            evidence_summary=f"Fallback generated due to error: {reason}",
            raw_llm_response="",
            is_fallback=True,
        )
