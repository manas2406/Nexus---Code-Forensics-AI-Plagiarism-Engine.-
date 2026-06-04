import asyncio
import time
from dataclasses import dataclass

@dataclass
class TokenBucketConfig:
    rate: float          # tokens added per second
    capacity: float      # max tokens in bucket

class TokenBucket:
    def __init__(self, config: TokenBucketConfig):
        self.config = config
        self._tokens = config.capacity
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0) -> None:
        """
        Block until enough tokens are available, then consume them.
        Never raises — always eventually returns.
        """
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._tokens = min(self.config.capacity, self._tokens + self.config.rate * elapsed)
                self._last_refill = now

                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                
                # Need to wait
                wait = (tokens - self._tokens) / self.config.rate
            
            # Sleep outside the lock so others can acquire it
            await asyncio.sleep(wait)

    def available(self) -> float:
        """Return current token count without consuming."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        return min(self.config.capacity, self._tokens + self.config.rate * elapsed)
