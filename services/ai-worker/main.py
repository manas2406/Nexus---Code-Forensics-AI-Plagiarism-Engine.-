import asyncio
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)


async def main() -> None:
    logger.info("[ai-worker] starting")


if __name__ == "__main__":
    asyncio.run(main())
