
import asyncio
import logging
import random
from typing import Awaitable, Callable

logger = logging.getLogger(__name__)


async def retry_async(
    fn: Callable[[], Awaitable],
    *,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
    base: float = 0.5,
    cap: float = 30.0,
    max_tries: int | None = None,
    on_error: Callable[[BaseException, int, float], None] | None = None,
):
    attempt = 0
    while True:
        try:
            return await fn()
        except exceptions as exc:
            attempt += 1
            if max_tries is not None and attempt >= max_tries:
                raise
            delay = min(cap, base * (2 ** (attempt - 1)))
            delay *= 0.5 + random.random() / 2
            if on_error is not None:
                on_error(exc, attempt, delay)
            else:
                logger.warning('Попытка %s не удалась (%s), повтор через %.1fs',
                               attempt, exc, delay)
            await asyncio.sleep(delay)
