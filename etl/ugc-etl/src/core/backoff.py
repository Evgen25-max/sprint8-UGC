"""Асинхронный экспоненциальный backoff с джиттером и прерыванием по сигналу."""

import asyncio
import logging
import random
from contextlib import suppress
from typing import Awaitable, Callable

logger = logging.getLogger(__name__)


class RetryAborted(Exception):
    """Ретраи прерваны внешним сигналом остановки (SIGTERM/SIGINT)."""


async def retry_async(
    fn: Callable[[], Awaitable],
    *,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
    base: float = 0.5,
    cap: float = 30.0,
    max_tries: int | None = None,
    on_error: Callable[[BaseException, int, float], None] | None = None,
    stop_event: asyncio.Event | None = None,
):
    attempt = 0
    while True:
        if stop_event is not None and stop_event.is_set():
            raise RetryAborted('Остановка сервиса: ретраи прерваны')
        try:
            return await fn()
        except exceptions as exc:
            attempt += 1
            if max_tries is not None and attempt >= max_tries:
                raise
            delay = min(cap, base * (2 ** (attempt - 1)))
            delay *= 0.5 + random.random() / 2  # джиттер 50–100%
            if on_error is not None:
                on_error(exc, attempt, delay)
            else:
                logger.warning('Попытка %s не удалась (%s), повтор через %.1fs',
                               attempt, exc, delay)
            if stop_event is None:
                await asyncio.sleep(delay)
            else:
                # просыпаемся сразу по сигналу, не досиживая backoff до 30с
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop_event.wait(), timeout=delay)
                if stop_event.is_set():
                    raise RetryAborted('Остановка сервиса: ретраи прерваны')