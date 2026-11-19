#!/usr/bin/env python3
"""
Нагрузочный генератор UGC-событий.
Шлёт события в ugc-api через nginx.

Использование:
    python3 tools/ugc_generator.py --rate 20 --duration 60 --url http://localhost/api/v1/ugc/events
    python3 tools/ugc_generator.py --rate 50 --duration 300 --users 50

Требует: aiohttp  (pip install aiohttp)
"""
import argparse
import asyncio
import json
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass, field

try:
    import aiohttp
except ImportError:
    sys.stderr.write('Нужен aiohttp: pip install aiohttp\n')
    sys.exit(2)


@dataclass
class Stats:
    total: int = 0
    success: int = 0
    failed: dict = field(default_factory=dict)  # status_code -> count
    latency_sum_ms: float = 0.0
    latency_max_ms: float = 0.0
    started_at: float = 0.0
    first_error_body: str | None = None  # тело первого не-2xx ответа (для отладки)
    first_error_status: int | None = None

    def record(self, status: int, latency_ms: float, body: str = '') -> None:
        self.total += 1
        if 200 <= status < 300:
            self.success += 1
        else:
            self.failed[status] = self.failed.get(status, 0) + 1
            if self.first_error_status is None:
                self.first_error_status = status
                self.first_error_body = body[:500]  # обрежем, если огромное
        self.latency_sum_ms += latency_ms
        if latency_ms > self.latency_max_ms:
            self.latency_max_ms = latency_ms

    def print_summary(self) -> None:
        elapsed = time.time() - self.started_at
        print('\n=== Сводка ===')
        print(f'Длительность:   {elapsed:.1f} с')
        print(f'Всего отправлено: {self.total}')
        print(f'Успешно (2xx):  {self.success}')
        if self.failed:
            print(f'Ошибки:         {self.failed}')
            if self.first_error_body:
                print(f'Тело первой ошибки ({self.first_error_status}): {self.first_error_body}')
        print(f'Real RPS:       {self.total / max(elapsed, 0.001):.1f}')
        if self.success:
            print(f'Avg latency:    {self.latency_sum_ms / self.success:.1f} мс')
            print(f'Max latency:    {self.latency_max_ms:.1f} мс')


# --- Реалистичные данные ---

FILMS = [str(uuid.UUID(int=i + 10**8)) for i in range(200)]
GENRES = ['Drama', 'Comedy', 'Sci-Fi', 'Action', 'Thriller', 'Horror', 'Documentary']
ACTORS = [f'Actor-{i}' for i in range(50)]
UA = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X) Safari/605.1',
    'Mozilla/5.0 (X11; Linux x86_64) Firefox/128.0',
    'okhttp/4.12.0 (Android 14)',
    'iOS/17.4 CFNetwork',
]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def random_session(user_idx: int) -> str:
    return f'20000000-0000-4000-8000-{user_idx:012d}'


def random_anon(user_idx: int) -> str:
    return f'device-{user_idx:04d}'


async def send_event(
    session: aiohttp.ClientSession,
    url: str,
    event: dict,
    stats: Stats,
    semaphore: asyncio.Semaphore,
    headers_extra: dict | None = None,
) -> None:
    """Отправляет одно событие, считает статус и latency."""
    async with semaphore:
        t0 = time.perf_counter()
        try:
            async with session.post(
                url,
                json=event,
                headers={'Content-Type': 'application/json', **(headers_extra or {})},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                body = await r.text()  # читаем для диагностики при ошибках
                latency = (time.perf_counter() - t0) * 1000
                stats.record(r.status, latency, body if r.status >= 400 else '')
        except Exception as exc:
            latency = (time.perf_counter() - t0) * 1000
            stats.record(599, latency, str(exc))


async def session_worker(
    user_idx: int,
    session: aiohttp.ClientSession,
    url: str,
    stop_event: asyncio.Event,
    stats: Stats,
    semaphore: asyncio.Semaphore,
    events_queue: asyncio.Queue,
) -> None:
    """
    Один виртуальный пользователь. Берёт задания из очереди
    и превращает их в реалистичные события.
    """
    anon = random_anon(user_idx)
    sid = random_session(user_idx)
    ua = random.choice(UA)
    current_view_id: str | None = None
    view_started_at: float = 0.0
    current_film: str | None = None

    while not stop_event.is_set():
        try:
            task = await asyncio.wait_for(events_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue

        if task is None:
            break

        kind = task['kind']
        event: dict | None = None

        if kind == 'click':
            # Неравномерно: карточки кликают чаще всего
            element_type = random.choices(
                ['film_card', 'trailer', 'category', 'banner', 'button', 'link', 'other'],
                weights=[40, 10, 15, 5, 10, 15, 5],
                k=1,
            )[0]
            current_film = random.choice(FILMS)

            # film_id — ТОЛЬКО когда клик относится к фильму
            film_id = current_film if element_type in ('film_card', 'trailer') else None

            # element_id и position осмысленны по типу элемента
            if element_type in ('film_card', 'trailer'):
                element_id = current_film
                position = random.randint(1, 20)          # позиция в карусели/списке
            elif element_type == 'category':
                element_id = random.choice(GENRES).lower().replace(' ', '-')
                position = None
            elif element_type == 'banner':
                element_id = f'promo-{random.randint(1, 10)}'
                position = None
            else:                                          # button, link, other
                element_id = f'{element_type}-{random.randint(1, 50)}'
                position = None

            event = {
                'event_id': str(uuid.uuid4()),
                'event_time': now_iso(),
                'session_id': sid,
                'anonymous_id': anon,
                'event_type': 'click',
                'element_type': element_type,
                'element_id': element_id,
                'film_id': film_id,
                'position': position,
                'page_url': f'/catalog/{current_film[:8]}',
                # user_agent убран: сервер берёт из HTTP-заголовка
            }
        elif kind == 'page_start':
            if current_film is None:
                current_film = random.choice(FILMS)
            current_view_id = str(uuid.uuid4())
            view_started_at = time.monotonic()
            event = {
                'event_id': str(uuid.uuid4()),
                'event_time': now_iso(),
                'session_id': sid,
                'anonymous_id': anon,
                'event_type': 'page_view',
                'phase': 'start',
                'view_id': current_view_id,
                'page_type': random.choice(['film', 'category', 'search', 'main']),
                'page_id': current_film,
                'duration_seconds': 0,
                'referrer': random.choice(['', '/catalog', '/search?q=sci-fi']),
                'page_url': f'/film/{current_film[:8]}',
            }
        elif kind == 'page_heartbeat':
            if current_view_id is None:
                continue
            elapsed = int(time.monotonic() - view_started_at)
            event = {
                'event_id': str(uuid.uuid4()),
                'event_time': now_iso(),
                'session_id': sid,
                'anonymous_id': anon,
                'event_type': 'page_view',
                'phase': 'heartbeat',
                'view_id': current_view_id,
                'page_type': 'film',
                'page_id': current_film or random.choice(FILMS),
                'duration_seconds': elapsed,
                'page_url': f'/film/{(current_film or "")[:8]}',
            }
        elif kind == 'page_end':
            if current_view_id is None:
                continue
            elapsed = int(time.monotonic() - view_started_at)
            event = {
                'event_id': str(uuid.uuid4()),
                'event_time': now_iso(),
                'session_id': sid,
                'anonymous_id': anon,
                'event_type': 'page_view',
                'phase': 'end',
                'view_id': current_view_id,
                'page_type': 'film',
                'page_id': current_film or random.choice(FILMS),
                'duration_seconds': elapsed,
                'page_url': f'/film/{(current_film or "")[:8]}',
            }
            current_view_id = None
            current_film = None
        elif kind == 'video_quality':
            from_q = random.choice(['1080p', '720p', '480p'])
            to_q = random.choice([q for q in ['1080p', '720p', '480p'] if q != from_q])
            event = {
                'event_id': str(uuid.uuid4()),
                'event_time': now_iso(),
                'session_id': sid,
                'anonymous_id': anon,
                'event_type': 'video_event',
                'subtype': 'quality_change',
                'film_id': current_film or random.choice(FILMS),
                'from_quality': from_q,
                'to_quality': to_q,
                'position_seconds': random.randint(10, 600),
                'page_url': f'/film/{(current_film or "")[:8]}',
            }
        elif kind == 'video_completed':
            event = {
                'event_id': str(uuid.uuid4()),
                'event_time': now_iso(),
                'session_id': sid,
                'anonymous_id': anon,
                'event_type': 'video_event',
                'subtype': 'completed',
                'film_id': current_film or random.choice(FILMS),
                'position_seconds': random.randint(60, 7200),
                'watched_seconds': random.randint(60, 7200),
                'video_duration_seconds': 7200,
                'page_url': f'/film/{(current_film or "")[:8]}',
            }
        elif kind == 'search':
            from_val = random.choice([0.0, 5.0, 7.0, 8.0])
            to_val = random.choice([v for v in [10.0, 9.0, 8.0, 7.0] if v >= from_val])
            event = {
                'event_id': str(uuid.uuid4()),
                'event_time': now_iso(),
                'session_id': sid,
                'anonymous_id': anon,
                'event_type': 'search_event',
                'query': random.choice(['sci-fi', 'action movies', 'comedy', 'drama 2020s', '']),
                'genres': random.sample(GENRES, k=random.randint(1, 3)),
                'rating_from': from_val,
                'rating_to': to_val,
                'actors': random.sample(ACTORS, k=random.randint(1, 4)),
                'year_from': random.choice([1990, 2000, 2010, 2020]),
                'year_to': 2025,
                'sort': random.choice(['rating', 'date', 'title']),
                'page_url': '/search',
            }

        if event is not None:
            await send_event(session, url, event, stats, semaphore, {'User-Agent': ua})


async def producer(
    events_queue: asyncio.Queue,
    stop_event: asyncio.Event,
    rate: float,
    duration: float,
) -> None:
    """
    Производит задания в очередь с нужным RPS.
    Соотношение типов имитирует реальный трафик.
    """
    choices = (
        ['click'] * 40
        + ['page_heartbeat'] * 35
        + ['page_start'] * 5
        + ['page_end'] * 5
        + ['video_quality'] * 3
        + ['video_completed'] * 7
        + ['search'] * 5
    )
    interval = 1.0 / rate
    deadline = time.monotonic() + duration
    while time.monotonic() < deadline and not stop_event.is_set():
        t0 = time.monotonic()
        kind = random.choice(choices)
        await events_queue.put({'kind': kind})
        sleep_for = interval - (time.monotonic() - t0)
        if sleep_for > 0:
            await asyncio.sleep(sleep_for)
    # Сигналим воркерам о завершении
    for _ in range(50):
        await events_queue.put(None)


async def main(rate: float, duration: float, users: int, url: str, concurrency: int) -> None:
    stats = Stats()
    stats.started_at = time.time()
    stop_event = asyncio.Event()
    semaphore = asyncio.Semaphore(concurrency)
    events_queue: asyncio.Queue = asyncio.Queue(maxsize=max(1000, rate * 10))

    connector = aiohttp.TCPConnector(limit=concurrency, limit_per_host=concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        workers = [
            asyncio.create_task(
                session_worker(i, session, url, stop_event, stats, semaphore, events_queue)
            )
            for i in range(users)
        ]
        producer_task = asyncio.create_task(producer(events_queue, stop_event, rate, duration))

        last_print = time.monotonic()
        while not producer_task.done():
            await asyncio.sleep(0.5)
            if time.monotonic() - last_print >= 2.0:
                elapsed = time.time() - stats.started_at
                real_rps = stats.total / max(elapsed, 0.001)
                print(
                    f'\r[{elapsed:5.1f}s] '
                    f'sent={stats.total} ok={stats.success} '
                    f'rps={real_rps:5.1f}/{rate:.0f} '
                    f'max_ms={stats.latency_max_ms:5.1f}',
                    end='',
                    flush=True,
                )
                last_print = time.monotonic()

        stop_event.set()
        await asyncio.gather(*workers, return_exceptions=True)

    stats.print_summary()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='UGC load generator')
    parser.add_argument('--rate', type=float, default=20.0, help='Target RPS')
    parser.add_argument('--duration', type=float, default=60.0, help='Duration in seconds')
    parser.add_argument('--users', type=int, default=10, help='Virtual users (concurrent sessions)')
    parser.add_argument('--concurrency', type=int, default=50, help='Max parallel requests')
    parser.add_argument(
        '--url',
        default='http://localhost/api/v1/ugc/events',
        help='UGC endpoint URL (nginx by default)',
    )
    args = parser.parse_args()
    print(
        f'Запуск генератора: rate={args.rate} rps, '
        f'duration={args.duration}s, users={args.users}, url={args.url}'
    )
    try:
        asyncio.run(main(args.rate, args.duration, args.users, args.url, args.concurrency))
    except KeyboardInterrupt:
        print('\nПрервано пользователем')
