import asyncio
import time
import uuid

import aiohttp
import jwt
import pytest_asyncio

from settings import CH_PASSWORD, CH_URL, CH_USER, JWT_SECRET_KEY


@pytest_asyncio.fixture(scope='function')
async def client():
    async with aiohttp.ClientSession() as session:
        yield session


def make_token(sub: str | None = None) -> tuple[str, str]:
    sub = sub or str(uuid.uuid4())
    payload = {
        'sub': sub,
        'jti': str(uuid.uuid4()),
        'type': 'access',
        'is_superuser': False,
        'ver': 1,
        'iat': int(time.time()),
        'exp': int(time.time()) + 3600
    }
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm='HS256'), sub


async def ch_query(session: aiohttp.ClientSession, query: str) -> str:
    async with session.get(
        CH_URL,
        params={'query': query},
        headers={
            'X-ClickHouse-User': CH_USER,
            'X-ClickHouse-Key': CH_PASSWORD,
        },
    ) as resp:
        assert resp.status == 200, await resp.text()
        return (await resp.text()).strip()


async def wait_count(session, query: str, expected: int, timeout: float = 20.0) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = int(await ch_query(session, query) or 0)
        if value >= expected:
            return value
        await asyncio.sleep(0.5)
    raise AssertionError(f'Не дождались {expected} по запросу: {query}')