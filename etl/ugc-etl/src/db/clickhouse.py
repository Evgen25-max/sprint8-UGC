
import asyncio
import logging
from contextlib import suppress

import clickhouse_connect

from core.config import config

logger = logging.getLogger(__name__)


class ClickHouseStorage:
    def __init__(self) -> None:
        self._client = None
        self._lock = asyncio.Lock()  # clickhouse-connect Client не потокобезопасен

    def _connect(self):
        return clickhouse_connect.get_client(
            host=config.CLICKHOUSE_HOST,
            port=config.CLICKHOUSE_PORT,
            username=config.CLICKHOUSE_USER,
            password=config.CLICKHOUSE_PASSWORD,
            database=config.CLICKHOUSE_DB,
            connect_timeout=config.CH_CONNECT_TIMEOUT,
            send_receive_timeout=config.CH_SEND_RECEIVE_TIMEOUT,
        )

    async def start(self) -> None:
        self._client = await asyncio.to_thread(self._connect)
        await asyncio.to_thread(self._client.command, 'SELECT 1')
        logger.info('ClickHouse доступен: %s:%s/%s',
                    config.CLICKHOUSE_HOST, config.CLICKHOUSE_PORT, config.CLICKHOUSE_DB)

    async def check_tables(self, tables) -> None:
        for table in tables:
            exists = await asyncio.to_thread(
                self._client.command, f'EXISTS TABLE {config.CLICKHOUSE_DB}.{table}')
            if not exists:
                raise RuntimeError(
                    f'Таблица {config.CLICKHOUSE_DB}.{table} отсутствует. Запустите ugc-migrations.')

    async def insert(self, table: str, column_names: list[str], rows: list[list]) -> None:
        if not rows:
            return
        async with self._lock:
            await asyncio.to_thread(
                self._client.insert,
                table=table, data=rows, column_names=column_names,
            )

    async def reconnect(self) -> None:
        async with self._lock:
            with suppress(Exception):
                if self._client is not None:
                    await asyncio.to_thread(self._client.close)
            self._client = await asyncio.to_thread(self._connect)

    async def close(self) -> None:
        with suppress(Exception):
            if self._client is not None:
                await asyncio.to_thread(self._client.close)
        self._client = None
