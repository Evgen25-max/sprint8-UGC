
import logging
import sys
import time
from pathlib import Path

import clickhouse_connect

from core.config import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('ugc-migrations')

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / 'migrations'


def split_statements(sql: str) -> list[str]:
    statements = []
    for chunk in sql.split(';'):
        body = '\n'.join(
            line for line in chunk.splitlines() if not line.strip().startswith('--')
        ).strip()
        if body:
            statements.append(chunk.strip())
    return statements


def connect_with_retry(attempts: int = 10, delay: float = 2.0):
    for attempt in range(1, attempts + 1):
        try:
            client = clickhouse_connect.get_client(
                host=config.CLICKHOUSE_HOST, port=config.CLICKHOUSE_PORT,
                username=config.CLICKHOUSE_USER, password=config.CLICKHOUSE_PASSWORD,
            )
            client.command('SELECT 1')
            return client
        except Exception as exc:
            logger.warning('ClickHouse недоступен (%s/%s): %s', attempt, attempts, exc)
            if attempt == attempts:
                raise
            time.sleep(delay)


def main() -> None:
    client = connect_with_retry()
    db = config.CLICKHOUSE_DB
    client.command(f'CREATE DATABASE IF NOT EXISTS {db}')
    client.command(
        f'CREATE TABLE IF NOT EXISTS {db}.schema_migrations '
        '(version String, applied_at DateTime DEFAULT now()) '
        'ENGINE MergeTree ORDER BY version'
    )
    applied = {row[0] for row in client.query(
        f'SELECT version FROM {db}.schema_migrations').result_rows}

    for path in sorted(MIGRATIONS_DIR.glob('*.sql')):
        version = path.stem
        if version in applied:
            logger.info('Миграция %s уже применена, пропуск', version)
            continue
        logger.info('Применяю миграцию %s...', version)
        sql = path.read_text(encoding='utf-8').replace('__DB__', db)
        for statement in split_statements(sql):
            client.command(statement)
        client.insert('schema_migrations', [[version]],
                      column_names=['version'], database=db)
        logger.info('Миграция %s применена', version)
    logger.info('Все миграции применены')
    client.close()


if __name__ == '__main__':
    try:
        main()
    except Exception:
        logger.exception('Ошибка миграций')
        sys.exit(1)
