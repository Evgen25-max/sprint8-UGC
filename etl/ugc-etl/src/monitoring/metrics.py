
import asyncio
import gc
import logging

import psutil
from prometheus_client import Counter, Gauge, Histogram, start_http_server

logger = logging.getLogger(__name__)

CONSUMED = Counter('ugc_etl_consumed_total', 'Прочитано сообщений', ['topic'])
INSERTED = Counter('ugc_etl_inserted_rows_total', 'Вставлено строк', ['table'])
INSERT_ERRORS = Counter('ugc_etl_insert_errors_total', 'Ошибок вставки', ['table'])
DLQ = Counter('ugc_etl_dlq_total', 'Отправлено в DLQ', ['topic'])
COMMIT_ERRORS = Counter('ugc_etl_commit_errors_total', 'Ошибок коммита оффсетов')
BATCH_ROWS = Histogram('ugc_etl_batch_rows', 'Размер пачки', buckets=(1, 10, 50, 100, 500, 1000, 5000))
INSERT_LATENCY = Histogram('ugc_etl_insert_seconds', 'Время вставки', ['table'])
LAG = Gauge('ugc_etl_consumer_lag', 'Лаг консьюмера', ['topic', 'partition'])
RSS = Gauge('ugc_etl_memory_rss_bytes', 'RSS процесса, байт')
DLQ_ERRORS = Counter('ugc_etl_dlq_errors_total', 'Ошибок отправки в DLQ', ['topic'])
REWINDS = Counter('ugc_etl_batch_rewinds_total',
                  'Откатов позиции чтения после сбоя пачки', ['topic'])


def serve_metrics(port: int) -> None:
    start_http_server(port)  # фоновый поток prometheus-client
    logger.info('Метрики Prometheus на :%s/metrics', port)


async def memory_watcher(cfg, pause_event: asyncio.Event) -> None:
    proc = psutil.Process()
    limit = cfg.MEMORY_HARD_LIMIT_MB * 1024 * 1024
    while True:
        rss = proc.memory_info().rss
        RSS.set(rss)
        if rss > limit:
            if pause_event.is_set():
                logger.error('RSS %.1f МБ > лимита %s МБ: пауза чтения из Kafka',
                             rss / 1024 / 1024, cfg.MEMORY_HARD_LIMIT_MB)
            pause_event.clear()
            gc.collect()
        elif rss < limit * 0.85:
            pause_event.set()
        else:
            logger.debug('RSS %.1f МБ', rss / 1024 / 1024)
        await asyncio.sleep(cfg.MEMORY_CHECK_INTERVAL)
