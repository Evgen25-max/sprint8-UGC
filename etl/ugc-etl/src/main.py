
import asyncio
import json
import logging
import logging.config
import signal
from contextlib import suppress
from pathlib import Path

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError

from core.backoff import retry_async
from core.config import config
from db.clickhouse import ClickHouseStorage
from monitoring import metrics
from services.pipeline import process_batch
from services.transform import COLUMNS

Path('logs').mkdir(exist_ok=True)
logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {'verbose': {'format': '%(asctime)s %(name)s %(levelname)s %(message)s'}},
    'handlers': {
        'console': {'class': 'logging.StreamHandler', 'formatter': 'verbose'},
        'file': {'class': 'logging.handlers.RotatingFileHandler', 'formatter': 'verbose',
                 'filename': 'logs/ugc-etl.log', 'maxBytes': 10485760, 'backupCount': 5,
                 'encoding': 'utf-8'},
    },
    'root': {'level': config.LOG_LEVEL, 'handlers': ['console', 'file']},
})
logger = logging.getLogger(__name__)


async def run() -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    # --- Стартовые проверки: критичные зависимости недоступны -> НЕ стартуем ---
    storage = ClickHouseStorage()
    await storage.start()
    await storage.check_tables(COLUMNS.keys())

    consumer = AIOKafkaConsumer(
        *config.KAFKA_TOPICS,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        group_id=config.KAFKA_GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset='earliest',
        max_poll_records=config.KAFKA_MAX_POLL_RECORDS,
        max_poll_interval_ms=config.KAFKA_MAX_POLL_INTERVAL_MS,
        fetch_max_wait_ms=config.KAFKA_FETCH_MAX_WAIT_MS,
    )
    dlq_producer = AIOKafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        acks='all', enable_idempotence=True, compression_type='lz4',
    )
    await consumer.start()

    await dlq_producer.start()

    metrics.serve_metrics(config.METRICS_PORT)
    pause_event = asyncio.Event()
    pause_event.set()
    watcher = asyncio.create_task(metrics.memory_watcher(config, pause_event))
    logger.info('ETL запущен: topics=%s group=%s', config.KAFKA_TOPICS, config.KAFKA_GROUP_ID)

    try:
        while not stop_event.is_set():
            await pause_event.wait()  # backpressure по памяти
            try:
                batch = await consumer.getmany(
                    timeout_ms=config.BATCH_TIMEOUT_MS,
                    max_records=config.KAFKA_MAX_POLL_RECORDS,
                )
            except KafkaError as exc:
                logger.error('Kafka недоступна: %s. Пауза 2с, продолжаем.', exc)
                await asyncio.sleep(2)
                continue
            if not batch:
                continue

            for tp, msgs in batch.items():
                metrics.CONSUMED.labels(tp.topic).inc(len(msgs))
            rows_by_table, offsets, dlq_items = process_batch(batch)

            try:
                # 1. Poison messages -> DLQ (ограниченные ретраи; не вышло -> батч перечитаем)
                for item in dlq_items:
                    async def _send_dlq(item=item):
                        await dlq_producer.send_and_wait(
                            config.DLQ_TOPIC,
                            key=item.key,
                            value=json.dumps({
                                'topic': item.topic, 'partition': item.partition,
                                'offset': item.offset, 'error': item.error,
                                'raw': item.raw.decode('utf-8', errors='replace'),
                            }, ensure_ascii=False).encode('utf-8'),
                            headers=[('error', item.error[:200].encode('utf-8'))],
                        )
                    await retry_async(_send_dlq, exceptions=(KafkaError,), max_tries=5,
                                      base=config.RETRY_BASE_SECONDS, cap=config.RETRY_CAP_SECONDS)
                    metrics.DLQ.labels(item.topic).inc()

                # 2. Вставка в ClickHouse: бесконечный backoff, оффсеты НЕ коммитим до успеха
                for table, rows in rows_by_table.items():
                    async def _insert(table=table, rows=rows):
                        try:
                            await storage.insert(table, COLUMNS[table], rows)
                        except Exception:
                            with suppress(Exception):
                                await storage.reconnect()
                            raise

                    def _on_error(exc, attempt, delay, table=table):
                        metrics.INSERT_ERRORS.labels(table).inc()
                        logger.warning('CH insert %s: попытка %s не удалась (%s), повтор через %.1fs',
                                       table, attempt, exc, delay)

                    with metrics.INSERT_LATENCY.labels(table).time():
                        await retry_async(_insert, exceptions=(Exception,), max_tries=None,
                                          base=config.RETRY_BASE_SECONDS,
                                          cap=config.RETRY_CAP_SECONDS, on_error=_on_error)
                    metrics.INSERTED.labels(table).inc(len(rows))

                # 3. Коммит оффсетов ТОЛЬКО после успешной вставки всего батча
                try:
                    await retry_async(lambda: consumer.commit(offsets),
                                      exceptions=(KafkaError,), max_tries=5,
                                      base=config.RETRY_BASE_SECONDS, cap=config.RETRY_CAP_SECONDS)
                except Exception as exc:
                    metrics.COMMIT_ERRORS.inc()
                    logger.warning('Коммит оффсетов не удался (%s): будут дубли, '
                                   'ReplacingMergeTree их схлопнет', exc)

                for tp, offset in offsets.items():
                    hw = consumer.highwater(tp)
                    if hw is not None:
                        metrics.LAG.labels(tp.topic, str(tp.partition)).set(max(0, hw - offset))
                metrics.BATCH_ROWS.observe(sum(len(m) for m in batch.values()))

            except Exception:
                logger.exception('Батч не обработан целиком — оффсеты не коммитим, перечитаем')
                await asyncio.sleep(2)
    finally:
        watcher.cancel()
        with suppress(asyncio.CancelledError):
            await watcher
        await consumer.stop()
        await dlq_producer.stop()
        await storage.close()
        logger.info('ETL остановлен')


if __name__ == '__main__':
    asyncio.run(run())
