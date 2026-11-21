"""ETL Kafka -> ClickHouse: at-least-once, ручные оффсеты, DLQ, backpressure.

Инвариант: закоммиченный оффсет означает «всё до него либо в ClickHouse,
либо в DLQ». Пачка не обработана целиком -> consumer.seek() на её начало.
getmany() сдвигает позицию при ВЫДАЧЕ, а не при коммите, поэтому без
явного отката следующая пачка перезаписала бы оффсеты провалившейся.
"""

import asyncio
import json
import logging
import logging.config
import signal
from contextlib import suppress
from pathlib import Path

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError

from core.backoff import RetryAborted, retry_async
from core.config import config
from db.clickhouse import ClickHouseStorage
from monitoring import metrics
from services.pipeline import DlqItem, process_batch
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

# 64 КБ: даже при раздувании в 6 раз (errors='replace' + JSON-escaping)
# запись гарантированно укладывается в max_request_size ~1 МБ.
DLQ_RAW_LIMIT_BYTES = 64 * 1024


def _is_retryable(exc: BaseException) -> bool:
    """Ответы брокера в aiokafka несут флаг retriable.
    Сетевые ошибки флага не имеют -> считаем их временными."""
    return getattr(exc, 'retriable', True) is not False


def _dlq_payload(item: DlqItem, *, with_raw: bool) -> bytes:
    raw = item.raw[:DLQ_RAW_LIMIT_BYTES] if with_raw else b''
    return json.dumps({
        'topic': item.topic, 'partition': item.partition, 'offset': item.offset,
        'error': item.error,
        'raw': raw.decode('utf-8', errors='replace'),
        'raw_size': len(item.raw),
        'raw_truncated': bool(with_raw and len(item.raw) > DLQ_RAW_LIMIT_BYTES),
        'raw_omitted': not with_raw,
    }, ensure_ascii=False).encode('utf-8')


def _rewind(consumer: AIOKafkaConsumer, positions: dict) -> None:
    """Откат позиции чтения на начало необработанных пачек."""
    assigned = consumer.assignment()
    for tp, offset in positions.items():
        metrics.REWINDS.labels(tp.topic).inc()
        if tp not in assigned:
            # после ребаланса партиция нам не принадлежит: новый владелец
            # начнёт с последнего закоммиченного оффсета, потери нет
            logger.warning('Партиция %s не назначена — откат не нужен', tp)
            continue
        try:
            consumer.seek(tp, offset)
            logger.info('Позиция чтения %s откатана на оффсет %s', tp, offset)
        except Exception:
            logger.exception('Не удалось откатить %s на оффсет %s', tp, offset)


async def _send_to_dlq(producer: AIOKafkaProducer, item: DlqItem,
                       stop_event: asyncio.Event) -> None:
    """Сначала с усечённым raw; если брокер отвергает безвозвратно — без raw."""
    last_exc: Exception | None = None
    for with_raw in (True, False):
        payload = _dlq_payload(item, with_raw=with_raw)
        headers = [
            ('error', item.error[:200].encode('utf-8')),
            ('orig_topic', item.topic.encode('utf-8')),
            ('orig_partition', str(item.partition).encode()),
            ('orig_offset', str(item.offset).encode()),
        ]

        async def _send(payload=payload, headers=headers):
            await producer.send_and_wait(
                config.DLQ_TOPIC, key=item.key, value=payload, headers=headers,
            )

        def _on_error(exc, attempt, delay):
            metrics.DLQ_ERRORS.labels(item.topic).inc()
            logger.warning('DLQ %s/%s@%s: попытка %s не удалась (%s), повтор через %.1fs',
                           item.topic, item.partition, item.offset, attempt, exc, delay)

        try:
            await retry_async(_send, exceptions=(KafkaError,), max_tries=5,
                              base=config.RETRY_BASE_SECONDS, cap=config.RETRY_CAP_SECONDS,
                              on_error=_on_error, stop_event=stop_event)
            return
        except KafkaError as exc:
            last_exc = exc
            if _is_retryable(exc):
                raise  # временная -> откат пачки и перечитывание
            logger.error('DLQ отверг %s/%s@%s безвозвратно (%s), raw=%s — пробуем без тела',
                         item.topic, item.partition, item.offset, exc, with_raw)

    metrics.DLQ_ERRORS.labels(item.topic).inc()
    raise RuntimeError(
        f'DLQ недоступен для {item.topic}/{item.partition}@{item.offset}: {last_exc}')


async def _insert_rows(storage: ClickHouseStorage, table: str, rows: list[list],
                       stop_event: asyncio.Event) -> None:
    async def _insert():
        try:
            await storage.insert(table, COLUMNS[table], rows)
        except Exception:
            with suppress(Exception):
                await storage.reconnect()
            raise

    def _on_error(exc, attempt, delay):
        metrics.INSERT_ERRORS.labels(table).inc()
        logger.warning('CH insert %s: попытка %s не удалась (%s), повтор через %.1fs',
                       table, attempt, exc, delay)

    with metrics.INSERT_LATENCY.labels(table).time():
        await retry_async(_insert, exceptions=(Exception,), max_tries=None,
                          base=config.RETRY_BASE_SECONDS, cap=config.RETRY_CAP_SECONDS,
                          on_error=_on_error, stop_event=stop_event)
    metrics.INSERTED.labels(table).inc(len(rows))


async def _commit_offsets(consumer: AIOKafkaConsumer, offsets: dict,
                          stop_event: asyncio.Event) -> None:
    """Коммитим только свои партиции и держим пачку до успеха коммита."""
    assigned = consumer.assignment()
    to_commit = {tp: off for tp, off in offsets.items() if tp in assigned}
    if not to_commit:
        logger.warning('Партиции %s потеряны при ребалансе — коммитить нечего', list(offsets))
        return
    if len(to_commit) != len(offsets):
        logger.warning('Часть партиций потеряна при ребалансе: %s',
                       set(offsets) - set(to_commit))

    def _on_error(exc, attempt, delay):
        metrics.COMMIT_ERRORS.inc()
        logger.warning('Коммит оффсетов не удался (%s), попытка %s, повтор через %.1fs',
                       exc, attempt, delay)

    # max_tries=None: не переходим к следующей пачке, пока эта не подтверждена.
    # Дубли не плодим — повторяем только коммит, а не вставку.
    await retry_async(lambda: consumer.commit(to_commit),
                      exceptions=(KafkaError,), max_tries=None,
                      base=config.RETRY_BASE_SECONDS, cap=config.RETRY_CAP_SECONDS,
                      on_error=_on_error, stop_event=stop_event)


async def _update_lag(consumer: AIOKafkaConsumer, tp) -> None:
    """Лаг от ЗАКОММИЧЕННОГО оффсета: локальная позиция врёт при сбое коммита."""
    hw = consumer.highwater(tp)
    committed = await consumer.committed(tp)
    if hw is None or committed is None:
        return
    metrics.LAG.labels(tp.topic, str(tp.partition)).set(max(0, hw - committed))


async def _handle_partition(*, consumer, dlq_producer, storage, tp, messages,
                            stop_event) -> None:
    start_offset = messages[0].offset
    rows_by_table, offsets, dlq_items = process_batch({tp: messages})

    try:
        # 1. Валидные строки -> ClickHouse (основной путь данных).
        for table, rows in rows_by_table.items():
            await _insert_rows(storage, table, rows, stop_event)

        # 2. Poison messages -> DLQ. После вставки: сбой DLQ не задерживает данные.
        for item in dlq_items:
            await _send_to_dlq(dlq_producer, item, stop_event)
            metrics.DLQ.labels(item.topic).inc()

        # 3. Коммит только когда всё легло.
        await _commit_offsets(consumer, offsets, stop_event)

    except RetryAborted:
        _rewind(consumer, {tp: start_offset})
        raise
    except Exception:
        logger.exception('Пачка %s [%s..%s] не обработана целиком: оффсеты не коммитим, '
                         'откатываем позицию и перечитываем',
                         tp, start_offset, messages[-1].offset)
        _rewind(consumer, {tp: start_offset})
        await asyncio.sleep(2)
        return

    metrics.BATCH_ROWS.observe(len(messages))
    await _update_lag(consumer, tp)


async def run() -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    storage = ClickHouseStorage()
    consumer: AIOKafkaConsumer | None = None
    dlq_producer: AIOKafkaProducer | None = None
    watcher: asyncio.Task | None = None

    # try охватывает и старт: иначе падение serve_metrics оставит
    # consumer/producer незакрытыми
    try:
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

        # DLQ проверяем на старте
        if not await dlq_producer.partitions_for(config.DLQ_TOPIC):
            raise RuntimeError(
                f'DLQ-топик {config.DLQ_TOPIC} отсутствует. Запустите kafka-init.')

        metrics.serve_metrics(config.METRICS_PORT)
        pause_event = asyncio.Event()
        pause_event.set()
        watcher = asyncio.create_task(metrics.memory_watcher(config, pause_event))
        logger.info('ETL запущен: topics=%s group=%s',
                    config.KAFKA_TOPICS, config.KAFKA_GROUP_ID)

        while not stop_event.is_set():
            await pause_event.wait()
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

            items = list(batch.items())
            for idx, (tp, messages) in enumerate(items):
                if stop_event.is_set():
                    _rewind(consumer, {t: m[0].offset for t, m in items[idx:]})
                    break
                try:
                    await _handle_partition(
                        consumer=consumer, dlq_producer=dlq_producer, storage=storage,
                        tp=tp, messages=messages, stop_event=stop_event,
                    )
                except RetryAborted:
                    _rewind(consumer, {t: m[0].offset for t, m in items[idx + 1:]})
                    break
    finally:
        if watcher is not None:
            watcher.cancel()
            with suppress(asyncio.CancelledError):
                await watcher
        if consumer is not None:
            await consumer.stop()
        if dlq_producer is not None:
            await dlq_producer.stop()
        await storage.close()
        logger.info('ETL остановлен')


if __name__ == '__main__':
    asyncio.run(run())