"""Обёртка над AIOKafkaProducer: fail-fast при старте, честные ошибки при отправке."""
import asyncio
import logging

from aiokafka import AIOKafkaProducer

from core.config import config
from models.events import TOPIC_BY_TYPE

logger = logging.getLogger(__name__)


class EventProducer:
    def __init__(self) -> None:
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        producer = AIOKafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            acks='all',
            enable_idempotence=True,
            linger_ms=config.KAFKA_LINGER_MS,
            compression_type=config.KAFKA_COMPRESSION,
            request_timeout_ms=10000,
            retry_backoff_ms=200,
        )
        await producer.start()  # Kafka недоступна -> исключение -> сервис НЕ стартует
        missing = [
            topic for topic in TOPIC_BY_TYPE.values()
            if not await producer.partitions_for(topic)
        ]
        if missing:
            await producer.stop()
            raise RuntimeError(f'Топики Kafka отсутствуют: {missing}. Запустите kafka-init.')
        self._producer = producer
        logger.info('Kafka producer запущен: %s', config.KAFKA_BOOTSTRAP_SERVERS)


    async def check(self) -> None:
        """Liveness: брокер отвечает метаданными по нашему топику."""
        if self._producer is None:
            raise RuntimeError('Producer не инициализирован')
        topic = next(iter(TOPIC_BY_TYPE.values()))
        if not await self._producer.partitions_for(topic):
            raise RuntimeError(f'Нет метаданных по топику {topic}')
            
    async def send(self, topic: str, key: str, value: bytes,
                   headers: list[tuple[str, bytes]]) -> None:
        if self._producer is None:
            raise RuntimeError('Producer не инициализирован')
        await asyncio.wait_for(
            self._producer.send_and_wait(topic, value=value, key=key.encode(), headers=headers),
            timeout=config.KAFKA_SEND_TIMEOUT_SECONDS,
        )

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()  # flush буферов
            self._producer = None
