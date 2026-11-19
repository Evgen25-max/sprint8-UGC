import asyncio
import logging
from datetime import datetime, timezone
from uuid import UUID

from aiokafka.errors import KafkaError
from fastapi import APIRouter, Depends, HTTPException, Request, status
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from core.auth import get_optional_user_id
from models.events import TOPIC_BY_TYPE, AnyEvent, EventBatch, KafkaEnvelope

logger = logging.getLogger(__name__)
router = APIRouter()
propagator = TraceContextTextMapPropagator()


def _build_value(event, user_id: UUID | None, request: Request) -> bytes:
    envelope = KafkaEnvelope(
        event=event,
        user_id=user_id,
        received_at=datetime.now(timezone.utc),
        user_agent=(request.headers.get('User-Agent') or '')[:512] or None,
        request_id=getattr(request.state, 'request_id', None),
    )
    return envelope.model_dump_json().encode('utf-8')


def _build_headers(event, request: Request) -> list[tuple[str, bytes]]:
    headers = [
        ('event_type', event.event_type.encode()),
        ('schema_version', str(event.schema_version).encode()),
        ('request_id', (getattr(request.state, 'request_id', '') or '').encode()),
    ]
    carrier: dict[str, str] = {}
    propagator.inject(carrier)  # traceparent -> Kafka headers
    headers.extend((k, v.encode()) for k, v in carrier.items())
    return headers


async def _accept(events: list, user_id: UUID | None, request: Request) -> dict:
    for event in events:
        if user_id is None and not event.anonymous_id:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail='Без авторизации обязателен anonymous_id',
            )
    producer = request.app.state.producer

    async def _send_one(event) -> None:
        key = str(user_id) if user_id else event.anonymous_id
        await producer.send(
            TOPIC_BY_TYPE[event.event_type], key,
            _build_value(event, user_id, request),
            _build_headers(event, request),
        )

    try:
        await asyncio.gather(*(_send_one(e) for e in events))
    except (KafkaError, asyncio.TimeoutError, RuntimeError) as exc:
        logger.error('Kafka недоступна при отправке событий: %s', exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail='Коллектор временно недоступен. Повторите запрос с теми же event_id.',
        )
    return {'accepted': len(events), 'event_ids': [str(e.event_id) for e in events]}


@router.post('/events', status_code=status.HTTP_202_ACCEPTED, summary='Одно событие')
async def collect_event(
    event: AnyEvent,
    request: Request,
    user_id: UUID | None = Depends(get_optional_user_id),
) -> dict:
    return await _accept([event], user_id, request)


@router.post('/events/batch', status_code=status.HTTP_202_ACCEPTED, summary='Пачка событий (<=100)')
async def collect_batch(
    body: EventBatch,
    request: Request,
    user_id: UUID | None = Depends(get_optional_user_id),
) -> dict:
    return await _accept(body.events, user_id, request)
