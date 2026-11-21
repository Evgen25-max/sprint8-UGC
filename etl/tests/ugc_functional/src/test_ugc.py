
import uuid
from datetime import datetime, timezone

import pytest
from datetime import timedelta
from conftest import ch_query, make_token, wait_count
from settings import SERVICE_URL

URL = f'{SERVICE_URL}/api/v1/ugc/events'


def base_event(**kw):
    """
    Базовое валидное событие: event_id, event_time=now(UTC),
    session_id и anonymous_id.
    """
    e = {'event_id': str(uuid.uuid4()), 'event_time': datetime.now(timezone.utc).isoformat(),
         'session_id': str(uuid.uuid4()), 'anonymous_id': f'anon-{uuid.uuid4().hex[:12]}'}
    e.update(kw)
    return e


@pytest.mark.asyncio
async def test_anonymous_click_stored(client):
    """
    Проверяет сквозной путь без JWT: nginx -> ugc-api -> Kafka (ugc.clicks)
    -> ugc-etl -> ClickHouse. Успех = не только 202 от API, но и строка
    в ugc.clicks с тем же anonymous_id, что ушёл в запросе.
    Подтверждает, что ETL не теряет и не искажает идентификатор субъекта.
    """
    e = base_event(event_type='click', element_type='film_card', element_id='card-1')
    async with client.post(URL, json=e) as r:
        assert r.status == 202, await r.text()
    q = f"SELECT count() FROM ugc.clicks WHERE event_id='{e['event_id']}'"
    await wait_count(client, q, 1)
    anon = await ch_query(client, f"SELECT anonymous_id FROM ugc.clicks WHERE event_id='{e['event_id']}'")
    assert anon == e['anonymous_id']


@pytest.mark.asyncio
async def test_user_id_taken_from_jwt(client):
    """
    Безопасность: user_id берётся ТОЛЬКО из подписанного JWT, никогда из тела.

    В теле anonymous_id отсутствует — значит единственный источник личности
    Проверяет, что в ClickHouse записан именно sub, а не что-то из запроса:
    клиент не может подменить чужой user_id. Заодно подтверждает, что
    авторизованному событию anonymous_id не требуется.
    """
    token, sub = make_token()
    e = base_event(event_type='click', element_type='trailer', element_id='t-1')
    e.pop('anonymous_id')
    async with client.post(URL, json=e, headers={'Authorization': f'Bearer {token}'}) as r:
        assert r.status == 202, await r.text()
    q = f"SELECT user_id FROM ugc.clicks WHERE event_id='{e['event_id']}'"
    await wait_count(client, f"SELECT count() FROM ugc.clicks WHERE event_id='{e['event_id']}'", 1)
    assert (await ch_query(client, q)) == sub


@pytest.mark.asyncio
async def test_no_anonymous_id_rejected(client):
    """
    Контракт: событие без субъекта не принимается.

    Нет JWT и нет anonymous_id -> событие невозможно связать ни с
    пользователем, ни с устройством, для аналитики оно бесполезно.
    Проверка `_accept()` в api/v1/events.py отдаёт 422 ДО отправки в Kafka.
    """
    e = base_event(event_type='click', element_type='button', element_id='b')
    e.pop('anonymous_id')
    async with client.post(URL, json=e) as r:
        assert r.status == 422


@pytest.mark.asyncio
async def test_invalid_token_rejected(client):
    """
    Безопасность: предъявленный, но невалидный токен = 401, а не аноним.
    """
    e = base_event(event_type='click', element_type='button', element_id='b')
    async with client.post(URL, json=e, headers={'Authorization': 'Bearer bad.token.x'}) as r:
        assert r.status == 401


@pytest.mark.asyncio
async def test_duplicate_event_id_deduplicated(client):
    """
    Идемпотентность: at-least-once доставка + дедуп на чтении.

    Один и тот же event_id отправляется дважды — API принимает оба (202)
    В ClickHouse физически лежат 2 строки, но SELECT ... FINAL возвращает 1:
    ReplacingMergeTree с ORDER BY event_id схлопывает версии.
    """
    e = base_event(event_type='click', element_type='banner', element_id='dup')
    for _ in range(2):
        async with client.post(URL, json=e) as r:
            assert r.status == 202
    await wait_count(client, f"SELECT count() FROM ugc.clicks WHERE event_id='{e['event_id']}'", 2)
    final = await ch_query(client, f"SELECT count() FROM ugc.clicks FINAL WHERE event_id='{e['event_id']}'")
    assert int(final) == 1


@pytest.mark.asyncio
async def test_page_view_duration(client):
    """
    Бизнес-требование №2: сколько времени пользователь провёл на странице.

    Один view_id, две фазы: start (0 сек) и end (42 сек).
    """
    view_id = str(uuid.uuid4())
    for phase, dur in (('start', 0), ('end', 42)):
        e = base_event(event_type='page_view', view_id=view_id, phase=phase,
                       page_type='film', duration_seconds=dur)
        async with client.post(URL, json=e) as r:
            assert r.status == 202
    q = f"SELECT max(duration_seconds) FROM ugc.page_views WHERE view_id='{view_id}'"
    assert await wait_count(client, q, 42) == 42


@pytest.mark.asyncio
async def test_batch_limit(client):
    """
    Защита от перегрузки: лимит размера пачки.

    EventBatch объявлен как list[AnyEvent] с max_length=100, поэтому 101
    событие отсекается Pydantic на входе (422) — до продюсера и до Kafka.
    Ограничение держит память ugc-api (mem_limit 256m) и согласуется с
    client_max_body_size 1m на nginx для /api/v1/ugc.
    """
    events = [base_event(event_type='click', element_type='link', element_id=str(i))
              for i in range(101)]
    async with client.post(f'{SERVICE_URL}/api/v1/ugc/events/batch', json={'events': events}) as r:
        assert r.status == 422


@pytest.mark.asyncio
async def test_request_id_generated(client):
    """
    Наблюдаемость: X-Request-Id есть в ответе всегда, даже если не прислали.

    Запрос уходит без заголовка. Дальше два допустимых источника:
    nginx (map $http_x_request_id -> $request_id, 32 hex) или
    RequestIDMiddleware в ugc-api (uuid4, 36 символов).
    Тест проверяет длину, а не формат, чтобы проходить и через nginx,
    и при прямом обращении к ugc-api.
    """
    e = base_event(event_type='click', element_type='link', element_id='rid')
    async with client.post(URL, json=e) as r:
        assert r.status == 202
        request_id = r.headers.get('X-Request-Id', '')
        # nginx генерирует 32 hex (MD5), ugc-api может сгенерить UUID (36)
        assert len(request_id) in (32, 36)


@pytest.mark.asyncio
async def test_batch_event_id_required(client):
    """event_id обязателен: без него API вернёт 422, а не сгенерирует."""
    e = {
        'event_time': datetime.now(timezone.utc).isoformat(),
        'session_id': str(uuid.uuid4()),
        'anonymous_id': f'anon-{uuid.uuid4().hex[:12]}',
        'event_type': 'click',
        'element_type': 'link',
        'element_id': 'no-id',
        # event_id намеренно пропущен
    }
    async with client.post(URL, json=e) as r:
        assert r.status == 422

@pytest.mark.asyncio
async def test_retry_with_same_event_id_deduplicated(client):
    """Повтор запроса с тем же event_id не создаёт дубликат."""
    event_id = str(uuid.uuid4())
    e = base_event(event_type='click', element_type='button', element_id='retry')
    e['event_id'] = event_id

    # Первая отправка
    async with client.post(URL, json=e) as r:
        assert r.status == 202

    # Повтор (имитация ретрая после 503)
    async with client.post(URL, json=e) as r:
        assert r.status == 202

    await wait_count(client,
                     f"SELECT count() FROM ugc.clicks WHERE event_id='{event_id}'",
                     2)
    final = await ch_query(client,
                           f"SELECT count() FROM ugc.clicks FINAL WHERE event_id='{event_id}'")
    assert int(final) == 1


@pytest.mark.asyncio
async def test_page_view_same_view_id_different_users(client):
    """Два разных пользователя с одинаковым view_id — два просмотра."""
    view_id = str(uuid.uuid4())

    a = base_event(event_type='page_view', view_id=view_id, phase='end',
                   page_type='film', duration_seconds=100)
    b = base_event(event_type='page_view', view_id=view_id, phase='end',
                   page_type='film', duration_seconds=200)

    for e in (a, b):
        async with client.post(URL, json=e) as r:
            assert r.status == 202

    await wait_count(client,
                     f"SELECT count() FROM ugc.page_views WHERE view_id='{view_id}'",
                     2)
    final = await ch_query(
        client,
        f"SELECT count() FROM ugc.page_views FINAL WHERE view_id='{view_id}'")
    assert int(final) == 2


@pytest.mark.asyncio
async def test_event_age_near_boundary(client):
    """Событие ровно 29 дней назад должно пройти и через API, и через ETL."""
    old_time = (datetime.now(timezone.utc) - timedelta(days=29, hours=23)).isoformat()
    e = base_event(event_type='click', element_type='button', element_id='old')
    e['event_time'] = old_time

    async with client.post(URL, json=e) as r:
        assert r.status == 202, await r.text()

    q = f"SELECT count() FROM ugc.clicks WHERE event_id='{e['event_id']}'"
    await wait_count(client, q, 1)