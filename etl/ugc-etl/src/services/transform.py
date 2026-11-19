
from models.events import (ClickEvent, KafkaEnvelope, PageViewEvent,
                           SearchEvent, VideoEvent)

COLUMNS: dict[str, list[str]] = {
    'clicks': [
        'event_id', 'event_time', 'received_at', 'user_id', 'anonymous_id',
        'session_id', 'page_url', 'element_type', 'element_id', 'film_id',
        'position', 'user_agent', 'schema_version',
    ],
    'page_views': [
        'view_id', 'event_id', 'event_time', 'received_at', 'user_id',
        'anonymous_id', 'session_id', 'page_type', 'page_id',
        'duration_seconds', 'referrer', 'page_url', 'user_agent', 'schema_version',
    ],
    'video_events': [
        'event_id', 'event_time', 'received_at', 'user_id', 'anonymous_id',
        'session_id', 'subtype', 'film_id', 'from_quality', 'to_quality',
        'position_seconds', 'video_duration_seconds', 'watched_seconds',
        'page_url', 'user_agent', 'schema_version',
    ],
    'search_events': [
        'event_id', 'event_time', 'received_at', 'user_id', 'anonymous_id',
        'session_id', 'query', 'genres', 'rating_from', 'rating_to',
        'actors', 'year_from', 'year_to', 'sort', 'page_url', 'user_agent',
        'schema_version',
    ],
}


def envelope_to_row(env: KafkaEnvelope) -> tuple[str, list]:
    e = env.event
    row = {
        'event_id': e.event_id,
        'event_time': e.event_time,
        'received_at': env.received_at,
        'user_id': env.user_id,
        'anonymous_id': e.anonymous_id or '',
        'session_id': e.session_id,
        'page_url': e.page_url or '',
        'user_agent': env.user_agent or '',
        'schema_version': e.schema_version,
    }
    if isinstance(e, ClickEvent):
        table = 'clicks'
        row.update(element_type=e.element_type, element_id=e.element_id,
                   film_id=e.film_id, position=e.position)
    elif isinstance(e, PageViewEvent):
        table = 'page_views'
        row.update(view_id=e.view_id, page_type=e.page_type,
                   page_id=e.page_id or '', duration_seconds=e.duration_seconds,
                   referrer=e.referrer or '')
    elif isinstance(e, VideoEvent):
        table = 'video_events'
        row.update(subtype=e.subtype, film_id=e.film_id,
                   from_quality=e.from_quality or '', to_quality=e.to_quality or '',
                   position_seconds=e.position_seconds,
                   video_duration_seconds=e.video_duration_seconds,
                   watched_seconds=e.watched_seconds)
    elif isinstance(e, SearchEvent):
        table = 'search_events'
        row.update(query=e.query or '', genres=e.genres,
                   rating_from=e.rating_from, rating_to=e.rating_to,
                   actors=e.actors, year_from=e.year_from, year_to=e.year_to,
                   sort=e.sort or '')
    else:
        raise TypeError(f'Неизвестный тип события: {type(e).__name__}')
    return table, [row[c] for c in COLUMNS[table]]
