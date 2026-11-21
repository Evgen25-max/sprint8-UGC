"""Контракт UGC-событий для ETL.

Отличается от ugc-api версии: НЕ проверяем MAX_EVENT_AGE и MAX_FUTURE_SKEW,
потому что API уже принял событие, и ETL не должен его отклонять из-за
задержки в Kafka. Проверяем только корректность формата datetime.
"""
from datetime import datetime, timezone
from typing import Annotated, Literal, Union
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Quality = Literal['auto', '360p', '480p', '720p', '1080p', '1440p', '2160p']


class EventBase(BaseModel):
    model_config = ConfigDict(extra='forbid', str_strip_whitespace=True)

    event_id: UUID
    schema_version: int = Field(1, ge=1, le=1)
    event_time: datetime
    session_id: UUID
    anonymous_id: str | None = Field(None, min_length=8, max_length=64)
    page_url: str | None = Field(None, max_length=2048)

    @field_validator('event_time')
    @classmethod
    def normalize_event_time(cls, v: datetime) -> datetime:
        """Только нормализация к UTC. Проверка возраста уже выполнена API."""
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)


class ClickEvent(EventBase):
    event_type: Literal['click'] = 'click'
    element_type: Literal['film_card', 'trailer', 'category', 'banner',
                          'button', 'link', 'other']
    element_id: str = Field(..., min_length=1, max_length=128)
    film_id: UUID | None = None
    position: int | None = Field(None, ge=0, le=10000)


class PageViewEvent(EventBase):
    event_type: Literal['page_view'] = 'page_view'
    view_id: UUID
    phase: Literal['start', 'heartbeat', 'end']
    page_type: Literal['film', 'category', 'promo', 'search', 'main', 'other']
    page_id: str | None = Field(None, max_length=128)
    duration_seconds: int = Field(0, ge=0, le=86400)
    referrer: str | None = Field(None, max_length=2048)


class VideoEvent(EventBase):
    event_type: Literal['video_event'] = 'video_event'
    subtype: Literal['quality_change', 'completed']
    film_id: UUID
    from_quality: Quality | None = None
    to_quality: Quality | None = None
    position_seconds: int = Field(0, ge=0, le=604800)
    video_duration_seconds: int | None = Field(None, ge=0, le=604800)
    watched_seconds: int | None = Field(None, ge=0, le=604800)

    @model_validator(mode='after')
    def check_quality_change(self):
        if self.subtype == 'quality_change':
            if not self.from_quality or not self.to_quality:
                raise ValueError('для quality_change нужны from_quality и to_quality')
            if self.from_quality == self.to_quality:
                raise ValueError('from_quality и to_quality совпадают')
        return self


class SearchEvent(EventBase):
    event_type: Literal['search_event'] = 'search_event'
    query: str | None = Field(None, max_length=255)
    genres: list[str] = Field(default_factory=list, max_length=20)
    rating_from: float | None = Field(None, ge=0, le=10)
    rating_to: float | None = Field(None, ge=0, le=10)
    actors: list[str] = Field(default_factory=list, max_length=20)
    year_from: int | None = Field(None, ge=1880, le=2100)
    year_to: int | None = Field(None, ge=1880, le=2100)
    sort: str | None = Field(None, max_length=32)

    @model_validator(mode='after')
    def check_filters(self):
        if (self.rating_from is not None and self.rating_to is not None
                and self.rating_from > self.rating_to):
            raise ValueError('rating_from больше rating_to')
        if (self.year_from is not None and self.year_to is not None
                and self.year_from > self.year_to):
            raise ValueError('year_from больше year_to')
        if not any([
            self.query, self.genres, self.actors, self.sort,
            self.rating_from is not None, self.rating_to is not None,
            self.year_from is not None, self.year_to is not None,
        ]):
            raise ValueError('search_event без единого фильтра')
        return self


AnyEvent = Annotated[
    Union[ClickEvent, PageViewEvent, VideoEvent, SearchEvent],
    Field(discriminator='event_type'),
]


class KafkaEnvelope(BaseModel):
    model_config = ConfigDict(extra='ignore')
    event: AnyEvent
    user_id: UUID | None = None
    received_at: datetime
    user_agent: str | None = Field(None, max_length=512)
    request_id: str | None = Field(None, max_length=64)