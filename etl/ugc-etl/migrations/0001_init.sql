-- 0001_init.sql: базовые таблицы UGC
CREATE TABLE IF NOT EXISTS __DB__.clicks
(
    event_id        UUID,
    event_time      DateTime64(3, 'UTC'),
    received_at     DateTime64(3, 'UTC'),
    user_id         Nullable(UUID),
    anonymous_id    String,
    session_id      UUID,
    page_url        String,
    element_type    LowCardinality(String),
    element_id      String,
    film_id         Nullable(UUID),
    position        Nullable(UInt32),
    user_agent      String,
    schema_version  UInt8
)
ENGINE = ReplacingMergeTree(received_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, event_id);

CREATE TABLE IF NOT EXISTS __DB__.page_views
(
    view_id           UUID,
    event_id          UUID,
    event_time        DateTime64(3, 'UTC'),
    received_at       DateTime64(3, 'UTC'),
    user_id           Nullable(UUID),
    anonymous_id      String,
    session_id        UUID,
    page_type         LowCardinality(String),
    page_id           String,
    duration_seconds  UInt32,
    referrer          String,
    page_url          String,
    user_agent        String,
    schema_version    UInt8
)
ENGINE = ReplacingMergeTree(duration_seconds)
PARTITION BY toYYYYMM(event_time)
ORDER BY (view_id);

CREATE TABLE IF NOT EXISTS __DB__.video_events
(
    event_id                UUID,
    event_time              DateTime64(3, 'UTC'),
    received_at             DateTime64(3, 'UTC'),
    user_id                 Nullable(UUID),
    anonymous_id            String,
    session_id              UUID,
    subtype                 LowCardinality(String),
    film_id                 UUID,
    from_quality            LowCardinality(String),
    to_quality              LowCardinality(String),
    position_seconds        UInt32,
    video_duration_seconds  Nullable(UInt32),
    watched_seconds         Nullable(UInt32),
    page_url                String,
    user_agent              String,
    schema_version          UInt8
)
ENGINE = ReplacingMergeTree(received_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (film_id, event_time, event_id);

CREATE TABLE IF NOT EXISTS __DB__.search_events
(
    event_id        UUID,
    event_time      DateTime64(3, 'UTC'),
    received_at     DateTime64(3, 'UTC'),
    user_id         Nullable(UUID),
    anonymous_id    String,
    session_id      UUID,
    query           String,
    genres          Array(String),
    rating_from     Nullable(Float32),
    rating_to       Nullable(Float32),
    actors          Array(String),
    year_from       Nullable(UInt16),
    year_to         Nullable(UInt16),
    sort            LowCardinality(String),
    page_url        String,
    user_agent      String,
    schema_version  UInt8
)
ENGINE = ReplacingMergeTree(received_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, event_id);
