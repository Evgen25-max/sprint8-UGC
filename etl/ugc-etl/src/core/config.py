from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    PROJECT_NAME: str = 'ugc-etl'
    LOG_LEVEL: str = 'INFO'

    KAFKA_BOOTSTRAP_SERVERS: str = 'kafka:9092'
    KAFKA_GROUP_ID: str = 'ugc-etl-v1'
    KAFKA_TOPICS: list[str] = [
        'ugc.clicks', 'ugc.page_views', 'ugc.video_events', 'ugc.search_events',
    ]
    KAFKA_MAX_POLL_RECORDS: int = 500
    KAFKA_MAX_POLL_INTERVAL_MS: int = 900000  # 15 мин: переживаем долгий простой CH без ребаланса
    KAFKA_FETCH_MAX_WAIT_MS: int = 500
    BATCH_TIMEOUT_MS: int = 1000
    DLQ_TOPIC: str = 'ugc.dlq'

    CLICKHOUSE_HOST: str = 'clickhouse'
    CLICKHOUSE_PORT: int = 8123
    CLICKHOUSE_USER: str = 'ugc'
    CLICKHOUSE_PASSWORD: str
    CLICKHOUSE_DB: str = 'ugc'
    CH_CONNECT_TIMEOUT: int = 5
    CH_SEND_RECEIVE_TIMEOUT: int = 30

    RETRY_BASE_SECONDS: float = 0.5
    RETRY_CAP_SECONDS: float = 30.0

    METRICS_PORT: int = 9101
    MEMORY_HARD_LIMIT_MB: int = 384
    MEMORY_CHECK_INTERVAL: int = 10

    model_config = SettingsConfigDict(
        env_file='.env', env_file_encoding='utf-8',
        env_ignore_empty=True, extra='ignore',
    )


config = Settings()
