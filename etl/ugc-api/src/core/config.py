from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    PROJECT_NAME: str = 'ugc-api'

    KAFKA_BOOTSTRAP_SERVERS: str = 'kafka:9092'
    KAFKA_SEND_TIMEOUT_SECONDS: float = 5.0
    KAFKA_LINGER_MS: int = 50
    KAFKA_COMPRESSION: str = 'lz4'

    JWT_SECRET_KEY: str
    JWT_ALGORITHM: str = 'HS256'

    REDIS_AUTH_HOST: str = 'auth-redis'
    REDIS_AUTH_PORT: int = 6380
    REDIS_AUTH_PASSW: str | None = None
    REDIS_AUTH_DB: int = 1
    REDIS_CONNECT_TIMEOUT: int = 5
    REDIS_SOCKET_TIMEOUT: int = 5

    RATELIMIT_REDIS_HOST: str = 'redis'
    RATELIMIT_REDIS_PORT: int = 6379
    RATELIMIT_REDIS_PASSW: str | None = None
    RATELIMIT_REDIS_DB: int = 2
    RATE_LIMIT_REQUESTS: int = 300
    RATE_LIMIT_PERIOD: int = 60

    OTEL_EXPORTER_OTLP_ENDPOINT: str = 'http://jaeger:4317'

    model_config = SettingsConfigDict(
        env_file='.env', env_file_encoding='utf-8',
        env_ignore_empty=True, extra='ignore',
    )


config = Settings()
