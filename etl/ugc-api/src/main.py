import logging
import logging.config
from contextlib import asynccontextmanager
import asyncio
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from redis.asyncio import Redis

from api.v1.events import router as events_router
from core.config import config
from core.logger import LOGGING
from middleware.rate_limit import RateLimitMiddleware
from middleware.request_id import RequestIDMiddleware
from services.producer import EventProducer
from tracing import configure_tracer

logging.config.dictConfig(LOGGING)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info('Запуск %s...', config.PROJECT_NAME)

    # 1. Auth-redis — КРИТИЧЕН (blacklist / user_ver). Недоступен -> не стартуем.
    auth_redis = Redis(
        host=config.REDIS_AUTH_HOST, port=config.REDIS_AUTH_PORT,
        db=config.REDIS_AUTH_DB, password=config.REDIS_AUTH_PASSW,
        socket_connect_timeout=config.REDIS_CONNECT_TIMEOUT,
        socket_timeout=config.REDIS_SOCKET_TIMEOUT, decode_responses=True,
    )
    try:
        await auth_redis.ping()
    except Exception as exc:
        logger.exception('Auth Redis недоступен при старте')
        raise RuntimeError('Auth Redis unavailable at startup') from exc
    app.state.auth_redis = auth_redis
    logger.info('Auth Redis доступен')

    # 2. Rate-limit redis — НЕ критичен (fail-open, как в movies).
    try:
        rl = Redis(
            host=config.RATELIMIT_REDIS_HOST, port=config.RATELIMIT_REDIS_PORT,
            db=config.RATELIMIT_REDIS_DB, password=config.RATELIMIT_REDIS_PASSW,
            socket_connect_timeout=config.REDIS_CONNECT_TIMEOUT,
            socket_timeout=config.REDIS_SOCKET_TIMEOUT, decode_responses=True,
        )
        await rl.ping()
        app.state.ratelimit_redis = rl
        logger.info('Rate-limit Redis доступен')
    except Exception as exc:
        app.state.ratelimit_redis = None
        logger.warning('Rate-limit Redis недоступен: %s. Лимиты отключены.', exc)

    # 3. Kafka — КРИТИЧНА. Недоступна -> не стартуем (никакого "приняли и потеряли").
    producer = EventProducer()
    await producer.start()
    app.state.producer = producer

    logger.info('%s запущен', config.PROJECT_NAME)
    try:
        yield
    finally:
        logger.info('Остановка %s...', config.PROJECT_NAME)
        await producer.stop()
        await auth_redis.aclose()
        if app.state.ratelimit_redis is not None:
            await app.state.ratelimit_redis.aclose()


app = FastAPI(
    title=config.PROJECT_NAME,
    version='1.0.0',
    docs_url='/api/v1/ugc/docs',
    redoc_url='/api/v1/ugc/redoc',
    openapi_url='/api/v1/ugc/openapi.json',
    lifespan=lifespan,
)

try:
    configure_tracer(service_name=config.PROJECT_NAME)
except Exception as exc:
    logger.warning('Не удалось настроить трассировку: %s', exc)

app.add_middleware(RateLimitMiddleware,
                   max_requests=config.RATE_LIMIT_REQUESTS,
                   window_seconds=config.RATE_LIMIT_PERIOD)
app.add_middleware(RequestIDMiddleware)
FastAPIInstrumentor.instrument_app(app)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    request_id = getattr(request.state, 'request_id', None)
    logger.exception('Необработанная ошибка: %s', exc)
    response = JSONResponse(status_code=500, content={'detail': 'Внутренняя ошибка сервера'})
    if request_id:
        response.headers['X-Request-Id'] = request_id
    return response


app.include_router(events_router, prefix='/api/v1/ugc', tags=['UGC'])

@app.get('/health', tags=['system'])
async def health_check():
    """Всегда 200: деградация зависимостей отражается в теле, а не в HTTP-коде."""
    deps: dict[str, str] = {}
    ok = True

    # Kafka
    try:
        producer = getattr(app.state, 'producer', None)
        if producer is None:
            raise RuntimeError('producer not initialized')
        await asyncio.wait_for(producer.check(), timeout=3.0)
        deps['kafka'] = 'ok'
    except Exception as exc:
        deps['kafka'] = f'degraded: {type(exc).__name__}'
        ok = False

    # auth-redis
    try:
        await asyncio.wait_for(app.state.auth_redis.ping(), timeout=3.0)
        deps['auth_redis'] = 'ok'
    except Exception as exc:
        deps['auth_redis'] = f'degraded: {type(exc).__name__}'
        ok = False

    # rate-limit redis — не критичен, на общий статус не влияет
    rl = getattr(app.state, 'ratelimit_redis', None)
    if rl is None:
        deps['ratelimit_redis'] = 'disabled'
    else:
        try:
            await asyncio.wait_for(rl.ping(), timeout=3.0)
            deps['ratelimit_redis'] = 'ok'
        except Exception as exc:
            deps['ratelimit_redis'] = f'degraded: {type(exc).__name__}'

    return {
        'status': 'ok' if ok else 'degraded',
        'service': config.PROJECT_NAME,
        'dependencies': deps,
    }
