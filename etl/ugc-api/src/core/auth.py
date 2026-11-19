
import logging
from uuid import UUID

import jwt
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from redis.exceptions import RedisError

from core.config import config

logger = logging.getLogger(__name__)
bearer_scheme = HTTPBearer(auto_error=False)

BLACKLIST_PREFIX = 'auth:blacklist'
USER_VER_PREFIX = 'auth:user_ver'


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={'WWW-Authenticate': 'Bearer'},
    )


async def get_optional_user_id(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> UUID | None:
    if credentials is None:
        return None  # анонимный режим

    try:
        payload = jwt.decode(
            credentials.credentials,
            config.JWT_SECRET_KEY,
            algorithms=[config.JWT_ALGORITHM],
        )
    except jwt.PyJWTError:
        raise _unauthorized('Невалидный токен')

    if payload.get('type') != 'access':
        raise _unauthorized('Требуется access-токен')
    jti, sub = payload.get('jti'), payload.get('sub')
    if not jti or not sub:
        raise _unauthorized('Некорректная структура токена')

    r = request.app.state.auth_redis
    try:
        blacklisted = bool(await r.exists(f'{BLACKLIST_PREFIX}:{jti}'))
        raw_ver = await r.get(f'{USER_VER_PREFIX}:{sub}')
    except RedisError as exc:
        # fail-closed: не можем проверить — не доверяем токену
        logger.warning('Auth-redis недоступен, fail-closed: %s', exc)
        raise _unauthorized('Сервис аутентификации временно недоступен')

    if blacklisted:
        raise _unauthorized('Токен отозван')
    if raw_ver is not None:
        try:
            if int(payload.get('ver', 1)) < int(raw_ver):
                raise _unauthorized('Сессия недействительна: пароль был изменён')
        except (TypeError, ValueError):
            raise _unauthorized('Некорректная версия токена')

    try:
        return UUID(sub)
    except ValueError:
        raise _unauthorized('Некорректный user_id в токене')
