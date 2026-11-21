#!/usr/bin/env bash
# Создаёт .env из .env.example и заполняет пустые секреты случайными значениями.
# Существующий .env НЕ перезаписывает.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

command -v openssl >/dev/null || { echo 'Нужен openssl' >&2; exit 1; }

if [[ -e .env ]]; then
  echo '.env уже существует — не перезаписываю.' >&2
  echo 'Заполни пустые секреты вручную или удали файл и запусти снова.' >&2
  exit 1
fi

cp .env.example .env
chmod 600 .env

# Подставляем значение ТОЛЬКО в строку вида ^KEY=$ (пустое значение),
# поэтому уже заполненные поля и несекретные настройки не затрагиваются.
set_secret() {
  sed -i "s|^${1}=\$|${1}=${2}|" .env
}

# Redis для кэша и rate-limit — пароль общий.
redis_pass="$(openssl rand -hex 32)"
set_secret REDIS_PASSW            "$redis_pass"
set_secret RATELIMIT_REDIS_PASSW  "$redis_pass"

for key in \
  SECRET_KEY \
  JWT_SECRET_KEY \
  PASSWORD_PEPPER \
  INTERNAL_API_KEY \
  POSTGRES_PASSWORD \
  POSTGRES_AUTH_PASSWORD \
  POSTGRES_AUTH_APP_PASSWORD \
  REDIS_AUTH_PASSW \
  STATS_REDIS_PASSW \
  CLICKHOUSE_PASSWORD \
  GRAFANA_PASS
do
  set_secret "$key" "$(openssl rand -hex 32)"
done

echo '✓ .env создан, секреты сгенерированы, права 600.'
echo
echo 'Осталось заполнить вручную (Yandex OAuth):'
grep -nE '^YANDEX_CLIENT_(ID|SECRET)=$' .env || echo '  (уже заполнено)'