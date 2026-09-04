#!/usr/bin/env bash
# Запуск дашборда нагрузки на d-gigachat-vision-1.
#   ./run.sh                  локально на 127.0.0.1:8765
#   PORT=9000 ./run.sh        другой порт
#   HOST=0.0.0.0 ./run.sh     доступ по сети
set -euo pipefail
cd "$(dirname "$0")"
# прокси сессии блокирует внутренние хосты
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy || true
exec python server.py \
  --host "${HOST:-127.0.0.1}" \
  --port "${PORT:-8765}" \
  --interval "${INTERVAL:-120}" \
  --window "${WINDOW:-60}" \
  --bootstrap "${BOOTSTRAP:-45}" \
  --workers "${WORKERS:-8}"
