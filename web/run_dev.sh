#!/usr/bin/env bash
# 개발용 실행 스크립트
set -e
cd "$(dirname "$0")"

python manage.py migrate
python manage.py collectstatic --noinput >/dev/null 2>&1 || true

# WSGI(runserver)가 아니라 ASGI 로 띄워야 MJPEG 가 워커를 잠그지 않는다
exec daphne -b 0.0.0.0 -p 8000 config.asgi:application
