#!/usr/bin/env python
"""Веб-дашборд нагрузки на d-gigachat-vision-1.

    python server.py [--port 8765] [--host 127.0.0.1] [--interval 120]

Держит в памяти скользящее окно access-логов и отдаёт:
    /            дашборд
    /api/state   JSON со всеми показателями
    /healthz     состояние сборщика
"""
import argparse
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from collector import Collector

HERE = os.path.dirname(os.path.abspath(__file__))
PAGE = os.path.join(HERE, "dashboard.html")


class Handler(BaseHTTPRequestHandler):
    collector = None
    server_version = "s3load/1.0"

    def log_message(self, fmt, *args):        # тише в консоли
        if "/api/" not in (args[0] if args else ""):
            sys.stderr.write("%s %s\n" % (self.address_string(), fmt % args))

    def _send(self, code, body, ctype):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?")[0]
        try:
            if path in ("/", "/index.html"):
                with open(PAGE, "r") as fh:
                    self._send(200, fh.read(), "text/html; charset=utf-8")
            elif path == "/api/state":
                payload = {"status": self.collector.status,
                           "state": self.collector.state}
                self._send(200, json.dumps(payload, ensure_ascii=False),
                           "application/json; charset=utf-8")
            elif path == "/healthz":
                st = self.collector.status
                ok = st["error"] is None and self.collector.state is not None
                self._send(200 if ok else 503,
                           json.dumps(st, ensure_ascii=False),
                           "application/json; charset=utf-8")
            else:
                self._send(404, "not found", "text/plain; charset=utf-8")
        except BrokenPipeError:
            pass
        except Exception as exc:                              # noqa: BLE001
            self._send(500, "error: %s" % exc, "text/plain; charset=utf-8")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default="127.0.0.1",
                    help="0.0.0.0 чтобы открыть доступ по сети (по умолчанию только локально)")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--interval", type=int, default=120, help="период опроса логов, с")
    ap.add_argument("--window", type=int, default=60, help="глубина окна, мин")
    ap.add_argument("--report-window", type=int, default=10,
                    help="сколько последних полных минут считать «сейчас»")
    ap.add_argument("--bootstrap", type=int, default=45,
                    help="сколько минут истории поднять при старте")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--ceiling", type=float, default=685.0,
                    help="ёмкость канала бакета в MiB/s — порог тревоги о насыщении")
    args = ap.parse_args()

    col = Collector(window_min=args.window, interval=args.interval,
                    workers=args.workers, bootstrap_min=args.bootstrap,
                    report_window=args.report_window, ceiling_mibs=args.ceiling)
    Handler.collector = col

    thread = threading.Thread(target=col.run_forever, daemon=True)
    thread.start()

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    shown = args.host if args.host != "0.0.0.0" else "<адрес машины>"
    print("дашборд:  http://%s:%d" % (shown, args.port))
    print("данные:   http://%s:%d/api/state" % (shown, args.port))
    print("сбор идёт в фоне, период %d с, окно %d мин." % (args.interval, args.window))
    print("первый цикл поднимает %d мин истории — займёт несколько минут."
          % args.bootstrap)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nостановка")
    finally:
        col.stop()
        httpd.server_close()


if __name__ == "__main__":
    main()
