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
import urllib.parse
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from collector import Collector, TMPBASE, sweep_tmp

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
            elif path == "/api/range":
                q = urllib.parse.parse_qs(self.path.partition("?")[2])
                frm = (q.get("from") or [""])[0].strip()
                to = (q.get("to") or [""])[0].strip()
                ok, msg = self.collector.start_range(frm, to)
                self._send(200 if ok else 400,
                           json.dumps({"ok": ok, "message": msg}, ensure_ascii=False),
                           "application/json; charset=utf-8")
            elif path == "/api/range/status":
                self._send(200, json.dumps(self.collector.range_job,
                                           ensure_ascii=False),
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
    ap.add_argument("--limit-mibs", type=float, default=640.0,
                    help="квота полосы бакета в MiB/с (по умолчанию 640 = 5*2^30 бит/с)")
    ap.add_argument("--max-range", type=int, default=1440,
                    help="максимальная длина разбираемого периода, мин (по умолчанию 1440 = сутки)")
    ap.add_argument("--slow-ms", type=int, default=200,
                    help="порог тревоги по медианной задержке, мс (по умолчанию 200)")
    ap.add_argument("--limit-combined", action="store_true",
                    help="считать квоту общей на чтение+запись; "
                         "по умолчанию она применяется к каждому направлению отдельно")
    args = ap.parse_args()

    stale = sweep_tmp()
    if stale:
        print("убрано остатков прошлого запуска: %d" % stale)

    col = Collector(window_min=args.window, interval=args.interval,
                    workers=args.workers, bootstrap_min=args.bootstrap,
                    report_window=args.report_window,
                    limit_mibs=args.limit_mibs,
                    per_direction=not args.limit_combined,
                    max_range_min=args.max_range, slow_ms=args.slow_ms)
    Handler.collector = col

    thread = threading.Thread(target=col.run_forever, daemon=True)
    thread.start()

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    shown = args.host if args.host != "0.0.0.0" else "<адрес машины>"
    print("дашборд:  http://%s:%d" % (shown, args.port))
    print("данные:   http://%s:%d/api/state" % (shown, args.port))
    print("период:   http://%s:%d/api/range?from=ГГГГ-ММ-ДД+ЧЧ:ММ&to=..." % (shown, args.port))
    print("сбор идёт в фоне, период %d с, окно %d мин." % (args.interval, args.window))
    print("логи качаются в %s и удаляются сразу после разбора" % TMPBASE)
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
