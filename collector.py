#!/usr/bin/env python
"""Фоновый сборщик нагрузки на d-gigachat-vision-1.

Инкрементально дочитывает новые объекты access-журнала из d-gigachat-logs01,
разбирает их и держит в памяти скользящее окно агрегатов. Разбор и расчёт
переиспользуются из скилла s3-load-report, чтобы логика была в одном месте.
"""
import collections
import os
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from multiprocessing import Pool

SKILL = os.environ.get(
    "S3LOAD_SKILL", "/home/jovyan/vspyatochkin/.cursor/skills/s3-load-report/scripts")
if SKILL not in sys.path:
    sys.path.insert(0, SKILL)
try:
    import parse_logs
    import make_report
except ImportError as exc:                                    # pragma: no cover
    raise SystemExit(
        "не найдены скрипты скилла s3-load-report в %s\n"
        "укажи путь через переменную окружения S3LOAD_SKILL (%s)" % (SKILL, exc))

RCLONE = os.environ.get("RCLONE", "/home/jovyan/miniconda3/bin/rclone")
CONFIG = os.environ.get("RCLONE_CONFIG",
                        "/home/jovyan/vspyatochkin/.config/rclone/rclone.conf")
REMOTE = "d-gigachat-logs01:d-gigachat-logs01/d-gigachat-vision-1"

COUNTER_KEYS = ("C", "B", "E", "WB", "WN", "IPB", "WIP", "LAT", "UA")
MIB = 1 << 20


def _env():
    """Окружение без прокси — прокси сессии блокирует внутренние хосты."""
    env = dict(os.environ)
    for var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        env.pop(var, None)
    return env


class Collector(object):
    def __init__(self, window_min=60, interval=120, workers=8,
                 bootstrap_min=45, report_window=10, ceiling_mibs=685.0):
        self.window_min = window_min          # глубина скользящего окна, мин
        self.interval = interval              # период опроса, с
        self.workers = workers
        self.bootstrap_min = bootstrap_min    # сколько истории взять на старте
        self.report_window = report_window    # окно «сейчас» для сводки, мин
        # Ёмкость канала бакета. Это НЕ максимум из наблюдений: при спокойной
        # нагрузке максимум в окне равен текущей нагрузке, и любой ровный
        # график читался бы как насыщение. Значение измерено по логам
        # 03-04.09.2026 (полка чтения 640-685 MiB/s); пересматривать при
        # изменении квоты.
        self.ceiling = ceiling_mibs

        self.agg = {k: collections.Counter() for k in COUNTER_KEYS}
        self.agg["K4"] = {}
        self.seen = set()                     # уже разобранные объекты журнала
        self.lock = threading.Lock()
        self.state = None                     # готовый снимок для отдачи
        self.status = {"phase": "старт", "updated": None, "cycles": 0,
                       "last_files": 0, "last_seconds": 0.0, "error": None,
                       "minutes_held": 0}
        self._stop = threading.Event()

    # ------------------------------------------------------------ выгрузка

    def _hours_to_scan(self, span_min):
        """Часы доставки (UTC), которые могут содержать нужные события."""
        now = datetime.now(timezone.utc)
        hours, cur = set(), now
        earliest = now - timedelta(minutes=span_min + 15)
        while cur >= earliest:
            hours.add(cur.strftime("%Y-%m-%d-%H"))
            cur -= timedelta(hours=1)
        return sorted(hours)

    def _list_remote(self, span_min):
        cmd = [RCLONE, "--config", CONFIG, "lsf", REMOTE]
        for h in self._hours_to_scan(span_min):
            cmd += ["--include", "%s-*" % h]
        out = subprocess.run(cmd, capture_output=True, text=True, env=_env(),
                             timeout=300)
        if out.returncode != 0:
            raise RuntimeError("rclone lsf: %s" % out.stderr.strip()[:300])
        return [n.strip() for n in out.stdout.splitlines() if n.strip()]

    def _download(self, names, dest):
        listing = os.path.join(dest, "_files.txt")
        with open(listing, "w") as fh:
            fh.write("\n".join(names))
        cmd = [RCLONE, "--config", CONFIG, "copy", REMOTE, dest,
               "--files-from", listing, "--transfers", "32", "--checkers", "32"]
        out = subprocess.run(cmd, capture_output=True, text=True, env=_env(),
                             timeout=1800)
        os.remove(listing)
        if out.returncode != 0:
            raise RuntimeError("rclone copy: %s" % out.stderr.strip()[:300])
        return [os.path.join(dest, n) for n in names
                if os.path.exists(os.path.join(dest, n))]

    # -------------------------------------------------------------- разбор

    def _parse(self, paths):
        if not paths:
            return
        n = min(self.workers, len(paths))
        results = []
        if n > 1:
            with Pool(n) as pool:
                results = pool.map(parse_logs.work, paths, chunksize=1)
        else:
            results = [parse_logs.work(p) for p in paths]

        k4 = collections.defaultdict(set)
        with self.lock:
            for res in results:
                for name, part in zip(COUNTER_KEYS, res[:9]):
                    self.agg[name].update(part)
                for pref, keys in res[9].items():
                    if len(k4[pref]) < 200:
                        k4[pref] |= set(keys)
            if k4:
                self.agg["K4"] = {p: sorted(v)[:200] for p, v in k4.items()}

    def _evict(self):
        """Выбрасывает минуты старше окна."""
        minutes = {mi for (mi, _n, _p) in self.agg["C"]}
        if not minutes:
            return 0
        keep = sorted(minutes)[-self.window_min:]
        keep = set(keep)
        for name in COUNTER_KEYS:
            src = self.agg[name]
            self.agg[name] = collections.Counter(
                {k: v for k, v in src.items()
                 if not isinstance(k, tuple) or k[0] in keep})
        return len(keep)

    # ------------------------------------------------------------- расчёты

    def _latency_series(self, minutes):
        """p50 и p90 по минутам — для графика задержки."""
        per = collections.defaultdict(collections.Counter)
        for (mi, bucket), v in self.agg["LAT"].items():
            per[mi][bucket] += v
        out = []
        for mi in minutes:
            hist = per.get(mi)
            if not hist:
                # минута без чтений — разрыв, а не нулевая задержка
                out.append([mi[11:], None, None])
                continue
            total = sum(hist.values())
            acc, p50, p90 = 0, 0, 0
            for bucket in sorted(hist):
                acc += hist[bucket]
                if not p50 and acc >= total * 0.5:
                    p50 = 2 ** bucket
                if not p90 and acc >= total * 0.9:
                    p90 = 2 ** bucket
                    break
            out.append([mi[11:], p50, p90])
        return out

    def _recompute(self):
        with self.lock:
            snapshot = {k: collections.Counter(v) for k, v in self.agg.items()
                        if k in COUNTER_KEYS}
            snapshot["K4"] = dict(self.agg["K4"])
        if not snapshot["C"]:
            return None
        data = make_report.analyse(snapshot, self.report_window)
        data["ceiling_observed"] = data["ceiling"]
        data["ceiling"] = self.ceiling
        minutes = sorted({mi for (mi, _n, _p) in snapshot["C"]})
        data["latency_series"] = self._latency_series(minutes)
        data["alert"] = self._alert(data)
        return data

    def _alert(self, d):
        """Тревога: канал насыщен и задержка выросла."""
        w, ceil = d["window"], d["ceiling"]
        saturated = ceil > 0 and w["read_mibs"] >= 0.9 * ceil
        slow = w["p50"] >= 200
        if saturated and slow:
            top = d["rd"][0] if d["rd"] else None
            return {
                "level": "critical",
                "title": "Канал насыщен, запросы стоят в очереди",
                "text": ("Чтение %.0f MiB/s при полке %.0f, медиана задержки %d мс. "
                         % (w["read_mibs"], ceil, w["p50"]) +
                         ("Больше всех берёт %s → %s (%.0f MiB/s)."
                          % (top["net"], top["prefix"], top["mibs"]) if top else "")),
            }
        if saturated:
            return {"level": "warning", "title": "Полоса на потолке",
                    "text": "Чтение %.0f MiB/s при полке %.0f MiB/s. Задержка пока в норме "
                            "(медиана %d мс), но запаса нет."
                            % (w["read_mibs"], ceil, w["p50"])}
        if slow:
            return {"level": "warning", "title": "Задержка выше обычной",
                    "text": "Медиана %d мс при незаполненном канале (%.0f из %.0f MiB/s)."
                            % (w["p50"], w["read_mibs"], ceil)}
        return {"level": "ok", "title": "Норма",
                "text": "Чтение %.0f MiB/s, запись %.0f MiB/s, медиана задержки %d мс."
                        % (w["read_mibs"], w["write_mibs"], w["p50"])}

    # --------------------------------------------------------------- цикл

    def cycle(self, first=False):
        started = time.time()
        span = self.bootstrap_min if first else max(self.window_min, 20)
        self.status["phase"] = "листинг"
        names = self._list_remote(span)
        fresh = [n for n in names if n not in self.seen]
        # на старте не тянем весь час подряд — только хвост по времени доставки
        if first:
            fresh = sorted(fresh)[-600:]
        self.status["phase"] = "выгрузка (%d файлов)" % len(fresh)

        paths = []
        tmp = tempfile.mkdtemp(prefix="s3load-")
        try:
            if fresh:
                paths = self._download(fresh, tmp)
                self.status["phase"] = "разбор (%d файлов)" % len(paths)
                self._parse(paths)
                self.seen.update(fresh)
        finally:
            subprocess.run(["rm", "-rf", tmp])

        held = self._evict()
        self.status["phase"] = "расчёт"
        state = self._recompute()
        if state is not None:
            self.state = state
        # не даём множеству имён расти бесконечно
        if len(self.seen) > 20000:
            self.seen = set(sorted(self.seen)[-8000:])
        self.status.update({
            "phase": "ожидание",
            "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "cycles": self.status["cycles"] + 1,
            "last_files": len(paths),
            "last_seconds": round(time.time() - started, 1),
            "minutes_held": held,
            "error": None,
        })

    def run_forever(self):
        first = True
        while not self._stop.is_set():
            try:
                self.cycle(first=first)
                first = False
            except Exception as exc:                          # noqa: BLE001
                self.status.update({"phase": "ошибка", "error": str(exc)[:400]})
                print("[collector] ошибка: %s" % exc, flush=True)
            self._stop.wait(self.interval)

    def stop(self):
        self._stop.set()
