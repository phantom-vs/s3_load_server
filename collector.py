#!/usr/bin/env python
"""Фоновый сборщик нагрузки на d-gigachat-vision-1.

Инкрементально дочитывает новые объекты access-журнала из d-gigachat-logs01,
разбирает их и держит в памяти скользящее окно агрегатов.

Зависимостей нет: разбор и расчёты — в соседнем logparse.py, наружу
вызывается только rclone.
"""
import collections
import os
import shutil
import subprocess
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from multiprocessing import Pool

import logparse

REMOTE = os.environ.get(
    "S3LOAD_REMOTE", "d-gigachat-logs01:d-gigachat-logs01/d-gigachat-vision-1")
# Куда класть скачанные логи на время разбора. По умолчанию — подкаталог tmp/
# рядом со скриптами, а не системный /tmp: один час логов под полной нагрузкой
# это 5-7 ГБ, и на многих машинах /tmp столько не держит.
# Каталог часа удаляется сразу после разбора; при старте убираются остатки,
# если прошлый запуск был убит на середине.
HERE = os.path.dirname(os.path.abspath(__file__))
TMPBASE = os.environ.get("S3LOAD_TMP") or os.path.join(HERE, "tmp")
MIB = 1 << 20


def find_rclone():
    """rclone из окружения, из PATH или из известных мест установки."""
    explicit = os.environ.get("RCLONE")
    if explicit:
        return explicit
    found = shutil.which("rclone")
    if found:
        return found
    for path in ("/home/jovyan/miniconda3/bin/rclone", "/usr/local/bin/rclone",
                 "/usr/bin/rclone", os.path.expanduser("~/bin/rclone")):
        if os.path.exists(path):
            return path
    return "rclone"          # пусть упадёт с внятной ошибкой при вызове


def find_config():
    """Конфиг rclone: из окружения, иначе стандартный путь, иначе None.

    None означает «не передавать --config» — rclone сам найдёт свой конфиг.
    """
    explicit = os.environ.get("RCLONE_CONFIG")
    if explicit:
        return explicit
    for path in (os.path.expanduser("~/.config/rclone/rclone.conf"),
                 os.path.expanduser("~/.rclone.conf")):
        if os.path.exists(path):
            return path
    return None


RCLONE = find_rclone()
CONFIG = find_config()


def _env():
    """Окружение без прокси — прокси сессии блокирует внутренние хосты."""
    env = dict(os.environ)
    for var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        env.pop(var, None)
    return env


def _mktemp(prefix):
    os.makedirs(TMPBASE, exist_ok=True)
    return tempfile.mkdtemp(prefix=prefix, dir=TMPBASE)


def sweep_tmp():
    """Убирает каталоги, оставшиеся от убитого прошлого запуска."""
    if not os.path.isdir(TMPBASE):
        return 0
    removed = 0
    for name in os.listdir(TMPBASE):
        if not name.startswith("s3load-"):
            continue
        path = os.path.join(TMPBASE, name)
        try:
            # rmtree не берёт обычные файлы, а они тут тоже могут остаться
            if os.path.isdir(path) and not os.path.islink(path):
                shutil.rmtree(path, ignore_errors=True)
            else:
                os.remove(path)
            removed += 1
        except OSError:
            pass
    return removed


def _base_cmd(*args):
    cmd = [RCLONE]
    if CONFIG:
        cmd += ["--config", CONFIG]
    return cmd + list(args)


class Collector(object):
    def __init__(self, window_min=60, interval=120, workers=8,
                 bootstrap_min=45, report_window=10,
                 limit_mibs=640.0, per_direction=True, max_range_min=1440,
                 slow_ms=200):
        self.window_min = window_min          # глубина скользящего окна, мин
        self.interval = interval              # период опроса, с
        self.workers = workers
        self.bootstrap_min = bootstrap_min    # сколько истории взять на старте
        self.report_window = report_window    # окно «сейчас» для сводки, мин

        # Квота полосы, MiB/с. Задаётся снаружи, а НЕ выводится из наблюдений:
        # при спокойной нагрузке максимум в окне равен текущей нагрузке, и
        # любой ровный график читался бы как насыщение.
        # 640 MiB/с — это ровно 5*2^30 бит/с; ночью 03-04.09.2026 чтение
        # держалось на 636-643 MiB/с, то есть на этом значении.
        self.limit_mibs = limit_mibs
        self.per_direction = per_direction
        self.max_range_min = max_range_min
        # Порог «медленно», мс. Наблюдения 03-04.09.2026 резко двумодальны:
        # при свободной полосе поминутная p50 держится в 3-21 мс, при полосе
        # у квоты — 825 мс. Между ними разрыв в 40 раз, поэтому любой порог
        # от 50 до 500 мс даёт один и тот же результат; 200 взято примерно
        # посередине разрыва в логарифмической шкале.
        self.slow_ms = slow_ms

        self.agg = logparse.empty_agg()
        self.seen = set()                     # уже разобранные объекты журнала
        self.lock = threading.Lock()
        self.state = None
        self.status = {"phase": "старт", "updated": None, "cycles": 0,
                       "last_files": 0, "last_seconds": 0.0, "error": None,
                       "minutes_held": 0, "rclone": RCLONE,
                       "config": CONFIG or "(по умолчанию rclone)",
                       "tmp": TMPBASE}
        self._stop = threading.Event()
        self.range_job = {"phase": "нет"}
        self._range_lock = threading.Lock()

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
        cmd = _base_cmd("lsf", REMOTE)
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
        cmd = _base_cmd("copy", REMOTE, dest, "--files-from", listing,
                        "--transfers", "32", "--checkers", "32")
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
        if n > 1:
            with Pool(n) as pool:
                results = pool.map(logparse.work, paths, chunksize=1)
        else:
            results = [logparse.work(p) for p in paths]
        with self.lock:
            logparse.merge(self.agg, results)

    def _evict(self):
        """Выбрасывает минуты старше окна."""
        minutes = {mi for (mi, _n, _p) in self.agg["C"]}
        if not minutes:
            return 0
        keep = set(sorted(minutes)[-self.window_min:])
        for name in logparse.COUNTER_KEYS:
            src = self.agg[name]
            self.agg[name] = collections.Counter(
                {k: v for k, v in src.items()
                 if not isinstance(k, tuple) or k[0] in keep})
        return len(keep)

    # ------------------------------------------------------------- расчёты

    def _latency_series(self, minutes, agg=None):
        """p50 и p90 по минутам. Минута без чтений — разрыв, а не ноль."""
        agg = self.agg if agg is None else agg
        per = collections.defaultdict(collections.Counter)
        for (mi, bucket), v in agg["LAT"].items():
            per[mi][bucket] += v
        out = []
        for mi in minutes:
            hist = per.get(mi)
            if not hist:
                out.append([mi[11:], None, None])
                continue
            total = sum(hist.values())
            acc, p50, p90 = 0, 0, 0
            for bucket in sorted(hist):
                acc += hist[bucket]
                if not p50 and acc >= total * 0.5:
                    p50 = round(2 ** (bucket / 4))
                if not p90 and acc >= total * 0.9:
                    p90 = round(2 ** (bucket / 4))
                    break
            out.append([mi[11:], p50, p90])
        return out

    def _recompute(self):
        with self.lock:
            snapshot = {k: collections.Counter(v) for k, v in self.agg.items()
                        if k in logparse.COUNTER_KEYS}
            snapshot["K4"] = dict(self.agg["K4"])
        if not snapshot["C"]:
            return None
        data = logparse.analyse(snapshot, self.report_window)
        if data is None:
            return None
        minutes = sorted({mi for (mi, _n, _p) in snapshot["C"]})
        data["latency_series"] = self._latency_series(minutes, snapshot)
        data["is_range"] = False
        return self._decorate(data)

    def _alert(self, d):
        """Тревога: полоса у квоты и запросы встали в очередь."""
        w, lim = d["window"], self.limit_mibs
        if self.per_direction:
            used = max(w["read_mibs"], w["write_mibs"])
            which = "Чтение" if w["read_mibs"] >= w["write_mibs"] else "Запись"
        else:
            used = w["total_mibs"]
            which = "Суммарно"
        saturated = lim > 0 and used >= 0.9 * lim
        slow = w["p50"] >= self.slow_ms
        pct = 100 * used / lim if lim else 0

        if saturated and slow:
            top = d["rd"][0] if d["rd"] else None
            return {"level": "critical",
                    "title": "Полоса у квоты, запросы стоят в очереди",
                    "text": ("%s %.0f MiB/с — %.0f%% квоты, медиана задержки %d мс. "
                             % (which, used, pct, w["p50"]) +
                             ("Больше всех берёт %s -> %s (%.0f MiB/с)."
                              % (top["net"], top["prefix"], top["mibs"]) if top else ""))}
        if saturated:
            return {"level": "warning", "title": "Полоса у квоты",
                    "text": "%s %.0f MiB/с — %.0f%% квоты. Задержка пока в норме "
                            "(медиана %d мс), но запаса нет." % (which, used, pct, w["p50"])}
        if slow:
            return {"level": "warning", "title": "Задержка выше обычной",
                    "text": "Медиана %d мс при незаполненной полосе "
                            "(чтение %.0f, запись %.0f из %.0f MiB/с)."
                            % (w["p50"], w["read_mibs"], w["write_mibs"], lim)}
        return {"level": "ok", "title": "Норма",
                "text": "Чтение %.0f, запись %.0f MiB/с из %.0f, медиана задержки %d мс."
                        % (w["read_mibs"], w["write_mibs"], lim, w["p50"])}

    def _decorate(self, data):
        """Добавляет к сводке квоту, ряд задержки и тревогу."""
        data["ceiling"] = self.limit_mibs
        data["limit_mibs"] = self.limit_mibs
        data["limit_gbit"] = self.limit_mibs * 8 * MIB / 1e9
        data["per_direction"] = self.per_direction
        data["alert"] = self._alert(data)
        return data

    # ------------------------------------------------------- выбранный период

    def start_range(self, frm, to):
        """Запускает разбор произвольного периода. Возвращает (ok, сообщение)."""
        try:
            f = datetime.strptime(frm, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
            t = datetime.strptime(to, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        except ValueError:
            return False, "формат времени: ГГГГ-ММ-ДД ЧЧ:ММ (UTC)"
        if t <= f:
            return False, "конец периода должен быть позже начала"
        span = int((t - f).total_seconds() // 60)
        if span > self.max_range_min:
            return False, ("период %d мин больше предела %d мин — "
                           "сузьте интервал" % (span, self.max_range_min))
        with self._range_lock:
            if self.range_job.get("phase") not in (None, "нет", "готово", "ошибка"):
                return False, "разбор периода уже идёт"
            self.range_job = {"from": frm, "to": to, "phase": "запуск",
                              "hours_done": 0, "hours_total": 0, "files": 0,
                              "bytes": 0, "error": None, "state": None,
                              "span_min": span}
        threading.Thread(target=self._run_range, args=(frm, to, f, t),
                         daemon=True).start()
        return True, "запущено"

    def _run_range(self, frm, to, f, t):
        job = self.range_job
        try:
            # доставка идёт почти в реальном времени; час запаса с обеих
            # сторон покрывает и отставание, и события на границе часа
            hours, cur = [], f.replace(minute=0, second=0, microsecond=0)
            end = t + timedelta(hours=1)
            while cur <= end:
                hours.append(cur.strftime("%Y-%m-%d-%H"))
                cur += timedelta(hours=1)
            job["hours_total"] = len(hours)

            agg = logparse.empty_agg()
            for i, h in enumerate(hours, 1):
                job["phase"] = "час %s" % h[-2:]
                cmd = _base_cmd("lsf", REMOTE, "--include", "%s-*" % h)
                out = subprocess.run(cmd, capture_output=True, text=True,
                                     env=_env(), timeout=300)
                names = [n.strip() for n in out.stdout.splitlines() if n.strip()]
                if names:
                    tmp = _mktemp("s3load-range-")
                    try:
                        paths = self._download(names, tmp)
                        job["files"] += len(paths)
                        job["bytes"] += sum(os.path.getsize(x) for x in paths)
                        n = min(self.workers, len(paths)) if paths else 0
                        if n > 1:
                            with Pool(n) as pool:
                                res = pool.map(logparse.work, paths, chunksize=1)
                        else:
                            res = [logparse.work(p) for p in paths]
                        logparse.merge(agg, res)
                    finally:
                        shutil.rmtree(tmp, ignore_errors=True)
                job["hours_done"] = i

            job["phase"] = "расчёт"
            lo, hi = f.strftime("%Y-%m-%d %H:%M"), t.strftime("%Y-%m-%d %H:%M")
            keep = {mi for (mi, _n, _p) in agg["C"] if lo <= mi <= hi}
            if not keep:
                job.update({"phase": "ошибка",
                            "error": "за выбранный период записей нет — "
                                     "возможно, логи уже удалены"})
                return
            for name in logparse.COUNTER_KEYS:
                agg[name] = collections.Counter(
                    {k: v for k, v in agg[name].items()
                     if not isinstance(k, tuple) or k[0] in keep})
            data = logparse.analyse(agg, win_minutes=len(keep), trim=False)
            data = self._decorate(data)
            data["latency_series"] = self._latency_series(sorted(keep), agg)
            data["is_range"] = True
            data["range_from"], data["range_to"] = frm, to
            job["state"] = data
            job["phase"] = "готово"
        except Exception as exc:                              # noqa: BLE001
            job.update({"phase": "ошибка", "error": str(exc)[:400]})

    # --------------------------------------------------------------- цикл

    def cycle(self, first=False):
        started = time.time()
        span = self.bootstrap_min if first else max(self.window_min, 20)
        self.status["phase"] = "листинг"
        names = self._list_remote(span)
        fresh = [n for n in names if n not in self.seen]
        if first:
            fresh = sorted(fresh)[-600:]
        self.status["phase"] = "выгрузка (%d файлов)" % len(fresh)

        paths = []
        tmp = _mktemp("s3load-")
        try:
            if fresh:
                paths = self._download(fresh, tmp)
                self.status["phase"] = "разбор (%d файлов)" % len(paths)
                self._parse(paths)
                self.seen.update(fresh)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

        held = self._evict()
        self.status["phase"] = "расчёт"
        state = self._recompute()
        if state is not None:
            self.state = state
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
